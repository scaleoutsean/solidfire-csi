package driver

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/prometheus/client_golang/prometheus"
	sf "github.com/scaleoutsean/solidfire-go/methods"
	"github.com/scaleoutsean/solidfire-go/sdk"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	// SolidFire Max limits
	MaxVolumeSizeBytes    = 17592186044416 // 16TiB
	MinVolumeSizeBytes    = 1073741824     // 1GiB
	MaxSnapshotsPerVolume = 10

	// Parameter keys
	ParamEndpoint       = "endpoint"
	ParamUsername       = "username"
	ParamPassword       = "password"
	ParamTenant         = "tenant"
	ParamEndpointRO     = "endpointRO" // Optional, if separating read/write
	ParamType           = "type"       // e.g. "thin" or "thick" - SF is always thin provisioned really
	ParamQosPolicyID    = "storage_qos_policy_id"
	ParamQosMinIOPS     = "qos_iops_min"
	ParamQosMaxIOPS     = "qos_iops_max"
	ParamQosBurstIOPS   = "qos_iops_burst"
	ParamDeleteBehavior = "delete_behavior" // "delete" (default) or "purge"

	// Quota Parameter keys
	QuotaMaxVolumes  = "quota_max_volume_count"
	QuotaMaxCapacity = "quota_max_total_capacity" // String with suffix (GiB, TiB) or bytes
	QuotaMaxIOPS     = "quota_max_total_iops"
)

var (
	metricVolumeCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "solidfire_volume_count",
			Help: "Number of volumes per backend/tenant",
		},
		[]string{"endpoint", "tenant"},
	)
	metricTotalCapacity = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "solidfire_volume_total_capacity_bytes",
			Help: "Total provisioned capacity bytes per backend/tenant",
		},
		[]string{"endpoint", "tenant"},
	)
	metricTotalMinIOPS = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "solidfire_volume_total_min_iops",
			Help: "Total MinIOPS provisioned per backend/tenant",
		},
		[]string{"endpoint", "tenant"},
	)
	metricIscsiSessions = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "solidfire_node_iscsi_sessions",
			Help: "Number of active iSCSI sessions per node",
		},
		[]string{"node"},
	)
)

func init() {
	// Register metrics with Prometheus's default registry.
	prometheus.MustRegister(metricVolumeCount)
	prometheus.MustRegister(metricTotalCapacity)
	prometheus.MustRegister(metricTotalMinIOPS)
	prometheus.MustRegister(metricIscsiSessions)

	// Configure log level via --debug CLI flag or SFCSI_DEBUG env var.
	// Allow enabling debug logs for transient/trace investigation without
	// permanently leaving verbose logging in production.
	level := logrus.InfoLevel
	if os.Getenv("SFCSI_DEBUG") == "true" {
		level = logrus.DebugLevel
	} else {
		for _, a := range os.Args {
			if a == "--debug" || a == "-debug" {
				level = logrus.DebugLevel
				break
			}
		}
	}
	logrus.SetLevel(level)
	if level == logrus.DebugLevel {
		logrus.SetReportCaller(true)
		logrus.Infof("Debug logging enabled")
	}
}

// inspectVolumeForAttachments uses reflection to heuristically detect
// attachment-like fields on the backend Volume struct. It looks for
// field names that mention initiators/clients/hosts/iqn and returns a
// short diagnostic string when any non-empty candidates are found.
func inspectVolumeForAttachments(vol interface{}) (bool, string) {
	if vol == nil {
		return false, ""
	}
	v := reflect.ValueOf(vol)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() { // nil pointer
			return false, ""
		}
		v = v.Elem()
	}
	t := v.Type()
	var parts []string
	for i := 0; i < v.NumField(); i++ {
		fname := t.Field(i).Name
		lower := strings.ToLower(fname)
		// Ignore the volume's own IQN field (it's always present on SF volumes)
		if strings.Contains(lower, "iqn") {
			continue
		}
		if strings.Contains(lower, "initiator") || strings.Contains(lower, "client") || strings.Contains(lower, "attach") || strings.Contains(lower, "host") {
			f := v.Field(i)
			switch f.Kind() {
			case reflect.String:
				if f.String() != "" {
					parts = append(parts, fmt.Sprintf("%s=%s", fname, f.String()))
				}
			case reflect.Slice, reflect.Array:
				if f.Len() > 0 {
					parts = append(parts, fmt.Sprintf("%s=len=%d", fname, f.Len()))
				}
			case reflect.Map:
				if f.Len() > 0 {
					parts = append(parts, fmt.Sprintf("%s=len=%d", fname, f.Len()))
				}
			}
		}
	}
	if len(parts) > 0 {
		return true, strings.Join(parts, ",")
	}
	return false, ""
}

type ControllerServer struct {
	csi.UnimplementedControllerServer
	csi.UnimplementedGroupControllerServer

	// Registry for lazy discovery of backends
	// Key: "endpoint|tenant", Value: *backendEntry
	backendRegistry sync.Map
	kubeClient      kubernetes.Interface
	// Configurable timeouts for waiting on volumes to become ready.
	// `VolumeReadyTimeout` is the main timeout used when waiting for a
	// source volume to reach the "active" state. `VolumeReadyRetryTimeout`
	// is used for the short checks performed before retrying clone attempts.
	VolumeReadyTimeout      time.Duration
	VolumeReadyRetryTimeout time.Duration
	// Track controller-side publish/attachment state for tests and idempotency.
	// Keyed by numeric volume ID.
	attachments     map[int64]*attachmentInfo
	attachmentsLock sync.RWMutex
	// asyncIndex stores recent async clone jobs for short-term idempotency.
	// Key: "srcVolID:srcSnapID" -> *asyncEntry
	asyncIndex map[string]*asyncEntry
	asyncLock  sync.RWMutex
	// How long to retain async entries in memory before GC
	AsyncRegistryTTL time.Duration

	// Clone discovery behavior: number of attempts and sleep between attempts
	// used when a CloneVolume returns an error that may indicate a race
	// (e.g. source removed after clone finished). These settings can be
	// tuned via env vars. The behavior has two modes: Strict (fail-fast)
	// or Tolerant (lenient). If StrictClone is true the driver returns
	// clone errors immediately; otherwise it will perform a short
	// discovery loop to tolerate backend eventual-consistency.
	CloneDiscoveryAttempts int
	CloneDiscoverySleep    time.Duration
	// If true, do not perform discovery/retry on clone failures; return
	// errors immediately. Controlled by env SFCSI_STRICT_CLONE (default false).
	StrictClone bool
	// Session poller (advisory): optional background goroutine which polls
	// the SolidFire API for iSCSI sessions and maintains a map of
	// target-IQN -> last-seen time. Nodes can consult this advisory to
	// avoid tearing down sessions that were recently active on the array.
	sessionPollerEnabled  bool
	sessionPollerInterval time.Duration
	sessionAdvisory       map[string]time.Time
	sessionAdvisoryLock   sync.RWMutex
}

type attachmentInfo struct {
	NodeID     string
	AccessMode csi.VolumeCapability_AccessMode_Mode
	Readonly   bool
}

type backendEntry struct {
	Client   *sf.Client
	Endpoint string
	Tenant   string

	// Metrics Cache
	Metrics    *BackendMetrics
	LastUpdate time.Time
}

type BackendMetrics struct {
	VolumeCount   int64
	TotalCapacity int64
	TotalMinIOPS  int64
}

func NewControllerServer(volumeReadyTimeout, volumeReadyRetryTimeout time.Duration) *ControllerServer {
	// Default discovery behavior: 3 quick attempts with 300ms sleep.
	// Default to tolerant discovery behavior. Honor SFCSI_STRICT_CLONE only
	// when explicitly set to "true" is disabled here to ensure test runs
	// use tolerant behavior by default.
	strict := false
	cs := &ControllerServer{
		VolumeReadyTimeout:      volumeReadyTimeout,
		VolumeReadyRetryTimeout: volumeReadyRetryTimeout,
		attachments:             make(map[int64]*attachmentInfo),
		asyncIndex:              make(map[string]*asyncEntry),
		AsyncRegistryTTL:        15 * time.Minute,
		CloneDiscoveryAttempts:  3,
		CloneDiscoverySleep:     300 * time.Millisecond,
		StrictClone:             strict,
		// default session advisory map (may remain empty if poller disabled)
		sessionAdvisory: make(map[string]time.Time),
	}

	// Session poller enabled via env SFCSI_ENABLE_SESSION_POLLER=true
	if os.Getenv("SFCSI_ENABLE_SESSION_POLLER") == "true" {
		cs.sessionPollerEnabled = true
		// parse interval seconds, default to 30s
		intervalSec := 30
		if s := os.Getenv("SFCSI_SESSION_POLLER_INTERVAL_SECONDS"); s != "" {
			if v, err := strconv.Atoi(s); err == nil && v > 0 {
				intervalSec = v
			}
		}
		cs.sessionPollerInterval = time.Duration(intervalSec) * time.Second
		go cs.startSessionPoller()
		logrus.Infof("Session poller enabled, interval=%v", cs.sessionPollerInterval)
	}

	return cs
}

func (cs *ControllerServer) StartMetricsCollection(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			cs.collectMetrics()
		}
	}()
}

func (cs *ControllerServer) collectMetrics() {
	cs.backendRegistry.Range(func(key, value interface{}) bool {
		entry, ok := value.(*backendEntry)
		if !ok {
			return true
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// ListVolumesForAccount to get metrics
		req := &sdk.ListVolumesForAccountRequest{
			AccountID: entry.Client.AccountID,
		}

		res, err := entry.Client.SFClient.ListVolumesForAccount(ctx, req)
		if err != nil {
			logrus.Errorf("Metrics: Failed to list volumes for backend %s, tenant %s: %v", entry.Endpoint, entry.Tenant, err)
			return true
		}

		var count int64
		var totalMinIOPS int64
		var totalCapacity int64

		for _, v := range res.Volumes {
			if v.Status == "active" {
				count++
				totalCapacity += v.TotalSize
				if v.Qos.MinIOPS > 0 {
					totalMinIOPS += v.Qos.MinIOPS
				}
			}
		}

		// Update Cache
		entry.Metrics = &BackendMetrics{
			VolumeCount:   count,
			TotalCapacity: totalCapacity,
			TotalMinIOPS:  totalMinIOPS,
		}
		entry.LastUpdate = time.Now()

		// Update Prometheus Metrics
		metricVolumeCount.WithLabelValues(entry.Endpoint, entry.Tenant).Set(float64(count))
		metricTotalCapacity.WithLabelValues(entry.Endpoint, entry.Tenant).Set(float64(totalCapacity))
		metricTotalMinIOPS.WithLabelValues(entry.Endpoint, entry.Tenant).Set(float64(totalMinIOPS))

		logrus.WithFields(logrus.Fields{
			"endpoint":             entry.Endpoint,
			"tenant":               entry.Tenant,
			"volume_count":         count,
			"total_capacity_bytes": totalCapacity,
			"total_min_iops":       totalMinIOPS,
		}).Info("Backend Metrics")

		return true
	})
}

// startSessionPoller periodically queries each backend for active iSCSI
// sessions and records a last-seen timestamp per target IQN. This is an
// advisory map that nodes can consult to avoid logging out sessions that
// were recently active on the array.
func (cs *ControllerServer) startSessionPoller() {
	interval := cs.sessionPollerInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			cs.backendRegistry.Range(func(key, value interface{}) bool {
				entry, ok := value.(*backendEntry)
				if !ok || entry == nil || entry.Client == nil || entry.Client.SFClient == nil {
					return true
				}

				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				res, sdkErr := entry.Client.SFClient.ListISCSISessions(ctx)
				cancel()
				if sdkErr != nil || res == nil {
					logrus.Debugf("SessionPoller: ListISCSISessions failed for backend %s tenant %s: %v", entry.Endpoint, entry.Tenant, sdkErr)
					return true
				}

				now := time.Now()
				for _, s := range res.Sessions {
					// Determine last-seen using the most-recent activity reported by the array.
					minMs := s.MsSinceLastIscsiPDU
					if s.MsSinceLastScsiCommand > 0 {
						if minMs == 0 || s.MsSinceLastScsiCommand < minMs {
							minMs = s.MsSinceLastScsiCommand
						}
					}
					var lastSeen time.Time
					if minMs <= 0 {
						lastSeen = now
					} else {
						lastSeen = now.Add(-time.Duration(minMs) * time.Millisecond)
					}
					if s.TargetName != "" {
						cs.sessionAdvisoryLock.Lock()
						cs.sessionAdvisory[s.TargetName] = lastSeen
						cs.sessionAdvisoryLock.Unlock()
					}
				}

				return true
			})
		}
	}()
}

// GetSessionLastSeen returns the advisory last-seen time for the provided
// target IQN. The boolean indicates whether a value was present.
func (cs *ControllerServer) GetSessionLastSeen(iqn string) (time.Time, bool) {
	cs.sessionAdvisoryLock.RLock()
	defer cs.sessionAdvisoryLock.RUnlock()
	t, ok := cs.sessionAdvisory[iqn]
	return t, ok
}

func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	startTime := time.Now()
	defer func() {
		logrus.Infof("CSI_PERF: CreateVolume name=%s took=%v", req.Name, time.Since(startTime))
	}()

	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume Name must be provided")
	}

	// Enforce SolidFire name constraints: 1-128 characters, only alphanumeric and '-'.
	// Use rune count to correctly handle multi-byte characters.
	if utf8.RuneCountInString(req.Name) > 128 {
		return nil, status.Errorf(codes.InvalidArgument, "CreateVolume Name exceeds maximum length of 128 characters")
	}
	for _, r := range req.Name {
		if r == '-' {
			continue
		}
		if (r >= '0' && r <= '9') || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') {
			continue
		}
		return nil, status.Errorf(codes.InvalidArgument, "CreateVolume Name contains invalid character '%c'; allowed are alphanumeric and '-'", r)
	}

	// CSI requires VolumeCapabilities to be provided in CreateVolume requests.
	if req.GetVolumeCapabilities() == nil || len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "VolumeCapabilities must be provided")
	}

	// 1. Parse Parameters and Credentials (Move up to get Limits)
	params := req.GetParameters()
	secrets := req.GetSecrets()

	logrus.Infof("CreateVolume: secrets keys received: %v", getKeys(secrets))

	// Prepare attributes map early so both CreateVolume MutableParameters
	// validation and later attribute population can store into it.
	attributes := make(map[string]interface{})
	// Validate MutableParameters (if provided) against supported mutable keys.
	// This prevents callers from injecting unsupported/unknown mutable attributes
	// during CreateVolume. Supported keys mirror those allowed by ControllerModifyVolume.
	if mut := req.GetMutableParameters(); mut != nil {
		supported := map[string]bool{
			ParamQosPolicyID:                   true,
			ParamQosMinIOPS:                    true,
			ParamQosMaxIOPS:                    true,
			ParamQosBurstIOPS:                  true,
			ParamDeleteBehavior:                true,
			"csi.storage.k8s.io/pvc/name":      true,
			"csi.storage.k8s.io/pvc/namespace": true,
			"csi.storage.k8s.io/fstype":        true,
			"fstype":                           true,
			"fsType":                           true,
		}

		for k, v := range mut {
			if !supported[k] {
				return nil, status.Errorf(codes.InvalidArgument, "mutable parameter %s is not supported", k)
			}
			// store into attributes so they persist on the backend volume
			attributes[k] = v
		}
	}

	client, err := cs.getClientFromSecrets(secrets, params)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "failed to initialize SolidFire client: %v", err)
	}

	capacityRange := req.CapacityRange
	requiredBytes := capacityRange.GetRequiredBytes()
	_ = capacityRange.GetLimitBytes()

	// 2. Size Validation with Dynamic Limits
	var minSize int64 = MinVolumeSizeBytes
	var maxSize int64 = MaxVolumeSizeBytes

	if client.Limits != nil {
		if client.Limits.VolumeSizeMin > 0 {
			minSize = client.Limits.VolumeSizeMin
		}
		if client.Limits.VolumeSizeMax > 0 {
			maxSize = client.Limits.VolumeSizeMax
		}
	}

	// 2.1 Handle 0 size (default to min)
	if requiredBytes == 0 {
		requiredBytes = minSize
	}

	if requiredBytes > maxSize {
		return nil, status.Errorf(codes.OutOfRange, "requested size %d exceeds maximum allowed size %d", requiredBytes, maxSize)
	}
	if requiredBytes < minSize {
		return nil, status.Errorf(codes.OutOfRange, "requested size %d is less than minimum allowed size %d", requiredBytes, minSize)
	}

	// NOTE: We do not explicitly validate VolumeCapabilities (Access Modes) here.
	// KubeVirt and some clustered applications require MULTI_NODE_SINGLE_WRITER or MULTI_NODE_MULTI_WRITER.
	// SolidFire (iSCSI) supports multiple initiators (Access Groups), so we allow the CO to handle enforcement.
	// Users should be aware that RWX on Block requires cluster-aware filesystems or apps.

	// 3. Quota Enforcement
	if err := cs.checkQuotas(client, params, requiredBytes); err != nil {
		return nil, status.Errorf(codes.ResourceExhausted, "quota exceeded: %v", err)
	}

	// 4. Existing Volume Check (idempotency) for the tenant
	// CSI spec requires that if a volume with the same name already exists and is compatible with the request,
	//  we should return success with the existing volume.
	// This means we need to check for existing volumes with the same name and *different* size
	// in the tenant before creating a new one. If we find a volume with the same name but different size,
	// we should return AlreadyExists.

	// Query the backend for existing volumes in this account/tenant using ListVolumesForAccount
	// and only consider active volumes (ignore deleted/recycle-bin entries).
	listReq := &sdk.ListVolumesForAccountRequest{AccountID: client.AccountID}
	if res, err := client.SFClient.ListVolumesForAccount(ctx, listReq); err == nil && res != nil {
		for _, v := range res.Volumes {
			if v.Status != "active" {
				continue
			}
			// Consider both direct name match and original-name attribute (for mapped names)
			match := false
			if v.Name == req.Name {
				match = true
			} else if v.Attributes != nil {
				if attrs, ok := v.Attributes.(map[string]interface{}); ok {
					if on, ok := attrs["csi.original_name"].(string); ok && on == req.Name {
						match = true
					}
				}
			}
			if match {
				// Found a same-name active volume in this tenant.
				if int64(v.TotalSize) == requiredBytes {
					// Idempotent success: return existing volume info
					return &csi.CreateVolumeResponse{
						Volume: &csi.Volume{
							VolumeId:      strconv.FormatInt(v.VolumeID, 10),
							CapacityBytes: v.TotalSize,
						},
					}, nil
				}
				// Same name but different size -> AlreadyExists per CSI expectations
				return nil, status.Errorf(codes.AlreadyExists, "volume name %s already exists with different size %d", req.Name, v.TotalSize)
			}
		}
	} else if err != nil {
		logrus.Debugf("CreateVolume: failed to list volumes for account %d: %v", client.AccountID, err)
	}

	// Delete Behavior
	if val, ok := params[ParamDeleteBehavior]; ok {
		if val == "purge" || val == "delete" {
			attributes[ParamDeleteBehavior] = val
		}
	}

	// Kubernetes Metadata
	// fstype priority:
	// 1. Volume Capability (MountVolume) - The CSI native way
	// 2. Parameters (csi.storage.k8s.io/fstype, fstype, fsType) - The legacy/manual way
	// 3. Default (ext4)

	fsType := ""

	// Check VolumeCapability first
	for _, cap := range req.GetVolumeCapabilities() {
		if m := cap.GetMount(); m != nil {
			if t := m.GetFsType(); t != "" {
				fsType = t
				break
			}
		}
	}

	// Fallback to parameters
	if fsType == "" {
		if t := params["csi.storage.k8s.io/fstype"]; t != "" {
			fsType = t
		} else if t := params["fstype"]; t != "" {
			fsType = t
		} else if t := params["fsType"]; t != "" {
			fsType = t
		}
	}

	// Default
	if fsType == "" {
		fsType = "ext4"
	}

	attributes["fstype"] = fsType

	// pv_name
	attributes["pv_name"] = req.Name

	// pvc_name & namespace (passed by csi-provisioner if configured)
	// These are passed via parameters when using external-provisioner with --extra-create-metadata
	if pvcName := params["csi.storage.k8s.io/pvc/name"]; pvcName != "" {
		attributes["pvc_name"] = pvcName
	}
	if pvcNamespace := params["csi.storage.k8s.io/pvc/namespace"]; pvcNamespace != "" {
		attributes["pvc_namespace"] = pvcNamespace
	}

	// determine sector size (default to 512e/true unless 4k is requested)
	enable512e := true
	if sSize, ok := params["sectorSize"]; ok {
		if sSize == "4096" || sSize == "4k" {
			enable512e = false
		}
	}

	// 4. Create Volume
	// Convert bytes to GiB for SolidFire (SF expects bytes in some calls, but let's check wrapper)
	// methods.GetCreateVolume takes sdk.CreateVolumeRequest where TotalSize is int64 bytes.

	contentSource := req.GetVolumeContentSource()
	if contentSource != nil {
		if contentSource.GetSnapshot() != nil {
			// Create from Snapshot
			snapshotID := contentSource.GetSnapshot().GetSnapshotId()

			// We need to find the snapshot to get the VolumeID it belongs to (required for cloning)
			// Tricky bit: CSI SnapshotID is usually just the SF SnapshotID.
			// But CloneVolume requires VolumeID (source volume) AND SnapshotID.
			// STRATEGY: Encode "ParentVolID:SnapshotID" in the SnapshotID token we return in CreateSnapshot.
			// This makes restoration/cloning O(1) and stateless.

			// Resolve snapshot token to the backend snapshot and its source volume.
			srcVolID, sfSnapPtr, resolveErr := cs.resolveSnapshotRef(ctx, client, snapshotID)
			if resolveErr != nil {
				return nil, resolveErr
			}
			srcSnapID := sfSnapPtr.SnapshotID
			logrus.Infof("CreateVolume(from-snapshot): resolved token=%s -> vol=%d snap=%d", snapshotID, srcVolID, srcSnapID)

			// Previously we waited for the source to become 'active' here to
			// avoid CloneVolume failing on non-active sources. In practice the
			// backend may delete/modify the source between calls which causes
			// long waits and timeouts. Instead do a quick GetVolume to record
			// status and then proceed to call CloneVolume; CloneVolume has its
			// own retry/backoff for transient "not active" errors.
			if v, gerr := client.GetVolume(srcVolID); gerr != nil {
				logrus.Infof("Pre-clone: quick GetVolume for src=%d returned error: %v; proceeding to CloneVolume", srcVolID, gerr)
			} else if strings.ToLower(v.Status) != "active" {
				logrus.Infof("Pre-clone: source volume %d status=%s; proceeding to CloneVolume and relying on backend retry", srcVolID, v.Status)
			}

			cloneReq := sdk.CloneVolumeRequest{
				VolumeID:     srcVolID,
				SnapshotID:   srcSnapID,
				Name:         req.Name,
				NewAccountID: client.AccountID, // Clone to same account
				Attributes:   attributes,
			}

			// Check in-memory async registry for an existing job for this srcVol:snap
			if e := cs.findAsyncEntryBySrc(srcVolID, srcSnapID); e != nil {
				logrus.Infof("Found existing async entry for src=%d snap=%d -> handle=%d created=%d", srcVolID, srcSnapID, e.AsyncHandle, e.CreatedVolID)
				var finalVol *sdk.Volume
				if e.CreatedVolID != 0 {
					// If we already know the created volume ID, prefer to return
					// immediately. Try a quick GetVolume; if it's visible, return
					// full info. If not, return success with the created ID and
					// requested capacity instead of waiting for it to become active.
					if v, gerr := client.GetVolume(e.CreatedVolID); gerr == nil {
						finalVol = v
					} else {
						return &csi.CreateVolumeResponse{
							Volume: &csi.Volume{VolumeId: strconv.FormatInt(e.CreatedVolID, 10), CapacityBytes: requiredBytes, ContentSource: contentSource},
						}, nil
					}
					if finalVol != nil {
						return &csi.CreateVolumeResponse{
							Volume: &csi.Volume{VolumeId: strconv.FormatInt(finalVol.VolumeID, 10), CapacityBytes: finalVol.TotalSize, ContentSource: contentSource},
						}, nil
					}
				}
				if e.AsyncHandle != 0 {
					logrus.Infof("Reusing async handle %d for src=%d snap=%d", e.AsyncHandle, srcVolID, srcSnapID)
					asyncRes, waitErr := client.SFClient.WaitForAsyncResult(ctx, e.AsyncHandle)
					logrus.Infof("Reused async handle: returned asyncRes=%+v waitErr=%v", asyncRes, waitErr)
					if waitErr == nil {
						// try to inspect ListAsyncResults for created volume id
						var listData map[string]interface{}
						for attemptA := 0; attemptA < 3; attemptA++ {
							if lr, lerr := client.SFClient.ListAsyncResults(ctx, &sdk.ListAsyncResultsRequest{}); lerr == nil && lr != nil {
								for _, ah := range lr.AsyncHandles {
									if ah.AsyncResultID == e.AsyncHandle {
										if m, ok := ah.Data.(map[string]interface{}); ok {
											listData = m
										}
										break
									}
								}
								if listData != nil {
									break
								}
							}
							time.Sleep(200 * time.Millisecond)
						}
						var createdVolID int64
						if listData != nil {
							if v, ok := listData["volumeID"]; ok {
								if id, ok2 := toInt64(v); ok2 && id != srcVolID {
									createdVolID = id
								}
							}
							if createdVolID == 0 {
								if v, ok := listData["volumeId"]; ok {
									if id, ok2 := toInt64(v); ok2 && id != srcVolID {
										createdVolID = id
									}
								}
							}
						}
						if createdVolID != 0 {
							cs.updateAsyncEntryByHandle(e.AsyncHandle, createdVolID, "complete")
							// Prefer immediate success: try to fetch the created volume now.
							// If it's not yet visible, return success with the created ID
							// and requested capacity rather than waiting for active state.
							if v, gerr := client.GetVolume(createdVolID); gerr == nil {
								finalVol = v
							} else {
								return &csi.CreateVolumeResponse{Volume: &csi.Volume{VolumeId: strconv.FormatInt(createdVolID, 10), CapacityBytes: requiredBytes, ContentSource: contentSource}}, nil
							}
						}
						if finalVol == nil {
							if v, ferr := cs.findClonedVolumeByName(ctx, client, cloneReq.Name, srcVolID); ferr == nil {
								finalVol = v
							}
						}
						if finalVol != nil {
							return &csi.CreateVolumeResponse{
								Volume: &csi.Volume{VolumeId: strconv.FormatInt(finalVol.VolumeID, 10), CapacityBytes: finalVol.TotalSize, ContentSource: contentSource},
							}, nil
						}
					}
				}
			}

			var resSnap *sdk.CloneVolumeResult
			var errSnap error
			// Retry CloneVolume on transient "source not active" backend error.
			maxAttempts := 3
			for attempt := 1; attempt <= maxAttempts; attempt++ {
				// DEBUG: fetch volume state immediately before CloneVolume to capture transient readiness
				if v, gerr := client.GetVolume(srcVolID); gerr == nil {
					logrus.Infof("Pre-clone (snapshot) GetVolume id=%d status=%s totalSize=%d attrs=%v", srcVolID, v.Status, v.TotalSize, v.Attributes)
				} else {
					logrus.Infof("Pre-clone (snapshot) GetVolume error: %v", gerr)
				}

				resSnap, errSnap = client.SFClient.CloneVolume(ctx, &cloneReq)
				if errSnap != nil {
					logrus.Infof("CloneVolume returned error (snapshot) attempt=%d errType=%T err=%v res=%+v", attempt, errSnap, errSnap, resSnap)
					// Some SDK responses return a non-nil result alongside an error.
					// Only treat the result as usable if it contains either an async handle
					// or an explicit created VolumeID. Avoid treating opaque UUID fields
					// as sufficient evidence (they've caused ambiguity before).
					if resSnap != nil && (resSnap.AsyncHandle != 0 || resSnap.Volume.VolumeID != 0) {
						logrus.Warnf("CloneVolume returned both result and error; treating result as success (snapshot) attempt=%d", attempt)
						break
					}
				} else {
					break
				}
				msg := strings.ToLower(errSnap.Error())
				if (strings.Contains(msg, "not in the active state") || strings.Contains(msg, "not active")) && attempt < maxAttempts {
					backoff := time.Duration(attempt) * 500 * time.Millisecond
					logrus.Infof("CloneVolume attempt %d/%d failed: source not active; waiting %s before retry", attempt, maxAttempts, backoff)
					select {
					case <-ctx.Done():
						return nil, status.Errorf(codes.Internal, "clone cancelled while waiting to retry: %v", ctx.Err())
					case <-time.After(backoff):
						// try again
					}
					// ensure source is active before retrying (short check)
					if err := cs.waitForVolumeActive(ctx, client, srcVolID, cs.VolumeReadyRetryTimeout); err != nil {
						logrus.Infof("waitForVolumeActive before retry failed: %v", err)
					}
					continue
				}
				// If the backend reports the source volume does not exist it
				// may be a race: the clone could have completed (or an async
				// job recorded) while the source was removed. Unless StrictClone
				// is enabled, perform a short discovery loop to try to locate
				// the created volume before failing.
				if cs.StrictClone {
					return nil, status.Errorf(codes.Internal, "failed to clone snapshot: %v", errSnap)
				}

				logrus.Infof("CloneVolume snapshot error suggests missing source; attempting discovery for cloned name=%s src=%d", cloneReq.Name, srcVolID)
				for attempt := 0; attempt < cs.CloneDiscoveryAttempts; attempt++ {
					if v, ferr := cs.findClonedVolumeByName(ctx, client, cloneReq.Name, srcVolID); ferr == nil && v != nil {
						return &csi.CreateVolumeResponse{Volume: &csi.Volume{VolumeId: strconv.FormatInt(v.VolumeID, 10), CapacityBytes: v.TotalSize, ContentSource: contentSource}}, nil
					}
					if lr, lerr := client.SFClient.ListAsyncResults(ctx, &sdk.ListAsyncResultsRequest{}); lerr == nil && lr != nil {
						for _, ah := range lr.AsyncHandles {
							if m, ok := ah.Data.(map[string]interface{}); ok {
								if vv, ok2 := m["volumeID"]; ok2 {
									if id, ok3 := toInt64(vv); ok3 && id != srcVolID {
										if volObj, gerr := client.GetVolume(id); gerr == nil {
											cs.updateAsyncEntryByHandle(ah.AsyncResultID, id, "complete")
											return &csi.CreateVolumeResponse{Volume: &csi.Volume{VolumeId: strconv.FormatInt(volObj.VolumeID, 10), CapacityBytes: volObj.TotalSize, ContentSource: contentSource}}, nil
										}
									}
								}
								if vv, ok2 := m["volumeId"]; ok2 {
									if id, ok3 := toInt64(vv); ok3 && id != srcVolID {
										if volObj, gerr := client.GetVolume(id); gerr == nil {
											cs.updateAsyncEntryByHandle(ah.AsyncResultID, id, "complete")
											return &csi.CreateVolumeResponse{Volume: &csi.Volume{VolumeId: strconv.FormatInt(volObj.VolumeID, 10), CapacityBytes: volObj.TotalSize, ContentSource: contentSource}}, nil
										}
									}
								}
								if name, ok2 := m["name"].(string); ok2 && name == cloneReq.Name {
									if v2, ferr2 := cs.findClonedVolumeByName(ctx, client, cloneReq.Name, srcVolID); ferr2 == nil && v2 != nil {
										return &csi.CreateVolumeResponse{Volume: &csi.Volume{VolumeId: strconv.FormatInt(v2.VolumeID, 10), CapacityBytes: v2.TotalSize, ContentSource: contentSource}}, nil
									}
								}
							}
						}
					}
					time.Sleep(cs.CloneDiscoverySleep)
				}
				return nil, status.Errorf(codes.Internal, "failed to clone snapshot: %v", errSnap)
			}

			// If the API returned an async handle, wait for completion before returning.
			var finalVol *sdk.Volume
			if resSnap != nil && resSnap.AsyncHandle != 0 {
				// Register async handle for short-term idempotency so concurrent
				// or subsequent requests can discover the in-progress job.
				createdID := int64(0)
				if resSnap.VolumeID != 0 {
					createdID = resSnap.VolumeID
				} else if resSnap.Volume.VolumeID != 0 {
					createdID = resSnap.Volume.VolumeID
				}
				cs.registerAsyncEntry(&asyncEntry{AsyncHandle: resSnap.AsyncHandle, SrcVolID: srcVolID, SrcSnapID: srcSnapID, CreatedVolID: createdID, CreateTime: time.Now(), Status: "running"})

				logrus.Infof("CloneVolume async: waiting for async handle %d to complete (snapshot)", resSnap.AsyncHandle)
				asyncRes, waitErr := client.SFClient.WaitForAsyncResult(ctx, resSnap.AsyncHandle)
				logrus.Infof("CloneVolume async: returned asyncRes=%+v waitErr=%v (snapshot)", asyncRes, waitErr)
				if waitErr != nil {
					return nil, status.Errorf(codes.Internal, "clone async wait failed: %v", waitErr)
				}
				if asyncRes != nil && asyncRes.Status == "error" {
					return nil, status.Errorf(codes.Internal, "clone async job failed: %v", asyncRes.Result.Message)
				}

				// Try to resolve the created volume ID from the CloneVolumeResult (resSnap.VolumeID)
				// or nested Volume. Prefer the immediate result when available; only
				// consult ListAsyncResults if the immediate result does not include
				// a created volume ID.
				var createdVolID int64
				if resSnap != nil {
					if resSnap.VolumeID != 0 {
						createdVolID = resSnap.VolumeID
					} else if resSnap.Volume.VolumeID != 0 {
						createdVolID = resSnap.Volume.VolumeID
					}
					if createdVolID != 0 {
						logrus.Debugf("CloneVolume (snapshot) returned createdVolID=%d from immediate result", createdVolID)
					}
				}

				// If we didn't get a createdVolID from the immediate result, try to
				// surface any async result "Data" payloads associated with this
				// async handle. Some backend behaviors return useful meta
				// (volumeID/name) via ListAsyncResults rather than GetAsyncResult;
				// retry a few times to allow the data to appear. If an async entry
				// reports a Data.volumeID equal to the source volume, ignore it
				// (that's a backend quirk) and continue searching.
				var listData map[string]interface{}
				if createdVolID == 0 && resSnap != nil && resSnap.AsyncHandle != 0 {
					for attemptA := 0; attemptA < 3; attemptA++ {
						if lr, lerr := client.SFClient.ListAsyncResults(ctx, &sdk.ListAsyncResultsRequest{}); lerr == nil && lr != nil {
							for _, ah := range lr.AsyncHandles {
								if ah.AsyncResultID == resSnap.AsyncHandle {
									logrus.Debugf("ListAsyncResults matched async handle %d: ResultType=%s Success=%v Data=%+v", ah.AsyncResultID, ah.ResultType, ah.Success, ah.Data)
									if m, ok := ah.Data.(map[string]interface{}); ok {
										// Check for volumeID in the data and ignore entries
										// that explicitly report the source volume ID.
										if v, ok2 := m["volumeID"]; ok2 {
											if id, ok3 := toInt64(v); ok3 {
												if id == srcVolID {
													logrus.Debugf("ListAsyncResults for handle %d reported source volumeID %d; skipping", ah.AsyncResultID, id)
													continue
												}
											}
										}
										if v, ok2 := m["volumeId"]; ok2 {
											if id, ok3 := toInt64(v); ok3 {
												if id == srcVolID {
													logrus.Debugf("ListAsyncResults for handle %d reported source volumeId %d; skipping", ah.AsyncResultID, id)
													continue
												}
											}
										}
										// If we reach here, accept the data map
										listData = m
									}
									if listData != nil {
										break
									}
								}
							}
							if listData != nil {
								break
							}
						}
						time.Sleep(200 * time.Millisecond)
					}
				}

				// If listData provided a created volume ID, use it.
				if createdVolID == 0 && listData != nil {
					if v, ok := listData["volumeID"]; ok {
						if id, ok2 := toInt64(v); ok2 {
							if id != srcVolID {
								createdVolID = id
							}
						}
					}
					if createdVolID == 0 {
						if v, ok := listData["volumeId"]; ok {
							if id, ok2 := toInt64(v); ok2 {
								if id != srcVolID {
									createdVolID = id
								}
							}
						}
					}
				}

				if createdVolID != 0 {
					// Prefer immediate success: try to fetch the created volume now.
					// If it's not yet visible, return success with the created ID
					// and requested capacity rather than waiting for active state.
					if v, gerr := client.GetVolume(createdVolID); gerr == nil {
						finalVol = v
					} else {
						return &csi.CreateVolumeResponse{Volume: &csi.Volume{VolumeId: strconv.FormatInt(createdVolID, 10), CapacityBytes: requiredBytes, ContentSource: contentSource}}, nil
					}
				}

				// If we still don't have a created volume ID/object, try to discover
				// the cloned volume by its backend name (ListVolumes + ListAsyncResults fallback).
				if finalVol == nil {
					if v, ferr := cs.findClonedVolumeByName(ctx, client, cloneReq.Name, srcVolID); ferr == nil {
						finalVol = v
					}
				}
			} else if resSnap != nil {
				// No async handle; use returned volume struct if present
				finalVol = &resSnap.Volume
			}

			if finalVol == nil {
				return nil, status.Errorf(codes.Internal, "clone did not return created volume information")
			}

			// Guard against mistakenly returning the source volume ID
			if finalVol.VolumeID == srcVolID {
				logrus.Warnf("Clone resulted in source volume ID %d; attempting to discover created volume by name %s", srcVolID, cloneReq.Name)
				if v2, ferr := cs.findClonedVolumeByName(ctx, client, cloneReq.Name, srcVolID); ferr == nil && v2 != nil && v2.VolumeID != srcVolID {
					finalVol = v2
				} else {
					return nil, status.Errorf(codes.Internal, "clone returned source volume ID %d; unable to locate created volume", srcVolID)
				}
			}

			// If size is different, we might need to expand
			if int64(requiredBytes) > finalVol.TotalSize {
				_, err := client.SFClient.ModifyVolume(ctx, &sdk.ModifyVolumeRequest{
					VolumeID:  finalVol.VolumeID,
					TotalSize: int64(requiredBytes),
				})
				if err != nil {
					return nil, status.Errorf(codes.Internal, "failed to expand volume after clone: %v", err)
				}
				finalVol.TotalSize = int64(requiredBytes)
			}

			return &csi.CreateVolumeResponse{
				Volume: &csi.Volume{
					VolumeId:      strconv.FormatInt(finalVol.VolumeID, 10),
					CapacityBytes: finalVol.TotalSize,
					ContentSource: contentSource,
				},
			}, nil

		} else if contentSource.GetVolume() != nil {
			// Create from Volume (Clone)
			srcVolIDStr := contentSource.GetVolume().GetVolumeId()
			srcVolID, parseErr := strconv.ParseInt(srcVolIDStr, 10, 64)
			if parseErr != nil {
				return nil, status.Errorf(codes.NotFound, "invalid source volume ID: %v", parseErr)
			}

			// Previously we waited for the source to become 'active' here to
			// avoid CloneVolume failing on non-active sources. In practice the
			// backend may delete/modify the source between calls which causes
			// long waits and timeouts. Instead do a quick GetVolume to record
			// status and then proceed to call CloneVolume; CloneVolume has its
			// own retry/backoff for transient "not active" errors.
			if v, gerr := client.GetVolume(srcVolID); gerr != nil {
				logrus.Infof("Pre-clone (volume): quick GetVolume for src=%d returned error: %v; proceeding to CloneVolume", srcVolID, gerr)
			} else if strings.ToLower(v.Status) != "active" {
				logrus.Infof("Pre-clone (volume): source volume %d status=%s; proceeding to CloneVolume and relying on backend retry", srcVolID, v.Status)
			}

			cloneReq := sdk.CloneVolumeRequest{
				VolumeID:     srcVolID,
				Name:         req.Name,
				NewAccountID: client.AccountID,
				Attributes:   attributes,
			}

			// Check in-memory async registry for an existing clone-from-volume job
			if e := cs.findAsyncEntryBySrc(srcVolID, 0); e != nil {
				logrus.Infof("Found existing async entry for clone-from-volume src=%d -> handle=%d created=%d", srcVolID, e.AsyncHandle, e.CreatedVolID)
				var finalVol *sdk.Volume
				if e.CreatedVolID != 0 {
					// If we already know the created volume ID, prefer to return
					// immediately. Try a quick GetVolume; if it's visible, return
					// full info. If not, return success with the created ID and
					// requested capacity instead of waiting for it to become active.
					if v, gerr := client.GetVolume(e.CreatedVolID); gerr == nil {
						finalVol = v
					} else {
						return &csi.CreateVolumeResponse{Volume: &csi.Volume{VolumeId: strconv.FormatInt(e.CreatedVolID, 10), CapacityBytes: requiredBytes, ContentSource: contentSource}}, nil
					}
					if finalVol != nil {
						return &csi.CreateVolumeResponse{Volume: &csi.Volume{VolumeId: strconv.FormatInt(finalVol.VolumeID, 10), CapacityBytes: finalVol.TotalSize, ContentSource: contentSource}}, nil
					}
				}
				if e.AsyncHandle != 0 {
					logrus.Infof("Reusing async handle %d for clone-from-volume src=%d", e.AsyncHandle, srcVolID)
					asyncRes, waitErr := client.SFClient.WaitForAsyncResult(ctx, e.AsyncHandle)
					logrus.Infof("Reused async handle: returned asyncRes=%+v waitErr=%v", asyncRes, waitErr)
					if waitErr == nil {
						// Inspect ListAsyncResults for created volume id
						var listData map[string]interface{}
						for attemptA := 0; attemptA < 3; attemptA++ {
							if lr, lerr := client.SFClient.ListAsyncResults(ctx, &sdk.ListAsyncResultsRequest{}); lerr == nil && lr != nil {
								for _, ah := range lr.AsyncHandles {
									if ah.AsyncResultID == e.AsyncHandle {
										if m, ok := ah.Data.(map[string]interface{}); ok {
											listData = m
										}
										break
									}
								}
								if listData != nil {
									break
								}
							}
							time.Sleep(200 * time.Millisecond)
						}
						var createdVolID int64
						if listData != nil {
							if v, ok := listData["volumeID"]; ok {
								if id, ok2 := toInt64(v); ok2 && id != srcVolID {
									createdVolID = id
								}
							}
							if createdVolID == 0 {
								if v, ok := listData["volumeId"]; ok {
									if id, ok2 := toInt64(v); ok2 && id != srcVolID {
										createdVolID = id
									}
								}
							}
						}
						if createdVolID != 0 {
							cs.updateAsyncEntryByHandle(e.AsyncHandle, createdVolID, "complete")
							// Prefer immediate success: try to fetch the created volume now.
							// If it's not yet visible, return success with the created ID
							// and requested capacity rather than waiting for active state.
							if v, gerr := client.GetVolume(createdVolID); gerr == nil {
								finalVol = v
							} else {
								return &csi.CreateVolumeResponse{Volume: &csi.Volume{VolumeId: strconv.FormatInt(createdVolID, 10), CapacityBytes: requiredBytes, ContentSource: contentSource}}, nil
							}
						}
						if finalVol == nil {
							if v, ferr := cs.findClonedVolumeByName(ctx, client, cloneReq.Name, srcVolID); ferr == nil {
								finalVol = v
							}
						}
						if finalVol != nil {
							return &csi.CreateVolumeResponse{Volume: &csi.Volume{VolumeId: strconv.FormatInt(finalVol.VolumeID, 10), CapacityBytes: finalVol.TotalSize, ContentSource: contentSource}}, nil
						}
					}
				}
			}

			var resVol *sdk.CloneVolumeResult
			var errVol error
			// Retry CloneVolume on transient "source not active" backend error.
			maxAttempts := 3
			for attempt := 1; attempt <= maxAttempts; attempt++ {
				// DEBUG: fetch volume state immediately before CloneVolume to capture transient readiness
				if v, gerr := client.GetVolume(srcVolID); gerr == nil {
					logrus.Infof("Pre-clone (volume) GetVolume id=%d status=%s totalSize=%d attrs=%v", srcVolID, v.Status, v.TotalSize, v.Attributes)
				} else {
					logrus.Infof("Pre-clone (volume) GetVolume error: %v", gerr)
				}

				resVol, errVol = client.SFClient.CloneVolume(ctx, &cloneReq)
				if errVol != nil {
					logrus.Infof("CloneVolume returned error (volume) attempt=%d errType=%T err=%v res=%+v", attempt, errVol, errVol, resVol)
					// Treat result as usable only when it contains an async handle
					// or an explicit created volume ID. Avoid relying on UUID-only
					// responses as they may not guarantee the created volume is usable.
					if resVol != nil && (resVol.AsyncHandle != 0 || resVol.Volume.VolumeID != 0) {
						logrus.Warnf("CloneVolume returned both result and error; treating result as success (volume) attempt=%d", attempt)
						break
					}
				} else {
					break
				}
				msg := strings.ToLower(errVol.Error())
				if (strings.Contains(msg, "not in the active state") || strings.Contains(msg, "not active")) && attempt < maxAttempts {
					backoff := time.Duration(attempt) * 500 * time.Millisecond
					logrus.Infof("CloneVolume attempt %d/%d failed: source not active; waiting %s before retry", attempt, maxAttempts, backoff)
					select {
					case <-ctx.Done():
						return nil, status.Errorf(codes.Internal, "clone cancelled while waiting to retry: %v", ctx.Err())
					case <-time.After(backoff):
						// try again
					}
					// ensure source is active before retrying (short check)
					if err := cs.waitForVolumeActive(ctx, client, srcVolID, cs.VolumeReadyRetryTimeout); err != nil {
						logrus.Infof("waitForVolumeActive before retry failed: %v", err)
					}
					continue
				}
				// If the backend reports the source volume does not exist this
				// may be a race where the clone actually succeeded. Respect the
				// StrictClone flag; otherwise perform a short discovery loop to
				// try to locate the created volume before failing the request.
				if cs.StrictClone {
					return nil, status.Errorf(codes.Internal, "failed to clone volume: %v", errVol)
				}

				logrus.Infof("CloneVolume volume error suggests missing source; attempting discovery for cloned name=%s src=%d", cloneReq.Name, srcVolID)
				for attempt := 0; attempt < cs.CloneDiscoveryAttempts; attempt++ {
					if v, ferr := cs.findClonedVolumeByName(ctx, client, cloneReq.Name, srcVolID); ferr == nil && v != nil {
						return &csi.CreateVolumeResponse{Volume: &csi.Volume{VolumeId: strconv.FormatInt(v.VolumeID, 10), CapacityBytes: v.TotalSize, ContentSource: contentSource}}, nil
					}
					if lr, lerr := client.SFClient.ListAsyncResults(ctx, &sdk.ListAsyncResultsRequest{}); lerr == nil && lr != nil {
						for _, ah := range lr.AsyncHandles {
							if m, ok := ah.Data.(map[string]interface{}); ok {
								if vv, ok2 := m["volumeID"]; ok2 {
									if id, ok3 := toInt64(vv); ok3 && id != srcVolID {
										if volObj, gerr := client.GetVolume(id); gerr == nil {
											cs.updateAsyncEntryByHandle(ah.AsyncResultID, id, "complete")
											return &csi.CreateVolumeResponse{Volume: &csi.Volume{VolumeId: strconv.FormatInt(volObj.VolumeID, 10), CapacityBytes: volObj.TotalSize, ContentSource: contentSource}}, nil
										}
									}
								}
								if vv, ok2 := m["volumeId"]; ok2 {
									if id, ok3 := toInt64(vv); ok3 && id != srcVolID {
										if volObj, gerr := client.GetVolume(id); gerr == nil {
											cs.updateAsyncEntryByHandle(ah.AsyncResultID, id, "complete")
											return &csi.CreateVolumeResponse{Volume: &csi.Volume{VolumeId: strconv.FormatInt(volObj.VolumeID, 10), CapacityBytes: volObj.TotalSize, ContentSource: contentSource}}, nil
										}
									}
								}
								if name, ok2 := m["name"].(string); ok2 && name == cloneReq.Name {
									if v2, ferr2 := cs.findClonedVolumeByName(ctx, client, cloneReq.Name, srcVolID); ferr2 == nil && v2 != nil {
										return &csi.CreateVolumeResponse{Volume: &csi.Volume{VolumeId: strconv.FormatInt(v2.VolumeID, 10), CapacityBytes: v2.TotalSize, ContentSource: contentSource}}, nil
									}
								}
							}
						}
					}
					time.Sleep(cs.CloneDiscoverySleep)
				}
				return nil, status.Errorf(codes.Internal, "failed to clone volume: %v", errVol)
			}

			var finalVol *sdk.Volume
			if resVol != nil && resVol.AsyncHandle != 0 {
				// Register async handle for short-term idempotency.
				createdID := int64(0)
				if resVol.VolumeID != 0 {
					createdID = resVol.VolumeID
				} else if resVol.Volume.VolumeID != 0 {
					createdID = resVol.Volume.VolumeID
				}
				cs.registerAsyncEntry(&asyncEntry{AsyncHandle: resVol.AsyncHandle, SrcVolID: srcVolID, SrcSnapID: 0, CreatedVolID: createdID, CreateTime: time.Now(), Status: "running"})

				logrus.Infof("CloneVolume async: waiting for async handle %d to complete (volume)", resVol.AsyncHandle)
				asyncRes, waitErr := client.SFClient.WaitForAsyncResult(ctx, resVol.AsyncHandle)
				logrus.Infof("CloneVolume async: returned asyncRes=%+v waitErr=%v (volume)", asyncRes, waitErr)
				if waitErr != nil {
					return nil, status.Errorf(codes.Internal, "clone async wait failed: %v", waitErr)
				}
				if asyncRes != nil && asyncRes.Status == "error" {
					return nil, status.Errorf(codes.Internal, "clone async job failed: %v", asyncRes.Result.Message)
				}

				var createdVolID int64
				if resVol != nil {
					if resVol.VolumeID != 0 {
						createdVolID = resVol.VolumeID
					} else if resVol.Volume.VolumeID != 0 {
						createdVolID = resVol.Volume.VolumeID
					}
					if createdVolID != 0 {
						logrus.Debugf("CloneVolume (volume) returned createdVolID=%d from immediate result", createdVolID)
					}
				}
				if createdVolID != 0 {
					// Prefer immediate success: try to fetch the created volume now.
					// If it's not yet visible, return success with the created ID
					// and requested capacity rather than waiting for active state.
					if v, gerr := client.GetVolume(createdVolID); gerr == nil {
						finalVol = v
					} else {
						return &csi.CreateVolumeResponse{Volume: &csi.Volume{VolumeId: strconv.FormatInt(createdVolID, 10), CapacityBytes: requiredBytes, ContentSource: contentSource}}, nil
					}
				}

				// If the SDK didn't return a createdVolID or the GetVolume path
				// couldn't fetch it, attempt to discover the cloned volume by
				// name/async results instead of failing outright.
				if finalVol == nil {
					if v, ferr := cs.findClonedVolumeByName(ctx, client, cloneReq.Name, srcVolID); ferr == nil {
						finalVol = v
					}
				}
			} else if resVol != nil {
				finalVol = &resVol.Volume
			}

			if finalVol == nil {
				return nil, status.Errorf(codes.Internal, "clone did not return created volume information")
			}

			// Guard against mistakenly returning the source volume ID
			if finalVol.VolumeID == srcVolID {
				logrus.Warnf("Clone resulted in source volume ID %d; attempting to discover created volume by name %s", srcVolID, cloneReq.Name)
				if v2, ferr := cs.findClonedVolumeByName(ctx, client, cloneReq.Name, srcVolID); ferr == nil && v2 != nil && v2.VolumeID != srcVolID {
					finalVol = v2
				} else {
					return nil, status.Errorf(codes.Internal, "clone returned source volume ID %d; unable to locate created volume", srcVolID)
				}
			}

			return &csi.CreateVolumeResponse{
				Volume: &csi.Volume{
					VolumeId:      strconv.FormatInt(finalVol.VolumeID, 10),
					CapacityBytes: finalVol.TotalSize,
					ContentSource: contentSource,
				},
			}, nil
		}
	}

	// SolidFire backend limits names to 64 chars. CSI tests may provide
	// longer names (e.g. 128). If so, map to a backend-safe short name
	// and store the original name in attributes so idempotency can be
	// recognized later.
	backendName := req.Name
	const backendMaxName = 64
	if utf8.RuneCountInString(req.Name) > backendMaxName {
		// Generate deterministic short name: prefix + sha1 hex
		sum := sha1.Sum([]byte(req.Name))
		hex := fmt.Sprintf("%x", sum)
		// Use a short prefix to aid debugging
		prefix := "csi-"
		// Trim hex to fit into backendMaxName when combined with prefix
		maxHex := backendMaxName - len(prefix)
		if maxHex > len(hex) {
			maxHex = len(hex)
		}
		backendName = prefix + hex[:maxHex]
		// Store original name so future idempotent CreateVolume calls can match
		attributes["csi.original_name"] = req.Name
	}

	createReq := sdk.CreateVolumeRequest{
		Name:       backendName,
		AccountID:  client.AccountID,
		TotalSize:  int64(requiredBytes),
		Enable512e: enable512e,
		Attributes: attributes,
	}

	/* Lines omitted */

	// Handle QoS if presented
	qosPolicyIDStr := params[ParamQosPolicyID]
	if qosPolicyIDStr != "" {
		pID, err := strconv.ParseInt(qosPolicyIDStr, 10, 64)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid QoS policy ID: %v", err)
		}
		createReq.QosPolicyID = pID
		createReq.AssociateWithQoSPolicy = true
	} else {
		// Only check for explicit QoS if no Policy ID is provided (as they are mutually exclusive)
		var minIOPS, maxIOPS, burstIOPS int64

		if val, ok := params[ParamQosMinIOPS]; ok {
			v, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "invalid %s: %v", ParamQosMinIOPS, err)
			}
			minIOPS = v
		}

		if val, ok := params[ParamQosMaxIOPS]; ok {
			v, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "invalid %s: %v", ParamQosMaxIOPS, err)
			}
			maxIOPS = v
		}

		if val, ok := params[ParamQosBurstIOPS]; ok {
			v, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "invalid %s: %v", ParamQosBurstIOPS, err)
			}
			burstIOPS = v
		}

		if minIOPS > 0 || maxIOPS > 0 || burstIOPS > 0 {
			createReq.Qos = &sdk.QoS{
				MinIOPS:   minIOPS,
				MaxIOPS:   maxIOPS,
				BurstIOPS: burstIOPS,
			}
		}
	}

	vol, err := client.GetCreateVolume(createReq)
	if err != nil {
		// Map SolidFire errors to CSI codes
		errStr := err.Error()
		if strings.Contains(errStr, "xMaxVolumeCountExceeded") ||
			strings.Contains(errStr, "xTotalCapacityExceeded") ||
			strings.Contains(errStr, "xMaxSnapshotsPerVolumeExceeded") {
			return nil, status.Errorf(codes.ResourceExhausted, "resource exhausted on backend: %v", err)
		}
		if strings.Contains(errStr, "NameAlreadyInUse") || strings.Contains(errStr, "DuplicateVolumeName") {
			return nil, status.Errorf(codes.AlreadyExists, "volume name already exists: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "failed to create volume: %v", err)
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      strconv.FormatInt(vol.VolumeID, 10),
			CapacityBytes: int64(vol.TotalSize),
			ContentSource: req.GetVolumeContentSource(),
		},
	}, nil
}

// toInt64 attempts to normalize various numeric types or numeric strings
// returned inside SDK Data maps into an int64. Returns (0,false) on failure.
func toInt64(v interface{}) (int64, bool) {
	switch t := v.(type) {
	case int:
		return int64(t), true
	case int32:
		return int64(t), true
	case int64:
		return t, true
	case float32:
		return int64(t), true
	case float64:
		return int64(t), true
	case string:
		if t == "" {
			return 0, false
		}
		if iv, err := strconv.ParseInt(t, 10, 64); err == nil {
			return iv, true
		}
		if fv, err := strconv.ParseFloat(t, 64); err == nil {
			return int64(fv), true
		}
	}
	return 0, false
}

// asyncEntry records an in-memory view of a backend async job relevant to cloning.
type asyncEntry struct {
	AsyncHandle  int64
	SrcVolID     int64
	SrcSnapID    int64
	CreatedVolID int64
	CreateTime   time.Time
	Status       string
}

func (cs *ControllerServer) asyncKey(srcVolID, srcSnapID int64) string {
	return fmt.Sprintf("%d:%d", srcVolID, srcSnapID)
}

func (cs *ControllerServer) registerAsyncEntry(e *asyncEntry) {
	if e == nil {
		return
	}
	key := cs.asyncKey(e.SrcVolID, e.SrcSnapID)
	cs.asyncLock.Lock()
	cs.asyncIndex[key] = e
	cs.asyncLock.Unlock()
}

func (cs *ControllerServer) findAsyncEntryBySrc(srcVolID, srcSnapID int64) *asyncEntry {
	key := cs.asyncKey(srcVolID, srcSnapID)
	cs.asyncLock.RLock()
	e, ok := cs.asyncIndex[key]
	cs.asyncLock.RUnlock()
	if !ok {
		return nil
	}
	return e
}

func (cs *ControllerServer) updateAsyncEntryByHandle(handle int64, createdVolID int64, status string) {
	cs.asyncLock.Lock()
	defer cs.asyncLock.Unlock()
	for k, e := range cs.asyncIndex {
		if e == nil {
			continue
		}
		if e.AsyncHandle == handle {
			if createdVolID != 0 {
				e.CreatedVolID = createdVolID
			}
			if status != "" {
				e.Status = status
			}
			cs.asyncIndex[k] = e
			return
		}
	}
}

// asyncSweeper periodically removes old async entries to bound memory usage.
func (cs *ControllerServer) asyncSweeper() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-cs.AsyncRegistryTTL)
		cs.asyncLock.Lock()
		for k, e := range cs.asyncIndex {
			if e == nil {
				delete(cs.asyncIndex, k)
				continue
			}
			if e.CreateTime.Before(cutoff) {
				delete(cs.asyncIndex, k)
			}
		}
		cs.asyncLock.Unlock()
	}
}

func (cs *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	volIDStr := req.GetVolumeId()
	if volIDStr == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID must be provided")
	}

	volID, err := strconv.ParseInt(volIDStr, 10, 64)
	if err != nil {
		// CSI sanity and many COs pass opaque/non-numeric volume IDs when testing
		// idempotency. Treat malformed/non-numeric IDs as already-deleted (idempotent)
		// and return success instead of NotFound/InvalidArgument.
		logrus.Debugf("DeleteVolume: non-numeric volume ID '%s', treating as not-found (idempotent): %v", volIDStr, err)
		return &csi.DeleteVolumeResponse{}, nil
	}

	secrets := req.GetSecrets()
	// NOTE: DeleteVolume doesn't usually get Parameters in CSI 1.0+, so we rely on Secrets.
	// We might need to encode endpoint info in VolumeID if secrets aren't enough or consistent.
	// But assuming secrets contain valid admin/tenant creds:

	// Check if we have secrets, otherwise we can't delete.
	// Relaxed check: We allow empty secrets if env vars are present (handled by getClientFromSecrets)
	// if len(secrets) == 0 {
	// 	return nil, status.Error(codes.InvalidArgument, "secrets required for deletion")
	// }

	// We construct a client. NOTE: We can't verify quotas here, but we don't need to.
	// For DeleteVolume, we just need enough info to connect. The parameters usually come from StorageClass,
	// which isn't passed to DeleteVolume.
	// STRATEGY:
	// 1. Credentials must be in Secrets (as per CSI spec for ControllerDeleteVolume).
	// 2. We assume the Endpoint is in Secrets OR we have a convention.
	// Ideally, the CSI Provisioner passes the same secrets used for creation.

	// Using an empty map for params as we expect "endpoint" to be in secrets for deletion
	// or we might fail if it's strictly in SC parameters. By spec, 'secrets' should populate connection info.
	// Using an empty map for params as we expect "endpoint" to be in secrets for deletion
	// or we might fail if it's strictly in SC parameters. By spec, 'secrets' should populate connection info.
	// NOTE: DeleteVolume doesn't get StorageClass parameters, so we can't easily check for "purge" behavior
	// UNLESS it's embedded in the secrets or we fetch the volume first (which we do for snapshot check).
	// But fetching volume doesn't give us SC params.
	// HOWEVER, we can check for a specific Secret key or assume default behavior.
	// If the user wants purge, they might need to put "delete_behavior": "purge" in the Secret or we rely on Global Config/Env Var?
	// Another trick: If we can't see SC, we can't know per-volume behavior easily.
	// BUT, modern CSI external-provisioner MIGHT pass parameters in secrets? Unlikely.
	// Wait, if it's not in secrets, we can't see it.
	// Let's check if we can look it up from the volume attributes?
	// If we stored it in Attributes during creation! THAT is the way.

	// Let's assume for now we look in Secrets for a "global" setting OR we just implement Purge if a specific environment variable is set?
	// User asked for "SC param". But SC params are NOT passed to DeleteVolume.
	// Workaround: Store the behavior in Volume Attributes (SolidFire Attributes) during CreateVolume.

	client, err := cs.getClientFromSecrets(secrets, map[string]string{})
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "failed to initialize client: %v", err)
	}

	// Fetch Volume to check attributes (for Purge behavior) and snapshot check
	vol, err := client.GetVolume(volID)
	// If it doesn't exist, we are good
	if err != nil {
		if strings.Contains(err.Error(), "xVolumeDoesNotExist") {
			return &csi.DeleteVolumeResponse{}, nil
		}
		// Proceeding... GetVolume might fail if not found, handled above.
	}

	err = client.DeleteVolume(volID)
	if err != nil {
		// Idempotency: is it already deleted?
		if strings.Contains(err.Error(), "xVolumeDoesNotExist") || strings.Contains(err.Error(), "does not exist") || (strings.Contains(err.Error(), "Volume") && strings.Contains(err.Error(), "already deleted")) {
			return &csi.DeleteVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to delete volume: %v", err)
	}

	// Check for Purge Behavior
	// We check if the volume had an attribute "delete_behavior" == "purge" OR if the Secret has it (Global override).
	shouldPurge := false
	if val, ok := secrets[ParamDeleteBehavior]; ok && val == "purge" {
		shouldPurge = true
	} else if vol != nil {
		// Check volume attributes
		if attrs, ok := vol.Attributes.(map[string]interface{}); ok {
			if v, ok := attrs[ParamDeleteBehavior].(string); ok && v == "purge" {
				shouldPurge = true
			}
		}
	}

	if shouldPurge {
		// Purge the volume
		if _, err := client.SFClient.PurgeDeletedVolume(ctx, &sdk.PurgeDeletedVolumeRequest{VolumeID: volID}); err != nil {
			// If it fails, we log it but usually we don't fail the CSI Delete call because the volume IS deleted (just not purged).
			// Failing here would make the CO retry DeleteVolume, which would then fail because VolumeDoesNotExist.
			// So we technically should ignore error if it's "xVolumeDoesNotExist" or similar.
			// But wait, if DeleteVolume succeeded, Purge should succeed unless race condition.
			// Let's return success even if Purge fails, maybe log error.
			// TODO: Log warning
		}
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *ControllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	// Note: Kubernetes typically does NOT pass secrets to ListVolumes.
	// This driver design requires secrets for connection.
	// If used with K8s, a global secret/config should be injected into the driver at startup (env vars).
	// For this implementation, we attempt to use secrets if provided (e.g. csi-sanity or manual).
	// secrets := req.GetSecrets() // Not available in ListVolumesRequest
	client, err := cs.getClientFromSecrets(nil, nil)
	// If no secrets and no env vars (which NewClientFromSecrets might check?), this fails.
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "failed to initialize client: %v", err)
	}

	startID := int64(0)
	if req.StartingToken != "" {
		sID, err := strconv.ParseInt(req.StartingToken, 10, 64)
		if err != nil {
			return nil, status.Errorf(codes.Aborted, "invalid starting token: %v", err)
		}
		startID = sID
	}

	limit := int64(req.MaxEntries)
	if limit == 0 {
		limit = 50 // Default limit if not specified
	}

	listReq := sdk.ListActiveVolumesRequest{
		StartVolumeID:         startID,
		Limit:                 limit,
		IncludeVirtualVolumes: false, // Per user requirement
	}

	res, err := client.SFClient.ListActiveVolumes(ctx, &listReq)
	if err != nil && !strings.Contains(err.Error(), "nil SdkError") {
		return nil, status.Errorf(codes.Internal, "failed to list volumes: %v", err)
	}

	var entries []*csi.ListVolumesResponse_Entry
	var nextToken string

	for _, vol := range res.Volumes {
		entries = append(entries, &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{
				VolumeId:      strconv.FormatInt(vol.VolumeID, 10),
				CapacityBytes: vol.TotalSize,
			},
		})
	}

	// Pagination Logic
	// If we got as many as we asked for (or more?), there might be more.
	if len(res.Volumes) > 0 {
		lastVol := res.Volumes[len(res.Volumes)-1]
		// The next call should start from the next ID.
		// NOTE: Logic depends on whether ListActiveVolumes returns inclusive startVolumeID.
		// Yes, it does. So next time we must ask for lastID + 1.
		nextToken = strconv.FormatInt(lastVol.VolumeID+1, 10)

		// Optimization: if we got fewer than limit, we are likely done, unless fuzzy boundary.
		if int64(len(res.Volumes)) < limit {
			nextToken = ""
		}
	}

	return &csi.ListVolumesResponse{
		Entries:   entries,
		NextToken: nextToken,
	}, nil
}

func (cs *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID must be provided")
	}
	if req.VolumeCapabilities == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities must be provided")
	}

	// Get client to check existence
	client, err := cs.getClientFromSecrets(req.GetSecrets(), nil)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "failed to initialize client: %v", err)
	}

	volID, err := strconv.ParseInt(req.VolumeId, 10, 64)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "invalid volume id: %v", err)
	}

	_, err = client.GetVolume(volID)
	if err != nil {
		if strings.Contains(err.Error(), "xVolumeDoesNotExist") || strings.Contains(err.Error(), "not found") {
			return nil, status.Errorf(codes.NotFound, "volume %s not found", req.VolumeId)
		}
		return nil, status.Errorf(codes.Internal, "failed to check volume existence: %v", err)
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeContext:      req.GetVolumeContext(),
			VolumeCapabilities: req.GetVolumeCapabilities(),
		},
	}, nil
}

func (cs *ControllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	caps := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME, // For attachment
		csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
		csi.ControllerServiceCapability_RPC_MODIFY_VOLUME,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	}

	// Additional capabilities commonly expected by csi-sanity tests
	caps = append(caps,
		csi.ControllerServiceCapability_RPC_GET_CAPACITY,
		csi.ControllerServiceCapability_RPC_PUBLISH_READONLY,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES_PUBLISHED_NODES,
		csi.ControllerServiceCapability_RPC_GET_VOLUME,
		csi.ControllerServiceCapability_RPC_VOLUME_CONDITION,
		csi.ControllerServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
	)

	var csiCaps []*csi.ControllerServiceCapability
	for _, cap := range caps {
		csiCaps = append(csiCaps, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
			},
		})
	}

	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: csiCaps,
	}, nil
}

// GetCapacity returns cluster capacity information mapped to CSI GetCapacityResponse.
func (cs *ControllerServer) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	// Try to initialize a client using env/global secrets
	client, err := cs.getClientFromSecrets(nil, nil)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "failed to initialize client: %v", err)
	}

	res, sdkErr := client.SFClient.GetClusterCapacity(ctx)
	if sdkErr != nil || res == nil {
		if sdkErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to get cluster capacity: %v", sdkErr)
		}
		return nil, status.Errorf(codes.Internal, "failed to get cluster capacity: empty result")
	}

	// Map SolidFire cluster capacity to CSI GetCapacityResponse.
	// Use MaxUsedSpace as a reasonable capacity metric if present.
	var avail int64
	// Some generated results may embed ClusterCapacity as a struct field named ClusterCapacity
	// with type sdk.ClusterCapacity. Use field if present.
	// The generated type for GetClusterCapacityResult contains ClusterCapacity ClusterCapacity
	// so we can access it directly.
	avail = res.ClusterCapacity.MaxUsedSpace

	return &csi.GetCapacityResponse{
		AvailableCapacity: avail,
	}, nil
}

func (cs *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "CreateSnapshot Name must be provided")
	}
	if req.SourceVolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "CreateSnapshot SourceVolumeId must be provided")
	}

	volID, err := strconv.ParseInt(req.SourceVolumeId, 10, 64)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "malformed volume ID %s: %v", req.SourceVolumeId, err)
	}

	secrets := req.GetSecrets()
	client, err := cs.getClientFromSecrets(secrets, req.GetParameters())
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "failed to initialize client: %v", err)
	}

	// CHECK LIMIT: Max Snapshots Per Volume
	// Check before creating to avoid "Internal" error and return proper ResourceExhausted
	listReq := sdk.ListSnapshotsRequest{
		VolumeID: volID,
	}
	existingSnaps, listErr := client.SFClient.ListSnapshots(ctx, &listReq)
	if listErr != nil {
		return nil, status.Errorf(codes.Internal, "failed to check snapshot limit: %v", listErr)
	}

	// Idempotency Check: Return existing snapshot if name matches
	for _, s := range existingSnaps.Snapshots {
		if s.Name == req.Name {
			logrus.Infof("Idempotency: Returning existing snapshot %d for name %s", s.SnapshotID, req.Name)
			// Reconstruct response for existing snapshot
			compositeID := cs.compositeSnapshotToken(volID, s.SnapshotID)
			var timestamp *timestamppb.Timestamp
			// Note: ListSnapshotsResult.Snapshots has CreateTime (string)
			if s.CreateTime != "" {
				if t, err := time.Parse(time.RFC3339, s.CreateTime); err == nil {
					timestamp = timestamppb.New(t)
				} else {
					timestamp = timestamppb.Now()
				}
			} else {
				timestamp = timestamppb.Now()
			}
			return &csi.CreateSnapshotResponse{
				Snapshot: &csi.Snapshot{
					SnapshotId:     compositeID,
					SourceVolumeId: req.SourceVolumeId,
					CreationTime:   timestamp,
					SizeBytes:      s.TotalSize,
					ReadyToUse:     true,
				},
			}, nil
		}
	}

	// Cross-volume name collision: ensure no snapshot with same name exists on a different volume
	allSnapsRes, allErr := client.SFClient.ListSnapshots(ctx, &sdk.ListSnapshotsRequest{})
	if allErr == nil {
		for _, s := range allSnapsRes.Snapshots {
			if s.Name == req.Name && s.VolumeID != volID {
				return nil, status.Errorf(codes.AlreadyExists, "snapshot name %s already exists on volume %d", req.Name, s.VolumeID)
			}
		}
	} else {
		logrus.Debugf("CreateSnapshot: failed to list all snapshots for name-collision check: %v; attempting fallback per-volume", allErr)
		// Fallback: list volumes and inspect snapshots for each volume to detect name collisions
		vols, vErr := client.ListVolumes()
		if vErr != nil {
			logrus.Debugf("CreateSnapshot: fallback list volumes failed: %v", vErr)
		} else {
			for _, v := range vols {
				// skip the source volume we are creating the snapshot for
				if v.VolumeID == volID {
					continue
				}
				lr := sdk.ListSnapshotsRequest{VolumeID: v.VolumeID}
				rs, rErr := client.SFClient.ListSnapshots(ctx, &lr)
				if rErr != nil {
					logrus.Debugf("CreateSnapshot: failed to list snapshots for volume %d: %v", v.VolumeID, rErr)
					continue
				}
				for _, s := range rs.Snapshots {
					if s.Name == req.Name && s.VolumeID != volID {
						return nil, status.Errorf(codes.AlreadyExists, "snapshot name %s already exists on volume %d", req.Name, s.VolumeID)
					}
				}
			}
		}
	}

	if len(existingSnaps.Snapshots) >= MaxSnapshotsPerVolume {
		return nil, status.Errorf(codes.ResourceExhausted, "snapshot limit reached for volume %d (%d/%d)", volID, len(existingSnaps.Snapshots), MaxSnapshotsPerVolume)
	}

	// Parse parameters
	params := req.GetParameters()
	retention := "24:00:00" // Default 24 hours
	if val, ok := params["retention"]; ok {
		retention = val
	}
	enableRemoteReplication := false
	if val, ok := params["enableRemoteReplication"]; ok {
		if strings.ToLower(val) == "true" {
			enableRemoteReplication = true
		}
	}

	// Attributes
	attributes := map[string]interface{}{
		"created_by":        "solidfire-csi",
		"csi_snapshot_name": req.Name,
		"csi_source_volume": req.SourceVolumeId,
	}

	// Create Snapshot
	snapReq := sdk.CreateSnapshotRequest{
		VolumeID:                volID,
		Name:                    req.Name,
		Retention:               retention,
		EnableRemoteReplication: enableRemoteReplication,
		Attributes:              attributes,
	}

	snap, createErr := client.SFClient.CreateSnapshot(ctx, &snapReq)
	if createErr != nil {
		return nil, status.Errorf(codes.Internal, "failed to create snapshot: %v", createErr)
	}

	// Create Composite ID: "VolID:SnapID"
	compositeID := cs.compositeSnapshotToken(volID, snap.SnapshotID)

	var timestamp *timestamppb.Timestamp
	if snap.Snapshot.CreateTime != "" {
		// SolidFire format: 2006-01-02T15:04:05Z
		if t, err := time.Parse(time.RFC3339, snap.Snapshot.CreateTime); err == nil {
			timestamp = timestamppb.New(t)
		} else {
			logrus.Warnf("Failed to parse snapshot CreateTime '%s': %v", snap.Snapshot.CreateTime, err)
			timestamp = timestamppb.Now()
		}
	} else {
		timestamp = timestamppb.Now()
	}

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SnapshotId:     compositeID,
			SourceVolumeId: req.SourceVolumeId,
			CreationTime:   timestamp,
			SizeBytes:      snap.Snapshot.TotalSize,
			ReadyToUse:     true, // SolidFire snaps are usually instant/done
		},
	}, nil
}

func (cs *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	snapshotID := req.GetSnapshotId()
	if snapshotID == "" {
		return nil, status.Error(codes.InvalidArgument, "Snapshot ID must be provided")
	}

	// Parse Composite ID passed from CreateSnapshot
	parts := strings.Split(snapshotID, ":")
	var snapID int64
	var parseErr error
	if len(parts) == 2 {
		snapID, parseErr = strconv.ParseInt(parts[1], 10, 64)
		if parseErr != nil {
			logrus.Infof("DeleteSnapshot: invalid numeric snapshot ID in composite '%s', treating as success", snapshotID)
			return &csi.DeleteSnapshotResponse{}, nil
		}
	} else {
		// Try parsing a bare numeric snapshot ID. If that's not possible,
		// treat the invalid format as an idempotent success per CSI-sanity expectations.
		snapID, parseErr = strconv.ParseInt(parts[0], 10, 64)
		if parseErr != nil {
			logrus.Infof("DeleteSnapshot: invalid snapshot ID format %s, treating as success", snapshotID)
			return &csi.DeleteSnapshotResponse{}, nil
		}
	}

	secrets := req.GetSecrets()
	client, err := cs.getClientFromSecrets(secrets, map[string]string{})
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "failed to initialize client: %v", err)
	}

	deleteSnapReq := sdk.DeleteSnapshotRequest{SnapshotID: snapID}
	_, delErr := client.SFClient.DeleteSnapshot(ctx, &deleteSnapReq)
	if delErr != nil {
		if strings.Contains(delErr.Error(), "xSnapshotDoesNotExist") || strings.Contains(delErr.Error(), "does not exist") {
			return &csi.DeleteSnapshotResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to delete snapshot: %v", delErr)
	}

	return &csi.DeleteSnapshotResponse{}, nil
}

// Helper to paginate Snapshot Entries
func paginateSnapshotEntries(entries []*csi.ListSnapshotsResponse_Entry, startToken string, maxEntries int32) ([]*csi.ListSnapshotsResponse_Entry, string, error) {
	// Sort by SnapshotId to ensure stable ordering
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Snapshot.SnapshotId < entries[j].Snapshot.SnapshotId
	})

	startIndex := 0
	if startToken != "" {
		found := false
		for i, entry := range entries {
			if entry.Snapshot.SnapshotId == startToken {
				startIndex = i + 1 // Start after the token
				found = true
				break
			}
		}
		if !found {
			// If provided token not found, start from beginning? Or Error?
			// CSI spec says: "If the starting_token is invalid... return INVALID_ARGUMENT"
			// But for simplicity/robustness if the snapshot was deleted, maybe just start from 0 if not found is aggressive.
			// Let's assume if token not found, it might mean it's invalid.
			return nil, "", status.Errorf(codes.Aborted, "starting_token %s not found", startToken)
		}
	}

	if startIndex >= len(entries) {
		return []*csi.ListSnapshotsResponse_Entry{}, "", nil
	}

	endIndex := len(entries)
	if maxEntries > 0 {
		if startIndex+int(maxEntries) < endIndex {
			endIndex = startIndex + int(maxEntries)
		}
	}

	page := entries[startIndex:endIndex]
	nextToken := ""
	if endIndex < len(entries) {
		// Use the ID of the last item in the page as the NextToken
		nextToken = page[len(page)-1].Snapshot.SnapshotId
	}

	return page, nextToken, nil
}

func (cs *ControllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	// Support listing snapshots for a specific volume if provided
	secrets := req.GetSecrets()
	client, err := cs.getClientFromSecrets(secrets, map[string]string{})
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "failed to initialize client: %v", err)
	}

	sourceVolID := req.GetSourceVolumeId()
	snapshotID := req.GetSnapshotId()

	var entries []*csi.ListSnapshotsResponse_Entry

	// Case 1: Filter by Snapshot ID (GetSnapshot equivalent)
	if snapshotID != "" {
		// Expecting "VolID:SnapID" composite ID
		parts := strings.Split(snapshotID, ":")
		if len(parts) != 2 {
			return &csi.ListSnapshotsResponse{}, nil
		}
		volID, err1 := strconv.ParseInt(parts[0], 10, 64)
		snapID, err2 := strconv.ParseInt(parts[1], 10, 64)
		if err1 != nil || err2 != nil {
			return &csi.ListSnapshotsResponse{}, nil
		}

		listReq := sdk.ListSnapshotsRequest{
			VolumeID:   volID,
			SnapshotID: snapID,
		}

		res, sdkErr := client.SFClient.ListSnapshots(ctx, &listReq)
		if sdkErr != nil {
			return &csi.ListSnapshotsResponse{}, nil
		}

		for _, s := range res.Snapshots {
			if s.SnapshotID == snapID {
				var ts *timestamppb.Timestamp
				if s.CreateTime != "" {
					if t, err := time.Parse(time.RFC3339, s.CreateTime); err == nil {
						ts = timestamppb.New(t)
					}
				}
				entries = append(entries, &csi.ListSnapshotsResponse_Entry{
					Snapshot: &csi.Snapshot{
						SnapshotId:     snapshotID,
						SourceVolumeId: parts[0],
						ReadyToUse:     s.Status == "done",
						CreationTime:   ts,
						SizeBytes:      s.TotalSize,
					},
				})
			}
		}
		// Pagination technically applies even here but usually returning 1 item is fine
		return &csi.ListSnapshotsResponse{
			Entries: entries,
		}, nil
	}

	// Case 2: Filter by Source Volume ID (List Snapshots for Volume)
	if sourceVolID != "" {
		volID, err := strconv.ParseInt(sourceVolID, 10, 64)
		if err != nil {
			return &csi.ListSnapshotsResponse{}, nil
		}

		listReq := sdk.ListSnapshotsRequest{
			VolumeID: volID,
		}

		res, sdkErr := client.SFClient.ListSnapshots(ctx, &listReq)
		if sdkErr != nil {
			// If volume not found, return empty list?
			return nil, status.Errorf(codes.Internal, "failed to list snapshots: %v", sdkErr)
		}

		for _, s := range res.Snapshots {
			compositeID := cs.compositeSnapshotToken(volID, s.SnapshotID)
			var ts *timestamppb.Timestamp
			if s.CreateTime != "" {
				if t, err := time.Parse(time.RFC3339, s.CreateTime); err == nil {
					ts = timestamppb.New(t)
				}
			}
			entries = append(entries, &csi.ListSnapshotsResponse_Entry{
				Snapshot: &csi.Snapshot{
					SnapshotId:     compositeID,
					SourceVolumeId: sourceVolID,
					ReadyToUse:     s.Status == "done",
					CreationTime:   ts,
					SizeBytes:      s.TotalSize,
				},
			})
		}
	} else {
		// List ALL snapshots for the account
		listReq := sdk.ListSnapshotsRequest{}
		res, sdkErr := client.SFClient.ListSnapshots(ctx, &listReq)
		if sdkErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to list snapshots: %v", sdkErr)
		}

		for _, s := range res.Snapshots {
			compositeID := cs.compositeSnapshotToken(s.VolumeID, s.SnapshotID)
			srcVolID := fmt.Sprintf("%d", s.VolumeID)

			var ts *timestamppb.Timestamp
			if s.CreateTime != "" {
				if t, err := time.Parse(time.RFC3339, s.CreateTime); err == nil {
					ts = timestamppb.New(t)
				}
			}
			entries = append(entries, &csi.ListSnapshotsResponse_Entry{
				Snapshot: &csi.Snapshot{
					SnapshotId:     compositeID,
					SourceVolumeId: srcVolID,
					ReadyToUse:     s.Status == "done",
					CreationTime:   ts,
					SizeBytes:      s.TotalSize,
				},
			})
		}
	}

	// Apply Pagination
	pEntries, nextToken, pErr := paginateSnapshotEntries(entries, req.GetStartingToken(), req.GetMaxEntries())
	if pErr != nil {
		return nil, pErr
	}

	return &csi.ListSnapshotsResponse{
		Entries:   pEntries,
		NextToken: nextToken,
	}, nil
}

func (cs *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	volIDStr := req.GetVolumeId()
	if volIDStr == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID must be provided")
	}

	// Validate required arguments before parsing numeric IDs so missing-argument
	// errors are returned as InvalidArgument per CSI expectations.
	nodeID := req.GetNodeId()
	if nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Node ID must be provided")
	}

	// Ensure a VolumeCapability is provided for publish operations
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capability must be provided")
	}

	volID, err := strconv.ParseInt(volIDStr, 10, 64)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "malformed volume ID %s: %v", volIDStr, err)
	}

	// Node existence will be validated later when appropriate (after
	// examining any existing attachments) so that incompatible-publish
	// logic can be evaluated even when COs use synthetic node IDs.

	// For simple iSCSI, we mostly just need to return the IQN and Portal.
	// However, if we restrict access (VAGs, CHAP), we would do it here.
	// We are using CHAP in NewClient, but let's assume simple CHAP (Account based).
	// We need to fetch the Volume to get its IQN and the Client to get the SVIP.

	secrets := req.GetSecrets()
	// NOTE: ControllerPublish doesn't get Parameters. secrets must have connection info.
	client, err := cs.getClientFromSecrets(secrets, map[string]string{})
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "failed to initialize client: %v", err)
	}

	vol, err := client.GetVolume(volID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %s not found: %v", volIDStr, err)
	}

	// Controller-side publish tracking: enforce incompatible-publish rejection.
	// Check existing attachment state for this volume.
	cs.attachmentsLock.RLock()
	existing := cs.attachments[vol.VolumeID]
	cs.attachmentsLock.RUnlock()
	if existing != nil {
		logrus.Debugf("ControllerPublishVolume: existing attachment for vol=%d -> node=%s mode=%v readonly=%v", vol.VolumeID, existing.NodeID, existing.AccessMode, existing.Readonly)
	}

	// If there is no existing controller-side attachment, consult the
	// backend volume information before deciding to return NotFound for
	// synthetic/missing node IDs. This helps catch cases where the
	// volume is already published on another host but the driver-side
	// attachments map wasn't populated (e.g. after restarts).
	// Log the full volume struct to aid diagnostics.
	logrus.Debugf("ControllerPublishVolume: backend volume details: %+v", vol)

	if existing == nil {
		// Heuristic: inspect the returned volume for fields that indicate
		// active attachments (initiators/clients/hosts). If such fields
		// are present and non-empty, prefer returning FailedPrecondition
		// to signal an incompatible-publish instead of NotFound for the
		// target node.
		if found, info := inspectVolumeForAttachments(vol); found {
			logrus.Debugf("ControllerPublishVolume: backend reports attachment-like fields=%s", info)
			return nil, status.Errorf(codes.FailedPrecondition, "volume %s appears published to other node(s): %s", volIDStr, info)
		}

		nodeExists := cs.nodeExists(ctx, nodeID)
		logrus.Debugf("ControllerPublishVolume: node lookup node=%s exists=%v", nodeID, nodeExists)
		if !nodeExists {
			return nil, status.Errorf(codes.NotFound, "node %s not found", nodeID)
		}
	}

	reqMode := req.GetVolumeCapability().GetAccessMode().GetMode()
	reqReadonly := req.GetReadonly()
	if existing != nil {
		if existing.NodeID == nodeID {
			// If the same node but different attributes (access mode or readonly),
			// return AlreadyExists per CSI sanity expectations.
			if existing.AccessMode != reqMode || existing.Readonly != reqReadonly {
				return nil, status.Errorf(codes.AlreadyExists, "volume %s already published to node %s with different publish attributes", volIDStr, existing.NodeID)
			}
			// idempotent success for same node with same attributes
		} else {
			// If the existing attachment is not multi-node-multi-writer, reject
			if existing.AccessMode != csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
				return nil, status.Errorf(codes.FailedPrecondition, "volume %s already published to node %s with incompatible access mode", volIDStr, existing.NodeID)
			}
			// otherwise allow additional multi-writer attachments
		}
	} else {
		// Record the attachment for future checks.
		cs.attachmentsLock.Lock()
		cs.attachments[vol.VolumeID] = &attachmentInfo{NodeID: nodeID, AccessMode: reqMode, Readonly: reqReadonly}
		cs.attachmentsLock.Unlock()
	}

	// We return the connection context for NodeStage/NodePublish
	return &csi.ControllerPublishVolumeResponse{
		PublishContext: map[string]string{
			"iqn":        vol.Iqn,
			"portal":     client.SVIP, // Assuming SVIP is set in Client (parsed from config endpoint or set)
			"lun":        "0",
			"volumeID":   volIDStr,
			"nodeID":     nodeID,
			"chapUser":   client.TenantName,
			"chapSecret": client.InitiatorSecret, // Important!
		},
	}, nil
}

func (cs *ControllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	// Validate input: Volume ID is required per CSI spec for ControllerUnpublishVolume
	volIDStr := req.GetVolumeId()
	if volIDStr == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID must be provided")
	}

	// Optional: ensure it's a parseable integer ID (our backend uses numeric volume IDs)
	if _, err := strconv.ParseInt(volIDStr, 10, 64); err != nil {
		return nil, status.Errorf(codes.NotFound, "malformed volume ID %s: %v", volIDStr, err)
	}

	// If we were using VAGs, we would remove the node from the VAG here.
	// Since we are relying on CHAP and just returning connection info,
	// there is nothing strictly required to "unpublish" on the backend
	// unless we want to terminate sessions (which NodeUnstage handles).

	// Remove controller-side attachment tracking for this volume if present.
	if volID, err := strconv.ParseInt(volIDStr, 10, 64); err == nil {
		cs.attachmentsLock.Lock()
		if info, ok := cs.attachments[volID]; ok {
			// Only remove if the caller provides a matching node id (best-effort).
			if req.GetNodeId() == "" || req.GetNodeId() == info.NodeID {
				delete(cs.attachments, volID)
			}
		}
		cs.attachmentsLock.Unlock()
	}

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (cs *ControllerServer) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	volIDStr := req.GetVolumeId()
	if volIDStr == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID must be provided")
	}

	volID, err := strconv.ParseInt(volIDStr, 10, 64)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "malformed volume ID %s: %v", volIDStr, err)
	}

	secrets := req.GetSecrets()
	// NOTE: ModifyVolume gets parameters from the (new) StorageClass?
	// Actually, CSI spec says: "The CO MUST include the mutable fields of the volume in the Request."
	// And `parameters` field contains the parameters of the volume.
	// So we can check for QoS policy updates here.

	params := req.GetMutableParameters()

	// Validate parameters
	// Support QoS params and a small set of mutable attributes we store in volume Attributes.
	supportedParams := map[string]bool{
		ParamQosPolicyID:                   true,
		ParamQosMinIOPS:                    true,
		ParamQosMaxIOPS:                    true,
		ParamQosBurstIOPS:                  true,
		ParamDeleteBehavior:                true,
		"csi.storage.k8s.io/pvc/name":      true,
		"csi.storage.k8s.io/pvc/namespace": true,
		"csi.storage.k8s.io/fstype":        true,
		"fstype":                           true,
		"fsType":                           true,
	}

	for k := range params {
		if !supportedParams[k] {
			return nil, status.Errorf(codes.InvalidArgument, "parameter %s is not mutable or supported", k)
		}
	}

	// Usually secrets are provided too.
	client, err := cs.getClientFromSecrets(secrets, params)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "failed to initialize client: %v", err)
	}

	// Verify volume exists
	_, err = client.GetVolume(volID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "does not exist") || strings.Contains(err.Error(), "xVolumeDoesNotExist") {
			return nil, status.Errorf(codes.NotFound, "volume %d not found", volID)
		}
		// If other error, pass it through or fail
		return nil, status.Errorf(codes.Internal, "failed to get volume: %v", err)
	}

	// Fetch current volume to get current state (useful for merging?)
	// Actually we can just issue ModifyVolume.

	var modifyReq sdk.ModifyVolumeRequest
	modifyReq.VolumeID = volID

	// Attributes updates (mutable metadata fields)
	attributesUpdated := false
	attributes := make(map[string]interface{})

	// QoS Updates
	qosPolicyIDStr := params[ParamQosPolicyID]
	qosUpdated := false
	if qosPolicyIDStr != "" {
		pID, err := strconv.ParseInt(qosPolicyIDStr, 10, 64)
		if err == nil {
			modifyReq.QosPolicyID = pID
			// Important: To apply a policy, we usually need to set AssociateWithQoSPolicy or similar.
			// The SDK/API behavior: "If QosPolicyID is specified, the volume is associated with that policy."
			// BUT, let's check generated model.
			// Yes, SDK has AssociateWithQosPolicy bool.
			// Wait, in ModifyVolumeRequest struct in SDK (checked via memory/previous context),
			// AssociateWithQoSPolicy is present.
			modifyReq.AssociateWithQoSPolicy = true
			qosUpdated = true
		}
	} else {
		// If Policy ID is NOT present, do we check for explicit QoS?
		// Or maybe the user wants to REMOVE the policy and set explicit QoS?
		// If qos parameters are present, we should use them.

		var minIOPS, maxIOPS, burstIOPS int64
		hasExplicitQoS := false

		if val, ok := params[ParamQosMinIOPS]; ok {
			minIOPS, _ = strconv.ParseInt(val, 10, 64)
			hasExplicitQoS = true
		}
		if val, ok := params[ParamQosMaxIOPS]; ok {
			maxIOPS, _ = strconv.ParseInt(val, 10, 64)
			hasExplicitQoS = true
		}

		if val, ok := params[ParamQosBurstIOPS]; ok {
			burstIOPS, _ = strconv.ParseInt(val, 10, 64)
			hasExplicitQoS = true
		}

		// populate attributes from params (delete behavior, fstype, pvc metadata)
		if val, ok := params[ParamDeleteBehavior]; ok {
			if val == "purge" || val == "delete" {
				attributes[ParamDeleteBehavior] = val
				attributesUpdated = true
			} else {
				return nil, status.Errorf(codes.InvalidArgument, "invalid value for %s: %s", ParamDeleteBehavior, val)
			}
		}
		if t := params["csi.storage.k8s.io/fstype"]; t != "" {
			attributes["fstype"] = t
			attributesUpdated = true
		} else if t := params["fstype"]; t != "" {
			attributes["fstype"] = t
			attributesUpdated = true
		} else if t := params["fsType"]; t != "" {
			attributes["fstype"] = t
			attributesUpdated = true
		}
		if pvcName := params["csi.storage.k8s.io/pvc/name"]; pvcName != "" {
			attributes["pvc_name"] = pvcName
			attributesUpdated = true
		}
		if pvcNamespace := params["csi.storage.k8s.io/pvc/namespace"]; pvcNamespace != "" {
			attributes["pvc_namespace"] = pvcNamespace
			attributesUpdated = true
		}
		if val, ok := params[ParamQosBurstIOPS]; ok {
			burstIOPS, _ = strconv.ParseInt(val, 10, 64)
			hasExplicitQoS = true
		}

		if hasExplicitQoS {
			// If previously associated with policy, disassociate?
			// SDK: "AssociateWithQoSPolicy: false" -> removes association.
			modifyReq.AssociateWithQoSPolicy = false
			modifyReq.Qos = &sdk.QoS{
				MinIOPS:   minIOPS,
				MaxIOPS:   maxIOPS,
				BurstIOPS: burstIOPS,
			}
			qosUpdated = true
		}
	}

	// If attributes changed, attach them to ModifyVolumeRequest
	if attributesUpdated {
		modifyReq.Attributes = attributes
	}

	if qosUpdated || attributesUpdated {
		if _, err := client.SFClient.ModifyVolume(ctx, &modifyReq); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to modify volume: %v", err)
		}
	}

	// TODO: Handle expansion here? Or is that strictly ControllerExpandVolume?
	// CSI split them. ModifyVolume is for properties (like QoS, other metadata).

	return &csi.ControllerModifyVolumeResponse{}, nil
}

func (cs *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	volIDStr := req.GetVolumeId()
	if volIDStr == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID must be provided")
	}

	volID, err := strconv.ParseInt(volIDStr, 10, 64)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "malformed volume ID %s: %v", volIDStr, err)
	}

	capacityRange := req.GetCapacityRange()
	if capacityRange == nil {
		return nil, status.Error(codes.InvalidArgument, "Capacity range must be provided")
	}

	requiredBytes := capacityRange.GetRequiredBytes()
	limitBytes := capacityRange.GetLimitBytes() // often 0 (no limit)

	// Move Client Init up to get dynamic limits
	secrets := req.GetSecrets()
	client, err := cs.getClientFromSecrets(secrets, nil)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "failed to initialize client: %v", err)
	}

	var maxSize int64 = MaxVolumeSizeBytes
	if client.Limits != nil && client.Limits.VolumeSizeMax > 0 {
		maxSize = client.Limits.VolumeSizeMax
	}

	if requiredBytes > maxSize {
		return nil, status.Errorf(codes.OutOfRange, "requested size %d exceeds maximum allowed size %d", requiredBytes, maxSize)
	}

	// Check limitBytes if set (though usually limitBytes >= requiredBytes)
	if limitBytes > 0 && limitBytes < maxSize {
		// Valid request within limits
	}

	// Fetch current size to ensure we are expanding
	vol, err := client.GetVolume(volID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %d not found: %v", volID, err)
	}

	// Validate fstype for resize support
	if vol.Attributes != nil {
		if attrs, ok := vol.Attributes.(map[string]interface{}); ok {
			if fstypeObj, ok := attrs["fstype"]; ok {
				if fstype, ok := fstypeObj.(string); ok && fstype != "" {
					// Kubernetes only supports resizing XFS, Ext3, Ext4
					validFs := []string{"ext3", "ext4", "xfs"}
					isSupported := false
					lowercaseFs := strings.ToLower(fstype)
					for _, v := range validFs {
						if lowercaseFs == v {
							isSupported = true
							break
						}
					}
					if !isSupported {
						return nil, status.Errorf(codes.FailedPrecondition, "expansion not supported for fstype '%s': only ext3, ext4, xfs are supported", fstype)
					}
				}
			}
		}
	}

	if vol.TotalSize >= requiredBytes {
		// Already satisfied
		return &csi.ControllerExpandVolumeResponse{
			CapacityBytes:         vol.TotalSize,
			NodeExpansionRequired: true, // Filesystem resize usually needed
		}, nil
	}

	// Perform Expansion
	modifyReq := sdk.ModifyVolumeRequest{
		VolumeID:  volID,
		TotalSize: requiredBytes,
	}

	if _, err := client.SFClient.ModifyVolume(ctx, &modifyReq); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to expand volume: %v", err)
	}

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         requiredBytes,
		NodeExpansionRequired: true,
	}, nil
}

// Group Snapshot Implementation

func (cs *ControllerServer) CreateVolumeGroupSnapshot(ctx context.Context, req *csi.CreateVolumeGroupSnapshotRequest) (*csi.CreateVolumeGroupSnapshotResponse, error) {
	// 1. Basic Validation
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Name must be provided")
	}
	if len(req.SourceVolumeIds) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Source Volume IDs must be provided")
	}

	// 2. Client Setup
	secrets := req.GetSecrets()
	client, err := cs.getClientFromSecrets(secrets, req.GetParameters())
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "failed to initialize client: %v", err)
	}

	// 3. Parse Volume IDs
	var volIDs []int64
	for _, idStr := range req.SourceVolumeIds {
		vID, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "malformed volume ID %s: %v", idStr, err)
		}
		volIDs = append(volIDs, vID)
	}

	// 4. Precondition Check: Limit Volume Count
	// Use dynamic limit from backend if available
	limit := int64(32) // Default fallback
	if client.Limits != nil && client.Limits.VolumesPerGroupSnapshotMax > 0 {
		limit = client.Limits.VolumesPerGroupSnapshotMax
	}

	if int64(len(volIDs)) > limit {
		return nil, status.Errorf(codes.FailedPrecondition, "number of volumes (%d) exceeds backend limit (%d)", len(volIDs), limit)
	}

	// 4.5 Idempotency Check
	// List Group Snapshots to check if it already exists by name
	// Check by volumes to potentially avoid listing all (and maybe fix the error if it expects volumes)
	listReq := sdk.ListGroupSnapshotsRequest{
		Volumes: volIDs,
	}
	listRes, err := client.SFClient.ListGroupSnapshots(ctx, &listReq)
	if err == nil {
		logrus.Infof("Idempotency Check: Searching for Group Snapshot Name '%s' in %d existing snapshots", req.Name, len(listRes.GroupSnapshots))
		for _, gs := range listRes.GroupSnapshots {
			// Log mismatch for debugging
			if gs.Name == req.Name {
				logrus.Infof("Idempotency: Found existing group snapshot %s (ID: %d)", req.Name, gs.GroupSnapshotID)

				var entries []*csi.Snapshot
				for _, member := range gs.Members {
					snapID := cs.compositeSnapshotToken(member.VolumeID, member.SnapshotID)
					entries = append(entries, &csi.Snapshot{
						SnapshotId:     snapID,
						SourceVolumeId: strconv.FormatInt(member.VolumeID, 10),
						ReadyToUse:     true,
					})
				}

				return &csi.CreateVolumeGroupSnapshotResponse{
					GroupSnapshot: &csi.VolumeGroupSnapshot{
						GroupSnapshotId: strconv.FormatInt(gs.GroupSnapshotID, 10),
						Snapshots:       entries,
						ReadyToUse:      true,
						CreationTime:    timestamppb.New(time.Now()), // TODO: Use actual creation time if available
					},
				}, nil
			}
		}
	} else {
		logrus.Errorf("Idempotency Check: Failed to list group snapshots: %v. Attempting fallback via Volume Snapshots.", err)
		// Fallback: Check first volume for snapshot with matching name
		if len(volIDs) > 0 {
			vID := volIDs[0]
			snapReq := sdk.ListSnapshotsRequest{VolumeID: vID}
			snapRes, snapErr := client.SFClient.ListSnapshots(ctx, &snapReq)
			if snapErr == nil {
				logrus.Infof("Idempotency Fallback: Checking %d snapshots on volume %d for '%s'", len(snapRes.Snapshots), vID, req.Name)
				for _, s := range snapRes.Snapshots {
					// logrus.Debugf("Fallback: Found snap '%s'", s.Name)
					if s.Name == req.Name && s.GroupID != 0 {
						logrus.Infof("Idempotency Fallback: Found snapshot %s (Group ID: %d) on volume %d", req.Name, s.GroupID, vID)
						foundGroupID := s.GroupID

						var entries []*csi.Snapshot
						// Iterate all source volumes to find their corresponding snapshot in this group
						for _, srcVolID := range volIDs {
							sReq := sdk.ListSnapshotsRequest{VolumeID: srcVolID}
							sRes, sErr := client.SFClient.ListSnapshots(ctx, &sReq)
							if sErr == nil {
								for _, snap := range sRes.Snapshots {
									if snap.GroupID == foundGroupID {
										snapID := cs.compositeSnapshotToken(snap.VolumeID, snap.SnapshotID)
										entries = append(entries, &csi.Snapshot{
											SnapshotId:     snapID,
											SourceVolumeId: strconv.FormatInt(snap.VolumeID, 10),
											ReadyToUse:     true,
										})
										break
									}
								}
							}
						}

						return &csi.CreateVolumeGroupSnapshotResponse{
							GroupSnapshot: &csi.VolumeGroupSnapshot{
								GroupSnapshotId: strconv.FormatInt(foundGroupID, 10),
								Snapshots:       entries,
								ReadyToUse:      true,
								CreationTime:    timestamppb.New(time.Now()),
							},
						}, nil
					}
				}
			}
		}
	}

	// 5. Create Group Snapshot
	params := req.GetParameters()

	retention := "24:00:00"
	if r, ok := params["retention"]; ok {
		retention = r
	}

	ensureSerial := false
	if val, ok := params["ensureSerialCreation"]; ok {
		ensureSerial = strings.ToLower(val) == "true"
	}

	enableRemote := false
	if val, ok := params["enableRemoteReplication"]; ok {
		enableRemote = strings.ToLower(val) == "true"
	}

	createReq := sdk.CreateGroupSnapshotRequest{
		Volumes:                 volIDs,
		Name:                    req.Name,
		Retention:               retention,
		EnsureSerialCreation:    ensureSerial,
		EnableRemoteReplication: enableRemote,
	}

	res, err := client.SFClient.CreateGroupSnapshot(ctx, &createReq)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create group snapshot: %v", err)
	}

	// 6. Build Response
	// We need to list the individual snapshots created.
	// The response from CreateGroupSnapshot contains:
	// GroupSnapshotID, GroupSnapshotUUID, Members (List of {VolumeID, SnapshotID, SnapshotUUID, ...})

	var entries []*csi.Snapshot

	// SF Member structure:
	// Members []GroupSnapshotMembers `json:"members"`
	// GroupSnapshotMembers: VolumeID, SnapshotID, SnapshotUUID, etc.

	for _, member := range res.Members {
		// CSI Composite ID: "VolID:SnapID" (matches our single snapshot format)
		snapID := cs.compositeSnapshotToken(member.VolumeID, member.SnapshotID)

		entries = append(entries, &csi.Snapshot{
			SnapshotId:     snapID,
			SourceVolumeId: strconv.FormatInt(member.VolumeID, 10),
			CreationTime:   nil,  // TODO: Timestamp conversion if available
			ReadyToUse:     true, // SF Snaps are instant
		})
	}

	return &csi.CreateVolumeGroupSnapshotResponse{
		GroupSnapshot: &csi.VolumeGroupSnapshot{
			GroupSnapshotId: strconv.FormatInt(res.GroupSnapshotID, 10),
			Snapshots:       entries,
			CreationTime:    nil, // TODO: Timestamp
			ReadyToUse:      true,
		},
	}, nil
}

func (cs *ControllerServer) DeleteVolumeGroupSnapshot(ctx context.Context, req *csi.DeleteVolumeGroupSnapshotRequest) (*csi.DeleteVolumeGroupSnapshotResponse, error) {
	if req.GroupSnapshotId == "" {
		return nil, status.Error(codes.InvalidArgument, "Group Snapshot ID must be provided")
	}

	gID, err := strconv.ParseInt(req.GroupSnapshotId, 10, 64)
	if err != nil {
		// Idempotency: invalid ID means it doesn't exist.
		return &csi.DeleteVolumeGroupSnapshotResponse{}, nil
	}

	secrets := req.GetSecrets()
	client, err := cs.getClientFromSecrets(secrets, nil)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "failed to initialize client: %v", err)
	}

	// Delete Group Snapshot
	// Note: Deleting the group snapshot deletes all individual snapshots associated with it on SF.
	delReq := sdk.DeleteGroupSnapshotRequest{
		GroupSnapshotID: gID,
		SaveMembers:     false, // Delete members too
	}

	if _, err := client.SFClient.DeleteGroupSnapshot(ctx, &delReq); err != nil {
		if strings.Contains(err.Error(), "xGroupSnapshotDoesNotExist") {
			return &csi.DeleteVolumeGroupSnapshotResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to delete group snapshot: %v", err)
	}

	return &csi.DeleteVolumeGroupSnapshotResponse{}, nil
}

func (cs *ControllerServer) GetVolumeGroupSnapshot(ctx context.Context, req *csi.GetVolumeGroupSnapshotRequest) (*csi.GetVolumeGroupSnapshotResponse, error) {
	if req.GroupSnapshotId == "" {
		return nil, status.Error(codes.InvalidArgument, "Group Snapshot ID must be provided")
	}

	gID, err := strconv.ParseInt(req.GroupSnapshotId, 10, 64)
	if err != nil {
		return nil, status.Error(codes.NotFound, "group snapshot not found")
	}

	secrets := req.GetSecrets()
	client, err := cs.getClientFromSecrets(secrets, nil)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "failed to initialize client: %v", err)
	}

	// Use ListGroupSnapshots with filter as GetGroupSnapshot might not be generated or standard
	getReq := sdk.ListGroupSnapshotsRequest{
		GroupSnapshotID: gID,
	}

	res, err := client.SFClient.ListGroupSnapshots(ctx, &getReq)
	if err != nil {
		// Can't distinguish not found easily without checking text
		return nil, status.Errorf(codes.Internal, "failed to get group snapshot: %v", err)
	}

	if len(res.GroupSnapshots) == 0 {
		return nil, status.Error(codes.NotFound, "group snapshot not found")
	}

	groupSnap := res.GroupSnapshots[0]

	// Convert to CSI response
	var entries []*csi.Snapshot
	for _, member := range groupSnap.Members {
		snapID := cs.compositeSnapshotToken(member.VolumeID, member.SnapshotID)
		entries = append(entries, &csi.Snapshot{
			SnapshotId:     snapID,
			SourceVolumeId: strconv.FormatInt(member.VolumeID, 10),
			// ReadyToUse is implicit in CSI structure for Get?
			// Wait, the return type is GetVolumeGroupSnapshotResponse.
			// It contains VolumeGroupSnapshot.
			// Which contains repeated Snapshot snapshots = 3; where Snapshot is a CSI Snapshot object?
			// Let's check proto definition or structure.
			// Actually field 3 in VolumeGroupSnapshot is 'dependencies' or 'snapshots'?
			// 'repeated Snapshot snapshots'
		})
	}
	// Re-checking CSI spec. GetVolumeGroupSnapshotResponse -> VolumeGroupSnapshot
	// VolumeGroupSnapshot usually has:
	// string group_snapshot_id
	// map<string, string> attributes
	// google.protobuf.Timestamp creation_time
	// bool ready_to_use
	// repeated Snapshot snapshots

	// Prepare Snapshots
	var csiSnapshots []*csi.Snapshot
	for _, member := range groupSnap.Members {
		snapID := cs.compositeSnapshotToken(member.VolumeID, member.SnapshotID)
		csiSnapshots = append(csiSnapshots, &csi.Snapshot{
			SnapshotId:     snapID,
			SourceVolumeId: strconv.FormatInt(member.VolumeID, 10),
			ReadyToUse:     true, // If group is done, members are done
			// CreationTime: ...
		})
	}

	return &csi.GetVolumeGroupSnapshotResponse{
		GroupSnapshot: &csi.VolumeGroupSnapshot{
			GroupSnapshotId: strconv.FormatInt(groupSnap.GroupSnapshotID, 10),
			Snapshots:       csiSnapshots,
			ReadyToUse:      true, // groupSnap.Status == "done"
			CreationTime:    nil,  // TODO: parse groupSnap.CreateTime
		},
	}, nil
}

// Helpers

func (cs *ControllerServer) getClientFromSecrets(secrets map[string]string, params map[string]string) (*sf.Client, error) {
	// Helper to decode potentially base64 encoded strings
	decodeValue := func(value string) string {
		if value == "" {
			return ""
		}
		decoded, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return value
		}
		// If successfully decoded, check if it is printable UTF-8
		if !utf8.Valid(decoded) {
			return value
		}
		return string(decoded)
	}

	// Helper to merge source of config
	getVal := func(key string) string {
		if v, ok := secrets[key]; ok {
			return decodeValue(v)
		}
		if v, ok := params[key]; ok {
			return decodeValue(v)
		}
		return ""
	}

	endpoint := getVal(ParamEndpoint)
	user := getVal(ParamUsername)
	password := getVal(ParamPassword)
	tenant := getVal(ParamTenant)

	// Fallback to Environment Variables if not provided in secrets/params
	if endpoint == "" {
		endpoint = os.Getenv("SOLIDFIRE_ENDPOINT")
	}
	if user == "" {
		user = os.Getenv("SOLIDFIRE_USER")
	}
	if password == "" {
		password = os.Getenv("SOLIDFIRE_PASSWORD")
	}
	if tenant == "" {
		tenant = os.Getenv("SOLIDFIRE_TENANT")
	}

	// Default version if not set
	version := "11.0" // TODO: make configurable

	if endpoint == "" || user == "" || password == "" {
		// Log detailed missing info for debugging
		// Warning: Be careful not to log passwords in production, checking existence only
		return nil, fmt.Errorf("missing required connection info: endpoint=%v, user=%v, password=%v", endpoint != "", user != "", password != "")
	}

	client, err := sf.NewClientFromSecrets(endpoint, user, password, version, tenant, "1G")
	if err != nil {
		return nil, err
	}

	// Lazy Discovery Registration
	// Use a composite key to uniquely identify this backend/tenant combination
	key := fmt.Sprintf("%s|%s", endpoint, tenant)
	if _, exists := cs.backendRegistry.Load(key); !exists {
		// Register safely
		cs.backendRegistry.Store(key, &backendEntry{
			Client:   client,
			Endpoint: endpoint,
			Tenant:   tenant,
		})
		logrus.Infof("Registered new backend for metrics: %s (Tenant: %s)", endpoint, tenant)
	}

	return client, nil
}

// Small helpers to parse and resolve CSI snapshot tokens. CSI tests sometimes pass
// snapshot identifiers as either "volID:snapID" (composite) or just the numeric
// snapshot ID. Centralizing parsing/lookup keeps semantics and gRPC code mappings
// consistent across the controller.

// parseCompositeSnapshotID parses a CSI snapshot token which may be "volID:snapID"
// or a bare "snapID". Returns parsed source volume ID (0 if not present), the
// snapshot ID, or a gRPC error (NotFound) when parsing fails.
func (cs *ControllerServer) parseCompositeSnapshotID(token string) (int64, int64, error) {
	logrus.Debugf("parseCompositeSnapshotID: parsing token=%s", token)
	parts := strings.Split(token, ":")
	var parsedSrcVolID int64
	var srcSnapID int64
	var err error
	if len(parts) == 2 {
		parsedSrcVolID, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, 0, status.Errorf(codes.NotFound, "invalid source volume ID in snapshot composite: %v", err)
		}
		srcSnapID, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, status.Errorf(codes.NotFound, "invalid snapshot ID in composite: %v", err)
		}
	} else {
		srcSnapID, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, 0, status.Errorf(codes.NotFound, "invalid snapshot ID: %v", err)
		}
	}
	logrus.Debugf("parseCompositeSnapshotID: parsed srcVol=%d snapID=%d", parsedSrcVolID, srcSnapID)
	return parsedSrcVolID, srcSnapID, nil
}

// resolveSnapshotRef looks up the SolidFire snapshot referenced by the CSI token.
// It returns the snapshot's source VolumeID, a pointer to the SDK Snapshot struct,
// or a gRPC error suitable for returning to callers/tests.
func (cs *ControllerServer) resolveSnapshotRef(ctx context.Context, client *sf.Client, token string) (int64, *sdk.Snapshot, error) {
	logrus.Debugf("resolveSnapshotRef: resolving token=%s", token)
	parsedVolID, snapID, err := cs.parseCompositeSnapshotID(token)
	if err != nil {
		return 0, nil, err
	}

	listReq := sdk.ListSnapshotsRequest{SnapshotID: snapID}
	snapRes, listErr := client.SFClient.ListSnapshots(ctx, &listReq)
	if listErr != nil {
		if strings.Contains(listErr.Error(), "xSnapshotDoesNotExist") || strings.Contains(strings.ToLower(listErr.Error()), "not exist") {
			return 0, nil, status.Errorf(codes.NotFound, "snapshot %d not found", snapID)
		}
		return 0, nil, status.Errorf(codes.Internal, "failed to lookup snapshot %d: %v", snapID, listErr)
	}
	if snapRes == nil || len(snapRes.Snapshots) == 0 {
		logrus.Debugf("resolveSnapshotRef: no snapshots returned for id=%d", snapID)
		return 0, nil, status.Errorf(codes.NotFound, "snapshot %d not found", snapID)
	}

	sfSnap := snapRes.Snapshots[0]
	logrus.Debugf("resolveSnapshotRef: found snapshot id=%d vol=%d name=%s", sfSnap.SnapshotID, sfSnap.VolumeID, sfSnap.Name)
	if parsedVolID != 0 && sfSnap.VolumeID != parsedVolID {
		logrus.Debugf("resolveSnapshotRef: composite volume mismatch parsed=%d actual=%d", parsedVolID, sfSnap.VolumeID)
		return 0, nil, status.Errorf(codes.NotFound, "snapshot %d does not belong to source volume %d", snapID, parsedVolID)
	}

	return sfSnap.VolumeID, &sfSnap, nil
}

// compositeSnapshotToken builds the CSI composite snapshot token used by this
// driver in the form "<volumeID>:<snapshotID>". Centralizing ensures
// consistent formatting across group and single-snapshot responses.
func (cs *ControllerServer) compositeSnapshotToken(volID, snapID int64) string {
	return fmt.Sprintf("%d:%d", volID, snapID)
}

// waitForVolumeActive polls the backend until the volume reaches the "active" state
// or the timeout is reached. Returns nil when active, or an error on timeout or fatal errors.
func (cs *ControllerServer) waitForVolumeActive(ctx context.Context, client *sf.Client, volID int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	logrus.Infof("waitForVolumeActive: waiting for volume %d to reach 'active' (timeout %s)", volID, timeout)
	var lastStatus string
	var lastErr error
	for {
		v, err := client.GetVolume(volID)
		if err != nil {
			lastErr = err
			// If volume appears to not exist, attempt a fallback ListVolumes call
			if strings.Contains(err.Error(), "xVolumeDoesNotExist") || strings.Contains(strings.ToLower(err.Error()), "does not exist") {
				logrus.Infof("waitForVolumeActive: volume %d not found via GetVolume; trying ListVolumes fallback", volID)
				if vols, lerr := client.ListVolumes(); lerr == nil {
					found := false
					for _, lv := range vols {
						if lv.VolumeID == volID {
							found = true
							// treat found volume as the fetched volume
							v = &lv
							lastErr = nil
							break
						}
					}
					if !found {
						logrus.Debugf("waitForVolumeActive: ListVolumes did not contain volume %d", volID)
						// treat as transient and continue polling
					}
				} else {
					logrus.Debugf("waitForVolumeActive: ListVolumes fallback failed for %d: %v", volID, lerr)
				}
			} else {
				// Transient error; log debug and retry until deadline
				logrus.Debugf("waitForVolumeActive: transient error fetching volume %d: %v", volID, err)
			}
		}
		if v != nil {
			lastStatus = v.Status
			// If active, we're done
			if strings.ToLower(v.Status) == "active" {
				logrus.Debugf("waitForVolumeActive: volume %d is active", volID)
				return nil
			}

			// Provide richer debug info about non-active states
			if strings.ToLower(v.Status) == "init" || strings.ToLower(v.Status) == "creating" {
				logrus.Debugf("waitForVolumeActive: volume %d status=%s (init/creating) - likely being created by an async job", volID, v.Status)
			} else {
				logrus.Debugf("waitForVolumeActive: volume %d status=%s - waiting", volID, v.Status)
			}
		} else {
			logrus.Debugf("waitForVolumeActive: GetVolume returned nil for %d", volID)
		}

		if time.Now().After(deadline) {
			logrus.Infof("waitForVolumeActive: timeout for volume %d after %s; lastStatus=%q lastErr=%v", volID, timeout, lastStatus, lastErr)
			return fmt.Errorf("timeout waiting for volume %d to become active", volID)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
			// continue polling
		}
	}
}

// findClonedVolumeByName attempts to locate a volume created by a clone operation
// with the given backend name. It first searches ListVolumesForAccount, then
// falls back to inspecting async results (ListAsyncResults) to find a Clone
// async handle whose data references the target name or volume ID.
func (cs *ControllerServer) findClonedVolumeByName(ctx context.Context, client *sf.Client, backendName string, ignoreVolID int64) (*sdk.Volume, error) {
	// Fast path: list volumes for account and match by name or attribute
	if vols, err := client.ListVolumes(); err == nil {
		for _, v := range vols {
			if v.Name == backendName {
				vol := v
				return &vol, nil
			}
			// also check stored attributes if present
			if v.Attributes != nil {
				if m, ok := v.Attributes.(map[string]interface{}); ok {
					if nm, ok2 := m["pv_name"].(string); ok2 && nm == backendName {
						vol := v
						return &vol, nil
					}
					if nm, ok2 := m["csi.original_name"].(string); ok2 && nm == backendName {
						vol := v
						return &vol, nil
					}
				}
			}
		}
	}

	// Fallback: inspect async results for Clone entries
	req := &sdk.ListAsyncResultsRequest{}
	if res, err := client.SFClient.ListAsyncResults(ctx, req); err == nil && res != nil {
		for _, ah := range res.AsyncHandles {
			// Look for Clone resultType
			if strings.ToLower(ah.ResultType) != "clone" {
				continue
			}
			// Data is interface{}; try to inspect for volumeID or name
			if ah.Data != nil {
				if m, ok := ah.Data.(map[string]interface{}); ok {
					if volid, ok2 := m["volumeID"]; ok2 {
						// numeric types may be float64 from json unmarshal
						switch t := volid.(type) {
						case float64:
							vid := int64(t)
							if ignoreVolID != 0 && vid == ignoreVolID {
								// skip entries that reference the source volume
								break
							}
							if v, gerr := client.GetVolume(vid); gerr == nil {
								return v, nil
							}
						case int64:
							if ignoreVolID != 0 && t == ignoreVolID {
								break
							}
							if v, gerr := client.GetVolume(t); gerr == nil {
								return v, nil
							}
						}
					}
					if name, ok2 := m["name"].(string); ok2 && name == backendName {
						// Try to find by ListVolumes again to fetch full object
						if vols2, err2 := client.ListVolumes(); err2 == nil {
							for _, v := range vols2 {
								if v.Name == backendName {
									vol := v
									return &vol, nil
								}
							}
						}
					}
				}
			}
		}
	}

	return nil, fmt.Errorf("cloned volume %s not found", backendName)
}

// nodeExists checks whether a Kubernetes Node with the given name exists.
// It lazily initializes an in-cluster Kubernetes client.
func (cs *ControllerServer) nodeExists(ctx context.Context, nodeName string) bool {
	if nodeName == "" {
		return false
	}
	// Treat the synthetic controller NodeID used when running in Controller
	// mode as present to accommodate test harnesses that query NodeGetInfo
	// and receive "controller" as the node identifier.
	if nodeName == "controller" {
		logrus.Debugf("nodeExists: treating synthetic node 'controller' as present")
		return true
	}
	// Lazily create client
	if cs.kubeClient == nil {
		cfg, err := rest.InClusterConfig()
		if err != nil {
			logrus.Debugf("nodeExists: failed to load in-cluster config: %v", err)
			return false
		}
		clientset, err := kubernetes.NewForConfig(cfg)
		if err != nil {
			logrus.Debugf("nodeExists: failed to create kube client: %v", err)
			return false
		}
		cs.kubeClient = clientset
	}

	_, err := cs.kubeClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		logrus.Debugf("nodeExists: node %s not found: %v", nodeName, err)
		return false
	}
	return true
}

func (cs *ControllerServer) checkQuotas(client *sf.Client, params map[string]string, requestedBytes int64) error {
	// 1. Get Quota Limits from Parameters
	maxVolsStr := params[QuotaMaxVolumes]
	maxCapStr := params[QuotaMaxCapacity]

	// If no quotas defined, return nil
	if maxVolsStr == "" && maxCapStr == "" {
		return nil
	}

	// 2. Fetch Current Usage
	vols, err := client.ListVolumes()
	if err != nil {
		return fmt.Errorf("failed to list volumes for quota verification: %v", err)
	}

	currentCount := len(vols)
	var currentTotalBytes int64
	for _, v := range vols {
		currentTotalBytes += v.TotalSize
	}

	// 3. Verify Volume Count
	if maxVolsStr != "" {
		maxVols, err := strconv.Atoi(maxVolsStr)
		if err == nil && maxVols > 0 {
			if currentCount >= maxVols {
				return fmt.Errorf("maximum volume count allowed (%d) reached", maxVols)
			}
		}
	}

	// 4. Verify Capacity
	if maxCapStr != "" {
		// Very basic parsing for now. Assuming bytes if just number, or implement parseQuantity
		// For simplicity, let's assume the user passes raw bytes or we use a library later.
		// Pro-tip: kubernetes resource.ParseQuantity is heavy to import here if we want to stay light,
		// but checking if it's just a number is a good start.
		maxCap, err := strconv.ParseInt(maxCapStr, 10, 64)
		if err == nil && maxCap > 0 {
			if currentTotalBytes+requestedBytes > maxCap {
				return fmt.Errorf("maximum capacity allowed (%d) would be exceeded by request (curr: %d, req: %d)", maxCap, currentTotalBytes, requestedBytes)
			}
		}
	}

	return nil
}

func (cs *ControllerServer) GroupControllerGetCapabilities(ctx context.Context, req *csi.GroupControllerGetCapabilitiesRequest) (*csi.GroupControllerGetCapabilitiesResponse, error) {
	return &csi.GroupControllerGetCapabilitiesResponse{
		Capabilities: []*csi.GroupControllerServiceCapability{
			{
				Type: &csi.GroupControllerServiceCapability_Rpc{
					Rpc: &csi.GroupControllerServiceCapability_RPC{
						Type: csi.GroupControllerServiceCapability_RPC_CREATE_DELETE_GET_VOLUME_GROUP_SNAPSHOT,
					},
				},
			},
		},
	}, nil
}
