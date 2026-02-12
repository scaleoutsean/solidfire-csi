package driver

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	mount "k8s.io/mount-utils"
	kexec "k8s.io/utils/exec"
)

type NodeServer struct {
	csi.UnimplementedNodeServer
	NodeID string

	// reaper fields (opt-in)
	reaperEnabled       bool
	reaperInterval      time.Duration
	reaperStaleSeconds  int
	reaperMaxConcurrent int
	// Controller advisory: if Node is registered in controller process this
	// pointer is set so the reaper can consult controller session advisory.
	Controller                    *ControllerServer
	reaperUseControllerAdvisory   bool
	reaperAdvisoryCutoffSeconds   int
	reaperLocalCandidateThreshold int

	// unseenWithoutDevice tracks when a session was first observed without device
	unseenMu            sync.Mutex
	unseenWithoutDevice map[string]time.Time

	// attachInProgress tracks IQNs that are currently being attached/login
	attachMu         sync.Mutex
	attachInProgress map[string]bool
}

func NewNodeServer(nodeID string) *NodeServer {
	ns := &NodeServer{
		NodeID:              nodeID,
		unseenWithoutDevice: make(map[string]time.Time),
		attachInProgress:    make(map[string]bool),
	}

	// Reaper configuration (opt-in)
	if os.Getenv("SFCSI_ENABLE_REAPER") == "true" {
		ns.reaperEnabled = true
		interval := getEnvInt("SFCSI_REAPER_INTERVAL_SECONDS", 300)
		ns.reaperInterval = time.Duration(interval) * time.Second
		ns.reaperStaleSeconds = getEnvInt("SFCSI_REAPER_STALE_SECONDS", 60)
		ns.reaperMaxConcurrent = getEnvInt("SFCSI_REAPER_MAX_CONCURRENT", 2)
		// Advisory toggles
		ns.reaperUseControllerAdvisory = os.Getenv("SFCSI_REAPER_USE_CONTROLLER_ADVISORY") == "true"
		ns.reaperAdvisoryCutoffSeconds = getEnvInt("SFCSI_REAPER_ADVISORY_CUTOFF_SECONDS", 30)
		ns.reaperLocalCandidateThreshold = getEnvInt("SFCSI_REAPER_ADVISORY_LOCAL_CANDIDATE_THRESHOLD", 8)
		go ns.startReaper()
		logrus.Infof("Node reaper enabled: interval=%s stale=%ds max_concurrent=%d", ns.reaperInterval, ns.reaperStaleSeconds, ns.reaperMaxConcurrent)
	}

	return ns
}

// StartMetricsCollection begins a background goroutine that periodically
// counts iSCSI sessions visible in the host namespace and exposes the
// count as a Prometheus metric.
func (ns *NodeServer) StartMetricsCollection(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			count := ns.collectIscsiSessionCount()
			nodeLabel := ns.NodeID
			if nodeLabel == "" {
				nodeLabel = "controller"
			}
			metricIscsiSessions.WithLabelValues(nodeLabel).Set(float64(count))
		}
	}()
}

func (ns *NodeServer) collectIscsiSessionCount() int {
	// Use nsenter to observe sessions from the PID 1 namespace (host)
	cmd := exec.Command("/usr/bin/nsenter", "-t", "1", "-m", "-u", "-i", "-n", "-p", "--", "iscsiadm", "-m", "session")
	logrus.Debugf("collectIscsiSessionCount: running nsenter: %v", cmd.Args)
	output, err := cmd.CombinedOutput()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			// iscsiadm returns 21 when no sessions
			if exitErr.ExitCode() == 21 {
				return 0
			}
		}
		logrus.Debugf("collectIscsiSessionCount: iscsiadm error: %v output=%s", err, string(output))
		return 0
	}
	lines := strings.Split(string(output), "\n")
	count := 0
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		// Each session typically appears as one line starting with tcp: or similar
		if strings.HasPrefix(l, "tcp:") || strings.HasPrefix(l, "iscsi:") {
			count++
		}
	}
	return count
}

func (ns *NodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	// If running in Controller Mode (no NodeID), effectively disable Node Service capabilities
	// This helps csi-sanity (and CO) know not to try and unstage/mount things on the controller pod.
	if ns.NodeID == "" {
		return &csi.NodeGetCapabilitiesResponse{
			Capabilities: []*csi.NodeServiceCapability{},
		}, nil
	}

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
					},
				},
			},
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
					},
				},
			},
		},
	}, nil
}

func (ns *NodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	// If Controller mode, we might not have a NodeID.
	if ns.NodeID == "" {
		// Provide a dummy ID for Controller checks, or handle gracefully.
		// csi-sanity Controller tests might call NodeGetInfo to verify accessibility.
		return &csi.NodeGetInfoResponse{
			NodeId: "controller",
		}, nil
	}
	// Topology info can be added here
	return &csi.NodeGetInfoResponse{
		NodeId:            ns.NodeID,
		MaxVolumesPerNode: 0, // 0 = infinite (SolidFire doesn't limit attachments strictly per node)
	}, nil
}

func (ns *NodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	volID := req.GetVolumeId()
	if volID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing")
	}
	stagingTargetPath := req.GetStagingTargetPath()
	if stagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Staging Target Path missing")
	}

	// Example: Retrieve Service Account Token (K8s 1.35+ feature)
	// We don't use it for iSCSI/CHAP, but here is how to fetch it for future use (e.g. Vault auth)
	// saToken, err := getServiceAccountTokens(req)
	// if err == nil && saToken != "" {
	// 	// Log.Printf("Found SA Token for validation: %s...", saToken[:10])
	// }

	_ = req.GetVolumeContext()
	publishContext := req.GetPublishContext()

	// 1. Parse iSCSI info from PublishContext
	iqn := publishContext["iqn"]
	portal := publishContext["portal"]
	chapUser := publishContext["chapUser"]
	chapSecret := publishContext["chapSecret"]

	if iqn == "" || portal == "" {
		return nil, status.Error(codes.InvalidArgument, "missing iqn or portal in PublishContext")
	}

	// 2. Perform Discovery / Login
	// target := goiscsi.ISCSITarget{ ... } // Unused in optimized path

	// Optimized Login: Batch iscsiadm commands to reduce nsenter overhead
	// limiting context switches to 1 instead of ~5.
	if err := ns.optimizedIscsiLogin(ctx, portal, iqn, chapUser, chapSecret); err != nil {
		return nil, status.Errorf(codes.Internal, "optimized login failed: %v", err)
	}

	// 3. Find Device
	// Logic: /dev/disk/by-path/ip-<portal>-iscsi-<iqn>-lun-<lun>
	// Note: portal in path often includes port if non-default. SF is 3260.
	// goiscsi / standard linux behavior: "ip-10.10.10.10:3260-iscsi..."
	// We need to verify if SolidFire SVIP string includes port.
	// We assume inputs are clean.

	// Construct the by-path path
	// SF SVIP usually is just IP.
	portalPath := portal
	if !strings.Contains(portalPath, ":") {
		portalPath = portalPath + ":3260"
	}

	devicePath := fmt.Sprintf("/dev/disk/by-path/ip-%s-iscsi-%s-lun-%s", portalPath, iqn, "0")

	// Wait for device
	if !waitForPath(devicePath, 30) {
		return nil, status.Errorf(codes.DeadlineExceeded, "device path %s did not appear", devicePath)
	}

	// Check for multipath
	// Unless explicitly disabled via "disable_multipath_detection" parameter
	skipMultipath := req.GetVolumeContext()["disable_multipath_detection"]
	if skipMultipath == "true" {
		logrus.Infof("NodeStageVolume: disable_multipath_detection=true. Skipping DM check. Using path: %s", devicePath)
	} else {
		devicePath = resolveMultipathDevice(devicePath)
	}

	// resolve symlink to /dev/sdX for logging/formatting
	realDev, _ := os.Readlink(devicePath)
	if realDev != "" {
		if !strings.HasPrefix(realDev, "/") {
			realDev = "/dev/" + realDev // Readlink returns relative usually (../../sdb)
			// Wait, simple concat isn't safe.
			// Actually, let's just use the by-path. It's stable.
		}
	}

	// 4. Format and Mount
	// Use stagingTargetPath as mount point.
	// Check if mounted
	// Check if formatted
	// (We need a mounter helper. For this iteration, stub "Mount if not mounted")

	// Create Mount Point
	if err := os.MkdirAll(stagingTargetPath, 0750); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create staging path: %v", err)
	}

	// Get Mount Capability
	mountCap := req.GetVolumeCapability().GetMount()
	blockCap := req.GetVolumeCapability().GetBlock() // Check for Block Support

	if blockCap != nil {
		// BLOCK MODE:
		// We do NOT format. We do NOT mount a filesystem.
		// NodeStageVolume for Block is essentially a no-op or just ensuring the device exists.
		// Since we already attached it via iSCSI above, we are good.
		logrus.Debugf("NodeStageVolume: Block volume requested. Skipping format/mount. Device: %s", devicePath)
		return &csi.NodeStageVolumeResponse{}, nil
	}

	fsType := "ext4"
	var mountFlags []string

	if mountCap != nil {
		if mountCap.GetFsType() != "" {
			fsType = mountCap.GetFsType()
		}
		mountFlags = mountCap.GetMountFlags()
	}

	// Prepare mounter early so we can check idempotency (already-mounted).
	mounter := mount.New("")

	// If the staging target is already mounted, treat NodeStageVolume as
	// idempotent and return success. This avoids races where a previous
	// successful stage left the mount in place and subsequent repeated
	// NodeStageVolume calls would fail attempting to remount the device.
	notMnt, err := mounter.IsLikelyNotMountPoint(stagingTargetPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, status.Errorf(codes.Internal, "check staging mount point: %v", err)
	}
	if !notMnt {
		logrus.Debugf("NodeStageVolume: staging target %s already mounted", stagingTargetPath)
		return &csi.NodeStageVolumeResponse{}, nil
	}

	// AUTO-DISCARD: Inject 'discard' option if missing
	// This removes the burden of remembering it in StorageClass
	hasDiscard := false
	for _, f := range mountFlags {
		if f == "discard" {
			hasDiscard = true
			break
		}
	}
	if !hasDiscard {
		mountFlags = append(mountFlags, "discard")
	}

	// Check if formatted and Mount safely
	// Use SafeFormatAndMount from k8s.io/mount-utils
	// This handles the logic:
	// 1. Check if device has filesystem (blkid)
	// 2. If valid filesystem exists:
	//    a. If matches fsType: Mount
	//    b. If mismatch: Error (Safe!)
	// 3. If no filesystem: Format and Mount

	// Reuse mounter created above for SafeFormatAndMount
	safeMounter := &mount.SafeFormatAndMount{
		Interface: mounter,
		Exec:      kexec.New(),
	}

	// Safety Check: Before we trust SafeFormatAndMount, verify the device is actually empty.
	// If blkid returns nothing, SafeFormatAndMount will happily mkfs.
	// But if the device contains data (LUKS, Database, etc) that blkid misses, we destroy it.
	// SolidFire volumes are guaranteed zeroed when new. If the first 2MB aren't zero, it's not new.
	if err := verifyDeviceIsEmpty(devicePath, safeMounter); err != nil {
		return nil, status.Errorf(codes.Internal, "device safety check failed: %v", err)
	}

	if err := safeMounter.FormatAndMount(devicePath, stagingTargetPath, fsType, mountFlags); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to format and mount device: %v", err)
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

func verifyDeviceIsEmpty(devicePath string, mounter *mount.SafeFormatAndMount) error {
	// 1. Check if it has a filesystem signature using blkid (standard behavior)
	// We use mounter's internal check via GetDiskFormat, but unfortunately SafeFormatAndMount doesn't expose it directly conveniently for "check only".
	// So we trust standard tools.
	// Actually, we process the ZERO checks first. If it's zeroed, it has no FS.

	f, err := os.Open(devicePath)
	if err != nil {
		return fmt.Errorf("failed to open device %s: %v", devicePath, err)
	}
	defer f.Close()

	// 2MB buffer
	buf := make([]byte, 2*1024*1024)
	n, err := f.Read(buf)
	if err != nil {
		return fmt.Errorf("failed to read first 2MB of device: %v", err)
	}

	if n < len(buf) {
		// Device smaller than 2MB? Rare for SAN, but possible. Just check what we read.
		// For SolidFire, min vol size is bigger, so this implies read error or weirdness.
		// Proceeding with check on what we got.
	}

	for _, b := range buf[:n] {
		if b != 0 {
			// Found non-zero byte!
			// Check if it is a known filesystem.
			// misuse SafeFormatAndMount's helper if possible or run blkid manually.
			// If blkid finds nothing, but we found data -> DANGER (LUKS, Oracle ASM, etc).

			// Use 'blkid' command directly to see if the system knows what this is.
			cmd := exec.Command("blkid", devicePath)
			output, err := cmd.CombinedOutput()
			if err == nil && len(output) > 0 {
				outputStr := string(output)
				// Prevent LUKS-protected volume from being formatted (or attempted to mount if we don't support it)
				// We fail fast here with a clear error message.
				if strings.Contains(outputStr, "crypto_LUKS") {
					return fmt.Errorf("device %s is LUKS encrypted; this driver does not support LUKS", devicePath)
				}

				// blkid successfully identified a filesystem/partition.
				// This is safe: FormatAndMount will detect this same FS and either mount it (success) or fail (mismatch).
				// We return nil to let SafeFormatAndMount handle it.
				logrus.Infof("Device %s has non-zero data, but blkid identified a signature: %s. Proceeding to Mount.", devicePath, outputStr)
				return nil
			}

			// blkid failed (exit code != 0) OR returned empty string.
			// But we see data. This is the danger zone.
			return fmt.Errorf("device %s contains non-zero data (not a new volume) but contains no recognizable filesystem signature. Refusing to format to prevent data loss.", devicePath)
		}
	}

	// All bytes are zero. Safe to format.
	return nil
}

const serviceAccountTokenKey = "csi.storage.k8s.io/serviceAccount.tokens"

func getServiceAccountTokens(req *csi.NodeStageVolumeRequest) (string, error) {
	// Check secrets field first (new behavior when driver opts in)
	if tokens, ok := req.Secrets[serviceAccountTokenKey]; ok {
		return tokens, nil
	}

	// Fall back to volume context (existing behavior if podInfoOnMount=true)
	// Note: NodeStageVolumeRequest doesn't always have VolumeContext populated with pod info depending on setup
	if tokens, ok := req.GetVolumeContext()[serviceAccountTokenKey]; ok {
		return tokens, nil
	}

	return "", fmt.Errorf("service account tokens not found")
}

func (ns *NodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	volID := req.GetVolumeId()
	if volID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing")
	}

	stagingTargetPath := req.GetStagingTargetPath()
	if stagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Staging Target Path missing")
	}

	// Unmount
	cmd := exec.Command("umount", stagingTargetPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		if !strings.Contains(string(out), "not mounted") && !strings.Contains(string(out), "no mount point specified") {
			return nil, status.Errorf(codes.Internal, "failed to unmount staging path: %v, output: %s", err, string(out))
		}
	}

	// Cleanup: Logout iSCSI session
	// We do this after unmount to ensure the device is not in use.
	if err := ns.logoutForVolume(ctx, req.GetVolumeId()); err != nil {
		logrus.Warnf("NodeUnstageVolume: failed to logout iSCSI session for volume %s: %v", req.GetVolumeId(), err)
		// We log warning but do not fail the request, as unmount was successful.
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *NodeServer) logoutForVolume(ctx context.Context, volID string) error {
	// 1. Find the IQN associated with this volume ID.
	// We assume the SolidFire IQN ends with the VolumeID (e.g. .1499)
	iqn, err := ns.findIQNByVolumeID(volID)
	if err != nil {
		return err // Might be "not found", which is fine, we just can't logout what we can't find.
	}
	if iqn == "" {
		logrus.Debugf("logoutForVolume: no active session found for volume %s", volID)
		return nil
	}

	// 2. Optimized Logout
	return ns.optimizedIscsiLogout(ctx, iqn)
}

func (ns *NodeServer) findIQNByVolumeID(volID string) (string, error) {
	// Run "iscsiadm -m session" via nsenter
	// Output format: "tcp: [1] 10.0.0.1:3260,1 iqn.xxxx.yyyy.1499 (non-flash)"
	cmd := exec.Command("/usr/bin/nsenter", "-t", "1", "-m", "-u", "-i", "-n", "-p", "--", "iscsiadm", "-m", "session")
	logrus.Debugf("findIQNByVolumeID: running nsenter: %v", cmd.Args)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Exit code 21 means no sessions.
		if exitErr, ok := err.(*exec.ExitError); ok {
			if exitErr.ExitCode() == 21 {
				return "", nil
			}
		}
		return "", fmt.Errorf("failed to list sessions: %v", err)
	}

	suffix := fmt.Sprintf(".%s", volID)
	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		// Simple containment check for the IQN suffix.
		// We expect the line to contain "iqn.....<volID> "
		if strings.Contains(line, suffix+" ") || strings.HasSuffix(strings.TrimSpace(line), suffix) || strings.Contains(line, suffix+"(") {
			// Extract just the IQN.
			// Format: "tcp: [ID] IP:Port,TPGT IQN (extra)"
			fields := strings.Fields(line)
			for _, f := range fields {
				if strings.HasPrefix(f, "iqn.") && strings.HasSuffix(f, suffix) {
					return f, nil
				}
			}
		}
	}
	return "", nil
}

func (ns *NodeServer) optimizedIscsiLogout(ctx context.Context, iqn string) error {
	logrus.Infof("Optimized Logout: Logging out %s", iqn)
	t := shellEscape(iqn)
	iscsiCmd := "/usr/sbin/iscsiadm"

	// Command Chain:
	// 1. Logout
	// 2. Delete Node Record (Cleanup)
	cmdScript := fmt.Sprintf(`
		%s -m node -T %s --logout && \
		%s -m node -T %s -o delete
	`, iscsiCmd, t, iscsiCmd, t)

	nsCmd := exec.Command("/usr/bin/nsenter", "-t", "1", "-m", "-u", "-i", "-n", "-p", "--", "sh", "-c", cmdScript)
	logrus.Debugf("Optimized Logout: running nsenter: %v", nsCmd.Args)
	// For debugging, also log a truncated version of the script
	if len(cmdScript) > 300 {
		logrus.Debugf("Optimized Logout: script (truncated): %s", cmdScript[:300])
	} else {
		logrus.Debugf("Optimized Logout: script: %s", cmdScript)
	}
	output, err := nsCmd.CombinedOutput()
	if err != nil {
		outStr := string(output)
		// Ignore "no record found" or "not logged in" errors which imply it's already clean
		if strings.Contains(outStr, "No such file") || strings.Contains(outStr, "no session found") {
			return nil
		}
		logrus.Errorf("Optimized Logout Failed: %s", outStr)
		return fmt.Errorf("logout batch failed: %v", err)
	}
	return nil
}

func (ns *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	stagingPath := req.GetStagingTargetPath()

	logrus.Debugf("NodePublishVolume: targetPath=%s, stagingPath=%s, volID=%s", targetPath, stagingPath, req.GetVolumeId())

	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Target path missing")
	}

	// If stagingPath is empty, attempt to perform an on-demand attach using
	// the PublishContext (used when the CO does not perform stage/unstage).
	if stagingPath == "" {
		publishContext := req.GetPublishContext()
		iqn := publishContext["iqn"]
		portal := publishContext["portal"]
		chapUser := publishContext["chapUser"]
		chapSecret := publishContext["chapSecret"]

		if iqn == "" || portal == "" {
			return nil, status.Error(codes.InvalidArgument, "Staging path missing and publish context incomplete")
		}

		// Perform iSCSI login to attach device on the host
		if err := ns.optimizedIscsiLogin(ctx, portal, iqn, chapUser, chapSecret); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to login iSCSI target: %v", err)
		}

		// Construct expected by-path and wait for it to appear. Perform SCSI
		// rescans periodically to help the kernel discover the new device
		// when the iSCSI login races with device creation.
		portalPath := portal
		if !strings.Contains(portalPath, ":") {
			portalPath = portalPath + ":3260"
		}
		expected := fmt.Sprintf("/dev/disk/by-path/ip-%s-iscsi-%s-lun-%s", portalPath, iqn, "0")

		// Retry loop: give the kernel more time under heavy test load.
		// Increase total wait to 120s and poll every 2s. Request a targeted
		// iscsiadm rescan each iteration and sleep briefly to reduce bursts.
		found := false
		totalWait := getEnvInt("SFCSI_ATTACH_WAIT_SECONDS", 120)
		interval := getEnvInt("SFCSI_ATTACH_POLL_INTERVAL", 2)
		rescanSleepMs := getEnvInt("SFCSI_RESCAN_SLEEP_MS", 250)
		jitterMaxMs := getEnvInt("SFCSI_RESCAN_JITTER_MS", 200)
		for waited := 0; waited < totalWait; waited += interval {
			// First try a targeted iscsiadm rescan for this IQN/portal
			if rescanIscsiTarget(portal, iqn) {
				logrus.Debugf("Rescan request for %s@%s submitted", iqn, portal)
			}
			// Give the kernel a small moment to process the rescan request
			time.Sleep(time.Duration(rescanSleepMs) * time.Millisecond)
			if waitForPath(expected, interval) {
				found = true
				break
			}
			// Add a small random jitter to avoid synchronized bursts
			if jitterMaxMs > 0 {
				time.Sleep(time.Duration(rand.Intn(jitterMaxMs)) * time.Millisecond)
			}
		}
		if !found {
			// Fallback: if iscsi session exists for this volume on the host,
			// do a targeted extra wait/rescan before giving up. This handles
			// cases where iscsiadm reports a session but the kernel device
			// appears slightly later under load.
			volID := req.GetVolumeId()
			if volID != "" {
				if iqnFound, _ := ns.findIQNByVolumeID(volID); iqnFound != "" {
					logrus.Infof("NodePublishVolume: session exists for vol %s (iqn=%s). Performing extra rescan/wait", volID, iqnFound)
					extraWait := getEnvInt("SFCSI_ATTACH_EXTRA_WAIT_SECONDS", 30)
					extraInterval := interval
					extraRescanSleepMs := rescanSleepMs
					extraFound := false
					for ew := 0; ew < extraWait; ew += extraInterval {
						if rescanIscsiTarget(portal, iqn) {
							logrus.Debugf("Extra rescan request for %s@%s submitted", iqn, portal)
						}
						time.Sleep(time.Duration(extraRescanSleepMs) * time.Millisecond)
						if waitForPath(expected, extraInterval) {
							extraFound = true
							break
						}
					}
					if extraFound {
						found = true
					}
				}
			}
			if !found {
				return nil, status.Errorf(codes.DeadlineExceeded, "device path %s did not appear after attach", expected)
			}
		}
		stagingPath = expected
		logrus.Debugf("NodePublishVolume: resolved stagingPath=%s via publishContext", stagingPath)
	}

	isBlock := req.GetVolumeCapability().GetBlock() != nil

	// BLOCK MODE
	if isBlock {
		// Target path must be a file, not a directory.
		dir := filepath.Dir(targetPath)
		if err := os.MkdirAll(dir, 0750); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create target parent dir: %v", err)
		}

		// Create empty file if not exists
		f, err := os.OpenFile(targetPath, os.O_CREATE|os.O_RDWR, 0660)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create block target file: %v", err)
		}
		f.Close()

		// Bind Mount Device -> File
		options := []string{"bind"}
		if req.GetReadonly() {
			options = append(options, "ro")
		}

		logrus.Debugf("NodePublishVolume (Block): Mounting %s -> %s with opts %v", stagingPath, targetPath, options)
		mounter := mount.New("")
		if err := mounter.Mount(stagingPath, targetPath, "", options); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to bind mount block device: %v", err)
		}

		logrus.Debugf("NodePublishVolume: Successfully bound block device %s to %s", stagingPath, targetPath)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	// FILESYSTEM MODE
	// Mkdir target
	if err := os.MkdirAll(targetPath, 0750); err != nil {
		logrus.Errorf("NodePublishVolume: MkdirAll failed: %v", err)
		return nil, status.Errorf(codes.Internal, "NodePublishVolume: MkdirAll failed: %v", err)
	}

	mounter := mount.New("")

	// Check if already mounted
	notMnt, err := mounter.IsLikelyNotMountPoint(targetPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, status.Errorf(codes.Internal, "check mount point: %v", err)
	}
	if !notMnt {
		logrus.Debugf("NodePublishVolume: %s already mounted", targetPath)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	// Prepare Options
	options := []string{"bind"}
	if req.GetReadonly() {
		options = append(options, "ro")
	}
	if m := req.GetVolumeCapability().GetMount(); m != nil {
		options = append(options, m.GetMountFlags()...)
	}

	logrus.Debugf("NodePublishVolume: Mounting %s -> %s with opts %v", stagingPath, targetPath, options)
	if err := mounter.Mount(stagingPath, targetPath, "", options); err != nil {
		logrus.Errorf("NodePublishVolume: mount failed: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to mount: %v", err)
	}

	logrus.Debugf("NodePublishVolume: Successfully bound %s to %s", stagingPath, targetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	logrus.Debugf("NodeUnpublishVolume: targetPath=%s, volID=%s", targetPath, req.GetVolumeId())

	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Target path missing")
	}

	// Unmount targetPath
	cmd := exec.Command("umount", targetPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		outStr := string(out)
		if !strings.Contains(outStr, "not mounted") && !strings.Contains(outStr, "No such file or directory") && !strings.Contains(outStr, "no mount point specified") {
			logrus.Errorf("NodeUnpublishVolume: umount failed: %v, output: %s", err, outStr)
			return nil, status.Errorf(codes.Internal, "failed to unmount target: %v, output: %s", err, outStr)
		}
	}
	logrus.Debugf("NodeUnpublishVolume: Successfully unmounted %s", targetPath)

	// Ensure target path is removed. Use RemoveAll to handle files or non-empty dirs
	if err := os.RemoveAll(targetPath); err != nil {
		if !os.IsNotExist(err) {
			logrus.Errorf("NodeUnpublishVolume: failed to remove target path %s: %v", targetPath, err)
			return nil, status.Errorf(codes.Internal, "failed to remove target path: %v", err)
		}
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *NodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID must be provided")
	}

	volumePath := req.GetVolumePath()
	if volumePath == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume path must be provided")
	}

	logrus.Debugf("NodeExpandVolume: volumePath=%s", volumePath)

	var devicePath string

	// Step 1: Find device from mount point using df (findmnt hangs in some envs)
	cmdFind := exec.Command("df", "--output=source", volumePath)
	outFind, errFind := cmdFind.CombinedOutput()
	if errFind != nil {
		fmt.Printf("NodeExpandVolume: df failed: %v. Proceeding with resize...\n", errFind)
	} else {
		// output format:
		// Filesystem
		// /dev/sda
		lines := strings.Split(strings.TrimSpace(string(outFind)), "\n")
		if len(lines) >= 2 {
			devicePath = strings.TrimSpace(lines[len(lines)-1])
			fmt.Printf("NodeExpandVolume: found device %s for path %s (via df)\n", devicePath, volumePath)

			// Step 2: Rescan Device
			// Handle /dev/sdX
			if strings.HasPrefix(devicePath, "/dev/") {
				deviceName := filepath.Base(devicePath)
				// Check for /sys/class/block/sdX/device/rescan
				rescanFile := fmt.Sprintf("/sys/class/block/%s/device/rescan", deviceName)
				if _, err := os.Stat(rescanFile); err == nil {
					fmt.Printf("NodeExpandVolume: triggering rescan on %s\n", rescanFile)
					// Write "1"
					if err := os.WriteFile(rescanFile, []byte("1"), 0200); err != nil {
						fmt.Printf("NodeExpandVolume: failed to write to rescan file: %v\n", err)
					}
				} else {
					fmt.Printf("NodeExpandVolume: rescan file %s not found. Skipping rescan.\n", rescanFile)
				}
			}
		}
	}

	// Step 3: Resize Filesystem
	// 1. Try resize2fs (ext4)
	// Use device path if available (resize2fs works well with device path even if mounted)
	resizeTarget := volumePath
	if devicePath != "" {
		resizeTarget = devicePath
	}
	cmd := exec.Command("resize2fs", resizeTarget)
	out, err := cmd.CombinedOutput()
	if err == nil {
		fmt.Printf("NodeExpandVolume: resize2fs success on %s\n", resizeTarget)
		return &csi.NodeExpandVolumeResponse{}, nil
	}

	// Check for "No such file or directory" -> likely volume not found on node
	if strings.Contains(string(out), "No such file or directory") {
		return nil, status.Errorf(codes.NotFound, "volume path %s not found", resizeTarget)
	}

	// If resize2fs failed, it might be XFS?
	// xfs_growfs needs the MOUNT POINT, not the device.
	xfsCmd := exec.Command("xfs_growfs", "-d", volumePath)
	xfsOut, xfsErr := xfsCmd.CombinedOutput()
	if xfsErr == nil {
		fmt.Printf("NodeExpandVolume: xfs_growfs success on %s\n", volumePath)
		return &csi.NodeExpandVolumeResponse{}, nil
	}

	// If both failed, log output
	fmt.Printf("NodeExpandVolume: Failed to resize. \nresize2fs err: %v out: %s\nxfs_growfs err: %v out: %s\n", err, string(out), xfsErr, string(xfsOut))

	return nil, status.Errorf(codes.Internal, "failed to resize filesystem: %v", err)
}

// Helper
func waitForPath(path string, seconds int) bool {
	for i := 0; i < seconds; i++ {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(1 * time.Second)
	}
	return false
}

// rescanSCSIHosts triggers a SCSI rescan on all host adapters.
func rescanSCSIHosts() {
	hosts, err := filepath.Glob("/sys/class/scsi_host/host*")
	if err != nil {
		return
	}
	for _, h := range hosts {
		scanFile := filepath.Join(h, "scan")
		// Ignore errors
		_ = os.WriteFile(scanFile, []byte("- - -\n"), 0644)
	}
}

// rescanIscsiTarget performs a selective rescan for the given IQN and portal
// using iscsiadm in node mode with -R. It returns true if the command was
// executed successfully (regardless of whether the device immediately appears).
func rescanIscsiTarget(portal, iqn string) bool {
	iscsiCmd := "/usr/sbin/iscsiadm"
	// Ensure portal includes port if missing
	portalArg := portal
	if !strings.Contains(portalArg, ":") {
		portalArg = portalArg + ":3260"
	}

	// Build command using shell-escaped args
	cmdScript := fmt.Sprintf("%s -m node -T %s -p %s -R", iscsiCmd, shellEscape(iqn), shellEscape(portalArg))
	nsCmd := exec.Command("/usr/bin/nsenter", "-t", "1", "-m", "-u", "-i", "-n", "-p", "--", "sh", "-c", cmdScript)
	output, err := nsCmd.CombinedOutput()
	outStr := string(output)
	if err != nil {
		logrus.Debugf("rescanIscsiTarget failed: %v output=%s", err, outStr)
		return false
	}
	logrus.Debugf("rescanIscsiTarget succeeded: %s", outStr)
	return true
}

func resolveMultipathDevice(devicePath string) string {
	realPath, err := filepath.EvalSymlinks(devicePath)
	if err != nil {
		return devicePath
	}

	// realPath is like /dev/sda
	// Check /sys/block/sda/holders/dm-*
	deviceName := filepath.Base(realPath)
	holdersDir := fmt.Sprintf("/sys/block/%s/holders", deviceName)

	entries, err := os.ReadDir(holdersDir)
	if err != nil {
		return devicePath
	}

	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "dm-") {
			logrus.Infof("Multipath detected: %s resolves to %s (holder of %s). Swapping to /dev/%s", devicePath, entry.Name(), deviceName, entry.Name())
			return fmt.Sprintf("/dev/%s", entry.Name())
		}
	}

	return devicePath
}

func (ns *NodeServer) optimizedIscsiLogin(ctx context.Context, portal, iqn, username, password string) error {
	// Mark attach in progress for this IQN so reaper doesn't remove it
	if iqn != "" {
		ns.markAttachStart(iqn)
		defer ns.markAttachEnd(iqn)
	}
	// Escape strings for shell
	p := shellEscape(portal)
	t := shellEscape(iqn)
	u := shellEscape(username)
	s := shellEscape(password)

	// Construct the massive one-liner to execute on the host.
	// This reduces context switches from ~5 to 1.
	iscsiCmd := "/usr/sbin/iscsiadm"

	// Command Chain:
	// 1. Create Record (Manual, avoids unreliable Discovery "SendTargets")
	// 2. Set Auth Method
	// 3. Set Username
	// 4. Set Password
	// 5. Login
	cmdScript := fmt.Sprintf(`
		%s -m node -T %s -p %s -o new || true && \
		%s -m node -T %s -o update -n node.session.auth.authmethod -v CHAP && \
		%s -m node -T %s -o update -n node.session.auth.username -v %s && \
		%s -m node -T %s -o update -n node.session.auth.password -v %s && \
		%s -m node -T %s --login
	`, iscsiCmd, t, p,
		iscsiCmd, t,
		iscsiCmd, t, u,
		iscsiCmd, t, s,
		iscsiCmd, t)

	// Execute via nsenter
	// Note: We bypass the iscsi-wrapper.sh to invoke nsenter directly with 'sh -c'.
	nsCmd := exec.Command("/usr/bin/nsenter", "-t", "1", "-m", "-u", "-i", "-n", "-p", "--", "sh", "-c", cmdScript)

	logrus.Infof("Optimized Login: Executing batch iscsiadm command via nsenter for %s", iqn)
	logrus.Debugf("Optimized Login: nsenter args: %v", nsCmd.Args)
	if len(cmdScript) > 500 {
		logrus.Debugf("Optimized Login: script (trunc 500): %s", cmdScript[:500])
	} else {
		logrus.Debugf("Optimized Login: script: %s", cmdScript)
	}

	// Acquire login semaphore (if configured) to avoid bursts of concurrent
	// logins that can overwhelm targets or the host iSCSI layer.
	acquireLoginSem()
	defer releaseLoginSem()

	output, err := nsCmd.CombinedOutput()
	if err != nil {
		outStr := string(output)
		// Check for "session already exists" (Exit code 15)
		if strings.Contains(outStr, "already present") || strings.Contains(outStr, "session exists") {
			logrus.Infof("Optimized Login: Session already exists for %s", iqn)
			return nil
		}
		// Log full output for debugging
		logrus.Errorf("Optimized Login Failed: %s", outStr)
		return fmt.Errorf("batch execution failed: %v, output: %v", err, outStr)
	}

	logrus.Infof("Optimized Login: Success for %s", iqn)
	// Small delay to allow the kernel to create SCSI device entries
	// under heavy concurrent attach load. This reduces races where
	// iscsiadm returns before /dev/disk/by-path is created.
	time.Sleep(time.Duration(loginPostSleepMs()) * time.Millisecond)
	return nil
}

func shellEscape(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}

// getEnvInt reads an integer environment variable or returns the provided default.
func getEnvInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

// loginPostSleepMs returns the configured post-login sleep in milliseconds.
func loginPostSleepMs() int {
	return getEnvInt("SFCSI_LOGIN_POST_SLEEP_MS", 1000)
}

var (
	loginSem chan struct{}
)

func initLoginSem() {
	n := getEnvInt("SFCSI_MAX_CONCURRENT_LOGINS", 8)
	if n < 1 {
		n = 1
	}
	loginSem = make(chan struct{}, n)
}

func acquireLoginSem() {
	if loginSem == nil {
		initLoginSem()
	}
	loginSem <- struct{}{}
}

func releaseLoginSem() {
	// Non-blocking safety
	if loginSem == nil {
		return
	}
	<-loginSem
}

func init() {
	initLoginSem()
	// Seed RNG for jitter
	rand.Seed(time.Now().UnixNano())
}

// markAttachStart records that an attach/login is in progress for the given IQN.
func (ns *NodeServer) markAttachStart(iqn string) {
	ns.attachMu.Lock()
	defer ns.attachMu.Unlock()
	if ns.attachInProgress == nil {
		ns.attachInProgress = make(map[string]bool)
	}
	ns.attachInProgress[iqn] = true
}

// markAttachEnd clears the in-progress marker for the IQN.
func (ns *NodeServer) markAttachEnd(iqn string) {
	ns.attachMu.Lock()
	defer ns.attachMu.Unlock()
	if ns.attachInProgress != nil {
		delete(ns.attachInProgress, iqn)
	}
}

// startReaper runs a background loop that finds stale iSCSI sessions with
// no visible device and attempts a safe logout/delete. It rate-limits
// concurrent logouts via reaperMaxConcurrent.
func (ns *NodeServer) startReaper() {
	if !ns.reaperEnabled {
		return
	}
	ticker := time.NewTicker(ns.reaperInterval)
	defer ticker.Stop()

	sem := make(chan struct{}, ns.reaperMaxConcurrent)

	for range ticker.C {
		sessions := ns.scanSessions()

		// First pass: update unseen map and collect eligible candidates
		var eligible []iscsiSession
		for _, s := range sessions {
			iqn := s.IQN
			if iqn == "" {
				continue
			}

			ns.attachMu.Lock()
			inProg := ns.attachInProgress[iqn]
			ns.attachMu.Unlock()
			if inProg {
				continue
			}

			// If device exists, clear any unseen marker
			if ns.deviceExistsForIQN(iqn) {
				ns.unseenMu.Lock()
				delete(ns.unseenWithoutDevice, iqn)
				ns.unseenMu.Unlock()
				continue
			}

			// Not seen: mark first time seen without device
			ns.unseenMu.Lock()
			first, ok := ns.unseenWithoutDevice[iqn]
			if !ok {
				ns.unseenWithoutDevice[iqn] = time.Now()
				ns.unseenMu.Unlock()
				continue
			}
			ns.unseenMu.Unlock()

			// If old enough, add to eligible list
			if time.Since(first) >= time.Duration(ns.reaperStaleSeconds)*time.Second {
				eligible = append(eligible, s)
			}
		}

		// Determine whether to consult controller advisory based on local candidate count
		eligibleCount := len(eligible)
		useAdvisory := ns.reaperUseControllerAdvisory && ns.Controller != nil && ns.Controller.sessionPollerEnabled

		for _, s := range eligible {
			iqn := s.IQN

			// If there are many local candidates, prefer to reap locally without advisory.
			if eligibleCount >= ns.reaperLocalCandidateThreshold {
				useAdvisory = false
			}

			// If advisory is enabled and we should consult it, check controller last-seen
			if useAdvisory {
				if ns.Controller != nil {
					if lastSeen, ok := ns.Controller.GetSessionLastSeen(iqn); ok {
						// If controller reports activity within cutoff, skip logout
						if time.Since(lastSeen) <= time.Duration(ns.reaperAdvisoryCutoffSeconds)*time.Second {
							logrus.Debugf("Reaper: skipping logout for %s due to controller advisory lastSeen=%v", iqn, lastSeen)
							continue
						}
					}
				}
			}

			// Attempt logout with concurrency limit
			select {
			case sem <- struct{}{}:
				go func(iqn string) {
					defer func() { <-sem }()
					logrus.Infof("Reaper: attempting logout for stale iqn=%s", iqn)
					if err := ns.optimizedIscsiLogout(context.Background(), iqn); err != nil {
						logrus.Warnf("Reaper logout failed for %s: %v", iqn, err)
						return
					}
					ns.unseenMu.Lock()
					delete(ns.unseenWithoutDevice, iqn)
					ns.unseenMu.Unlock()
					logrus.Infof("Reaper: logout successful for %s", iqn)
				}(iqn)
			default:
				logrus.Debugf("Reaper: concurrency limit reached, skipping logout for %s", iqn)
			}
		}
	}
}

type iscsiSession struct {
	IQN    string
	Portal string
}

// scanSessions parses 'iscsiadm -m session' output to return IQN/Portal pairs.
func (ns *NodeServer) scanSessions() []iscsiSession {
	cmd := exec.Command("/usr/bin/nsenter", "-t", "1", "-m", "-u", "-i", "-n", "-p", "--", "iscsiadm", "-m", "session")
	out, err := cmd.CombinedOutput()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			if exitErr.ExitCode() == 21 {
				return nil
			}
		}
		logrus.Debugf("scanSessions: iscsiadm error: %v output=%s", err, string(out))
		return nil
	}

	lines := strings.Split(string(out), "\n")
	var sessions []iscsiSession
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		// Extract IQN and portal from fields
		fields := strings.Fields(l)
		var iqn, portal string
		for _, f := range fields {
			if strings.HasPrefix(f, "iqn.") {
				iqn = f
			}
			// portal looks like 10.244.0.5:3260,1 or IP:port,TPGT
			if strings.Contains(f, ":") && (strings.Contains(f, ",") || strings.Contains(f, ":3260")) {
				// pick the first candidate that looks like IP:port
				portal = strings.TrimSuffix(f, ",1")
			}
		}
		if iqn != "" {
			sessions = append(sessions, iscsiSession{IQN: iqn, Portal: portal})
		}
	}
	return sessions
}

// deviceExistsForIQN returns true if any /dev/disk/by-path entry references the IQN.
func (ns *NodeServer) deviceExistsForIQN(iqn string) bool {
	// glob for by-path entries containing the IQN
	pattern := fmt.Sprintf("/dev/disk/by-path/*%s*", iqn)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return false
	}
	for _, m := range matches {
		if _, err := os.Stat(m); err == nil {
			return true
		}
	}
	return false
}
