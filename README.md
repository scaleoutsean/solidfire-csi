# SolidFire CSI Driver

# ---------------------
# A lightweight, multi-tenant, stateless CSI driver for SolidFire.

This community project implements SolidFire CSI driver exactly the way I want it.

If you need a supported and certified CSI driver for SolidFire, just use Trident CSI. If you're after something else, you may consider this one.

There's nothing that prevents you from using both drivers with the same backend, even from same Kubernetes clusters (you'd need to have multipathing in place), and how-to's for moving back and forth (to/from Trident CSI) are available.

## Features

- **Simple:** smaller, simpler, lighter
- **Multi-tenancy:** Uses `StorageClass` parameters/secrets instead of "backends" enabling multiple tenants/clusters.
- **Stateless:** No persistent backend configuration file. Light ephemeral cache.
- **Quotas:** Local limit enforcement for `max_volume_count` and `max_total_capacity` per Tenant (configured in StorageClass).
- **Snapshots & Clones:** Native SolidFire snapshot and clone support.
- **Quality of Service:** Proper QoS support through QoS Policy IDs - never waste IOPS or come up short because you can't retype your PVCs.
- **Correct scheduling:** Pick the exact QoS policy ID you need; no more guesswork with overlapping QoS ranges.
- **Superior manageability:** Enhanced use of SolidFire [volume attributes](https://scaleoutsean.github.io/2024/07/02/solidfire-volume-attributes-from-trident-and-other-apps.html).
- **PVC retyping:** Modify volume's QoS Policy ID to change its performance - great for backup, and on-demand performance upgrades (or downgrades)!
- **Delete and Purge Behavior:** Lets you optionally take advantage of SolidFire volume restore feature to restore data from deleted volumes (requires SolidFire administrator's action before volume is auto-purged (8h)). Purge-on-delete remains as an option. Use Purge in high-churn environments and when you want to make sure deleted volumes cannot get accessed by storage administrators
- **Wider filesystem support:** ext4, XFS, Btrfs
- **Observability:** Automatic "lazy discovery" of backends keeps track of global metrics (Total PVCs, Total Capacity, Total MinIOPS) across all tenants via `/metrics`. Metrics are by default disabled to eliminate the need for network access to SolidFire CSI.
- **High Availability:** Fully stateless controller design supports multiple replicas (Active/Standby) using standard sidecar leader election.
- **Auto-Discard:** Automatically mounts volumes with `discard` enabled. No need to micromanage StorageClass options; blocks are freed immediately for SolidFire efficiency, backup efficiency and SSD health.
- **Dynamic Limits:** Automatically fetches cluster limits (e.g., Max Volume Size, Max Snapshots) to enforce bounds correctly across different SolidFire models (Demo VM vs. Production).
- **Other goodies:** Takes advantage of other SolidFire strengths without bloat or unresonable compromises.

## Requirements

- Kubernetes 1.25+
- iSCSI tools installed on worker nodes (`open-iscsi` or `iscsi-initiator-utils`).
- SolidFire Element OS 12.5+

## Build

Use Go 1.25.

### Local Binary

```bash
go mod tidy
go build -o solidfire-csi ./cmd/solidfire-csi
```

### Docker Image

```bash
docker build -t solidfire-csi:latest .
```

If you use containerd, you can import saved Docker image:

```bash
docker save solidfire-csi:latest | ctr -n k8s.io images import -
```

## Getting started

- Review and deploy YAML files from `deploy/`
- Read [DOCUMENTATION](./DOCUMENTATION.md) with step-by-step instructions

## Snapshot Import

To import an existing SolidFire snapshot into Kubernetes:

1. Locate the Snapshot ID on your SolidFire cluster.
2. Create a `VolumeSnapshotContent` object pointing to that Snapshot ID (`snapshotHandle`).
3. Create a `VolumeSnapshot` object binding to the Content.
4. (Optional) Create a `PersistentVolumeClaim` with `dataSource` set to the Snapshot to restore/clone a new volume.

See `tests/e2e/13-snapshot-import.yaml` for a template.

## Deployment

```
ctr images import solidfire.tar
```

## Configuration & Multi-Tenancy

### Secrets (Credentials)

The driver requires a Kubernetes Secret to store SolidFire credentials and connection details. 
**Crucially, the `tenant` field in the Secret determines the SolidFire Account used for volume provisioning and Access Control (CHAP).**

**Example `deploy/secret.yaml`:**

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: solidfire-secret-tenant1
  namespace: solidfire-csi
type: Opaque
stringData:
  # Management Endpoint with Admin credentials
  endpoint: "https://admin:password@10.10.10.10/json-rpc/11.0" 
  # MANDATORY: Configures which SolidFire Account owns the volumes
  tenant: "tenant1"
  # Optional: Default QoS Policy ID
  default_qos: "1"
```

### StorageClass

The StorageClass binds to a specific Secret. This allows you to support multi-tenancy by creating multiple StorageClasses (e.g., `gold-tenant-a`, `gold-tenant-b`) that point to different Secrets (and thus different SolidFire Accounts).

**Example `deploy/storageclass.yaml`:**

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: solidfire-gold-tenant1
provisioner: csi.solidfire.com
parameters:
  # QoS: Use a Policy ID (recommended) or explicit IOPS
  storage_qos_policy_id: "1"
  
  # Binding to the Secret (Required for all operations)
  csi.storage.k8s.io/provisioner-secret-name: solidfire-secret-tenant1
  csi.storage.k8s.io/provisioner-secret-namespace: solidfire-csi
  csi.storage.k8s.io/controller-publish-secret-name: solidfire-secret-tenant1
  csi.storage.k8s.io/controller-publish-secret-namespace: solidfire-csi
  csi.storage.k8s.io/node-stage-secret-name: solidfire-secret-tenant1
  csi.storage.k8s.io/node-stage-secret-namespace: solidfire-csi
  csi.storage.k8s.io/controller-expand-secret-name: solidfire-secret-tenant1
  csi.storage.k8s.io/controller-expand-secret-namespace: solidfire-csi
```

## Deployment

1. **Install Driver**

   ```bash
   kubectl apply -f deploy/rbac.yaml
   kubectl apply -f deploy/csi-driver.yaml
   kubectl apply -f deploy/secret.yaml     # Customize this first!
   kubectl apply -f deploy/controller.yaml # Modify image for your environment
   kubectl apply -f deploy/node.yaml       # Modify image for your environment
   ```

2. **Create StorageClass**

   Customize `deploy/storageclass.yaml` with your Secret name and apply it.

## k0s / K3s Support

If you are running k0s or k3s, the Kubelet directory is often non-standard (e.g., `/var/lib/k0s/kubelet`).
You must update `deploy/node.yaml` to mount the correct host path to `/var/lib/kubelet` inside the container.

**Example for k0s:**

```yaml
        volumeMounts:
        - name: kubelet-dir
          mountPath: /var/lib/kubelet
          mountPropagation: "Bidirectional"
      volumes:
      - name: kubelet-dir
        hostPath:
          path: /var/lib/k0s/kubelet  <-- Update this line
          type: Directory
```

## Disaster Recovery and Snapshots

### Snapshot Behavior

When creating a Volume from a Snapshot (CSI Clone), the CSI driver automatically sets new Volume Attributes (metadata) based on the *new* PVC's context (e.g., `pvc_name`, `pvc_namespace`, `pv_name`, `fstype`). This generally ignores the attributes stored in the original snapshot, ensuring the new volume correctly reflects its current Kubernetes environment.

### In-Place Restore / Rollback

If you perform an In-Place Restore (Rollback) using SolidFire API or scripts (outside of CSI):

1. Volume data: The volume data is reverted to the state at the time of the snapshot.
2. Volume Attributes (KVs): The volume attributes are **NOT** automatically reverted. The volume retains its *current* attributes.

If you need to restore or correct Volume Attributes (e.g., after failing over to a DR cluster where PVC names might differ, or simply to match the snapshot's original metadata), you must do so manually using the `ModifyVolume` API.

**Example `ModifyVolume` Request to set Attributes:**

```json
{
  "method": "ModifyVolume",
  "params": {
    "volumeID": 123,
    "attributes": {
       "fstype": "xfs",
       "pvc_name": "restored-pvc-name",
       "pvc_namespace": "production",
       "delete_behavior": "purge",
       "pv_name": "pvc-uuid-from-k8s..."
    }
  },
  "id": 1
}
```

## Troubleshooting

- SolidFire 12.5 or earlier requires `node.session.auth.chap_algs=MD5` (/etc/iscsi/iscsid.conf on workers). 12.7 supports other ciphers.
- Multipathing is discouraged. Use LACP for redundancy as usual with SolidFire (single fabric).

### Runtime tuning (iSCSI attach/rescan)

The Node DaemonSet exposes several environment variables to tune attach and rescan behavior when running under high churn (for CI or heavy test workloads):

- `SFCSI_ATTACH_WAIT_SECONDS` (default `120`): total seconds to wait for `/dev/disk/by-path` to appear after iSCSI login.
- `SFCSI_ATTACH_POLL_INTERVAL` (default `2`): seconds between polls for the device path.
- `SFCSI_LOGIN_POST_SLEEP_MS` (default `1000`): milliseconds to sleep after a successful `iscsiadm --login` to give the kernel a moment to create SCSI device nodes.
- `SFCSI_MAX_CONCURRENT_LOGINS` (default `8`): maximum concurrent iSCSI login operations performed by the driver (semaphore throttle).
- `SFCSI_RESCAN_SLEEP_MS` (default `250`): milliseconds to wait after issuing a targeted `iscsiadm -m node -T <iqn> -p <portal> -R` rescan.

- `SFCSI_RESCAN_JITTER_MS` (default `200`): maximum random jitter (ms) added between rescan attempts to avoid synchronized bursts.
- `SFCSI_ATTACH_EXTRA_WAIT_SECONDS` (default `30`): extra wait performed only when an iSCSI session for the volume is observed but the device path didn't appear within the main timeout.

These defaults are conservative for reliability during stress tests. To tune for your environment, edit `deploy/node.yaml` and set the environment variables on the `solidfire-csi-driver` container, then `kubectl rollout restart daemonset/solidfire-csi-node`.

## Reaper / Controller Session Advisory

The driver includes an opt-in Node "reaper" that safely logs out stale iSCSI sessions which have no visible local device. To reduce accidental logouts during cluster activity, a Controller-side session poller can provide an advisory `last-seen` timestamp per target IQN. The Node reaper uses this advisory as a sanity check when deciding whether to logout a session.

Key environment variables:

- `SFCSI_ENABLE_REAPER` (default `false`) — enable the Node reaper background task.
- `SFCSI_REAPER_STALE_SECONDS` (default `60`) — how long a session must be seen without a device before becoming eligible for reaping.
- `SFCSI_REAPER_MAX_CONCURRENT` (default `2`) — maximum concurrent reaper logout operations per node.
- `SFCSI_REAPER_USE_CONTROLLER_ADVISORY` (default `false`) — when `true`, consult the Controller's advisory map before logging out a session.
- `SFCSI_REAPER_ADVISORY_CUTOFF_SECONDS` (default `30`) — if the Controller reports activity within this many seconds, the reaper will skip logout for that IQN.
- `SFCSI_REAPER_ADVISORY_LOCAL_CANDIDATE_THRESHOLD` (default `8`) — when the number of local eligible candidates is greater-or-equal to this threshold, the reaper will *not* consult the Controller and will proceed locally (avoids blocking large-scale cleanup).

Controller poller envs (controller Deployment):

- `SFCSI_ENABLE_SESSION_POLLER` (default `false`) — enable periodic polling of SolidFire `ListISCSISessions` to populate the advisory map.
- `SFCSI_SESSION_POLLER_INTERVAL_SECONDS` (default `30`) — poll interval for the Controller session poller.

Recommended strategy:

- Keep the reaper disabled by default in production. Enable during CI, heavy test runs, or when reclaiming stale sessions on test clusters.
- Use the Controller advisory (`SFCSI_REAPER_USE_CONTROLLER_ADVISORY=true`) in small clusters or when you want extra safety — the `LOCAL_CANDIDATE_THRESHOLD` prevents the advisory from blocking large-scale cleanup.

To enable these for quick testing, set the envs in `deploy/controller.yaml` and `deploy/node.yaml` and `kubectl rollout restart` the affected pods.

## Acknowledgement

- John Griffith for vision and wisdom in all matters Cinder and CSI, `solidfire-go` and `solidfire-sdk` (which this CSI driver builds upon) available under the Apache 2.0 license
- Some of the SolidFire driver source code from NetApp Trident project (Apache 2.0)

## SolidFire CSI contributors

- scaleoutsean (Sean) - maintainer
- AI coding assistants

## License

Apache 2.0

## Scheduling the Controller onto Control-Plane Nodes

In multi-node clusters you may prefer to keep the controller deployment on
control-plane (control-plane-only) nodes and run the privileged Node
DaemonSet on worker nodes. This prevents unprivileged controller pods from
attempting host-level operations (e.g., `nsenter`) and aligns with the
usual CSI split between control-plane (controller) and data-plane (node).

The `deploy/controller.yaml` includes commented examples you can enable:

- Use the `nodeSelector` to pin the controller to control-plane nodes:

  nodeSelector:
    node-role.kubernetes.io/control-plane: ""

- Or use `affinity`/`nodeAffinity` for richer placement rules (example in
  the YAML). Also ensure `tolerations` for `node-role.kubernetes.io/control-plane`
  are present so the Deployment can be scheduled onto tainted control-plane nodes.

Notes:
- Leave the examples commented in single-node clusters — they will prevent
  the controller from scheduling if no control-plane nodes are available.
- For CI and csi-sanity runs, run node-focused tests against the privileged
  `DaemonSet` pods and controller-focused tests against the controller
  Deployment.

## Observability and monitoring

By default, metrics (Prometheus-style, read-only Web exporter) are off because:

- that makes potential attack surface smaller
- SolidFire Collector already collects everything SolidFire CSI does, and then some. It gathers (SolidFire CSI-set) Volume Attributes, too! Get it at [SFC](https://github.com/scaleoutsean/sfc).
- SolidFire Exporter (for Prometheus) is a Go-based collector that can run in any namespace using a "read-only" SolidFire cluster account. Get it [here](https://github.com/mjavier2k/solidfire-exporter). It may require some extra work on Prometheus to cross-reference volume IDs from Kubernetes, but it can do 10x more than basic CSI monitor tools

