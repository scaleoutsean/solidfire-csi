# SolidFire CSI Driver Documentation

## Overview

The SolidFire CSI driver provides persistent storage for Kubernetes clusters backed by NetApp SolidFire storage systems. This driver emphasizes simplicity, transparency, and standard Kubernetes workflows.

## Installation

### Using Helm (Recommended)

To install the driver using the included Helm chart:

1. Create a `my-values.yaml` file with your SolidFire credentials:
```yaml
solidfire:
  endpoint: "192.168.1.34"
  username: "admin"
  password: "your-password"
  defaultTenant: "k0s"
storageClass:
  create: true
  name: solidfire-bronze
  isDefault: false
  reclaimPolicy: Delete
  allowVolumeExpansion: true
  volumeBindingMode: Immediate
  parameters:
    storage_qos_policy_id: "56"
```

**Note:**
- See the `./helmvalues.yaml` for all available options. `storage_qos_policy_id` defaults to `1`, so if you don't have ... one, change it to whatever QoS Policy ID you want to use. Or use `create: false` and create your own SCs later.
- In SolidFire CSI, it is Storage Class tenants that enable storage-side multi-tenancy. 

For a comparison, Trident CSI uses a single tenant by default, and multi-tenancy can be realized by creating multiple Trident "backends" (and storage classes that use them), so there's one tenant per SolidFire cluster.

SolidFire CSI does not have "backends"; it defaults to `defaultTenant` and overrides are available on a per-Storage Class basis.

1. Install or upgrade the chart:

Use your customized values YAML:

```bash
helm upgrade --install solidfire-csi ./helm/solidfire-csi -f my-values.yaml -n solidfire-csi --create-namespace
# to upgrade, drop --create-namespace

```

You may remove the custom values file - or at least the password value - if you don't plan to reuse it.

### Using manifest files from source (optional)

#### Build and deploy from source

Use Go 1.26 (see `./go.mod`).

```bash
go mod tidy
go build -o solidfire-csi ./cmd/solidfire-csi
```

Build container image:

```bash
docker build -t solidfire-csi:latest .
```

If you use containerd, you can import saved Docker image:

```bash
docker save solidfire-csi:latest | ctr -n k8s.io images import -
```

#### Deploy from manifest files

- **Recommended:** use the Helm chart in `./helm/solidfire-csi/`. If installing on a micro-distribution such as k0s or k3s, customize `node.kubeletDir` in your Helm chart values file.
- To deploy manually, use `scripts/generate-manifests.sh` to generate manifests in the deploy subdirectory and install as per below

1. **Install Driver**

```bash
kubectl apply -f deploy/rbac.yaml
kubectl apply -f deploy/csi-driver.yaml
kubectl apply -f deploy/secret.yaml     # Customize this first!
kubectl apply -f deploy/controller.yaml # Modify image for your environment
kubectl apply -f deploy/node.yaml       # Modify image for your environment
```

2. **Create StorageClass**

If installing "manually", make sure generated `deploy/storageclass.yaml` has your Secret.

Additional examples of YAML files may be found in `./tests/e2e/`.

#### k0, K3s, MicroK8s Support

If you are running k0s or k3s, the Kubelet directory is often non-standard (e.g., `/var/lib/k0s/kubelet`).

This is addressed in the Helm chart, but if deploying manually, check and update `deploy/node.yaml` to mount the correct host path to `/var/lib/kubelet` inside the container.

**Example for k0s:**

```yaml
volumeMounts:
  - name: kubelet-dir
    mountPath: /var/lib/kubelet
    mountPropagation: "Bidirectional"
volumes:
  - name: kubelet-dir
    hostPath:
    path: /var/lib/k0s/kubelet  # <-- Update this line
    type: Directory
```

## Snapshot Import

To import an existing SolidFire snapshot into Kubernetes:

1. Locate the Snapshot ID on your SolidFire cluster.
2. Create a `VolumeSnapshotContent` object pointing to that Snapshot ID (`snapshotHandle`).
3. Create a `VolumeSnapshot` object binding to the Content.
4. (Optional) Create a `PersistentVolumeClaim` with `dataSource` set to the Snapshot to restore/clone a new volume.

See `tests/e2e/13-snapshot-import.yaml` for a template.

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
  endpoint: "https://admin:password@10.10.10.10/json-rpc/12.0" 
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

### Options for Replication and Disaster Recovery

Because SolidFire CSI deals with volume IDs and has no "backend", "flipping" read-write state of volumes paired for replication is easy. 

If your replication is simple (all volumes on source)

- [Longhorny](https://github.com/scaleoutsean/longhorny) lets you change direction of replication of paired clusters (for all replicated volumes at once)
  - If you use several Kubernetes clusters with one SolidFire, you likely have several tenants. Use own script - or modify Longhorny, or ask for a "per tenant" feature in Longorny in Longorhy's issues - to change direction of replication for individual tenant(s)
  - I keep Kubernetes-on-SolidFire failover- and failback-related instructions in the [Kubefire](https://github.com/scaleoutsean/kubefire) repository. If you need a scenario not described here or there, let me know in Kubefire issues
- [SolidFire Collector v2](https://github.com/scaleoutsean/sfc) lets you monitor replication status, pairings and more

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

By default, metrics (Prometheus-style, read-only Web exporter) are disabled because:

- That makes potential attack surface smaller
- SolidFire Collector (SFC) already collects everything SolidFire CSI does, and then some. It gathers (SolidFire CSI-set) Volume Attributes, too! Get it at [SFC](https://github.com/scaleoutsean/sfc).
- SolidFire Exporter (for Prometheus) is a Go-based collector that can run in any namespace using a "read-only" SolidFire cluster account. Get it [here](https://github.com/mjavier2k/solidfire-exporter). It may require some extra work on Prometheus to cross-reference volume IDs from Kubernetes, but it can do 10x more than basic CSI monitor tools

## Feature Guides

### Static Provisioning & Volume Import

The driver supports "importing" existing SolidFire volumes into Kubernetes (Static Provisioning) without complex CRDs or proprietary tools. This is achieved by creating a standard `PersistentVolume` (PV) that references the backend Volume ID.

**Key Concepts:**
- **Arbitrary Naming**: The Kubernetes PV name is independent of the SolidFire backend volume name. You can name your PV `pv-production-db` even if the backend volume is named `vol-715-uuid-legacy`.
- **Driver Logic**: The CSI driver uses the `volumeHandle` field in the PV spec to locate the volume. It ignores the PV name (volume name on SolidFire).
- **Metadata Management**: When importing, the driver **does not** automatically sync metadata (like filesystem type) from the backend. You must **correctly** specify these in the PV definition and do it.

**Example: Importing Volume 715 as XFS:**

You may take a snapshot on SolidFire before you try, to avoid losing data in case of a mistake.

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: pv-import-715 # Name for Kubernetes
spec:
  accessModes:
  - ReadWriteOnce
  capacity:
    storage: 10Gi
  csi:
    driver: csi.solidfire.com
    volumeHandle: "715" # REAL Backend Volume ID
    fsType: xfs         # CRITICAL: Must match desired/existing filesystem to avoid formatting with a wrong FS
    # ... secrets ...
  persistentVolumeReclaimPolicy: Retain # Recommended for imports to prevent accidental deletion
```

### Filesystem Type Handling (fstype)

Control over the filesystem (ext4, xfs, etc.) is handled at two distinct layers:

**A. Dynamic Provisioning (New Volumes)**

Controlled by the `StorageClass`.
- The driver reads `parameters.csi.storage.k8s.io/fstype` (or `fstype`) from the StorageClass.
- It saves this preference into the SolidFire Volume Attributes (metadata) at creation time.
- Default: `ext4` if unspecified.

**B. Static Provisioning (Imported Volumes)**

Controlled by the `PersistentVolume`.
- The driver reads `spec.csi.fsType` from the PV YAML.
- **Warning**: If omitted, the Node driver defaults to `ext4`. If you import a raw volume intended for XFS without specifying `fsType: xfs`, it may be formatted as ext4.

### Volume Permissions & Sidecars

The CSI driver mounts and formats volumes *before* any containers start.
- **InitContainers**: Can safely assume the volume is mounted at `/data` (or target path).
- **Probing**: Since the volume is mounted, InitContainers can perform `chown`, `chmod`, or data seeding operations safely.

### Custom filesystems or volume managers

Request SolidFire CSI to create raw `Block` devices and use a sidecar to create constructs on top of that (LVM, Zpool,...).
```yaml
volumes:
  - name: disk1
    persistentVolumeClaim:
      claimName: pvc-1  # volumeMode: Block
  - name: disk2
    persistentVolumeClaim:
      claimName: pvc-2  # volumeMode: Block
```

### Purge vs. Delete

`delete_behavior` in Storage Class definition refers to SolidFire-side behavior after a volume is deleted from Kubernetes. Default: `purge` (same as Trident CSI and different from native SolidFire behavior (`delete`).

Trident CSI uses `purge`, likely in order to avoid hitting volume count limit in high churn environments. 

SolidFire CSI defaults to `purge` as well because that is safer in high-churn environments. You may configure `delete` for Storage Classes with "pet" volumes. 

`delete` helps mitigate fat-finger behavior, especially for volumes with the retention policy `Retain`.

**Note:**
- if using one global "tenant" (default: off), your deleted PV could be recovered by a SolidFire cluster administrator which may be good or bad for you
- if you run a high-churn workload on a Storage Class that uses SolidFire Recycle Bin, you may max out the maximum volumes (as Recycle Bin could end up having hundreds of volumes), so do not enable this "just in case" on non-"pet" volumes

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

### Reaper / Controller Session Advisory

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

## Evaluation guide

This guide uses pre-created examples from this repository for faster evaluation.

It assumes your nodes and Kubernetes are ready to go and iSCSI already functioning (test from host using a temporary SolidFire tenant account and volume, both of which can be cleaned after a successful test).

If you can access the NetApp Support site, you may download a demo VM ("Element Demo VM") that can be used for all these examples. It requires VMware ESXi (on bare metal or nested (KVM)).

### Deploy

SolidFire CSI is stateless. Unless you have k0s or some other Kubernetes distribution with non-standard paths, you can deploy immediately. 

It is suggested to deploy with Helm 4, (see `./helm/solidfire-csi/README.md`) but you can do it manually as well using the steps below:
- Read the Helm chart README to generate manifest files
- Deploy individual manifest files below

```sh
# create namespace "solidfire-csi"
kubectl apply -f deploy/namespace.yaml
# deploy RBAC
kubectl apply -f rbac.yaml
# deploy CSI Controller
kubectl apply -f deploy/controller.yaml
# deploy CSI Node
kubectl apply -f deploy/node.yaml
# verify 
kubectl get pods -n solidfire-csi
```

SolidFire CSI stores key backend configuration details in two places:
- Secrets: SolidFire cluster MVIP and administrator's credentials. An optional "global" tenant (may be overriden by Storage Class settings) may be specified.
- Storage Classes: SolidFire tenant name (can be one or one per each storage class). If you use Storage QoS Policies, provide a Policy ID as well or otherwise SolidFire defaults will be used for new volumes.

In both cases, it is recomended to **not** use an existing tenant. If you don't have an account named `k0s`, you may simply leave it in place. Then, assuming you've made appropriate edits:

```sh
kubectl apply -f deploy/secret.yaml
kubectl apply -f deploy/storageclass-bronze.yaml
kubectl apply -f deploy/storageclass-purge.yaml
kubectl get storageclass
```

### Logs

Assuming SolidFire CSI is deployed to the namespace `solidfire-csi`, you're supposed to see two controllers (Active/Standby) and at least one node (if there's just one worker):

```sh
$ kubectl get pods -n solidfire-csi
NAME                                        READY   STATUS    RESTARTS      AGE
solidfire-csi-controller-688c544bb7-52sdf   5/5     Running   0             31h
solidfire-csi-controller-688c544bb7-56rrj   5/5     Running   0             31h
solidfire-csi-node-zhbcr                    2/2     Running   2 (32h ago)   32h
```

To see a controller's log: `kubectl log <pod-name> -n solidfire-csi`.

If you see something like `lock is held by 1770360814536-297-csi-solidfire-com and has not yet expired`, that's not the active CSI controller pod, so look at the other one.

To see a node's log, use the same command `kubectl log <pod-name> -n solidfire-csi` with the CSI Node pod name.

### PVC and Pod

**NOTE:** this walk-through uses the default namespace. Presumably you're not doing this this on a production cluster. Change examples to use another namespace if you need to.

Test PVC creation.

```sh
kubectl apply -f ./tests/e2e/01-pvc.yaml
kubectl get pvc
kubectl get pv
```

You may also inspect if the volume on SolidFire. This curl comand lists all active volume. Remember to change the credentials and MVIP.

```sh
PASS="admin" # example
MVIP="192.168.1.34" # example
curl -s -k -H "Content-Type: application/json" \
  -d '{"id":1, "jsonrpc":"2.0", "method": "ListActiveVolumes"' \
  https://admin:${PASS}$@${MVIP}/json-rpc/12.5/ | jq '.result'
```

Deploy Pod that uses the PVC:

```sh
kubectl apply -f ./tests/e2e/02-pod.yaml
kubectl get pods
kubectl describe pod test-pod
kubectl exec test-pod -- cat /mnt/data/hello.txt
```

### Snapshots

Install CRDs for single and group snapshots.

Online:

```sh
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/refs/heads/master/client/config/crd/groupsnapshot.storage.k8s.io_volumegroupsnapshots.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/refs/heads/master/client/config/crd/groupsnapshot.storage.k8s.io_volumegroupsnapshotcontents.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/refs/heads/master/client/config/crd/groupsnapshot.storage.k8s.io_volumegroupsnapshotclasses.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/refs/heads/master/client/config/crd/snapshot.storage.k8s.io_volumesnapshots.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/refs/heads/master/client/config/crd/snapshot.storage.k8s.io_volumesnapshotcontents.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/refs/heads/master/client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml
```

Offline:

```sh
git clone https://github.com/kubernetes-csi/external-snapshotter /tmp/
for CRD in `ls /tmp/external-snapshotter/client/config/crd/snapshot*.yaml`; do kubectl apply -f ${CRD}; done
for CRD in `ls /tmp/external-snapshotter/client/config/crd/groupsnapshot*.yaml`; do kubectl apply -f ${CRD}; done
```

Follow the guide in `./tests/e2e/README.md`.

## Troubleshooting

- SolidFire 12.5 or earlier requires `node.session.auth.chap_algs=MD5` (/etc/iscsi/iscsid.conf on workers). 12.7 supports other ciphers.
- Multipathing is discouraged, but available. It is recommended to use LACP on both clients and SolidFire (as usual) for redundancy which gives you single storage fabric and removes Device Mapper from the equation (you may uninstall it from workers if they don't need it for other storage, or blacklist SolidFire in multipath.conf if you must use Device Mapper for other storage)

### Kubelet directory

If you're on k0s or other micro-distribution, maybe you've forgotten to adjust kubelet directory when installing.

```yaml
node:
  kubeletDir: /var/lib/kubelet # Default, change this for k0s (or redeploy k0s with the standard path)
```

### Cleanup & Troubleshooting

- **Stuck PVs (Released state)**: If `delete pv` hangs, check for "zombie" `VolumeAttachment` objects.
  - `kubectl get volumeattachments`
  - Remove finalizers from stuck attachments to force cleanup.
- **Credentials**: CHAP secrets are visible in `VolumeAttachment` objects. This is standard CSI behavior but requires restricting access to `storage.k8s.io` resources via RBAC.

Examples:

- Remove finalizer for a dangling snapshot:

```sh
kubectl patch volumesnapshot test-snapshot -p '{"metadata":{"finalizers":[]}}' --type=merge && kubectl patch volumesnapshotcontent snapcontent-a757feae-4e1a-4364-abb2-bc843a037ffc -p '{"metadata":{"finalizers":[]}}' --type=merge
```
