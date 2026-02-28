# SolidFire CSI Driver Documentation

## Overview

The SolidFire CSI driver provides persistent storage for Kubernetes clusters backed by NetApp SolidFire storage systems. This driver emphasizes simplicity, transparency, and standard Kubernetes workflows.

## Installation

### Using Helm (Recommended)

To install the driver using the included Helm chart:

1. Create a `values.yaml` file with your SolidFire credentials:
   ```yaml
   solidfire:
     endpoint: "192.168.1.34"
     username: "admin"
     password: "your-password"
     defaultTenant: "k0s"
   ```

2. Install the chart:
   ```bash
   helm install solidfire-csi ./helm/solidfire-csi -f values.yaml -n solidfire-csi --create-namespace
   ```

## Feature Guides

### 1. Static Provisioning & Volume Import

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

### 2. Filesystem Type Handling (fstype)

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

### 3. Volume Permissions & Sidecars

The CSI driver mounts and formats volumes *before* any containers start.
- **InitContainers**: Can safely assume the volume is mounted at `/data` (or target path).
- **Probing**: Since the volume is mounted, InitContainers can perform `chown`, `chmod`, or data seeding operations safely.

### 4. Cleanup & Troubleshooting

- **Stuck PVs (Released state)**: If `delete pv` hangs, check for "zombie" `VolumeAttachment` objects.
  - `kubectl get volumeattachments`
  - Remove finalizers from stuck attachments to force cleanup.
- **Credentials**: CHAP secrets are visible in `VolumeAttachment` objects. This is standard CSI behavior but requires restricting access to `storage.k8s.io` resources via RBAC.

Examples:

- Remove finalizer for a dangling snapshot:

```sh
kubectl patch volumesnapshot test-snapshot -p '{"metadata":{"finalizers":[]}}' --type=merge && kubectl patch volumesnapshotcontent snapcontent-a757feae-4e1a-4364-abb2-bc843a037ffc -p '{"metadata":{"finalizers":[]}}' --type=merge
```

### 5. Custom filesystems or volume managers

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

### 6. Purge vs. Delete

`delete_behavior` in Storage Class definition refers to SolidFire-side behavior after a volume is deleted from Kubernetes. Default: `purge` (same as Trident CSI and different from native SolidFire behavior (`delete`).

Trident CSI uses `purge`, likely in order to avoid hitting volume count limit in high churn environments. 

SolidFire CSI defaults to `purge` as well because that is safer in high-churn environments. You may configure `delete` for Storage Classes with "pet" volumes. 

`delete` helps mitigate fat-finger behavior, especially for volumes with the retention policy `Retain`.

**Note:**
- if using one global "tenant" (default: off), your deleted PV could be recovered by a SolidFire cluster administrator which may be good or bad for you
- if you run a high-churn workload on a Storage Class that uses SolidFire Recycle Bin, you may max out the maximum volumes (as Recycle Bin could end up having hundreds of volumes), so do not enable this "just in case" on non-"pet" volumes

## Evaluation guide for SolidFire CSI

This guide uses pre-created examples from this repository for faster evaluation.

It assumes your nodes and Kubernetes are ready to go and iSCSI already functioning (test from host using a temporary SolidFire tenant account and volume, both of which can be cleaned after a successful test).

If you can access the NetApp Support site, you may download a demo VM ("Element Demo VM") that can be used for all these examples. It requires VMware ESXi (on bare metal or nested (KVM)).

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

To see a node's log, use the same approach.

### Deploy

SolidFire CSI is stateless. Unless you have k3s or some other Kubernetes distribution with non-standard paths, you can deploy immediately. Non-standard paths or other customizations may be made, of course.

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
curl -s -k -H "Content-Type: application/json" \
  -d '{"id":1, "jsonrpc":"2.0", "method": "ListActiveVolumes"' \
  https://admin:admin@192.168.1.34/json-rpc/12.5/ | jq '.result'
```

Start a Pod that uses the PVC

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


