# E2E Sanity Tests

This directory contains simple manifests and a script to verify the core functionality of the SolidFire CSI driver.

## Prerequisites

- Working Kubernetes Cluster
- SolidFire CSI Driver installed and running
- `kubectl` configured
- `StorageClass`es named `solidfire-bronze` (or update the test YAMLs)

## Workflow

The `run-e2e.ps1` script performs the following:
1. Creates a PVC (`1Gi`).
2. Creates a Pod that mounts it and writes "Hello SolidFire".
3. Takes a VolumeSnapshot of the PVC.
4. Restores (Clones) the Snapshot to a new PVC (`test-restore-pvc`).
5. Expands the original PVC to `2Gi`.

## Usage

Run the PowerShell script:
```powershell
$env:nmspc="demo-ns"
./run-e2e.ps1
# Clean up when done to remove the namespace:
# kubectl delete ns $env:nmspc
```

Or apply manually:

```bash
kubectl apply -f 01-pvc.yaml
kubectl apply -f 02-pod.yaml
# ... verify ...
kubectl apply -f 03-snapshotclass.yaml
kubectl apply -f 04-snapshot.yaml
kubectl apply -f 05-restore-pvc.yaml
```

## Group Snapshot Test

**Prerequisite**: Ensure `VolumeGroupSnapshot` CRDs and external-snapshotter controller are installed.

1. Create Multi-Volume App:
   ```bash
   kubectl apply -f 06-pvc-group.yaml
   ```
2. Create Group Snapshot Class:
   ```bash
   kubectl apply -f 07-group-snapshotclass.yaml
   ```
3. Take Group Snapshot:
   ```bash
   kubectl apply -f 08-group-snapshot.yaml
   ```
4. Verify:
   ```bash
   kubectl get volumegroupsnapshot
   kubectl get volumegroupsnapshotcontent
   ```
