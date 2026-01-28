# SolidFire CSI Driver
# ---------------------
# A lightweight, multi-tenant, stateless CSI driver for SolidFire.

## Features
- **Multi-tenancy:** Uses `StorageClass` parameters/secrets for connection, enabling multiple tenants/clusters without restarting the driver.
- **Stateless:** No persistent backend configuration file.
- **Direct iSCSI:** Connects directly using standard iSCSI on the node.
- **Quotas:** Local limit enforcement for `max_volume_count` and `max_total_capacity` per Tenant (configured in StorageClass).
- **Snapshots & Clones:** Native SolidFire snapshot and clone support.

## Requirements
- Kubernetes 1.18+
- iSCSI tools installed on worker nodes (`open-iscsi` or `iscsi-initiator-utils`)
- SolidFire Element OS 9.0+

## Build

### Local Binary
```bash
go mod tidy
go build -o solidfire-csi ./cmd/solidfire-csi
```

### Docker Image
```bash
docker build -t solidfire-csi:latest .
```

## Deployment

1. **Install Driver**
   ```bash
   kubectl apply -f deploy/rbac.yaml
   kubectl apply -f deploy/csi-driver.yaml
   kubectl apply -f deploy/secret.yaml  # Customize this first!
   kubectl apply -f deploy/controller.yaml
   kubectl apply -f deploy/node.yaml
   ```

2. **Create StorageClass**
   See `deploy/storageclass.yaml` for example.

   ```yaml
   platform: solidfire
   parameters:
     endpoint: "https://10.10.10.10/json-rpc/11.0" # Management Endpoint
     tenant: "tenantname"
     quota_max_volume_count: "10"
     quota_max_total_capacity: "1099511627776" # Bytes (1TiB)
   ```

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
1. **Data Blocks**: The volume data is reverted to the state at the time of the snapshot.
2. **Volume Attributes (KVs)**: The volume attributes are **NOT** automatically reverted. The volume retains its *current* attributes.

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

