# SolidFire CSI Driver Helm Chart

This Helm chart installs the SolidFire CSI Driver on a Kubernetes cluster.

## Prerequisites

- Kubernetes 1.34+
- Helm 3 or 4
- SolidFire (Element OS) Storage Cluster v12

## Installation

1. Create a `values.yaml` file with your SolidFire credentials and configuration:

```yaml
solidfire:
  endpoint: "192.168.1.34"
  username: "admin"
  password: "your-strong-password" # demo password
  defaultTenant: "k0s"

storageClass:
  create: true
  isDefault: true
```

2. Install the chart:

```bash
helm install solidfire-csi ./helm/solidfire-csi -f my-values.yaml -n solidfire-csi --create-namespace
```

If you prefer to generate and review manifests, use `scripts/generate-manifests.sh` to generate manifests in `./deploy/`.

## Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `solidfire.endpoint` | Management IP or Hostname of the SolidFire cluster | `192.168.1.34` |
| `solidfire.username` | SolidFire Cluster Admin Username | `admin` |
| `solidfire.password` | SolidFire Cluster Admin Password | `admin` |
| `solidfire.defaultTenant` | Default Tenant for volume creation | `k0s` |
| `controller.replicas` | Number of controller replicas | `2` |
| `driver.logLevel` | Driver log level | `info` |
| `driver.disableMetrics` | Disable Prometheus metrics endpoint | `true` |

## RBAC

The chart creates a ServiceAccount `solidfire-csi-controller` and binds it to a ClusterRole with necessary permissions for provisioner, attacher, resizer, and snapshotter sidecars.

## Node Setup

The DaemonSet automatically deploys the node plugin to all nodes. Make sure the nodes have `iscsiadm` installed and the iSCSI services running.

## Uninstall

```bash
helm uninstall solidfire-csi -n solidfire-csi
```

## Generate Static Manifests

If you prefer to use static YAML manifests instead of Helm, you can generate them from this chart.
We provide a script to update the manifests in `deploy/`:

```bash
# Run from project root
./scripts/generate-manifests.sh
```

This renders the templates and places them in `deploy/` with the appropriate filenames (`controller.yaml`, `node.yaml`, etc.).
