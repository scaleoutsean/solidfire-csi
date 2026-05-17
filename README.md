# SolidFire CSI Driver

[![GHCR Build and Push](https://github.com/scaleoutsean/solidfire-csi/actions/workflows/ghcr-publish.yml/badge.svg)](https://github.com/scaleoutsean/solidfire-csi/actions/workflows/ghcr-publish.yml)

This community project implements an optimized SolidFire CSI driver for NetApp SolidFire storage clusters.

How to choose?

- Need a supported and certified CSI driver for SolidFire: Trident CSI
- Kubernetes on OpenStack: Cinder CSI with SolidFire Cinder driver
- Other: **SolidFire CSI**

There's nothing that prevents you from using SolidFire CSI with another CSI driver for SolidFire, even from the same Kubernetes cluster. Cinder CSI and Trident CSI users: you'd need to have multipath tools (Trident CSI forces that requirement on SolidFire users). Otherwise, SolidFire CSI works fine (and in fact better) with LACP and without `multipath-tools`.

SolidFire CSI is very easy to move to/from without recreating data, because it's a simple, stateless CSI driver.

## Features

- **Simple:** smaller, simpler, lighter
- **Multi-tenancy:** Uses `StorageClass` parameters/secrets instead of "backends", enabling multiple tenants/clusters.
- **Stateless:** No persistent backend configuration file. We use light, ephemeral caching.
- **Quotas:** Local limit enforcement for `max_volume_count` and `max_total_capacity` per Tenant (configured in StorageClass).
- **Snapshots & Clones:** Native SolidFire snapshot and clone support.
- **Quality of Service:** Proper QoS support through QoS Policy IDs - never waste IOPS or come up short because you can't retype your PVCs.
- **Predictable and correct volume scheduling:** Pick the exact QoS policy ID you want for a PVC; no more guesswork with overlapping QoS ranges.
- **Superior manageability:** Enhanced use of SolidFire [volume attributes](https://scaleoutsean.github.io/2024/07/02/solidfire-volume-attributes-from-trident-and-other-apps.html). Volume and Snapshot Attributes are first class citizens in SolidFire CSI.
- **PVC retyping:** Modify volume's QoS Policy ID to change its performance - great for backup, and on-demand performance upgrades (or downgrades)!
- **Delete or Purge:** Lets you optionally take advantage of SolidFire volume restore feature to restore data from deleted volumes (requires SolidFire administrator's action before volume is auto-purged (8h)). Simply specify "delete" behavior (SolidFire volume delete option) in your Storage Class.
- **Wider filesystem support:** ext4, XFS, Btrfs.
- **Observability:** Automatic "lazy discovery" of storage array state keeps track of global metrics (Total PVCs, Total Capacity, Total MinIOPS) across all tenants via `/metrics`. Metrics are by default disabled to eliminate the need for network access to SolidFire CSI controller.
- **High Availability:** Fully stateless controller design supports multiple replicas (Active/Standby) using standard sidecar leader election.
- **Auto-Discard:** Automatically mounts volumes with `discard` enabled. No need to micromanage StorageClass options; blocks are freed immediately for SolidFire efficiency, backup efficiency and SSD health.
- **Dynamic Limits:** Automatically fetches cluster limits (e.g., Max Volume Size, Max Snapshots) to enforce bounds correctly across different SolidFire models (Demo VM vs. Production).
- **Other goodies:** Takes advantage of other SolidFire strengths without creating bloat or unreasonable compromises. For example, Consistency Group Snapshots are supported and LUKS is not.

## Requirements

- Kubernetes 1.35+ (developed and tested on 1.34 and 1.35) with optional multipath-tools
- iSCSI tools installed on worker nodes (`open-iscsi` or `iscsi-initiator-utils`)
- SolidFire Element OS 12.5+ (CHAP algorithm `MD5` for 12.5 and `SHA3-256` for higher versions)

## Getting started

- Go to `./helm/solidfire-csi`, copy `values.yaml` to `my-values.yaml`, edit `my-values.yaml` to customize, and deploy Helm chart for SolidFire CSI
- Read [DOCUMENTATION](./DOCUMENTATION.md) with step-by-step instructions

## Acknowledgement

- John Griffith for vision and wisdom in all matters Cinder and CSI, `solidfire-go` and `solidfire-sdk` (which this CSI driver builds upon) available under the Apache 2.0 license
- Some of the SolidFire driver source code from NetApp Trident project (Apache 2.0)

## SolidFire CSI contributors

- scaleoutsean (Sean) - maintainer (design, planning, testing, documentation)
- AI coding assistants

## License

- Apache 2.0
