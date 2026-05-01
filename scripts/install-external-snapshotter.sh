#!/usr/bin/env bash

set -euo pipefail

# This script downloads and deploys the latest external-snapshotter CRDs, 
# snapshot-controller, and webhook from the kubernetes-csi/external-snapshotter repository.

REPO_URL="https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master"

echo "Deploying external-snapshotter CRDs..."
for file in snapshot.storage.k8s.io_volumesnapshotclasses.yaml snapshot.storage.k8s.io_volumesnapshotcontents.yaml snapshot.storage.k8s.io_volumesnapshots.yaml groupsnapshot.storage.k8s.io_volumegroupsnapshotclasses.yaml groupsnapshot.storage.k8s.io_volumegroupsnapshotcontents.yaml groupsnapshot.storage.k8s.io_volumegroupsnapshots.yaml; do
  curl -sSL "${REPO_URL}/client/config/crd/${file}" | kubectl apply -f -
done

echo "Deploying snapshot-controller RBAC and Deployment..."
for file in rbac-snapshot-controller.yaml setup-snapshot-controller.yaml; do
  curl -sSL "${REPO_URL}/deploy/kubernetes/snapshot-controller/${file}" | kubectl apply -f -
done

echo "external-snapshotter components have been applied."
