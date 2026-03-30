#!/bin/bash
set -e

# Generate manifests from Helm chart
# This script updates the static YAML files in deploy/ directory to match the Helm chart templates.

# Ensure we are in the project root
cd "$(dirname "$0")/.."

OUTPUT_DIR=$(mktemp -d)
trap 'rm -rf "$OUTPUT_DIR"' EXIT

echo "Generating manifests from Helm chart..."
helm template solidfire-csi ./helm/solidfire-csi -n solidfire-csi --output-dir "$OUTPUT_DIR" > /dev/null

TEMPLATE_DIR="$OUTPUT_DIR/solidfire-csi/templates"

# Update deploy/ directory
echo "Updating deploy/rbac.yaml..."
cat "$TEMPLATE_DIR/serviceaccount.yaml" > ./deploy/rbac.yaml
echo "---" >> ./deploy/rbac.yaml
cat "$TEMPLATE_DIR/rbac.yaml" >> ./deploy/rbac.yaml

echo "Updating deploy/controller.yaml..."
cp "$TEMPLATE_DIR/deployment.yaml" ./deploy/controller.yaml

echo "Updating deploy/node.yaml..."
cp "$TEMPLATE_DIR/daemonset.yaml" ./deploy/node.yaml

echo "Updating deploy/csi-driver.yaml..."
cp "$TEMPLATE_DIR/csidriver.yaml" ./deploy/csi-driver.yaml

echo "Updating deploy/secret.yaml..."
cp "$TEMPLATE_DIR/secret.yaml" ./deploy/secret.yaml

echo "Updating deploy/storageclass.yaml..."
cp "$TEMPLATE_DIR/storageclass.yaml" ./deploy/storageclass.yaml

echo "Done. Please review changes in deploy/ directory."
