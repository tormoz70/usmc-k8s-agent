#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CLUSTER_NAME="k8s-agent"
IMAGE="k8s-agent:dev"

if ! command -v kind >/dev/null; then
  echo "kind is required: https://kind.sigs.k8s.io/"
  exit 1
fi

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  echo "Cluster '$CLUSTER_NAME' already exists, skipping create."
else
  echo "Creating kind cluster '$CLUSTER_NAME'..."
  if ! kind create cluster --name "$CLUSTER_NAME" --config hack/kind-config.yaml; then
    echo "kind create failed. Try: kind delete cluster --name $CLUSTER_NAME" >&2
    exit 1
  fi
fi

kubectl cluster-info --context "kind-$CLUSTER_NAME"

docker build -t "$IMAGE" .
kind load docker-image "$IMAGE" --name "$CLUSTER_NAME"

kubectl apply -k deploy/overlays/local

echo "Cluster ready. Kafka/MinIO: docker compose up -d"
