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

cluster_reachable() {
  kubectl cluster-info --context "kind-$CLUSTER_NAME" >/dev/null 2>&1
}

create_cluster() {
  echo "Creating kind cluster '$CLUSTER_NAME'..."
  if ! kind create cluster --name "$CLUSTER_NAME" --config hack/kind-config.yaml; then
    echo "kind create failed. Try: kind delete cluster --name $CLUSTER_NAME" >&2
    exit 1
  fi
}

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  echo "Cluster '$CLUSTER_NAME' already exists, verifying..."
  if ! cluster_reachable; then
    echo "Cluster is unreachable (control-plane likely stopped). Recreating..."
    kind delete cluster --name "$CLUSTER_NAME"
    create_cluster
  else
    echo "Cluster is reachable, skipping create."
  fi
else
  create_cluster
fi

if ! cluster_reachable; then
  echo "Cluster '$CLUSTER_NAME' is not reachable. Run: kind delete cluster --name $CLUSTER_NAME" >&2
  exit 1
fi

kubectl cluster-info --context "kind-$CLUSTER_NAME"

docker build -t "$IMAGE" .
kind load docker-image "$IMAGE" --name "$CLUSTER_NAME"

kubectl apply -k deploy/overlays/local

echo "Cluster ready. mock-core UI: http://localhost:8090"
