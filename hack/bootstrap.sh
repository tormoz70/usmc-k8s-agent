#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v kind >/dev/null; then
  echo "kind is required: https://kind.sigs.k8s.io/"
  exit 1
fi

kind create cluster --name k8s-agent --config hack/kind-config.yaml 2>/dev/null || true

docker build -t k8s-agent:dev .
kind load docker-image k8s-agent:dev --name k8s-agent

kubectl apply -k deploy/overlays/local

echo "Cluster ready. Kafka/MinIO: docker compose up -d"
