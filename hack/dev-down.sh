#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "Stopping docker compose..."
docker compose down

echo "Removing agent manifests from kind..."
kubectl delete -k deploy/overlays/local --ignore-not-found || true

echo "Done. Kind cluster 'k8s-agent' is still running."
echo "Full teardown: make dev-down-full"
