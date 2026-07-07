#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "Applying test data (deploy/test-data)..."
kubectl apply -k deploy/test-data

echo "Waiting for deployments..."
for entry in "default/web" "default/api" "payments/billing-api" "catalog/products" "catalog/indexer" "demo/worker"; do
  ns="${entry%%/*}"
  name="${entry##*/}"
  kubectl rollout status "deployment/$name" -n "$ns" --timeout=90s
done

echo
echo "Test data ready:"
echo "  kubectl get pods -A -l app.kubernetes.io/part-of=test-data"
echo "  kubectl logs -n default logger-a -f"
echo "  kubectl logs -n payments deploy/billing-api -f"
