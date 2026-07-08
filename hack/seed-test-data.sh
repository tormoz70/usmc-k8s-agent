#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "Applying test data (deploy/test-data)..."
kubectl apply -k deploy/test-data

echo "Waiting for deployments..."
for entry in "test-namespace-1/web" "test-namespace-1/api" "test-namespace-1/billing-api" "test-namespace-2/products" "test-namespace-2/indexer" "test-namespace-2/worker"; do
  ns="${entry%%/*}"
  name="${entry##*/}"
  kubectl rollout status "deployment/$name" -n "$ns" --timeout=90s
done

echo
echo "Test data ready:"
echo "  kubectl get pods -A -l app.kubernetes.io/part-of=test-data"
echo "  kubectl logs -n test-namespace-1 logger-a -f"
echo "  kubectl logs -n test-namespace-1 deploy/billing-api -f"
