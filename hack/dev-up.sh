#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

INFRA_TIMEOUT_SEC=60

require_command() {
  if ! command -v "$1" >/dev/null; then
    echo "$1 is required but not found in PATH." >&2
    exit 1
  fi
}

wait_tcp_port() {
  local host=$1 port=$2 deadline=$((SECONDS + INFRA_TIMEOUT_SEC))
  while (( SECONDS < deadline )); do
    if (echo >/dev/tcp/"$host"/"$port") 2>/dev/null; then
      return 0
    fi
    sleep 2
  done
  echo "Timeout waiting for ${host}:${port}" >&2
  return 1
}

wait_http_ok() {
  local url=$1 deadline=$((SECONDS + INFRA_TIMEOUT_SEC))
  while (( SECONDS < deadline )); do
    if curl -sf "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Timeout waiting for $url" >&2
  return 1
}

echo "=== dev-up: full local test environment ==="

require_command docker
require_command kind
require_command kubectl

echo
echo "[1/4] Starting docker compose (Kafka, MinIO, mock-core UI)..."
docker compose up -d --build

echo
echo "[2/4] Waiting for infra..."
wait_tcp_port localhost 9092
wait_tcp_port localhost 9000
wait_http_ok "http://localhost:8090/api/health"
echo "Ensuring Kafka topics..."
hack/kafka-init.sh || true
echo "Infra is ready."

echo
echo "[3/4] Bootstrapping kind cluster and deploying agent..."
hack/bootstrap.sh

echo "Waiting for uamc-agent rollouts..."
kubectl rollout status deployment/ingress -n uamc-agent --timeout=120s
kubectl rollout status deployment/egress -n uamc-agent --timeout=120s
kubectl rollout status deployment/agent-service -n uamc-agent --timeout=120s

echo
echo "[4/4] Seeding test cluster data..."
hack/seed-test-data.sh

echo
echo "=== Environment ready ==="
echo "  mock-core UI:  http://localhost:8090"
echo "  Kafka UI:      http://localhost:8088"
echo "  MinIO Console: http://localhost:9001"
echo
echo "  kubectl get pods -A -l app.kubernetes.io/part-of=test-data"
echo "  kubectl logs -n test-namespace-1 logger-a -f"
echo
echo "Smoke test: mock-core UI -> k8s-api-list-pods -> Send command"
