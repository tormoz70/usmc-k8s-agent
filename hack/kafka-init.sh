#!/usr/bin/env bash
# Create local Kafka topics in Redpanda (idempotent).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! docker compose ps redpanda --status running >/dev/null 2>&1; then
  echo "Redpanda is not running. Start infra first: docker compose up -d redpanda" >&2
  exit 1
fi

docker compose exec -T redpanda bash <<'EOF'
set -euo pipefail
create_topic() {
  rpk topic create "$1" --partitions "${2:-1}" --replicas 1 2>/dev/null || true
}
create_topic k8s.commands.request 1
create_topic core-client.dev.responses 1
create_topic cluster.events 1
create_topic logs.stream 1
create_topic cluster.health 1
create_topic agent.lifecycle 1
echo "Kafka topics:"
rpk topic list
EOF
