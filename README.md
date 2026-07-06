# usmc-k8s-agent

In-cluster Kubernetes agent: Kafka commands → kube-apiserver / Istio API, with modular Go packages for parallel development.

## Module layout

| Package | Owner focus | Phase |
| --- | --- | --- |
| `internal/config` | configuration | 1 |
| `internal/command` | envelope, router | 1 |
| `internal/policy` | allow-list | 1 |
| `internal/k8s` | apiserver client, trim | 1 |
| `internal/handlers/api` | `k8s.api` proxy | 1 |
| `internal/kafka` | consumer/producer | 1 |
| `internal/leaderelection` | Lease leader | 1 |
| `internal/httpapi` | health/metrics/cache HTTP | 1/4 |
| `internal/handlers/logs` | logs.collect | 2 ✅ |
| `internal/s3` | S3 upload | 2 |
| `internal/watch` | watch.subscribe → cluster.events | 3 ✅ |
| `internal/lifecycle` | agent.lifecycle | 3 ✅ (publish on leader start) |
| `internal/cache` + `handlers/cache` | cache.put + GET | 4 |
| `internal/handlers/health` | health.report | 4 |

Go module: `github.com/usmc/usmc-k8s-agent` (change via `go mod edit -module=...`).

## Local dev

Подробная инструкция по локальному тестовому контуру (включая проверку окружения на Windows): **[docs/local-test-contour.md](docs/local-test-contour.md)**.

```bash
# Full local test environment (one command)
make dev-up
# Smoke: http://localhost:8090 → k8s-api-list-deployments → Send command

# Or step by step:
# Kafka + MinIO + mock-core UI
docker compose up -d

# Build & test
make tidy test build

# Agent in kind only
make kind-up
```

### mock-core (Kafka CLI)

```bash
make mock-core
bin/mock-core -file test/fixtures/k8s-api-list-deployments.json
bin/mock-core -file test/fixtures/logs-collect.json
bin/mock-core -file test/fixtures/watch-subscribe-pods.json
bin/mock-core -listen -reply-topic core-client.dev.responses
# separate terminal: listen cluster.events with a dedicated consumer group
```

### Agent locally (kubeconfig, no leader election)

```bash
POLICY_FILE=deploy/base/policy/policy.yaml \
POLICY_NAMESPACES_FILE=deploy/base/policy/namespaces.yaml \
KAFKA_BROKERS=localhost:9092 \
S3_ENDPOINT=http://localhost:9000 \
S3_FORCE_PATH_STYLE=true \
go run ./cmd/agent --dev-no-leader-election
```

## Architecture reference

See [`.cursor/plans/k8s_kafka_agent_bbf1d29a.plan.md`](.cursor/plans/k8s_kafka_agent_bbf1d29a.plan.md) and `docs/`.
