# usmc-k8s-agent

In-cluster Go Kubernetes agent: Kafka commands → kube-apiserver / Istio API, with modular packages and dual-stack JSON + protobuf transport for uamc-core compatibility.

## Documentation

- Code overview: [docs/agent-go-code-overview.md](docs/agent-go-code-overview.md)
- Local test contour: [docs/local-test-contour.md](docs/local-test-contour.md) (Scenarios, MinIO `:9010`, `AGENT_HTTP_URL`)
- Architecture: [docs/architecture-core-client-k8s-agent.md](docs/architecture-core-client-k8s-agent.md)
- MVP decisions: [docs/mvp-plan.md](docs/mvp-plan.md)

> **Note:** Nested [`k8s-agent/`](k8s-agent/) is **archived** (alternate NUP contract). Use the **root** module (`cmd/agent` + `internal/`).

## Build

```bash
go mod tidy
go test ./...
go build -o bin/k8s-agent ./cmd/agent
```

## Module layout (root)

| Package | Role |
| --- | --- |
| `internal/modules` | Module registry (Spring-profile analogue) |
| `internal/config` | Env configuration (`KAFKA_MODE`, topics, …) |
| `internal/command` | JSON envelope + router |
| `internal/policy` + `internal/features` | Allow-list / capability flags |
| `internal/handlers/*` | `k8s.api`, logs, watch, cache, health |
| `internal/kafka` + `internal/transport` | Kafka I/O + transport abstractions |
| `internal/coreclient` / `internal/protoheaders` | uamc-core protobuf request/response |
| `internal/batcher` / `internal/keyedlock` | Shared concurrency helpers |

## Local dev

```bash
make dev-up
# Smoke: http://localhost:8090 → Scenarios → ui-list-deployments → Run

# Or:
docker compose up -d
make tidy test build
make kind-up
```

### Agent locally (kubeconfig, no leader election)

```bash
POLICY_FILE=deploy/base/policy/policy.yaml \
POLICY_NAMESPACES_FILE=deploy/base/policy/namespaces.yaml \
FEATURES_FILE=deploy/base/policy/features.yaml \
KAFKA_BROKERS=localhost:9092 \
S3_ENDPOINT=http://localhost:9010 \
S3_FORCE_PATH_STYLE=true \
go run ./cmd/agent --dev-no-leader-election
```

Kafka wire format: `KAFKA_MODE=json` (default), `protobuf`, or `dual`.

## Architecture reference

See [`.cursor/plans/k8s_kafka_agent_bbf1d29a.plan.md`](.cursor/plans/k8s_kafka_agent_bbf1d29a.plan.md) and `docs/`.
