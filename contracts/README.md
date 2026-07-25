# Contract freeze notes (Phase 2 gate)

Until official protobuf definitions are copied from Java `uamc-agent` / `uamc-core`,
this repository uses **stub** `.proto` files under [`../api/proto/`](../api/proto/).

## Topic map (from agent_migration.txt)

| Direction | Topic pattern |
| --- | --- |
| core → agent | `uamc-core.ssl.request.{cluster-id}-{uamc-agent}` |
| agent → response | `uamc-agent.ssl.response.{cluster-id}-{uamc-agent}` |
| agent → core request | `uamc-agent.ssl.request` |
| events watcher | `uamc-events-watcher.ssl.request` |
| metrics watcher | `uamc-metrics-watcher.ssl.request` |

## ProtoHeaders Kafka header keys

Wire headers use the Java-style names (see `internal/protoheaders`):

- `messageId`, `correlationId`, `topic`, `topicForResponse`
- `direction` (`REQUEST` / `RESPONSE`)
- `addressee`, `sender`, `messageType`, `requestType`, `timestamp`
- `zipped` (`true` / `false`)

## Golden fixtures

See [`fixtures/`](fixtures/) for synthetic round-trip samples. Replace with Java-captured bytes when available.

| Fixture | Purpose |
| --- | --- |
| `registration-request.json` | Agent v1 register (`agent_implementation=v1`, `logs_backend=api`) |
| `registration-request-v2.json` | Agent v2 register (`agent_implementation=v2`, `logs_backend=nodelocal`) |
| `registration-response-rejected.json` | Core rejects second agent on same `cluster_id` (`AgentAlreadyRegistered`) |

## Agent implementations (v1 / v2)

One Kubernetes cluster in PROD must run **exactly one** agent (v1 or v2). Test stands may run both in separate namespaces with distinct `cluster_id` / request topics. See [docs/agent-v1-v2.md](../docs/agent-v1-v2.md).

## Import checklist (when Java artifacts arrive)

1. Copy official `.proto` into `api/proto/` (overwrite stubs).
2. Run `make proto` / `buf generate`.
3. Re-run `go test ./internal/protoheaders/... ./internal/coreclient/... ./test/contract/...`.
4. Enable `buf breaking` in CI.
