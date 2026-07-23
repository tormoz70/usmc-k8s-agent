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

## Import checklist (when Java artifacts arrive)

1. Copy official `.proto` into `api/proto/` (overwrite stubs).
2. Run `make proto` / `buf generate`.
3. Re-run `go test ./internal/protoheaders/... ./internal/coreclient/... ./test/contract/...`.
4. Enable `buf breaking` in CI.
