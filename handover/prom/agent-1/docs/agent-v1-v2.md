# Agent v1 / v2 and Core UI targeting

## Implementations

| | **v1** | **v2** |
|---|--------|--------|
| Control plane | ingress / egress / agent-service Deployments | same |
| Log collect / stream | kube-apiserver `Pods.GetLogs` | DaemonSet `logs-node-agent` reads `/var/log/pods` |
| Env | `AGENT_IMPLEMENTATION=v1`, `LOGS_BACKEND=api` | `AGENT_IMPLEMENTATION=v2`, `LOGS_BACKEND=nodelocal` |
| Deploy | [deploy/overlays/test-v1](../deploy/overlays/test-v1), [prod-v1](../deploy/overlays/prod-v1) | [test-v2](../deploy/overlays/test-v2), [prod-v2](../deploy/overlays/prod-v2) |

External command contract (`logs.collect`, `logs.stream.*`, `k8s.api`, …) is **identical**.

## Local stand (default)

`make dev-up` / `deploy/overlays/local` → namespace `uamc-agent`, topic `k8s.commands.request`.

В mock-core-ui (http://localhost:8090) в шапке должен быть выбран **Local agent**.  
Иначе сценарии вроде `ui-list-pods` зависают на «Running…» (~45 с timeout): команды уходят в топик без consumer.

Конфиг целей: [hack/mock-core-ui/targets.yaml](../hack/mock-core-ui/targets.yaml).  
Пошагово в локальном контуре: [local-test-contour.md §5.2.0](./local-test-contour.md#520-выбор-agent-target).

| Target | Namespace | Request topic | Нужен overlay |
|--------|-----------|---------------|---------------|
| **Local agent** (default) | `uamc-agent` | `k8s.commands.request` | `overlays/local` |
| **Agent 1 - v1** | `uamc-agent-v1` | `k8s.commands.request.v1` | `overlays/test-v1` |
| **Agent 2 - v2** | `uamc-agent-v2` | `k8s.commands.request.v2` | `overlays/test-v2` |

## Test: two projects

```bash
kubectl apply -k deploy/overlays/test-v1   # ns uamc-agent-v1, CLUSTER_ID=test-v1, topic …request.v1
kubectl apply -k deploy/overlays/test-v2   # ns uamc-agent-v2, CLUSTER_ID=test-v2, topic …request.v2 + DaemonSet
```

После apply переключите **Agent target** в шапке UI на Agent 1 или Agent 2.

Overlays `test-v1` / `test-v2` мержат allow-list namespaces из `policy/namespaces.yaml` в overlay (как local: `test-namespace-1`…`5`). Без этого сценарии вроде `ui-list-pods` получают `PolicyDenied` / `FORBIDDEN_RESOURCE`.

## PROD: one agent per cluster

Install **either** `prod-v1` **or** `prod-v2`, never both.

Core registration must reject a second agent for the same `cluster_id`:

```json
{ "accepted": false, "reason": "AgentAlreadyRegistered", "message": "…" }
```

See [registration-response-rejected.json](../contracts/fixtures/registration-response-rejected.json).

## Registration body (Core contract)

```json
{
  "cluster_id": "prod-eu-1",
  "agent_instance_id": "agent-service-xyz",
  "cluster_name": "prod-eu-1",
  "modules": ["api", "watch", "logs"],
  "agent_implementation": "v1",
  "logs_backend": "api"
}
```

For v2: `"agent_implementation": "v2"`, `"logs_backend": "nodelocal"`.

### Core UI requirements

1. Persist `agent_implementation` (+ optional `logs_backend`) with the cluster registry entry.
2. Show a **v1 / v2** badge on the cluster list.
3. Command send / cluster picker: user selects the cluster (in test stands there may be two clusters — one per implementation).
4. On `REGISTER`, if `cluster_id` is already owned → `accepted=false`, `reason=AgentAlreadyRegistered`.
5. Optional filter: show only v2 agents on a comparison stand.

Lifecycle events (`agent.lifecycle`) also carry `agent_implementation` and `logs_backend`.

## Kafka isolation

| Mode | How targets are separated |
|------|---------------------------|
| Local JSON | Distinct `KAFKA_REQUEST_TOPIC` (`k8s.commands.request.v1` / `.v2`) + consumer groups |
| Protobuf / uamc-core | Distinct `CLUSTER_ID` → `uamc-core.ssl.request.{cluster-id}-uamc-agent` |

# Do not run two agents with the same `CLUSTER_ID` on a shared request topic.

## RBAC note (test overlays)

Shared ClusterRoleBindings (`uamcsa-cluster-read`, `uamcsa-watch`) must be **renamed per overlay**
(`…-v1` / `…-v2`). Otherwise `kubectl apply -k test-v2` rewrites the single binding’s subject
to `uamc-agent-v2` and Local/Agent 1 lose `list services` (and similar) → apiserver `Forbidden`.

Test-data RoleBindings in `test-namespace-*` include subjects for `uamc-agent`, `uamc-agent-v1`, and `uamc-agent-v2`.
