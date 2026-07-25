# Agent 2 — ПРОМ (v2 / DaemonSet nodelocal)

Реализация **v2**: control plane = `ingress` + `egress` + `agent-service` **+** DaemonSet `logs-node-agent`.  
Логи: чтение hostPath `/var/log/pods` на нодах, fan-out по `NodeName`.

> Взаимоисключающе с Agent 1. На один кластер — только этот пакет **или** `../agent-1`.

## Что внутри

| Путь | Назначение |
|------|------------|
| `kustomization.yaml` | Точка входа: prod-v2 + site-values |
| `site-values-patch.yaml` | **Обязательно** заполнить перед apply |
| `deploy/overlays/prod-v2` | Overlay реализации v2 |
| `deploy/components/logs-node` | DaemonSet + NetworkPolicy |
| `deploy/overlays/prod` | mTLS Kafka, HTTP tokens |
| `deploy/base` | Deployments, RBAC, policy |
| `deploy/overlays/profiles/*` | Опционально balanced / lean |
| `contracts/fixtures` | REGISTER body v2 / reject |
| `CHECKLIST.md` | Чеклист выката |

## Требования к кластеру (дополнительно к Agent 1)

- Права DaemonSet + mount hostPath `/var/log/pods` (и при необходимости `/var/log/containers`)
- PSP / PSA / SELinux: разрешить чтение логов kubelet на нодах
- Сеть: `agent-service` → pods `logs-node-agent` на порту **8083** (см. NetworkPolicy)
- Один и тот же `k8s-agent-internal-token` у agent-service и DaemonSet

## Перед установкой

1. TLS Kafka и токены — как в Agent 1 (`deploy/overlays/prod/secrets/`, `secretGenerator`).
2. `site-values-patch.yaml`: `CLUSTER_ID`, `KAFKA_BROKERS` (и для DaemonSet).
3. Образ в корневом `kustomization.yaml` (тот же image для Deployments и DaemonSet).
4. Allow-list namespaces: `deploy/base/policy/namespaces.yaml`.

## Установка

```bash
cd handover/prom/agent-2

kubectl kustomize .
kubectl apply -k .

# Опционально
kubectl apply -k deploy/overlays/profiles/balanced
```

## Проверка

```bash
kubectl get pods -n uamc-agent
kubectl get ds -n uamc-agent
# ожидается: ingress, egress, agent-service + logs-node-agent на каждой ноде

kubectl get ds logs-node-agent -n uamc-agent
kubectl logs -n uamc-agent -l app.kubernetes.io/component=logs-node-agent --tail=20
```

Регистрация в Core:

```json
{
  "cluster_id": "<CLUSTER_ID из site-values>",
  "agent_implementation": "v2",
  "logs_backend": "nodelocal",
  "modules": ["api", "watch", "logs"]
}
```

Пример: `contracts/fixtures/registration-request-v2.json`.

## Kafka

Как у Agent 1 — изоляция через уникальный `CLUSTER_ID`.  
Контракт команд идентичен v1.

## Удаление

```bash
kubectl delete -k .
```
