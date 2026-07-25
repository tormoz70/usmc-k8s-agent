# Agent 1 — ПРОМ (v1 / GetLogs)

Реализация **v1**: control plane = `ingress` + `egress` + `agent-service`.  
Логи: `logs.collect` / `logs.stream` через kube-apiserver **Pods.GetLogs**.  
DaemonSet **не** используется.

> Взаимоисключающе с Agent 2. На один кластер — только этот пакет **или** `../agent-2`.

## Что внутри

| Путь | Назначение |
|------|------------|
| `kustomization.yaml` | Точка входа: prod-v1 + site-values |
| `site-values-patch.yaml` | **Обязательно** заполнить перед apply |
| `deploy/overlays/prod-v1` | Overlay реализации v1 |
| `deploy/overlays/prod` | mTLS Kafka, HTTP tokens |
| `deploy/base` | Deployments, RBAC, policy |
| `deploy/overlays/profiles/*` | Опционально balanced / lean |
| `contracts/fixtures` | REGISTER body / reject |
| `CHECKLIST.md` | Чеклист выката |

## Перед установкой

1. Заменить placeholders в `deploy/overlays/prod/secrets/`:
   - `kafka-ca.crt.placeholder` → `ca.crt` (или обновить пути в `prod/kustomization.yaml`)
   - `kafka-client.crt.placeholder` → `tls.crt`
   - `kafka-client.key.placeholder` → `tls.key`
2. Задать токены в `deploy/overlays/prod/kustomization.yaml` (`secretGenerator`):
   - `k8s-agent-http-token`
   - `k8s-agent-internal-token`
3. Отредактировать `site-values-patch.yaml`:
   - `CLUSTER_ID` — уникальный ID кластера в Core
   - `KAFKA_BROKERS` — брокеры площадки
   - `images` в корневом `kustomization.yaml` — registry/tag образа
4. Заполнить allow-list namespaces: `deploy/base/policy/namespaces.yaml` (+ при необходимости `policy.yaml`).

## Установка

```bash
cd handover/prom/agent-1

# Просмотр манифеста
kubectl kustomize .

# Выкат
kubectl apply -k .

# Опционально: профиль ресурсов
kubectl apply -k deploy/overlays/profiles/balanced
```

## Проверка

```bash
kubectl get pods -n uamc-agent
kubectl get deploy -n uamc-agent
# ожидается: ingress, egress, agent-service (без DaemonSet logs-node-agent)

kubectl logs -n uamc-agent -l app.kubernetes.io/component=agent-service --tail=50
```

Регистрация в Core (поля):

```json
{
  "cluster_id": "<CLUSTER_ID из site-values>",
  "agent_implementation": "v1",
  "logs_backend": "api",
  "modules": ["api", "watch", "logs"]
}
```

Пример: `contracts/fixtures/registration-request.json`.

## Kafka (protobuf / uamc-core)

Топики строятся от `CLUSTER_ID`, например:

- core → agent: `uamc-core.ssl.request.{cluster-id}-uamc-agent`
- agent → reply: `uamc-agent.ssl.response.{cluster-id}-uamc-agent`

Не запускайте второй агент с тем же `CLUSTER_ID`.

## Удаление

```bash
kubectl delete -k .
```
