# Передача агентов на ПРОМ

Две **взаимоисключающие** поставки — на один Kubernetes-кластер ставят **ровно одну**:

| Папка | Реализация | Логи | Overlay |
|-------|------------|------|---------|
| [`agent-1/`](agent-1/) | v1 (GetLogs) | kube-apiserver `Pods.GetLogs` | `deploy/overlays/prod-v1` |
| [`agent-2/`](agent-2/) | v2 (DaemonSet) | hostPath `/var/log/pods` через `logs-node-agent` | `deploy/overlays/prod-v2` |

Контракт команд Kafka (`k8s.api`, `logs.collect`, `logs.stream.*`, …) **одинаковый**.  
Отличаются только способ сбора логов и поля регистрации `agent_implementation` / `logs_backend`.

## Правила ПРОМ

1. **Один агент на кластер** (один `CLUSTER_ID`). Второй REGISTER → `AgentAlreadyRegistered`.
2. Namespace по умолчанию: `uamc-agent`.
3. Перед apply заполнить **site-values** (брокеры Kafka, `CLUSTER_ID`, образ, TLS-секреты) — см. README внутри папки.
4. Не ставить `agent-1` и `agent-2` в один namespace одновременно.

## Быстрый старт

```bash
# Выбрать ОДНУ папку, например agent-1:
cd handover/prom/agent-1
# 1) отредактировать site-values-patch.yaml и secrets
# 2) проверить манифест
kubectl kustomize .
# 3) применить
kubectl apply -k .
```

## Состав каждой папки

- `README.md` — установка и проверка
- `CHECKLIST.md` — чеклист перед/после выката
- `site-values-patch.yaml` — обязательные значения площадки
- `kustomization.yaml` — точка входа (`kubectl apply -k .`)
- `deploy/` — самодостаточные манифесты (base + prod + prod-vN)
- `contracts/fixtures/` — примеры REGISTER / reject
- `docs/` — описание v1/v2, RBAC/ёмкости

## Контакты / исходники

Исходный репозиторий: `usmc-k8s-agent` (`deploy/overlays/prod-v1`, `prod-v2`).  
Пакеты в `handover/prom/` можно архивировать и передавать без остального monorepo.
