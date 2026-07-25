# RBAC-роли, feature flags и оценка ресурсов

Документ описывает группировку функционала агента по RBAC-ролям Kubernetes, отключение возможностей через `features.yaml` и ориентировочное потребление CPU/RAM.

## Архитектура компонентов

| Компонент | Kafka | apiserver | Назначение |
|-----------|-------|-----------|------------|
| **ingress** | — | — | HTTP proxy (cache read, metrics) |
| **egress** | consume/produce | — | Kafka ↔ internal API |
| **agent-service** | publish events | read/write | Leader: команды, watch, logs, health |

RBAC ServiceAccount `uamcsa` используется **только** pod'ами `agent-service` (leader выполняет команды).

---

## Группы функционала ↔ RBAC-роли

Каждая группа — отдельная **ClusterRole** (рекомендуется) + запись в `features.yaml`.

| Feature ID | ClusterRole | Kafka-команды | RBAC (apiserver) | Сценарии |
|------------|-------------|---------------|------------------|----------|
| `cluster_inventory` | `k8s-agent-cluster-read` | `k8s.api` | `namespaces`, `pods`, `services`, `configmaps`, `events`: get, list, watch | Список NS/pod, инвентаризация |
| `workload_manage` | `k8s-agent-workload-write` | `k8s.api` | `deployments`, `deploymentconfigs`: get, list, watch, create, update, patch, delete | CRUD Deployment |
| `istio_manage` | `k8s-agent-istio-write` | `k8s.api` | Istio CRD: get, list, watch, create, update, patch, delete | VS/DR/Gateway/AuthPolicy |
| `rbac_inspect` | `k8s-agent-rbac-read` | `k8s.api` | `roles`, `rolebindings`: get, list | Аудит RBAC |
| `logs_collect` | `k8s-agent-logs-export` | `logs.collect` | `pods/log`: get | Архив логов → S3 |
| `logs_stream` | `k8s-agent-logs-stream` | `logs.stream.start/stop` | `pods/log`: get (+ watch stream) | Онлайн-логи → Kafka |
| `watch_events` | `k8s-agent-watch` | `watch.subscribe/unsubscribe` | **watch** on `namespaces`, `pods`, `deployments`, `deploymentconfigs` | Pod/NS/Deployment events → `cluster.events` |
| `health_report` | `k8s-agent-health` | `health.report.start/stop` | `pods`: list | Статусы pod → `cluster.health` |
| `cache` | *(нет RBAC)* | `cache.put/delete/clear` | — | In-memory cache + HTTP GET |
| *(внутреннее)* | `k8s-agent-leader` | — | `leases`: * | Leader election |

Текущий монолитный `deploy/base/clusterrole.yaml` (`k8s-agent`) объединяет все правила для локальной разработки. В prod рекомендуется **RoleBinding только на нужные ClusterRole** в соответствии с включёнными feature.

Пример минимального набора binding'ов (только read + watch + health):

```yaml
# ClusterRoleBinding на uamcsa — только нужные роли
subjects:
  - kind: ServiceAccount
    name: uamcsa
    namespace: uamc-agent
roleRef: ... # k8s-agent-cluster-read, k8s-agent-watch, k8s-agent-health, k8s-agent-leader
```

---

## Конфигурация: `features.yaml`

Файл: `deploy/base/policy/features.yaml` (монтируется в ConfigMap `k8s-agent-policy`).

```yaml
features:
  cluster_inventory:
    enabled: true
    rbac_role: k8s-agent-cluster-read
    command_types: [k8s.api]
    allowed_gvk:
      - { group: "", version: v1, kind: Namespace }
      - { group: "", version: v1, kind: Pod }
  logs_collect:
    enabled: false   # ← отключено
    command_types: [logs.collect]
```

### Поведение при `enabled: false`

1. **Policy** — команды группы не входят в `allowed_command_types` → `PolicyDenied`
2. **Router** — handler не регистрируется → `UnknownCommand`
3. **RBAC** — ClusterRole можно не биндить (defence in depth)

### Переменные окружения

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `FEATURES_FILE` | `/etc/k8s-agent/policy/features.yaml` | Путь к feature flags |
| `POLICY_FILE` | `/etc/k8s-agent/policy/policy.yaml` | Issuer, reply topics, verbs, deny secrets |
| `POLICY_NAMESPACES_FILE` | `.../namespaces.yaml` | Allow-list namespace |

Если `features.yaml` **отсутствует** — работает полный профиль (все команды из `policy.yaml`).

### Готовый профиль «только observability»

`deploy/base/policy/features-minimal.yaml`:

- ✅ `cluster_inventory`, `watch_events`, `health_report`
- ❌ write (workload/istio), logs, cache

Подключение в overlay:

```yaml
# deploy/overlays/prod/kustomization.yaml
configMapGenerator:
  - name: k8s-agent-policy
    behavior: merge
    files:
      - features.yaml=policy/features-minimal.yaml
```

---

## Матрица: сценарий → feature → RBAC

| # | Сценарий | Feature(s) | RBAC role |
|---|----------|------------|-----------|
| 1 | List namespaces | `cluster_inventory` | cluster-read |
| 2 | List pods | `cluster_inventory` | cluster-read |
| 3 | Live logs | `logs_stream` | logs-stream |
| 4 | Logs → S3 | `logs_collect` | logs-export |
| 5 | Watch pods | `watch_events` + GVK Pod | watch |
| 6 | Health 10 min | `health_report` | health |
| 7 | Watch new Namespace | `watch_events` + GVK Namespace | cluster-read + watch |
| — | Deploy patch | `workload_manage` | workload-write |
| — | Istio policy | `istio_manage` | istio-write |

---

## Оценка ресурсов

Базовые requests/limits из манифестов:

| Pod | CPU req / limit | RAM req / limit |
|-----|-----------------|-----------------|
| ingress ×2 | 50m / 200m | 64Mi / 128Mi |
| egress ×2 | 100m / 500m | 128Mi / 512Mi |
| agent-service ×2 (1 leader) | 100m / 500m | 128Mi / 512Mi |

### Профили эксплуатации

#### A. Idle (лидер без активных подписок)

| Компонент | CPU | RAM | Комментарий |
|-----------|-----|-----|-------------|
| ingress ×2 | ~30m | ~50Mi each | HTTP health, policy mount |
| egress ×2 | ~40m | ~70Mi each | Kafka consumer poll |
| agent-service leader | ~50m | ~90Mi | informer cache пустой |
| agent-service standby | ~20m | ~60Mi | без Kafka consume |
| **Сумма кластера** | **~200m** | **~400Mi** | 6 pod'ов |

#### B. Observability (minimal features)

Watch 3 подписки (pods + namespaces) + health каждые 30s на 2 NS (~20 pod):

| | CPU | RAM |
|---|-----|-----|
| + к профилю A | +80–150m | +80–120Mi |
| **Итого** | **~350m** | **~550Mi** |

Пики CPU — list pod при health tick; watch держит steady state.

#### C. Standard (full features, умеренная нагрузка)

- 5 watch-подписок
- 2 log stream
- 1 health / 30s / 5 NS (~100 pod)
- до 2 параллельных `logs.collect`

| | CPU | RAM |
|---|-----|-----|
| Steady | 400–600m | 600–900Mi |
| Peak (logs.collect zip) | **до 500m** на leader | **300–450Mi** spike |

`logs.collect` — основной потребитель: чтение log API + zip + upload (emptyDir до 2Gi limit volume, RAM ~ размер батча).

#### D. Heavy ops (write + Istio + много watch)

- `k8s.api` write bursts
- 10+ informers
- 5 log streams
- health каждые 15s на 10 NS

| | CPU | RAM |
|---|-----|-----|
| Steady | 600m–1 core | 800Mi–1.2Gi |
| Peak | limit **500m/pod** (throttle) | limit **512Mi** — risk OOM при большом logs.collect |

### Рекомендации по sizing

| Профиль | features file | agent-service limits | Replicas |
|---------|---------------|----------------------|----------|
| Dev/local | `features.yaml` (full) | 500m / 512Mi | 1–2 |
| Prod read-only | `features-minimal.yaml` | 250m / 256Mi | 2 |
| Prod full | `features.yaml` tuned | 500m / 768Mi | 2–3 |
| Logs-heavy | + `logs_collect`, ↑ `LOGS_COLLECT_MAX_JOBS` | 1 CPU / 1Gi | 2 |

Тюнинг env для снижения нагрузки:

- `K8S_API_QPS` / `K8S_API_BURST` — default 50/100; для read-only снизить до 20/40
- `HEALTH_MAX_PODS_PER_MESSAGE` — меньше сообщений в Kafka
- `LOGS_COLLECT_MAX_JOBS` — default 20; снизить до 5
- `LOG_STREAM_MAX_PER_POD` — default 1

---

## Замер потребления (runbook)

```bash
# Usage (нужен metrics-server; в kind его нет из коробки)
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
# kind: kubelet без IP SANs — обязателен insecure TLS
kubectl patch deployment metrics-server -n kube-system --type=json --patch-file=- <<'EOF'
[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]
EOF
# PowerShell:
#   '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]' |
#     Set-Content -Encoding ascii ms-patch.json
#   kubectl patch deployment metrics-server -n kube-system --type=json --patch-file ms-patch.json
# проверка: kubectl get apiservice v1beta1.metrics.k8s.io  → Available=True
kubectl top pods -n uamc-agent
kubectl top pods -n uamc-agent-v1
kubectl top pods -n uamc-agent-v2

# Requests / limits из манифеста
kubectl get pods -n uamc-agent -o custom-columns=\
NAME:.metadata.name,\
CPU_REQ:.spec.containers[*].resources.requests.cpu,\
MEM_REQ:.spec.containers[*].resources.requests.memory,\
CPU_LIM:.spec.containers[*].resources.limits.cpu,\
MEM_LIM:.spec.containers[*].resources.limits.memory

# Runtime Prometheus (leader / ingress)
curl -s http://localhost:8080/metrics | egrep \
  'process_resident_memory_bytes|go_goroutines|go_memstats_alloc_bytes|k8s_agent_(logs_collect_jobs_active|watch_subscriptions_active|log_streams_active)'
```

В mock-core-ui: вкладка **Resources** — snapshot CPU/RAM + `/metrics`, сравнение Agent 1/2, применение профилей.

### Hotspots (что растёт от чего)

| Нагрузка | Что растёт | Главный рычаг |
|----------|------------|---------------|
| Idle HA (replicas=2) | requests CPU/RAM ×2 за standby | replicas → 1 |
| `watch.subscribe` ×N | RAM informer cache, CPU list/watch | features / число подписок |
| `health.report` частый | CPU list pods, Kafka bytes | interval / `HEALTH_MAX_PODS_PER_MESSAGE` |
| `logs.stream` | CPU + outbound Kafka | `LOG_STREAM_*` |
| `logs.collect` | CPU zip, RAM/disk emptyDir, apiserver (v1) или DS I/O (v2) | `LOGS_COLLECT_MAX_JOBS/BYTES`, emptyDir sizeLimit |
| Agent v2 DaemonSet | +50m/64Mi **на каждую ноду** | не ставить v2 на больших кластерах без нужды в nodelocal |

### Именованные профили экономии (UI + Kustomize)

| ID | Когда | Replicas | Env / features | emptyDir logs |
|----|-------|----------|----------------|---------------|
| **ha** | PROD HA (base) | 2 | QPS 50/100, full features, `LOGS_COLLECT_MAX_JOBS=20`, max bytes 500Mi | 2Gi |
| **balanced** | обычный PROD / тест | 1 | QPS 30/60, full features, jobs=5, max bytes 100Mi; agent-service mem limit 768Mi | 2Gi |
| **lean** | observability / демо | 1 | QPS 20/40, **features-minimal**, jobs=2, max bytes 50Mi | 512Mi |

Kustomize:

```bash
kubectl apply -k deploy/overlays/profiles/balanced
kubectl apply -k deploy/overlays/profiles/lean
```

Или mock-core-ui → **Resources** → Apply profile (нужен kubeconfig).

### Kafka / сеть (вне pod metrics)

| Поток | Типичный объём |
|-------|----------------|
| `cluster.events` | 1–5 KB/event; burst при mass pod churn |
| `cluster.health` | 10–200 KB/message (до 500 pod/msg) |
| `logs.stream` | 0.5–5 KB/batch, continuous |
| reply topic | 2–50 KB/sync command |

---

## Чеклист внедрения минимальных прав

1. Скопировать `features-minimal.yaml` → overlay prod
2. Убрать из ClusterRoleBinding лишние роли (workload, istio, logs-export)
3. `kubectl apply -k deploy/overlays/prod && rollout restart ...`
4. Проверить: отключённая команда → `PolicyDenied` / `UnknownCommand`
5. Мониторинг: `agent_watch_subscriptions`, `process_resident_memory_bytes`

---

## Связанные файлы

- `deploy/base/policy/features.yaml` — полный профиль
- `deploy/base/policy/features-minimal.yaml` — observability-only
- `deploy/overlays/profiles/balanced` — Kustomize профиль balanced
- `deploy/overlays/profiles/lean` — Kustomize профиль lean
- `deploy/base/policy/policy.yaml` — Kafka trust, verbs, issuers
- `deploy/base/clusterrole-watch.yaml` — watch RBAC (NS, Pod, Deployment, DeploymentConfig)
- `deploy/base/uamcsa-clusterrolebinding-watch.yaml` — ClusterRoleBinding для cluster-scoped watch
- `internal/features/features.go` — загрузка и merge с policy
- `hack/mock-core-ui` — вкладка Resources (snapshot + apply profile)
