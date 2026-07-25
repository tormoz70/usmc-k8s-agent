# Локальный тестовый контур

Гибридная схема для разработки и ручного E2E-тестирования **usmc-k8s-agent** (корневой модуль `cmd/agent` + `internal/`).

> **Быстрый старт:** `make dev-up` — compose (Kafka, MinIO, mock-core UI) + kind-кластер с агентом + test-data.  
> **UI:** http://localhost:8090 — вкладка **Scenarios** (UI-заглушки), Commands, Kafka Monitor, S3, **Agent Mode**, **Resources**.  
> **Agent target:** по умолчанию **Local agent** (`k8s.commands.request` / ns `uamc-agent`). См. [§5.2.0](#520-выбор-agent-target).  
> **Модель:** Java core — «чёрный ящик»; mock-core UI имитирует только запросы UI → агент (Kafka / S3 / REST).  
> **E2E (7 сценариев CLI):** [§7.3](#73-e2e-сценарии-реестр-и-алгоритм-проверки).  
> **RBAC / features:** [§4.5](#45-rbac-и-профили-features) · [`rbac-features-capacity.md`](./rbac-features-capacity.md).  
> `docker compose up -d` — только инфраструктура без агента.

> Агент работает **in-cluster** (ServiceAccount, RBAC, leader election).  
> Compose даёт Kafka, S3 и mock-core UI; Kubernetes — отдельно (kind).  
> Nested каталог [`k8s-agent/`](../k8s-agent/) — **archived**, не используйте для локального контура.

## Схема

```text
┌─────────────────────────────────────────────────────────────┐
│  Хост (Windows / Linux / macOS)                             │
│                                                             │
│  docker compose                                             │
│    ├── Redpanda (Kafka)      :9092                          │
│    ├── MinIO (S3)            :9010 / console :9011          │
│    ├── Kafka UI              :8088                          │
│    └── mock-core UI          :8090  (Scenarios / Commands)  │
│                                                             │
│  mock-core UI  ──Kafka──► k8s.commands.request              │
│                ◄───────── reply_topic (header)              │
│                ──REST───► agent HTTP :8080 (healthz/cache)  │
│                ──S3─────► MinIO :9010 (после logs.collect)  │
│                                                             │
│  kind cluster `k8s-agent`                                   │
│    namespace `uamc-agent`:                                  │
│      ├── ingress (входящий HTTP → agent-service)            │
│      ├── egress (Kafka ↔ agent-service)                     │
│      └── agent-service (K8s API, cache, команды)            │
│    namespaces `test-namespace-1`, `test-namespace-2`        │
│    └── kube-apiserver                                       │
└─────────────────────────────────────────────────────────────┘
```

| Компонент prod | Локальная замена | Где запускается |
| --- | --- | --- |
| Managed Kafka | Redpanda (PLAINTEXT) | `docker compose` |
| AWS S3 | MinIO (`:9010`) | `docker compose` |
| Kubernetes | kind | Docker |
| Java core-client | **mock-core UI** (`:8090`) или CLI `hack/mock-core` | compose / хост |
| Feature flags | `features.yaml` / `features-minimal.yaml` | ConfigMap + UI **Agent Mode** |
| mTLS Kafka | не используется локально | — |

Подробная диаграмма протоколов и транспортов: [`service-interaction-diagram.md`](./service-interaction-diagram.md).

---

## 1. Требования и проверка окружения

Перед запуском убедитесь, что все инструменты установлены и доступны в **PATH** текущего терминала.

| Инструмент | Обязателен | Назначение |
| --- | --- | --- |
| **Docker Desktop** / Docker Engine + Compose | да | Redpanda, MinIO, kind, сборка образа агента |
| **kind** | да | локальный Kubernetes |
| **kubectl** | да | деплой и отладка |
| **Go 1.22+** | нет* | локальная сборка агента / CLI `mock-core` (`go test`) |
| **make** | нет | удобные цели; на Windows без make — команды ниже |

\* **Go не нужен** для smoke-тестов через **mock-core UI** в compose ([§5](#5-mock-core-ui)). Go понадобится только для сборки агента или CLI `mock-core` на хосте.

### 1.1 Проверка инструментов (Linux / macOS / Git Bash)

```bash
docker version
docker compose version
kind version
kubectl version --client
go version          # опционально, если собираете mock-core локально
make --version      # опционально
```

### 1.2 Проверка инструментов (Windows PowerShell)

```powershell
docker version
docker compose version
kind version
kubectl version --client
go version          # если «не распознано» — Go не в PATH (см. ниже)
make --version      # если «не распознано» — используйте команды без make
```

**Ожидаемый результат:** каждая команда печатает версию и **не** выдаёт «не распознано» / «command not found».

### 1.3 Что делать, если проверка не прошла

| Симптом | Причина | Решение |
| --- | --- | --- |
| `make: не распознано` | GNU Make не установлен | Команды из Makefile вручную (см. [§3](#3-поднять-kind-и-задеплоить-агента)) или `choco install make` |
| `go: не распознано` при `make mock-core` | Go не установлен / не в PATH | Установить с https://go.dev/dl/ , перезапустить терминал; или сборка mock-core через Docker ([§6](#6-cli-mock-core-альтернатива)) |
| `make kind-up` → `CreateProcess ... bootstrap.sh failed` | bash недоступен | На Windows: `make kind-up` вызывает `hack/bootstrap.ps1`; или `powershell -File hack\bootstrap.ps1` |
| `kubectl apply -k` → `file ... is not in or below ...` | устаревший overlay | Обновите репозиторий; policy генерируется в `deploy/base`, не через `../../` в overlay |
| `kind: command not found` | kind не установлен | https://kind.sigs.k8s.io/docs/user/quick-start/#installation |
| Docker daemon not running | Docker не запущен | Запустите Docker Desktop |
| `kind create` → kubelet healthz timeout | мало RAM/CPU у Docker или нестабильный node image | См. [§13 kind не создаётся](#kind-не-создаётся-kubelet-healthz--no-nodes-found) |

### 1.4 Проверка манифестов (до деплоя)

Убедитесь, что kustomize собирает overlay без ошибок:

```bash
kubectl kustomize deploy/overlays/local
```

PowerShell:

```powershell
kubectl kustomize deploy/overlays/local | Select-Object -First 20
```

Должен вывести YAML (Namespace, ServiceAccount, Deployment, ConfigMap с `cache.put` в policy и т.д.). Ошибок быть не должно.

Проверка policy в сгенерированном ConfigMap:

```powershell
kubectl kustomize deploy/overlays/local | Select-String "cache.put"
```

---

## 2. Поднять инфраструктуру (Kafka + MinIO)

Из корня репозитория:

```bash
docker compose up -d
```

Проверка, что контейнеры запущены:

```bash
docker compose ps
```

Ожидаемо: сервисы `redpanda`, `minio`, `kafka-ui`, `mock-core-ui` в статусе **running**; `minio-init` завершится с кодом 0 (создаёт bucket `logs-bundles`).

После изменений UI или policy-профилей пересоберите контейнер:

```powershell
docker compose up -d --build mock-core-ui
```

`mock-core-ui` монтирует `./test/fixtures` и `./deploy/base/policy`; для вкладки **Agent Mode** нужен kubeconfig (см. [§5.10](#510-agent-mode--переключение-профиля-features)).  
Для REST-проб агента UI использует `AGENT_HTTP_URL` (по умолчанию `http://host.docker.internal:8080`).  
Сервис `k8s-agent-http` в kind — **ClusterIP**, поэтому на хосте нужен port-forward:

```powershell
powershell -File hack\port-forward-agent-http.ps1
# проверка:
curl http://localhost:8080/healthz
```

`make dev-up` поднимает этот port-forward автоматически (шаг 5/5).

PowerShell — дополнительно проверить порты:

```powershell
docker compose ps
Test-NetConnection localhost -Port 9092   # Kafka
Test-NetConnection localhost -Port 9010   # MinIO S3
Test-NetConnection localhost -Port 9011   # MinIO Console
Test-NetConnection localhost -Port 8088   # Kafka UI
Test-NetConnection localhost -Port 8090   # mock-core UI
```

Проверка:

| Сервис | URL / порт | Учётные данные |
| --- | --- | --- |
| Kafka (Redpanda) | `localhost:9092` | без auth |
| MinIO S3 API | `http://localhost:9010` | `minioadmin` / `minioadmin` |
| MinIO Console | http://localhost:9011 | те же |
| Kafka UI | http://localhost:8088 | — |
| **mock-core UI** | **http://localhost:8090** | — |

### Bucket `logs-bundles`

Сервис `minio-init` создаёт bucket автоматически при `docker compose up -d`.

Если bucket отсутствует (например, после `docker compose down -v`), перезапустите init:

```powershell
docker compose up -d minio-init
```

Ручное создание (альтернатива):

1. Откройте http://localhost:9011
2. Login: `minioadmin` / `minioadmin`
3. **Buckets → Create Bucket →** `logs-bundles`

Или через CLI (`mc`), если установлен:

```bash
mc alias set local http://localhost:9010 minioadmin minioadmin
mc mb local/logs-bundles
```

---

## 3. Поднять kind и задеплоить агента

**Linux / macOS:**

```bash
make kind-up
# или: bash hack/bootstrap.sh
```

**Windows (PowerShell):**

```powershell
make kind-up
# или напрямую:
powershell -NoProfile -ExecutionPolicy Bypass -File hack\bootstrap.ps1
```

**Без make (Windows):**

```powershell
kind create cluster --name k8s-agent --config hack/kind-config.yaml
docker build -t k8s-agent:dev .
kind load docker-image k8s-agent:dev --name k8s-agent
kubectl apply -k deploy/overlays/local
```

Скрипт:

1. Создаёт kind-кластер `k8s-agent` (`hack/kind-config.yaml`), либо **пересоздаёт**, если кластер есть, но API недоступен (control-plane остановлен)
2. Собирает Docker-образ `k8s-agent:dev`
3. Загружает образ в kind
4. Применяет `deploy/overlays/local` (Kafka/MinIO → `host.docker.internal`)

### 3.1 Проверка после деплоя

```bash
kubectl get pods -n uamc-agent
kubectl get lease -n uamc-agent          # leader election
kubectl get pods -n uamc-agent -l app.kubernetes.io/part-of=uamc-agent
kubectl logs -n uamc-agent -l app.kubernetes.io/component=agent-service --tail=20
```

Ожидаемо:

- **6 pod'ов** в `uamc-agent`: по 2× `ingress`, `egress`, `agent-service` — все **Running** / **Ready**
- ровно один `agent-service` с label `k8s-agent/leader=true`
- в logs нет постоянных ошибок подключения к Kafka
- ConfigMap `k8s-agent-policy` содержит `policy.yaml`, `namespaces.yaml`, `features.yaml`

```bash
kubectl get pods -n uamc-agent -l app.kubernetes.io/component=agent-service,k8s-agent/leader=true
kubectl get configmap k8s-agent-policy -n uamc-agent -o yaml | grep cache.put   # Linux/macOS only
```

PowerShell (Windows — **`grep` не работает**, используйте `Select-String`):

```powershell
kubectl get pods -n uamc-agent
kubectl get pods -n uamc-agent -l app.kubernetes.io/component=agent-service,k8s-agent/leader=true
kubectl get configmap k8s-agent-policy -n uamc-agent -o yaml | Select-String "cache.put"
```

### 3.2 HTTP агента с хоста

```bash
kubectl port-forward -n uamc-agent svc/k8s-agent-http 8080:8080
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
curl http://localhost:8080/metrics
```

Service `k8s-agent-http` маршрутизирует трафик **только на leader pod**.

---

## 4. Тестовые workload'ы в кластере

Демо-данные описаны в [`deploy/test-data/`](../deploy/test-data/) и применяются автоматически при `make dev-up` (или вручную: `make seed-test-data`).

### 4.1 Что создаётся

| Namespace | Deployment'ы | Pod'ы с логами в stdout |
| --- | --- | --- |
| **test-namespace-1** | `web` (nginx×2), `api` (busybox, пишет лог), `billing-api` (busybox×2) | `logger-a`, `logger-b` (`app=test`) |
| **test-namespace-2** | `products` (nginx×2), `indexer` (busybox, пишет лог), `worker` (busybox) | `logger` (`app=test`) |

Агент развёрнут в namespace **`uamc-agent`** и работает от ServiceAccount **`uamcsa`**.

**RBAC (локальный контур):**

| Binding | Scope | ClusterRole | Назначение |
| --- | --- | --- | --- |
| `uamcsa-agent` | RoleBinding в `uamc-agent` | `k8s-agent` | Операции в namespace агента, leader election |
| `uamcsa-agent` | RoleBinding в `test-namespace-*` | `k8s-agent` | k8s.api / logs / health в прикладных NS |
| `uamcsa-watch` | **ClusterRoleBinding** | `k8s-agent-watch` | **Cluster-scoped watch** (Namespace) + watch Deployment/Pod |

Файлы: [`clusterrole.yaml`](../deploy/base/clusterrole.yaml), [`clusterrole-watch.yaml`](../deploy/base/clusterrole-watch.yaml), [`uamcsa-clusterrolebinding-watch.yaml`](../deploy/base/uamcsa-clusterrolebinding-watch.yaml).

Policy allow-list namespace'ов: [`deploy/test-data/policy/namespaces.yaml`](../deploy/test-data/policy/namespaces.yaml) (`uamc-agent`, `test-namespace-1`, `test-namespace-2`).

### 4.5 RBAC и профили features

Функционал группируется по **feature groups** в `deploy/base/policy/features.yaml` (монтируется в ConfigMap). Каждая группа связана с RBAC-ролью и набором Kafka-команд.

| Feature | RBAC role | Команды | Watch GVK (если есть) |
| --- | --- | --- | --- |
| `cluster_inventory` | `k8s-agent-cluster-read` | `k8s.api` | Namespace, Pod, Service, … |
| `workload_manage` | `k8s-agent-workload-write` | `k8s.api` | Deployment, DeploymentConfig |
| `watch_events` | **`k8s-agent-watch`** | `watch.subscribe/unsubscribe` | **Namespace, Pod, Deployment, DeploymentConfig** |
| `logs_stream` / `logs_collect` | logs-* | logs.* | — |
| `health_report` | `k8s-agent-health` | health.report.* | — |
| `cache` | *(нет RBAC)* | cache.* | — |

Профили:

- **`features.yaml`** — полный (Full)
- **`features-minimal.yaml`** — observability-only (read + watch + health)

Переключение без правки YAML: вкладка **Agent Mode** в mock-core UI ([§5.10](#510-agent-mode--переключение-профиля-features)).  
Подробнее: [`docs/rbac-features-capacity.md`](./rbac-features-capacity.md).

### 4.2 Применить / обновить данные

```powershell
make seed-test-data
# или:
kubectl apply -k deploy/test-data
```

Идempotent: повторный `apply` обновит существующие объекты.

### 4.3 Просмотр логов с хоста

```powershell
kubectl get pods -A -l app.kubernetes.io/part-of=test-data
kubectl logs -n test-namespace-1 logger-a -f
kubectl logs -n test-namespace-1 deploy/billing-api -f
kubectl logs -n test-namespace-2 deploy/indexer -f
kubectl logs -n test-namespace-2 deploy/worker -f
```

Pod'ы с label `app=test` используются шаблоном `logs-collect` в mock-core UI.

### 4.4 Ручное создание (legacy)

Если нужен только минимальный набор в `test-namespace-1`:

```powershell
kubectl run test-nginx --image=nginx:1.27 --labels=app=test -n test-namespace-1
kubectl run test-busybox --image=busybox:1.36 --labels=app=test --command -- sh -c "while true; do echo hello; sleep 5; done" -n test-namespace-1
```

---

## 5. Тестирование агента через mock-core UI

Основной способ ручного E2E: веб-UI имитирует **запросы UI/core** к агенту. Сам Java core считается чёрным ящиком — интересны только каналы **Kafka → агент**, **S3 (MinIO)** и **REST агента**.

### 5.1 Подготовка окружения

**Одна команда** (рекомендуется):

```powershell
make dev-up
```

Поднимает compose (Kafka, MinIO, mock-core UI), kind-кластер с агентом, Kafka topics и test-data (`logger-a`, `logger-b`, deployments в `test-namespace-*`).

`make dev-up` также вызывает `hack/kafka-init.ps1` — создание 6 Kafka topics.

**Проверьте, что агент готов** (в отдельном терминале):

```powershell
kubectl get pods -n uamc-agent
kubectl get pods -n uamc-agent -l app.kubernetes.io/component=agent-service,k8s-agent/leader=true
kubectl rollout status deployment/ingress -n uamc-agent
kubectl rollout status deployment/egress -n uamc-agent
kubectl rollout status deployment/agent-service -n uamc-agent
```

Ожидаемо:

- 2 pod'а каждого компонента (`ingress`, `egress`, `agent-service`) в статусе **Running** и **Ready**
- ровно один pod `agent-service` с label `k8s-agent/leader=true`
- `docker compose ps` — сервисы `redpanda`, `minio`, `mock-core-ui` в **running**

Откройте в браузере: **http://localhost:8090**

> Если `make dev-up` падает на kind с «cluster is not reachable» — bootstrap автоматически пересоздаёт сломанный кластер. Вручную: `kind delete cluster --name k8s-agent` и снова `make dev-up`.

### 5.2 Интерфейс UI

| Вкладка | Назначение |
| --- | --- |
| **Scenarios** | UI-заглушки одним кликом: Kafka-команды, REST probes (`/healthz`, `/v1/cache`), flows (logs→S3, cache.put→GET) |
| **Commands** | Ручной шаблон из `test/fixtures`, JSON, **Reply topic**, **Send command** |
| **Kafka Monitor** | Live/history по топикам (`reply`, `cluster.events`, `logs.stream`, …), фильтр по correlation_id |
| **Cluster** | Inventory namespaces/pods/deployments через kubeconfig |
| **Agent Mode** | Профиль features Full ↔ Observability, RBAC-группы |
| **Resources** | Snapshot CPU/RAM + `/metrics`, сравнение агентов, apply profile |
| **S3 Check** | HEAD объекта в MinIO после `logs.collect` |

В шапке UI — переключатель **Agent target** (куда уходят Kafka-команды и какой namespace смотрит Resources). См. [§5.2.0](#520-выбор-agent-target).

Поток работы (рекомендуется через **Scenarios**):

```text
Scenarios: выбрать карточку → Run
    ↓
UI публикует в request_topic выбранного target (correlation_id + reply_topic)
  и/или дергает AGENT_HTTP_URL (REST)
    ↓
k8s-agent (leader) обрабатывает → reply topic / S3 / HTTP ответ
    ↓
Результат на панели + подсказка открыть Kafka Monitor / S3 Check
```

Альтернатива вручную:

```text
Commands: шаблон → Send command → Kafka Monitor (тот же correlation_id)
```

**Успешный Kafka-ответ** — JSON со `"status": "completed"`. Ошибки: `"status": "failed"` или `"rejected"` — смотрите поле `error` / `message` в теле.

### 5.2.0 Выбор Agent target

Конфиг целей: [`hack/mock-core-ui/targets.yaml`](../hack/mock-core-ui/targets.yaml). Подробнее про v1/v2: [`docs/agent-v1-v2.md`](./agent-v1-v2.md).

| Target в UI | Когда использовать | Namespace | Request topic |
| --- | --- | --- | --- |
| **Local agent** (по умолчанию) | Обычный локальный контур (`make dev-up` / `deploy/overlays/local`) | `uamc-agent` | `k8s.commands.request` |
| **Agent 1 - v1** | После `kubectl apply -k deploy/overlays/test-v1` | `uamc-agent-v1` | `k8s.commands.request.v1` |
| **Agent 2 - v2** | После `kubectl apply -k deploy/overlays/test-v2` | `uamc-agent-v2` | `k8s.commands.request.v2` |

**Важно:** сценарии и Commands ждут reply (~45 с). Если выбран Agent 1/2, а в кластере задеплоен только `uamc-agent`, UI пишет в `.v1`/`.v2` топик **без consumer** — на экране зависает «Running ui-list-pods…», затем timeout.

Перед Run проверьте в шапке:

1. Выбран **Local agent**, если агент в `uamc-agent`.
2. В метаданных target видно `request_topic: k8s.commands.request`.
3. Pod'ы агента Running: `kubectl get pods -n uamc-agent`.

Для сравнения v1/v2 сначала примените оба overlay, затем переключайте target в шапке.

### 5.2.1 UI-заглушки (Scenarios)

Каталог stubs: `GET /api/scenarios`, запуск: `POST /api/scenarios/run` с телом `{ "id": "…" }`.

| Группа | Примеры id | Канал |
| --- | --- | --- |
| Kafka / inventory | `ui-list-namespaces`, `ui-list-pods`, `ui-list-deployments` | Kafka `k8s.api` |
| Kafka / watch | `ui-watch-pods`, `ui-watch-pods-stop` | Kafka + топик `cluster.events` |
| Kafka / logs + S3 | `ui-logs-collect`, `ui-logs-stream-start` | Kafka; collect → MinIO |
| Kafka / cache | `ui-cache-put`, `ui-cache-delete` | Kafka |
| Kafka / health | `ui-health-report-start` | Kafka + `cluster.health` |
| REST agent | `rest-healthz`, `rest-readyz`, `rest-metrics`, `rest-cache-list` | HTTP `AGENT_HTTP_URL` |
| Flows | `flow-logs-to-s3`, `flow-cache-put-get` | Kafka + ожидание reply + S3/REST |

Код stubs: [`hack/mock-core-ui/scenarios.go`](../hack/mock-core-ui/scenarios.go).

### 5.3 Быстрые шаблоны для smoke-теста

**Вариант A (один клик):** http://localhost:8090 → **Scenarios** → `UI: список Namespace` → **Run**.

**Вариант B (Commands):** после `make dev-up` в dropdown **Template** доступны JSON из `test/fixtures`. Рекомендуемый порядок:

| # | Шаблон / Scenario | Что проверяет | Ожидание |
| --- | --- | --- | --- |
| 1 | `k8s-api-list-namespaces` / `ui-list-namespaces` | list Namespace | `"status": "completed"`, `test-namespace-1/2` |
| 2 | `k8s-api-list-pods` / `ui-list-pods` | list pod'ов `app=test` | `logger-a`, `logger-b` |
| 3 | `k8s-api-list-deployments` | list Deployment'ов | JSON `items` из `test-namespace-1` |
| 4 | `watch-subscribe-pods` / `ui-watch-pods` | watch Pod | события в `cluster.events` |
| 5 | `cache-put` → REST `rest-cache-list` / `flow-cache-put-get` | cache write + HTTP GET | Kafka completed + REST 200 |
| 6 | `logs-collect` / `flow-logs-to-s3` | zip → MinIO | `s3_bucket` / `s3_key`, HEAD ok |

> **Namespace list:** включён через feature `cluster_inventory` и RBAC `namespaces: get, list, watch` в `clusterrole.yaml`. Запросы к ресурсам вне allow-list namespace'ов по-прежнему отклоняются policy.

### 5.4 Базовый smoke-test: k8s.api

Проверяет, что агент читает Kafka и ходит в kube-apiserver.

**Через Scenarios:** карточка **UI: список Service** / **UI: список Namespace** → **Run**.

**Через Commands:**

1. Вкладка **Commands**
2. **Template** → `k8s-api-list-services (k8s.api)`
3. **Reply topic** — оставьте `core-client.dev.responses`
4. **Send command**
5. Вкладка **Kafka Monitor** — сообщение с вашим `correlation_id`
6. В теле: `"status": "completed"`, в `body` — JSON от apiserver

Если ответа нет более 1–2 минут — см. [§13 Troubleshooting](#13-troubleshooting).

### 5.5 logs.collect + проверка S3

Нужны test pod'ы с label `app=test` (создаёт `make dev-up`).

**Через Scenarios:** **Flow: logs.collect → проверить S3** (`flow-logs-to-s3`) — UI дождётся reply и сделает HEAD объекта.

**Вручную:**

1. **Commands** → шаблон `logs-collect (logs.collect)`
2. **Send command**
3. В **Kafka Monitor** дождитесь `"status": "completed"` — в ответе будут `s3_bucket` и `s3_key`
4. Вкладка **S3 Check** — bucket/key (из ответа или flow подставит сам) → **Check object**
5. Консоль MinIO: http://localhost:9011 → bucket `logs-bundles`

### 5.6 watch.subscribe — события pod'ов

1. Вкладка **Kafka Monitor**
2. Выберите топик `cluster.events` → **Start live**
3. **Correlation ID** — очистите (слушаем все события)
4. Вкладка **Commands** → шаблон `watch-subscribe-pods (watch.subscribe)` → **Send command**  
   (или **Scenarios** → `ui-watch-pods`)
5. В другом терминале создайте или удалите pod:

```powershell
kubectl run demo-pod --image=nginx:1.27 --labels=app=test -n test-namespace-1
kubectl delete pod demo-pod -n test-namespace-1
```

6. В **Kafka Monitor** на topic `cluster.events` появятся события ADDED/DELETED

### 5.6.1 «Контролировать namespace» — watch на новые Namespace

Задача: подписаться на появление/удаление namespace в кластере. Используется `watch.subscribe` с GVK `Namespace` (cluster-scoped, поле `namespace` в payload **не нужно**).

1. **Kafka Monitor** → Topic `cluster.events` → **Start live**
2. **Commands** → шаблон `watch-subscribe-namespaces` → **Send command**
3. Ответ в reply topic:

```json
{
  "status": "completed",
  "subscription_id": "sub-cluster-namespaces",
  "output_topic": "cluster.events"
}
```

4. Создайте namespace:

```powershell
kubectl create namespace watch-scenario-new-ns
```

5. В `cluster.events` появится событие с `"kind": "Namespace"`, `"name": "watch-scenario-new-ns"`.

6. Остановка подписки: `watch-unsubscribe-namespaces`

CLI / автотест:

```powershell
bin\mock-core -scenario 07-watch-namespaces
```

### 5.6.2 watch на Deployment

Feature `watch_events` + GVK `Deployment` (RBAC `k8s-agent-watch`).

1. **Kafka Monitor** → Topic `cluster.events` → **Start live**
2. **Commands** → `watch-subscribe-deployments` → **Send command**
3. В другом терминале перезапустите deployment:

```powershell
kubectl rollout restart deployment/web -n test-namespace-1
```

4. В stream — события `MODIFIED` / `ADDED` с `"kind": "Deployment"`
5. Остановка: `watch-unsubscribe-deployments`

### 5.7 cache.put + HTTP GET

1. **Commands** → `cache-put (cache.put)` → **Send command**
2. В **Kafka Monitor** — `"status": "completed"`
3. В **отдельном терминале** — port-forward на HTTP API leader pod:

```powershell
kubectl port-forward -n uamc-agent svc/k8s-agent-http 8080:8080
```

4. Проверка кэша:

```powershell
curl http://localhost:8080/v1/cache/feature/test-namespace-1/new-checkout
```

Ожидаемо: `"value": "enabled"`.

### 5.8 health.report — периодические snapshots

1. **Kafka Monitor** → **Topic** `cluster.health` → **Start live**
2. **Commands** → `health-report-start (health.report.start)` → **Send command**
3. В **Kafka Monitor** каждые ~30 с приходят snapshot'ы pod'ов из allow-list namespace'ов (TTL 600 с — автоостановка через 10 мин)

Остановка: **Commands** → `health-report-stop (health.report.stop)` или fixture `health-report-stop.json`. Подробнее — [§7.3.8](#738-сценарий-06--health-report-10-мин-ttl).

### 5.9 Сводка сценариев

Полный реестр из **7 сценариев**, алгоритм проверки и автопрогон — [§7.3](#73-e2e-сценарии-реестр-и-алгоритм-проверки).

| ID | Что проверяем | Шаблон / fixture | Topic ответа |
|----|---------------|------------------|--------------|
| 01 | Список namespace | `k8s-api-list-namespaces` | reply |
| 02 | Pod'ы в NS | `k8s-api-list-pods` | reply |
| 03 | Онлайн-логи | `logs-stream-start-logger-a` | `logs.stream` |
| 04 | Архив логов → S3 | `logs-collect-test-namespace-1` | reply + MinIO |
| 05 | Watch pod ADDED | `watch-subscribe-pods` | `cluster.events` |
| 06 | Health snapshot 10 мин | `health-report-start` | `cluster.health` |
| 07 | Watch новый Namespace | `watch-subscribe-namespaces` | `cluster.events` |
| — | Watch Deployment | `watch-subscribe-deployments` | `cluster.events` |

Дополнительно (не в autotest): cache, rolebindings, failover — см. [§7.3.11](#7311-дополнительно-вне-autotest).

### 5.10 Agent Mode — переключение профиля features

Вкладка **Agent Mode** в mock-core UI (http://localhost:8090) — переключение режима агента без ручного редактирования YAML.

| Режим | Файл | Что включено |
|-------|------|--------------|
| **Full** | `features.yaml` | Все группы: inventory, workload, Istio, logs, watch, health, cache |
| **Observability** | `features-minimal.yaml` | Только read + watch + health (без write, logs export, cache) |

**Apply mode** → патч ConfigMap `k8s-agent-policy` (ключ `features.yaml`) → rollout restart **`agent-service`** и **`egress`**.

#### Требования

| Условие | Compose (Docker) | UI на хосте |
| --- | --- | --- |
| kubeconfig | Монтируется `%USERPROFILE%\.kube\config` → `/kube/config` | при необходимости: `$env:KUBECONFIG_HOST_PATH="$env:USERPROFILE\.kube\config"` |
| API server rewrite | `KUBE_HOST_REWRITE=host.docker.internal` | kind слушает на `127.0.0.1` хоста |
| policy-профили | Volume `./deploy/base/policy:/policy` | `FEATURES_DIR=deploy/base/policy` |
| kind context | `kubectl config use-context kind-k8s-agent` | то же |

Проверка доступа из UI-контейнера:

```powershell
docker compose exec mock-core-ui ls /policy
# kubeconfig — с хоста:
kubectl get configmap k8s-agent-policy -n uamc-agent -o jsonpath='{.data.features\.yaml}' | Select-Object -First 5
```

Если **Agent Mode** / **Cluster** показывают ошибку kubeconfig — на Windows переменная `HOME` часто пустая. Compose берёт `%USERPROFILE%\.kube\config`. Пересоздайте UI:

```powershell
docker compose up -d --build mock-core-ui
```

Проверка mount: `docker inspect …-mock-core-ui-1` — Source должен быть `C:\Users\<you>\.kube\config`, не `\.kube\config`.

После **Apply mode** дождитесь rollout (~1–2 мин):

```powershell
kubectl rollout status deployment/agent-service -n uamc-agent
kubectl rollout status deployment/egress -n uamc-agent
```

#### Проверка переключения

1. **Agent Mode** → выберите **Observability** → **Apply mode**
2. **Commands** → `cache-put` → **Send command** → ожидаемо `PolicyDenied` / rejected (cache отключён)
3. **Commands** → `k8s-api-list-pods` → должно работать
4. Верните **Full** → `cache-put` снова успешен

На вкладке отображаются группы features, RBAC-роли и для `watch_events` — GVK: `Namespace`, `Pod`, `Deployment`, `DeploymentConfig`.

Шаблоны watch: `watch-subscribe-namespaces`, `watch-subscribe-pods`, `watch-subscribe-deployments`.

### 5.11 Запуск UI без Docker (опционально)

```powershell
make mock-core-ui
# или:
go build -o bin/mock-core-ui ./hack/mock-core-ui

$env:KAFKA_BROKERS="localhost:9092"
$env:S3_ENDPOINT="http://localhost:9010"
$env:S3_FORCE_PATH_STYLE="true"
$env:FIXTURES_DIR="test/fixtures"
$env:FEATURES_DIR="deploy/base/policy"
$env:AGENT_HTTP_URL="http://localhost:8080"
$env:MINIO_CONSOLE_URL="http://localhost:9011"
$env:KUBECONFIG="$env:USERPROFILE\.kube\config"
.\bin\mock-core-ui.exe
```

UI доступен на http://localhost:8090. Compose и kind должны быть уже запущены.  
REST-пробы из вкладки **Scenarios** ходят на `AGENT_HTTP_URL` (ingress/agent HTTP).

---

## 6. CLI mock-core (альтернатива)

CLI полезен для скриптов и CI. Для ручного E2E предпочтительнее [§5](#5-mock-core-ui).

### 6.0 Сборка

**С Go (Linux / macOS / Windows):**

```bash
make mock-core
# или:
go build -o bin/mock-core ./hack/mock-core
```

Windows:

```powershell
go build -o bin\mock-core.exe .\hack\mock-core
```

**Без Go — через Docker:**

```powershell
New-Item -ItemType Directory -Force -Path bin | Out-Null
docker run --rm -v "${PWD}:/src" -w /src golang:1.22 go build -o bin/mock-core.exe ./hack/mock-core
```

Проверка:

```powershell
.\bin\mock-core.exe -h
# или Linux/macOS:
# bin/mock-core -h
```

Должен вывести usage с флагами `-file`, `-listen`, `-topic`.

Reply topic по умолчанию: `core-client.dev.responses` (создаётся автоматически при первой записи).

---

## 7. Отправка команд (smoke-тесты)

### 7.1 Через mock-core UI (рекомендуется)

Пошаговые инструкции — в [§5](#5-тестирование-агента-через-mock-core-ui).

Кратко: `make dev-up` → http://localhost:8090 → **Scenarios** → `ui-list-deployments` → **Run** (или **Commands** → шаблон → **Send command** → **Kafka Monitor**).

### 7.2 Через CLI mock-core

#### 7.2.1 k8s.api — list deployments

```bash
bin/mock-core -file test/fixtures/k8s-api-list-deployments.json
```

Ответ приходит в stdout (ожидание до 2 мин). Статус `completed`, тело — JSON от apiserver.

### 7.2.2 cache.put + HTTP GET

```bash
bin/mock-core -file test/fixtures/cache-put.json
```

Проверка HTTP (после `port-forward` на `k8s-agent-http`):

```bash
curl http://localhost:8080/v1/cache/feature/test-namespace-1/new-checkout
```

### 7.2.3 watch.subscribe

Терминал 1 — слушать события:

```bash
bin/mock-core -listen -topic cluster.events
```

Терминал 2 — подписка:

```bash
bin/mock-core -file test/fixtures/watch-subscribe-pods.json
```

Создайте/удалите pod в `test-namespace-1` — в терминале 1 появятся события в `cluster.events`.

### 7.2.4 logs.collect → MinIO

```bash
bin/mock-core -file test/fixtures/logs-collect.json
```

Проверьте объект в MinIO Console: bucket `logs-bundles`, key `logs/test/bundle.zip`.

### 7.2.5 health.report

Терминал 1:

```bash
bin/mock-core -listen -topic cluster.health
```

Терминал 2:

```bash
bin/mock-core -file test/fixtures/health-report-start.json
```

Каждые 30s в `cluster.health` приходит snapshot pod'ов (TTL 600s — автоостановка через 10 мин).

### 7.2.6 logs.stream — онлайн-логи контейнера

Терминал 1:

```bash
bin/mock-core -listen -topic logs.stream
```

Терминал 2:

```bash
bin/mock-core -file test/fixtures/logs-stream-start-logger-a.json
```

Ожидаемо: строки `heartbeat` от pod `logger-a` в `logs.stream`. Остановка:

```bash
bin/mock-core -file test/fixtures/logs-stream-stop-logger-a.json
```

### 7.2.7 k8s.api — list namespaces

```bash
bin/mock-core -file test/fixtures/k8s-api-list-namespaces.json
```

Ожидаемо: `test-namespace-1`, `test-namespace-2` в `http_body`.

### 7.2.8 agent.lifecycle (failover)

Терминал 1:

```bash
bin/mock-core -listen -topic agent.lifecycle
```

Терминал 2 — chaos smoke:

```bash
bash hack/chaos-leader-failover.sh
```

Ожидаемо: событие `agent.started` / `agent.leader.changed` при смене leader.

### 7.2.9 Слушать только reply topic

```bash
bin/mock-core -listen -reply-topic core-client.dev.responses
```

### 7.3 E2E-сценарии: реестр и алгоритм проверки

Реестр: `test/scenarios/scenarios.yaml`. JSON-команды: `test/scenarios/01-….json` … `07-….json`.  
Шаблоны UI: `test/fixtures/*.json`.

#### 7.3.1 Предусловия (перед любым сценарием)

```text
1. make dev-up                    # compose + kind + agent + kafka-init + test-data
2. kubectl get pods -n uamc-agent # 6 pod'ов Running (2× ingress, egress, agent-service)
3. mock-core-ui: Agent target = Local agent (topic k8s.commands.request)
4. go build -o bin\mock-core.exe .\hack\mock-core
```

`kafka-init` уже входит в `make dev-up`. Повторно вручную: `powershell -File hack\kafka-init.ps1`.

Если UI зависает на «Running ui-list-pods…» — проверьте target ([§5.2.0](#520-выбор-agent-target)).

Подробнее: [§14](#14-быстрый-чеклист-всё-поднять-с-нуля), [§13](#13-troubleshooting).

#### 7.3.2 Общий алгоритм

**Sync-команда** (`k8s.api`, start/stop подписок):

1. Отправить JSON (UI **Send command** или `mock-core -file …`).
2. Дождаться reply в `core-client.dev.responses`.
3. Проверить `"status": "completed"`.

**Async-поток** (watch, logs.stream, health):

1. Подключить stream на нужный topic **до** триггера.
2. Отправить start → reply с `subscription_id`.
3. Действие в кластере или ожидание интервала.
4. Сообщение в stream с тем же `subscription_id`.
5. Stop/unsubscribe (cleanup).

#### 7.3.3 Сценарий 01 — список namespace

| Команда | `k8s.api` GET `/api/v1/namespaces` |
| Fixture | `k8s-api-list-namespaces.json` |
| Ответ | sync → reply |

1. Отправить fixture.
2. `"http_status": 200`, в `http_body`: `test-namespace-1`, `test-namespace-2`.

```powershell
bin\mock-core -scenario 01-list-namespaces
```

#### 7.3.4 Сценарий 02 — pod'ы в namespace

| Команда | `k8s.api` GET pods `labelSelector=app=test` в `test-namespace-1` |
| Fixture | `k8s-api-list-pods.json` |

1. `kubectl get pods -n test-namespace-1 -l app=test` → `logger-a`, `logger-b`.
2. Отправить команду → reply с теми же именами в теле.

```powershell
bin\mock-core -scenario 02-list-pods
```

#### 7.3.5 Сценарий 03 — онлайн-логи

| Команда | `logs.stream.start` → `logs.stream` |
| Fixture start/stop | `logs-stream-start-logger-a.json` / `logs-stream-stop-logger-a.json` |

1. Терминал 1: `bin\mock-core -listen -topic logs.stream`
2. Терминал 2: start fixture → `subscription_id=log-stream-logger-a`
3. В stream: `logger-a`, `heartbeat`
4. Stop fixture

```powershell
bin\mock-core -scenario 03-logs-stream
```

#### 7.3.6 Сценарий 04 — логи в S3

| Команда | `logs.collect` (async, до ~3 мин) |
| Fixture | `logs-collect-test-namespace-1.json` |
| S3 | bucket `logs-bundles`, key из ответа |

1. Отправить команду, дождаться reply.
2. `"s3_bucket": "logs-bundles"`, `byte_size` > 0.
3. MinIO Console http://localhost:9011 или UI **S3 Check**.

```powershell
bin\mock-core -scenario 04-logs-collect-s3
```

#### 7.3.7 Сценарий 05 — watch pod

| Команда | `watch.subscribe` → `cluster.events` |
| Fixture start/stop | `watch-subscribe-pods.json` / `watch-unsubscribe-pods.json` |

1. Stream: `cluster.events`
2. Start → `subscription_id=sub-pods-test-namespace-1`
3. `kubectl run watch-scenario-probe --image=busybox:1.36 --restart=Never -n test-namespace-1 -- sleep 120`
4. Событие `ADDED`, kind `Pod`
5. Unsubscribe fixture

```powershell
bin\mock-core -scenario 05-watch-pods
```

#### 7.3.8 Сценарий 06 — health report (10 мин TTL)

| Команда | `health.report.start` → `cluster.health`, interval 30s, TTL 600s |
| Fixture start/stop | `health-report-start.json` / `health-report-stop.json` (UI) или `06-health-report-*.json` (autotest) |

1. Stream: `cluster.health`
2. Start → `interval_seconds`: 30
3. Два snapshot'а (~0 с и ~30 с) с `"summary"` и `test-namespace-1`
4. Stop: `health-sub-scenario`

```powershell
bin\mock-core -scenario 06-health-report
```

#### 7.3.9 Сценарий 07 — новый Namespace

| Команда | `watch.subscribe`, GVK `Namespace` (без `namespace` в payload) |
| Fixture start/stop | `watch-subscribe-namespaces.json` / `watch-unsubscribe-namespaces.json` |

1. Stream: `cluster.events`
2. Start → `subscription_id=sub-cluster-namespaces`
3. `kubectl create namespace watch-scenario-new-ns`
4. `ADDED`, kind `Namespace`, name `watch-scenario-new-ns`
5. Unsubscribe + delete namespace

```powershell
bin\mock-core -scenario 07-watch-namespaces
```

#### 7.3.10 Автопрогон

```powershell
bin\mock-core -scenario all
$env:RUN_INTEGRATION="1"; go test -tags=integration ./hack/mockcorelib/... -timeout 20m -v
```

```bash
make mock-core
bin/mock-core -scenario all
make test-integration
# один сценарий:
RUN_INTEGRATION=1 SCENARIO_ID=03-logs-stream go test -tags=integration ./hack/mockcorelib/... -run TestIntegrationScenarioByID -v
```

Ожидаемо: `PASS 01-list-namespaces` … `PASS 07-watch-namespaces`.

#### 7.3.11 Дополнительно (вне autotest)

| Сценарий | Fixture | Проверка |
|----------|---------|----------|
| Cache | `cache-put`, `cache-delete` | port-forward :8080 → GET `/v1/cache/{key}` |
| Watch Deployment | `watch-subscribe-deployments` | stream `cluster.events` + `kubectl rollout restart` |
| Deployments list | `k8s-api-list-deployments-test-namespace-1` | reply 200 |
| Agent Mode | UI **Agent Mode** | Full ↔ Observability, cache on/off |
| Failover | — | `-listen -topic agent.lifecycle` + `make chaos-leader` |

#### 7.3.12 Сводная таблица

| ID | Type | Reply | Stream | Cleanup |
|----|------|-------|--------|---------|
| 01 | `k8s.api` | ✓ | — | — |
| 02 | `k8s.api` | ✓ | — | — |
| 03 | `logs.stream.start` | ✓ | `logs.stream` | stop |
| 04 | `logs.collect` | ✓ async | — | — |
| 05 | `watch.subscribe` | ✓ | `cluster.events` | unsubscribe |
| 06 | `health.report.start` | ✓ | `cluster.health` | stop |
| 07 | `watch.subscribe` (NS) | ✓ | `cluster.events` | unsubscribe |

---

## 8. Альтернатива: агент на хосте (без leader election)

Если kind не нужен, но есть kubeconfig на реальный/локальный кластер:

```bash
docker compose up -d

POLICY_FILE=deploy/base/policy/policy.yaml \
POLICY_NAMESPACES_FILE=deploy/base/policy/namespaces.yaml \
FEATURES_FILE=deploy/base/policy/features.yaml \
KAFKA_BROKERS=localhost:9092 \
S3_ENDPOINT=http://localhost:9010 \
S3_FORCE_PATH_STYLE=true \
go run ./cmd/agent --dev-no-leader-election
```

Флаг `--dev-no-leader-election` — command processor сразу на этом процессе, без Lease.

> HTTP cache API и leader-only подписки в этом режиме работают на единственном процессе; поведение failover не тестируется.

---

## 9. Переменные окружения (справочно)

| Переменная | Default (local) | Описание |
| --- | --- | --- |
| `KAFKA_BROKERS` | `localhost:9092` | брокеры |
| `KAFKA_REQUEST_TOPIC` | `k8s.commands.request` | входящие команды |
| `KAFKA_EVENTS_TOPIC` | `cluster.events` | watch-события |
| `KAFKA_LOGS_STREAM_TOPIC` | `logs.stream` | стрим логов |
| `KAFKA_HEALTH_TOPIC` | `cluster.health` | health snapshots |
| `KAFKA_LIFECYCLE_TOPIC` | `agent.lifecycle` | restart/failover |
| `S3_ENDPOINT` | — | MinIO URL |
| `S3_FORCE_PATH_STYLE` | `true` для MinIO | path-style addressing |
| `CLUSTER_ID` | `local` | prefix Kafka keys |
| `POLICY_FILE` | `/etc/k8s-agent/policy/policy.yaml` | allow-list (issuers, verbs) |
| `FEATURES_FILE` | `/etc/k8s-agent/policy/features.yaml` | feature groups / command toggles |
| `LEADER_ELECTION_ENABLED` | `true` in cluster | Lease election |

**mock-core UI** (дополнительно):

| Переменная | Default | Описание |
| --- | --- | --- |
| `FIXTURES_DIR` | `test/fixtures` | шаблоны Commands |
| `FEATURES_DIR` | `deploy/base/policy` | пресеты Full / Observability |
| `KUBECONFIG` | — | доступ к kind для **Agent Mode** / Cluster |
| `AGENT_NAMESPACE` | `uamc-agent` | namespace агента |
| `AGENT_CONFIGMAP` | `k8s-agent-policy` | ConfigMap policy + features |
| `AGENT_HTTP_URL` | `http://host.docker.internal:8080` | base URL REST агента (Scenarios REST/flows) |
| `AGENT_HTTP_BEARER` / `HTTP_BEARER_TOKEN` | — | Bearer для `/v1/cache`, `/metrics` при необходимости |
| `S3_ENDPOINT` | `http://localhost:9010` | MinIO API |
| `MINIO_CONSOLE_URL` | `http://localhost:9011` | ссылки Open in Console |
| `S3_FORCE_PATH_STYLE` | `true` | path-style для MinIO |

Полный список agent env — `internal/config/config.go`.

---

## 10. Kafka UI

http://localhost:8088 — просмотр топиков, сообщений, consumer groups.

Полезные топики:

- `k8s.commands.request`
- `core-client.dev.responses`
- `cluster.events`
- `cluster.health`
- `logs.stream`
- `agent.lifecycle`

---

## 11. Пересборка после изменений кода

```bash
docker build -t k8s-agent:dev .
kind load docker-image k8s-agent:dev --name k8s-agent
kubectl rollout restart deployment/ingress deployment/egress deployment/agent-service -n uamc-agent
kubectl rollout status deployment/ingress -n uamc-agent
kubectl rollout status deployment/egress -n uamc-agent
kubectl rollout status deployment/agent-service -n uamc-agent
```

---

## 12. Остановка и очистка

```bash
# удалить агента из kind (кластер остаётся)
kubectl delete -k deploy/overlays/local

# удалить kind-кластер
kind delete cluster --name k8s-agent

# остановить compose
docker compose down

# compose + volumes MinIO
docker compose down -v
```

---

## 13. Troubleshooting

### kind не создаётся (kubelet healthz / no nodes found)

**Симптомы:**

```text
[kubelet-check] The kubelet is not healthy after 4m0s
ERROR: no nodes found for cluster "k8s-agent"
```

**Причина:** kind не смог поднять control-plane (часто на **Docker Desktop / Windows**). Скрипт bootstrap **не должен** продолжать сборку, если кластер не создан.

**Шаги:**

1. Удалите сломанный кластер:

```powershell
kind delete cluster --name k8s-agent
```

2. Docker Desktop → **Settings → Resources**:
   - Memory: **≥ 4 GB** (лучше 6–8 GB)
   - CPUs: **≥ 2**
   - Перезапустите Docker Desktop

3. Повторите с зафиксированным node image (`hack/kind-config.yaml` → `kindest/node:v1.31.2`):

```powershell
make kind-up
```

4. Проверка:

```powershell
kind get clusters
kubectl cluster-info --context kind-k8s-agent
kubectl get nodes
```

**Если снова падает на kubelet** — посмотрите логи:

```powershell
docker logs k8s-agent-control-plane 2>&1 | Select-Object -Last 50
```

Обновите kind до последней версии: https://kind.sigs.k8s.io/docs/user/quick-start/#installation

### Leader pod не найден (`No resources found`)

```powershell
kubectl get pods -n uamc-agent -o wide
kubectl get lease -n uamc-agent
kubectl logs -n uamc-agent -l app.kubernetes.io/component=agent-service --tail=30
```

| Симптом | Что проверить |
| --- | --- |
| Pod'ы не `Running` | `kubectl describe pod -n uamc-agent -l app.kubernetes.io/part-of=uamc-agent` |
| Нет Lease / holder | Kafka недоступен → `docker compose ps`, логи агента |
| Pod Running, но нет label `k8s-agent/leader=true` | После обновления RBAC: `kubectl apply -k deploy/overlays/local` и `kubectl rollout restart deployment/ingress deployment/egress deployment/agent-service -n uamc-agent` |
| Label есть, selector не находит | Убедитесь в точном синтаксисе: `-l k8s-agent/leader=true` |

Проверка label на всех pod'ах:

```powershell
kubectl get pods -n uamc-agent --show-labels
```

Leader может работать через Lease **без** label, пока patch RBAC не применён; HTTP Service `k8s-agent-http` (cache API) **требует** label на leader.

### Pod'ы агента не Ready

```bash
kubectl describe pod -n uamc-agent -l app.kubernetes.io/component=agent-service
kubectl logs -n uamc-agent -l app.kubernetes.io/component=agent-service --tail=50
```

Частые причины:

- Redpanda/MinIO не запущены (`docker compose ps`)
- `host.docker.internal` недоступен из kind (на Linux иногда нужен `extra_hosts` в kind config)

### Unknown Topic Or Partition

Redpanda не создаёт топики при **чтении** (consumer). Ошибка `[3] Unknown Topic Or Partition` — топик ещё не существует.

**Быстрое исправление:**

```bash
hack/kafka-init.sh
# Windows:
powershell -File hack/kafka-init.ps1
```

Создаются топики:

- `k8s.commands.request`
- `core-client.dev.responses`
- `cluster.events`
- `logs.stream`
- `cluster.health`
- `agent.lifecycle`

Проверка:

```bash
docker compose exec redpanda rpk topic list
```

`mock-core` и `mock-core-ui` также пытаются создать топики автоматически при отправке/подписке (пересоберите: `make mock-core`).

### mock-core: timeout waiting for reply

- **Неверный Agent target** → UI шлёт в `k8s.commands.request.v1`/`.v2`, а local-агент слушает `k8s.commands.request`. Симптом: «Running ui-list-pods…» ~45 с. Выберите **Local agent** в шапке ([§5.2.0](#520-выбор-agent-target))
- Агент не leader → проверьте Lease и logs
- Policy denied → проверьте `allowed_command_types` в ConfigMap `k8s-agent-policy`
- Неверный reply topic → по умолчанию `core-client.dev.responses`
- Kafka недоступна из kind → egress должен ходить на `host.docker.internal:9092`

### logs.collect: bucket not found

Создайте bucket `logs-bundles` в MinIO (шаг 2).

### Policy rejected / UnknownCommand

In-cluster policy: ConfigMap `k8s-agent-policy` (`policy.yaml` + **`features.yaml`**).  
Отключённая feature → `PolicyDenied` или handler не зарегистрирован → `UnknownCommand`.

Проверка текущего профиля:

```powershell
kubectl get configmap k8s-agent-policy -n uamc-agent -o yaml | Select-String "enabled:"
```

После изменения policy или features:

```powershell
kubectl apply -k deploy/overlays/local
kubectl rollout restart deployment/ingress deployment/egress deployment/agent-service -n uamc-agent
```

Или через UI: **Agent Mode** → **Apply mode** (только `features.yaml`, без правки overlay).

### Windows

| Симптом | Решение |
| --- | --- |
| `make` не найден | Команды из Makefile вручную или `choco install make` |
| `go` не найден при `make mock-core` | Установить Go или собрать mock-core через Docker ([§6](#6-cli-mock-core-альтернатива)) |
| `bootstrap.sh` / bash не найден | `powershell -File hack\bootstrap.ps1` или обновлённый `make kind-up` |
| `grep` не распознано | В PowerShell: `Select-String "cache.put"` вместо `grep cache.put` |
| `host.docker.internal` | Поддерживается Docker Desktop на Windows |
| Agent Mode: K8s unavailable | Смонтировать kubeconfig; context `kind-k8s-agent` |
| Agent Mode: Apply failed | `kubectl auth can-i update configmaps -n uamc-agent` |

- `make kind-up` → `hack/bootstrap.ps1` (bash не нужен)
- Go: `go build -o bin\mock-core.exe .\hack\mock-core`

---

## 14. Быстрый чеклист «всё поднять с нуля»

### Одна команда (рекомендуется)

```powershell
make dev-up
# или: powershell -File hack\dev-up.ps1
```

Поднимает:

1. `docker compose up -d --build` — Redpanda, MinIO, Kafka UI, **mock-core UI**
2. `hack/kafka-init.ps1` — 6 Kafka topics
3. kind + agent (`deploy/overlays/local`)
4. test-data (`test-namespace-1/2`, `logger-a/b`, deployments)

**Smoke-тест после запуска:**

| # | Действие | Ожидание |
| --- | --- | --- |
| 1 | http://localhost:8090 → **Scenarios** → `ui-list-namespaces` → **Run** | Kafka reply `"status": "completed"` |
| 2 | **Scenarios** → `ui-list-pods` → **Run** | в теле `logger-a`, `logger-b` |
| 3 | **Scenarios** → `rest-healthz` → **Run** | HTTP 200 (нужен port-forward: `hack/port-forward-agent-http.ps1`) |
| 4 | **Agent Mode** → Observability → **Apply** → `cache-put` | rejected (cache off) |
| 5 | **Agent Mode** → Full → **Apply** → `flow-cache-put-get` | Kafka completed + REST cache |

Автопрогон E2E (опционально):

```powershell
go build -o bin\mock-core.exe .\hack\mock-core
bin\mock-core -scenario all
```

Подробнее — [§5](#5-тестирование-агента-через-mock-core-ui), [§7.3](#73-e2e-сценарии-реестр-и-алгоритм-проверки).

Остановка:

```powershell
make dev-down          # compose down + удалить agent из kind (кластер остаётся)
make dev-down-full     # + kind delete cluster
```

### Пошагово (если нужен контроль)

#### Шаг 0 — проверка окружения

```powershell
docker version
docker compose version
kind version
kubectl version --client
kubectl kustomize deploy/overlays/local | Select-String "features.yaml"
kubectl kustomize deploy/overlays/local | Select-String "k8s-agent-watch"
```

#### Шаг 1 — инфраструктура

```powershell
docker compose up -d --build
docker compose ps
powershell -File hack\kafka-init.ps1
```

#### Шаг 2 — кластер и агент

```powershell
make kind-up
kubectl get pods -n uamc-agent
kubectl get clusterrolebinding uamcsa-watch
kubectl get pods -n uamc-agent -l app.kubernetes.io/component=agent-service,k8s-agent/leader=true
```

#### Шаг 3 — test-data

```powershell
make seed-test-data
kubectl get pods -n test-namespace-1 -l app=test
```

#### Шаг 4 — smoke через UI

1. http://localhost:8090 → **Agent Mode** (K8s available)
2. **Commands** → `k8s-api-list-deployments` → **Send command**
3. **Kafka Monitor** → `"status": "completed"`

Linux / macOS — те же шаги; `hack/kafka-init.sh`, `bin/mock-core`.

---

## Ссылки

- RBAC, features, sizing: [`docs/rbac-features-capacity.md`](./rbac-features-capacity.md)
- Agent v1/v2 и targets UI: [`docs/agent-v1-v2.md`](./agent-v1-v2.md)
- Диаграмма сервисов: [`docs/service-interaction-diagram.md`](./service-interaction-diagram.md)
- Архитектура: [`docs/architecture-core-client-k8s-agent.md`](architecture-core-client-k8s-agent.md)
- Deploy overlays: [`deploy/README.md`](../deploy/README.md)
- Prod overlay (mTLS): [`deploy/overlays/prod/`](../deploy/overlays/prod/)
- План и контракты Kafka: [`.cursor/plans/k8s_kafka_agent_bbf1d29a.plan.md`](../.cursor/plans/k8s_kafka_agent_bbf1d29a.plan.md)
