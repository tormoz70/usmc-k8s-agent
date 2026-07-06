# Локальный тестовый контур

Гибридная схема для разработки и ручного E2E-тестирования **usmc-k8s-agent**.

> **Быстрый старт:** `make dev-up` — одна команда поднимает compose (Kafka, MinIO, mock-core UI), kind-кластер с агентом и тестовые pod'ы.  
> **Тест через UI:** http://localhost:8090 → [§5](#5-тестирование-агента-через-mock-core-ui).  
> `docker compose up -d` — только инфраструктура без агента.

> Агент по design работает **in-cluster** (ServiceAccount, RBAC, leader election).  
> Полностью в одном `docker-compose.yml` его не поднять — compose даёт Kafka, S3 и mock-core UI, кластер — отдельно (kind).

## Схема

```text
┌─────────────────────────────────────────────────────────────┐
│  Хост (Windows / Linux / macOS)                             │
│                                                             │
│  docker compose                                             │
│    ├── Redpanda (Kafka)      :9092                          │
│    ├── MinIO (S3)            :9000 / console :9001          │
│    ├── Kafka UI              :8088                          │
│    └── mock-core UI          :8090                          │
│                                                             │
│  mock-core UI / CLI  ──► k8s.commands.request               │
│                        ◄── reply_topic (header)             │
│                                                             │
│  kind cluster `k8s-agent`                                   │
│    ├── Deployment k8s-agent (2 replicas, leader election)   │
│    ├── test pods (default namespace)                        │
│    └── kube-apiserver                                       │
└─────────────────────────────────────────────────────────────┘
```

| Компонент prod | Локальная замена | Где запускается |
| --- | --- | --- |
| Managed Kafka | Redpanda (PLAINTEXT) | `docker compose` |
| AWS S3 | MinIO | `docker compose` |
| Kubernetes | kind | Docker |
| Java core-client | **mock-core UI** (`:8090`) или CLI `hack/mock-core` | compose / хост |
| mTLS Kafka | не используется локально | — |

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

PowerShell — дополнительно проверить порты:

```powershell
docker compose ps
Test-NetConnection localhost -Port 9092   # Kafka
Test-NetConnection localhost -Port 9000   # MinIO S3
Test-NetConnection localhost -Port 8088   # Kafka UI
Test-NetConnection localhost -Port 8090   # mock-core UI
```

Проверка:

| Сервис | URL / порт | Учётные данные |
| --- | --- | --- |
| Kafka (Redpanda) | `localhost:9092` | без auth |
| MinIO S3 API | `http://localhost:9000` | `minioadmin` / `minioadmin` |
| MinIO Console | http://localhost:9001 | те же |
| Kafka UI | http://localhost:8088 | — |
| **mock-core UI** | **http://localhost:8090** | — |

### Bucket `logs-bundles`

Сервис `minio-init` создаёт bucket автоматически при `docker compose up -d`.

Если bucket отсутствует (например, после `docker compose down -v`), перезапустите init:

```powershell
docker compose up -d minio-init
```

Ручное создание (альтернатива):

1. Откройте http://localhost:9001
2. Login: `minioadmin` / `minioadmin`
3. **Buckets → Create Bucket →** `logs-bundles`

Или через CLI (`mc`), если установлен:

```bash
mc alias set local http://localhost:9000 minioadmin minioadmin
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
kubectl get pods -n k8s-agent
kubectl get lease -n k8s-agent          # leader election
kubectl logs -n k8s-agent -l app=k8s-agent --tail=20
```

Ожидаемо:

- 2 pod'а в статусе **Running**
- один pod с label `k8s-agent/leader=true`
- в logs нет постоянных ошибок подключения к Kafka

```bash
kubectl get pods -n k8s-agent -l k8s-agent/leader=true
kubectl get configmap k8s-agent-policy -n k8s-agent -o yaml | grep cache.put   # Linux/macOS only
```

PowerShell (Windows — **`grep` не работает**, используйте `Select-String`):

```powershell
kubectl get pods -n k8s-agent
kubectl get pods -n k8s-agent -l k8s-agent/leader=true
kubectl get configmap k8s-agent-policy -n k8s-agent -o yaml | Select-String "cache.put"
```

### 3.2 HTTP агента с хоста

```bash
kubectl port-forward -n k8s-agent svc/k8s-agent-http 8080:8080
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
| **default** | `web` (nginx×2), `api` (busybox, пишет лог) | `logger-a`, `logger-b` (`app=test`) |
| **payments** | `billing-api` (busybox×2, пишет лог) | `logger` (`app=test`) |
| **catalog** | `products` (nginx×2), `indexer` (busybox, пишет лог) | `logger` (`app=test`) |
| **demo** | `worker` (busybox, пишет лог) | `logger` (`app=test`) |

Namespace'ы `payments`, `catalog`, `demo` добавлены в allow-list policy локального overlay ([`deploy/test-data/policy/namespaces.yaml`](../deploy/test-data/policy/namespaces.yaml)).

Агент работает от ServiceAccount **`uamcsa`** (namespace `k8s-agent`). В каждом тестовом namespace создан **RoleBinding** `uamcsa-agent` → ClusterRole `k8s-agent` ([`deploy/test-data/uamcsa-rolebindings.yaml`](../deploy/test-data/uamcsa-rolebindings.yaml)).

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
kubectl logs -n default logger-a -f
kubectl logs -n payments deploy/billing-api -f
kubectl logs -n catalog deploy/indexer -f
kubectl logs -n demo deploy/worker -f
```

Pod'ы с label `app=test` используются шаблоном `logs-collect` в mock-core UI.

### 4.4 Ручное создание (legacy)

Если нужен только минимальный набор в `default`:

```powershell
kubectl run test-nginx --image=nginx:1.27 --labels=app=test -n default
kubectl run test-busybox --image=busybox:1.36 --labels=app=test --command -- sh -c "while true; do echo hello; sleep 5; done" -n default
```

---

## 5. Тестирование агента через mock-core UI

Основной способ ручного E2E: веб-UI имитирует Java **core-client** — отправляет команды в Kafka и показывает ответы агента.

### 5.1 Подготовка окружения

**Одна команда** (рекомендуется):

```powershell
make dev-up
```

Поднимает compose (Kafka, MinIO, mock-core UI), kind-кластер с агентом и test pod'ы `test-nginx` / `test-busybox`.

**Проверьте, что агент готов** (в отдельном терминале):

```powershell
kubectl get pods -n k8s-agent
kubectl get pods -n k8s-agent -l k8s-agent/leader=true
kubectl rollout status deployment/k8s-agent -n k8s-agent
```

Ожидаемо:

- 2 pod'а `k8s-agent-*` в статусе **Running** и **Ready** (1/1)
- ровно один pod с label `k8s-agent/leader=true`
- `docker compose ps` — сервисы `redpanda`, `minio`, `mock-core-ui` в **running**

Откройте в браузере: **http://localhost:8090**

> Если `make dev-up` падает на kind с «cluster is not reachable» — bootstrap автоматически пересоздаёт сломанный кластер. Вручную: `kind delete cluster --name k8s-agent` и снова `make dev-up`.

### 5.2 Интерфейс UI

| Вкладка | Назначение |
| --- | --- |
| **Commands** | Шаблон команды, JSON, поле **Reply topic**, кнопка **Send command** |
| **Responses** | Live-лента ответов из Kafka; фильтр по **Correlation ID** |
| **S3 Check** | Проверка объекта в MinIO после `logs.collect` |

Поток работы:

```text
Commands: выбрать шаблон → Send command
    ↓
UI публикует в k8s.commands.request (correlation_id + reply_topic)
    ↓
k8s-agent (leader) обрабатывает → пишет ответ в reply topic
    ↓
Responses: сообщение с тем же correlation_id
```

После **Send command** UI автоматически переключается на **Responses** и подписывается на reply topic (`core-client.dev.responses` по умолчанию).

**Успешный ответ** — JSON со `"status": "completed"`. Ошибки: `"status": "failed"` или `"rejected"` — смотрите поле `error` / `message` в теле.

### 5.3 Быстрые шаблоны для smoke-теста

После `make dev-up` в dropdown **Template** доступны готовые JSON. Рекомендуемый порядок «за 2 минуты»:

| # | Шаблон | Что проверяет | Ожидание в Responses |
| --- | --- | --- | --- |
| 1 | `k8s-api-list-services` | Kafka + apiserver, list в `default` | `"status": "completed"`, JSON `items` |
| 2 | `k8s-api-list-pods` | list pod'ов с `app=test` | `test-nginx`, `test-busybox` в теле |
| 3 | `k8s-api-list-deployments` | list Deployment'ов | JSON `items` из `default` |
| 4 | `cache-put` → `cache-delete` | cache write + delete | оба `"status": "completed"` |
| 5 | `k8s-api-list-rolebindings-all` | RBAC: все RoleBinding в кластере | `uamcsa-agent` в `default`, `payments`, … |

Шаблоны 1–3 — тип `k8s.api`, достаточно **Send command** без дополнительных настроек.

> **Почему не `/api/v1/namespaces`:** у ServiceAccount агента нет cluster-scoped права `list namespaces` (см. `deploy/base/clusterrole.yaml`). Запросы только к ресурсам в allow-list namespace'ах (`default`, `k8s-agent`).

### 5.4 Базовый smoke-test: k8s.api

Проверяет, что агент читает Kafka и ходит в kube-apiserver.

1. Вкладка **Commands**
2. **Template** → `k8s-api-list-services (k8s.api)` — самый быстрый первый запрос (list Service в `default`)
3. **Reply topic** — оставьте `core-client.dev.responses`
4. Нажмите **Send command**
5. Вкладка **Responses** — через несколько секунд появится сообщение с вашим `correlation_id`
6. В теле ответа: `"status": "completed"`, в `body` — JSON от apiserver

Для проверки test pod'ов выберите `k8s-api-list-pods` — в ответе должны быть `test-nginx` и `test-busybox`.

Если ответа нет более 1–2 минут — см. [§13 Troubleshooting](#13-troubleshooting).

### 5.5 logs.collect + проверка S3

Нужны test pod'ы с label `app=test` (создаёт `make dev-up`).

1. **Commands** → шаблон `logs-collect (logs.collect)`
2. **Send command**
3. В **Responses** дождитесь `"status": "completed"` — в ответе будут `s3_bucket` и `s3_key`
4. Вкладка **S3 Check** — bucket и key подставятся автоматически из ответа
5. **Check object** — должны увидеть размер файла и ссылку **Open in MinIO Console**

Альтернатива: http://localhost:9001 → bucket `logs-bundles` → key `logs/test/bundle.zip`.

### 5.6 watch.subscribe — события pod'ов

1. Вкладка **Responses**
2. **Topic** → `cluster.events`
3. **Correlation ID** — очистите (слушаем все события)
4. **Connect stream**
5. Вкладка **Commands** → шаблон `watch-subscribe-pods (watch.subscribe)` → **Send command**
6. В другом терминале создайте или удалите pod:

```powershell
kubectl run demo-pod --image=nginx:1.27 --labels=app=test -n default
kubectl delete pod demo-pod -n default
```

7. В **Responses** на topic `cluster.events` появятся события ADDED/DELETED

### 5.7 cache.put + HTTP GET

1. **Commands** → `cache-put (cache.put)` → **Send command**
2. В **Responses** — `"status": "completed"`
3. В **отдельном терминале** — port-forward на HTTP API leader pod:

```powershell
kubectl port-forward -n k8s-agent svc/k8s-agent-http 8080:8080
```

4. Проверка кэша:

```powershell
curl http://localhost:8080/v1/cache/feature/payments/new-checkout
```

Ожидаемо: `"value": "enabled"`.

### 5.8 health.report — периодические snapshots

1. **Responses** → **Topic** `cluster.health` → **Connect stream**
2. **Commands** → `health-report-start (health.report.start)` → **Send command**
3. В **Responses** каждые ~60 с приходят snapshot'ы pod'ов из allow-list namespace'ов

Остановка (отдельная команда через UI — отредактируйте JSON или используйте CLI): шаблона `health.report.stop` в fixtures пока нет; для локального теста достаточно перезапуска pod'а агента.

### 5.9 Сводка сценариев

| Что проверяем | Шаблон в UI | Topic в Responses | Дополнительно |
| --- | --- | --- | --- |
| Быстрый smoke | `k8s-api-list-services` | reply topic (авто) | — |
| Test pod'ы | `k8s-api-list-pods` | reply topic (авто) | нужен `make dev-up` |
| Kafka + apiserver | `k8s-api-list-deployments` | reply topic (авто) | — |
| Сбор логов → S3 | `logs-collect` | reply topic (авто) | **S3 Check** |
| Watch pod'ов | `watch-subscribe-pods` | `cluster.events` | создать/удалить pod |
| Cache write / delete | `cache-put`, `cache-delete` | reply topic (авто) | port-forward :8080 |
| RBAC RoleBindings | `k8s-api-list-rolebindings-all` | reply topic (авто) | ищите `uamcsa-agent` |
| Health snapshots | `health-report-start` | `cluster.health` | подождать ~60 с |

### 5.10 Запуск UI без Docker (опционально)

```powershell
make mock-core-ui
# или:
go build -o bin/mock-core-ui ./hack/mock-core-ui

$env:KAFKA_BROKERS="localhost:9092"
$env:S3_ENDPOINT="http://localhost:9000"
$env:FIXTURES_DIR="test/fixtures"
.\bin\mock-core-ui.exe
```

UI доступен на http://localhost:8090. Compose и kind должны быть уже запущены.

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

Кратко: `make dev-up` → http://localhost:8090 → шаблон `k8s-api-list-deployments` → **Send command** → ответ на вкладке **Responses**.

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
curl http://localhost:8080/v1/cache/feature/payments/new-checkout
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

Создайте/удалите pod в `default` — в терминале 1 появятся события в `cluster.events`.

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

Каждые 60s в `cluster.health` приходит snapshot pod'ов.

### 7.2.6 agent.lifecycle (failover)

Терминал 1:

```bash
bin/mock-core -listen -topic agent.lifecycle
```

Терминал 2 — chaos smoke:

```bash
bash hack/chaos-leader-failover.sh
```

Ожидаемо: событие `agent.started` / `agent.leader.changed` при смене leader.

### 7.2.7 Слушать только reply topic

```bash
bin/mock-core -listen -reply-topic core-client.dev.responses
```

---

## 8. Альтернатива: агент на хосте (без leader election)

Если kind не нужен, но есть kubeconfig на реальный/локальный кластер:

```bash
docker compose up -d

POLICY_FILE=deploy/base/policy/policy.yaml \
POLICY_NAMESPACES_FILE=deploy/base/policy/namespaces.yaml \
KAFKA_BROKERS=localhost:9092 \
S3_ENDPOINT=http://localhost:9000 \
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
| `POLICY_FILE` | `/etc/k8s-agent/policy/policy.yaml` | allow-list |
| `LEADER_ELECTION_ENABLED` | `true` in cluster | Lease election |

Полный список — `internal/config/config.go`.

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
kubectl rollout restart deployment/k8s-agent -n k8s-agent
kubectl rollout status deployment/k8s-agent -n k8s-agent
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
kubectl get pods -n k8s-agent -o wide
kubectl get lease -n k8s-agent
kubectl logs -n k8s-agent -l app=k8s-agent --tail=30
```

| Симптом | Что проверить |
| --- | --- |
| Pod'ы не `Running` | `kubectl describe pod -n k8s-agent -l app=k8s-agent` |
| Нет Lease / holder | Kafka недоступен → `docker compose ps`, логи агента |
| Pod Running, но нет label `k8s-agent/leader=true` | После обновления RBAC: `kubectl apply -k deploy/overlays/local` и `kubectl rollout restart deployment/k8s-agent -n k8s-agent` |
| Label есть, selector не находит | Убедитесь в точном синтаксисе: `-l k8s-agent/leader=true` |

Проверка label на всех pod'ах:

```powershell
kubectl get pods -n k8s-agent --show-labels
```

Leader может работать через Lease **без** label, пока patch RBAC не применён; HTTP Service `k8s-agent-http` (cache API) **требует** label на leader.

### Pod'ы агента не Ready

```bash
kubectl describe pod -n k8s-agent -l app=k8s-agent
kubectl logs -n k8s-agent -l app=k8s-agent --tail=50
```

Частые причины:

- Redpanda/MinIO не запущены (`docker compose ps`)
- `host.docker.internal` недоступен из kind (на Linux иногда нужен `extra_hosts` в kind config)

### mock-core: timeout waiting for reply

- Агент не leader → проверьте Lease и logs
- Policy denied → проверьте `allowed_command_types` в ConfigMap `k8s-agent-policy`
- Неверный reply topic → по умолчанию `core-client.dev.responses`

### logs.collect: bucket not found

Создайте bucket `logs-bundles` в MinIO (шаг 2).

### Policy rejected

In-cluster policy берётся из ConfigMap `k8s-agent-policy`.  
Overlay `deploy/overlays/local` монтирует полный файл `deploy/base/policy/policy.yaml`.

После изменения policy:

```bash
kubectl apply -k deploy/overlays/local
kubectl rollout restart deployment/k8s-agent -n k8s-agent
```

### Windows

| Симптом | Решение |
| --- | --- |
| `make` не найден | Команды из Makefile вручную или `choco install make` |
| `go` не найден при `make mock-core` | Установить Go или собрать mock-core через Docker ([§6](#6-cli-mock-core-альтернатива)) |
| `bootstrap.sh` / bash не найден | `powershell -File hack\bootstrap.ps1` или обновлённый `make kind-up` |
| `grep` не распознано | В PowerShell: `Select-String "cache.put"` вместо `grep cache.put` |
| `host.docker.internal` | Поддерживается Docker Desktop на Windows |

- `make kind-up` → `hack/bootstrap.ps1` (bash не нужен)
- Go: `go build -o bin\mock-core.exe .\hack\mock-core`

---

## 14. Быстрый чеклист «всё поднять с нуля»

### Одна команда (рекомендуется)

```powershell
make dev-up
# или: powershell -File hack\dev-up.ps1
```

Поднимает: compose (Kafka, MinIO, mock-core UI, bucket `logs-bundles`) → kind + agent → test pods.

**Smoke-тест после запуска:**

1. http://localhost:8090
2. **Commands** → `k8s-api-list-deployments` → **Send command**
3. **Responses** → `"status": "completed"`

Подробнее — [§5](#5-тестирование-агента-через-mock-core-ui).

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
kubectl kustomize deploy/overlays/local | Select-String "cache.put"
```

Все команды должны завершиться успешно.

### Шаг 1–5 — запуск

```powershell
# 1. Инфра (Kafka, MinIO, mock-core UI, bucket logs-bundles)
docker compose up -d
docker compose ps
# → mock-core UI: http://localhost:8090

# 2. Кластер + агент
make kind-up
# или: powershell -File hack\bootstrap.ps1

# 3. Проверка агента
kubectl get pods -n k8s-agent
kubectl get pods -n k8s-agent -l k8s-agent/leader=true

# 4. Test pods
kubectl run test-nginx --image=nginx:1.27 --labels=app=test -n default
kubectl run test-busybox --image=busybox:1.36 --labels=app=test --command -- sh -c "while true; do echo hello; sleep 5; done" -n default

# 5. Smoke через UI
# Откройте http://localhost:8090 → шаблон k8s-api-list-deployments → Send command
```

Linux / macOS — те же шаги, пути через `/` и `bin/mock-core`.

---

## Ссылки

- План и контракты Kafka: [`.cursor/plans/k8s_kafka_agent_bbf1d29a.plan.md`](../.cursor/plans/k8s_kafka_agent_bbf1d29a.plan.md)
- Архитектура: [`docs/architecture-core-client-k8s-agent.md`](architecture-core-client-k8s-agent.md)
- Prod overlay (mTLS): [`deploy/overlays/prod/`](../deploy/overlays/prod/)
