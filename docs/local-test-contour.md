# Локальный тестовый контур

Гибридная схема для разработки и ручного E2E-тестирования **usmc-k8s-agent**.

> Агент по design работает **in-cluster** (ServiceAccount, RBAC, leader election).  
> Полностью в одном `docker-compose.yml` его не поднять — compose даёт только Kafka и S3, кластер — отдельно (kind).

## Схема

```text
┌─────────────────────────────────────────────────────────────┐
│  Хост (Windows / Linux / macOS)                             │
│                                                             │
│  docker compose                                             │
│    ├── Redpanda (Kafka)      :9092                          │
│    ├── MinIO (S3)            :9000 / console :9001          │
│    └── Kafka UI              :8088                          │
│                                                             │
│  mock-core (CLI)  ──► k8s.commands.request                  │
│                     ◄── reply_topic (header)                │
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
| Java core-client | `hack/mock-core` | хост |
| mTLS Kafka | не используется локально | — |

---

## 1. Требования и проверка окружения

Перед запуском убедитесь, что все инструменты установлены и доступны в **PATH** текущего терминала.

| Инструмент | Обязателен | Назначение |
| --- | --- | --- |
| **Docker Desktop** / Docker Engine + Compose | да | Redpanda, MinIO, kind, сборка образа агента |
| **kind** | да | локальный Kubernetes |
| **kubectl** | да | деплой и отладка |
| **Go 1.22+** | да* | `mock-core`, локальная сборка (`go test`) |
| **make** | нет | удобные цели; на Windows без make — команды ниже |

\* **Go можно не ставить**, если собирать `mock-core` через Docker (см. [§5](#5-собрать-mock-core)).

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
| `go: не распознано` при `make mock-core` | Go не установлен / не в PATH | Установить с https://go.dev/dl/ , перезапустить терминал; или сборка mock-core через Docker ([§5](#5-собрать-mock-core)) |
| `make kind-up` → `CreateProcess ... bootstrap.sh failed` | bash недоступен | На Windows: `make kind-up` вызывает `hack/bootstrap.ps1`; или `powershell -File hack\bootstrap.ps1` |
| `kubectl apply -k` → `file ... is not in or below ...` | устаревший overlay | Обновите репозиторий; policy генерируется в `deploy/base`, не через `../../` в overlay |
| `kind: command not found` | kind не установлен | https://kind.sigs.k8s.io/docs/user/quick-start/#installation |
| Docker daemon not running | Docker не запущен | Запустите Docker Desktop |

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

Ожидаемо: сервисы `redpanda`, `minio`, `kafka-ui` в статусе **running**.

PowerShell — дополнительно проверить порты:

```powershell
docker compose ps
Test-NetConnection localhost -Port 9092   # Kafka
Test-NetConnection localhost -Port 9000   # MinIO S3
Test-NetConnection localhost -Port 8088   # Kafka UI
```

Проверка:

| Сервис | URL / порт | Учётные данные |
| --- | --- | --- |
| Kafka (Redpanda) | `localhost:9092` | без auth |
| MinIO S3 API | `http://localhost:9000` | `minioadmin` / `minioadmin` |
| MinIO Console | http://localhost:9001 | те же |
| Kafka UI | http://localhost:8088 | — |

### Создать bucket в MinIO

Для `logs.collect` нужен bucket `logs-bundles` (см. `test/fixtures/logs-collect.json`):

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

1. Создаёт kind-кластер `k8s-agent` (`hack/kind-config.yaml`)
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
kubectl get configmap k8s-agent-policy -n k8s-agent -o yaml | grep cache.put
```

PowerShell:

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

Для `logs.collect` и `health.report` создайте pod'ы в `default`:

```bash
kubectl run test-nginx --image=nginx:1.27 --labels=app=test -n default
kubectl run test-busybox --image=busybox:1.36 --labels=app=test \
  --command -- sh -c 'while true; do echo hello; sleep 5; done' -n default
```

Allow-list policy разрешает namespace `default` и `k8s-agent`.

---

## 5. Собрать mock-core

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

## 6. Отправка команд (smoke-тесты)

### 6.1 k8s.api — list deployments

```bash
bin/mock-core -file test/fixtures/k8s-api-list-deployments.json
```

Ответ приходит в stdout (ожидание до 2 мин). Статус `completed`, тело — JSON от apiserver.

### 6.2 cache.put + HTTP GET

```bash
bin/mock-core -file test/fixtures/cache-put.json
```

Проверка HTTP (после `port-forward` на `k8s-agent-http`):

```bash
curl http://localhost:8080/v1/cache/feature/payments/new-checkout
```

### 6.3 watch.subscribe

Терминал 1 — слушать события:

```bash
bin/mock-core -listen -topic cluster.events
```

Терминал 2 — подписка:

```bash
bin/mock-core -file test/fixtures/watch-subscribe-pods.json
```

Создайте/удалите pod в `default` — в терминале 1 появятся события в `cluster.events`.

### 6.4 logs.collect → MinIO

```bash
bin/mock-core -file test/fixtures/logs-collect.json
```

Проверьте объект в MinIO Console: bucket `logs-bundles`, key `logs/test/bundle.zip`.

### 6.5 health.report

Терминал 1:

```bash
bin/mock-core -listen -topic cluster.health
```

Терминал 2:

```bash
bin/mock-core -file test/fixtures/health-report-start.json
```

Каждые 60s в `cluster.health` приходит snapshot pod'ов.

### 6.6 agent.lifecycle (failover)

Терминал 1:

```bash
bin/mock-core -listen -topic agent.lifecycle
```

Терминал 2 — chaos smoke:

```bash
bash hack/chaos-leader-failover.sh
```

Ожидаемо: событие `agent.started` / `agent.leader.changed` при смене leader.

### 6.7 Слушать только reply topic

```bash
bin/mock-core -listen -reply-topic core-client.dev.responses
```

---

## 7. Альтернатива: агент на хосте (без leader election)

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

## 8. Переменные окружения (справочно)

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

## 9. Kafka UI

http://localhost:8088 — просмотр топиков, сообщений, consumer groups.

Полезные топики:

- `k8s.commands.request`
- `core-client.dev.responses`
- `cluster.events`
- `cluster.health`
- `logs.stream`
- `agent.lifecycle`

---

## 10. Пересборка после изменений кода

```bash
docker build -t k8s-agent:dev .
kind load docker-image k8s-agent:dev --name k8s-agent
kubectl rollout restart deployment/k8s-agent -n k8s-agent
kubectl rollout status deployment/k8s-agent -n k8s-agent
```

---

## 11. Остановка и очистка

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

## 12. Troubleshooting

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
| `go` не найден при `make mock-core` | Установить Go или собрать mock-core через Docker ([§5](#5-собрать-mock-core)) |
| `bootstrap.sh` / bash не найден | `powershell -File hack\bootstrap.ps1` или обновлённый `make kind-up` |
| kustomize security error на `../../` | `git pull`; policy в `deploy/base/kustomization.yaml` |
| `host.docker.internal` | Поддерживается Docker Desktop на Windows |

- `make kind-up` → `hack/bootstrap.ps1` (bash не нужен)
- Go: `go build -o bin\mock-core.exe .\hack\mock-core`

---

## 13. Быстрый чеклист «всё поднять с нуля»

### Шаг 0 — проверка окружения

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
# 1. Инфра
docker compose up -d
docker compose ps
# → создать bucket logs-bundles в http://localhost:9001

# 2. Кластер + агент
make kind-up
# или: powershell -File hack\bootstrap.ps1

# 3. Проверка агента
kubectl get pods -n k8s-agent
kubectl get pods -n k8s-agent -l k8s-agent/leader=true

# 4. Test pods
kubectl run test-nginx --image=nginx:1.27 --labels=app=test -n default

# 5. CLI + smoke
make mock-core
# или: go build -o bin\mock-core.exe .\hack\mock-core
bin\mock-core.exe -file test\fixtures\k8s-api-list-deployments.json
bin\mock-core.exe -file test\fixtures\cache-put.json
bin\mock-core.exe -file test\fixtures\watch-subscribe-pods.json
```

Linux / macOS — те же шаги, пути через `/` и `bin/mock-core`.

---

## Ссылки

- План и контракты Kafka: [`.cursor/plans/k8s_kafka_agent_bbf1d29a.plan.md`](../.cursor/plans/k8s_kafka_agent_bbf1d29a.plan.md)
- Архитектура: [`docs/architecture-core-client-k8s-agent.md`](architecture-core-client-k8s-agent.md)
- Prod overlay (mTLS): [`deploy/overlays/prod/`](../deploy/overlays/prod/)
