# План K8s Agent (Go) — todo_agent.md

> **Статус:** утверждён, реализация в каталоге `k8s-agent/`  
> **Стек:** Go, Kafka (NUP), Kubernetes API, S3 presigned URL

---

## 1. Контекст и базовые решения

На основе [docs/k8s-agent-architecture-nup.md](docs/k8s-agent-architecture-nup.md), [docs/mvp-plan.md](docs/mvp-plan.md), [docs/design-draft.md](docs/design-draft.md):

| Решение | Значение |
|---|---|
| Язык / деплой | Go, in-cluster Deployment, 1 агент = 1 k8s-кластер |
| Kafka-модель | **NUP**: `commands.in` → `commands.results` + `cluster.events`, at-least-once |
| Идемпотентность | На стороне core (агент всегда публикует result) |
| Commit offset | Только после успешной публикации в `commands.results` или `commands.dlq` |
| S3 | **Presigned URL приходит в Kafka-команде** (PUT для upload); агент не хранит S3-креды |
| Состояние | Агент stateless: watch/log-stream подписки in-memory; core переотправляет при рестарте |

### Маппинг 5 функций на команды

| # | Функция | command.type | Выход |
|---|---|---|---|
| 1 | Перечень YAML определённого kind | `resource.list` | `commands.results` |
| 2 | Скачать все YAML в S3 | `file.fetch` (`source=resource_export`) | `commands.results` с `s3_uri` |
| 3 | Скачать логи по списку контейнеров | `file.fetch` (`source=pod_logs_batch`) | `commands.results` с `s3_uri` |
| 4 | Подписка на логи с обработкой по паттерну | `logs.stream.subscribe` / `logs.stream.unsubscribe` | `cluster.events` (`LOG_LINE`) |
| 5 | Подписка на изменения YAML | `watch.subscribe` / `watch.unsubscribe` | `cluster.events` (`ADD/UPDATE/DELETE`) |

---

## 2. Kafka-топики

| Топик | Направление | Назначение |
|---|---|---|
| `commands.in` | Core → Agent | Команды |
| `commands.results` | Agent → Core | Результаты |
| `cluster.events` | Agent → Core | Watch + log-stream |
| `commands.dlq` | Agent → Core | Poison messages |

---

## 3. Реализация по фазам

### Phase 0 — Scaffold ✅

- [x] Go module `github.com/usmc/k8s-agent`
- [x] `internal/config`, `internal/command`, `internal/kafka`, `internal/health`
- [x] `cmd/agent/main.go`

### Phase 1 — resource.list ✅

- [x] Dynamic client + RESTMapper (`internal/k8s`)
- [x] Policy engine (`internal/policy`)
- [x] `resource.list` handler + sanitize
- [x] Result publisher
- [x] Unit tests: validator, policy, sanitize

### Phase 2 — S3 export ✅

- [x] Presigned PUT uploader (`internal/s3`)
- [x] `file.fetch` + `resource_export` pipeline
- [x] Deploy manifests + RBAC (`deploy/manifests.yaml`)

### Phase 3 — Batch logs ✅

- [x] `pod_logs_batch` source
- [x] Worker pool, partial_errors, zip layout

### Phase 4 — Watch ✅

- [x] Leader election (`internal/leader`)
- [x] WatchManager + SharedInformer + JSON diff
- [x] `watch.subscribe` / `watch.unsubscribe`
- [x] Publisher `cluster.events`

### Phase 5 — Log stream ✅

- [x] LogStreamManager + regex filter
- [x] `logs.stream.subscribe` / `logs.stream.unsubscribe`
- [x] Follow reconnect with backoff

### Phase 6 — Hardening ✅

- [x] Prometheus metrics (`internal/metrics`)
- [x] Graceful shutdown (in-flight wait)
- [x] DLQ for poison messages
- [x] Rate limiting k8s API (`internal/ratelimit`)
- [x] Unit/integration tests (s3, local archive, logstream pattern)

---

## 4. Структура проекта

```
k8s-agent/
├── cmd/agent/main.go
├── internal/
│   ├── agent/service.go
│   ├── config/
│   ├── kafka/
│   ├── command/
│   ├── policy/
│   ├── handlers/resource|file|watch|logs/
│   ├── watchmanager/
│   ├── logstream/
│   ├── k8s/
│   ├── local/
│   ├── s3/
│   ├── result/
│   ├── leader/
│   ├── metrics/
│   ├── ratelimit/
│   └── health/
├── deploy/manifests.yaml
├── Dockerfile
└── README.md
```

---

## 5. Контракты команд (кратко)

### resource.list

```json
{
  "type": "resource.list",
  "target": {"group": "networking.istio.io", "version": "v1beta1", "kind": "VirtualService", "namespace": "app"},
  "payload": {"labelSelector": "app=x", "output_format": "yaml", "limit": 500}
}
```

### file.fetch (resource_export)

```json
{
  "type": "file.fetch",
  "payload": {
    "source": "resource_export",
    "source_params": {"gvk": {"group": "...", "version": "v1beta1", "kind": "VirtualService"}, "namespaces": ["app"]},
    "destination": {"presigned_put_url": "https://...", "content_type": "application/gzip", "object_key": "exports/vs.tar.gz", "s3_uri": "s3://exports/vs.tar.gz"},
    "local_processing": ["tar", "gzip"]
  }
}
```

### file.fetch (pod_logs_batch)

```json
{
  "type": "file.fetch",
  "payload": {
    "source": "pod_logs_batch",
    "source_params": {
      "targets": [{"namespace": "app", "pod": "p1", "container": "c1", "previous": false}],
      "since_seconds": 3600
    },
    "destination": {"presigned_put_url": "https://...", "content_type": "application/zip"}
  }
}
```

### logs.stream.subscribe

```json
{
  "type": "logs.stream.subscribe",
  "payload": {
    "subscription_id": "log-sub-1",
    "targets": [{"namespace": "app", "pod": "p1", "container": "c1"}],
    "pattern": "ERROR|Exception",
    "pattern_type": "regex",
    "case_insensitive": true,
    "follow": true
  }
}
```

### watch.subscribe

```json
{
  "type": "watch.subscribe",
  "payload": {
    "subscription_id": "sub-1",
    "gvk": {"group": "networking.istio.io", "version": "v1beta1", "kind": "VirtualService"},
    "namespace": "app",
    "event_filter": ["ADD", "UPDATE", "DELETE"]
  }
}
```

---

## 6. Запуск

```bash
cd k8s-agent
go mod tidy
go test ./...
go build -o bin/k8s-agent ./cmd/agent
kubectl apply -f deploy/manifests.yaml
```

Переменные окружения: `KAFKA_BROKERS`, `KAFKA_TOPIC_*`, `POLICY_FILE`, `LEADER_ELECTION_ENABLED`, `TEMP_DIR`.

---

## 7. Открытые вопросы

1. Presigned URL TTL для long-running export
2. Log stream при rollout: labelSelector vs переотправка subscribe от core
3. Порог inline list (по умолчанию 5 МБ)
4. Rate limits и sizing партиций Kafka
5. Schema Registry на external Kafka
