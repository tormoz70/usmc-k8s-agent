# План MVP Kubernetes Kafka Agent

## Зафиксированные решения

- Базовый вариант: гибридный агент из [design-draft.md](design-draft.md), стартующий как императивный исполнитель команд с отдельным watcher-контуром.
- Scope безопасности: агент работает только внутри одного Kubernetes-кластера через in-cluster `ServiceAccount`; внешнее управление кластерами не поддерживается.
- Авторизация (решение): единственная граница доступа — `ServiceAccount` агента + `RBAC`. Подтверждения опасных операций (`require_confirmation`), per-issuer allow-list namespaces/verbs и `dry_run`-gate в MVP не вводим. Следствие: все клиенты, имеющие доступ к request-топику, разделяют один и тот же набор прав агента (мультиарендность регулируется отдельными агентами/`ServiceAccount`, а не политикой внутри агента).
- Секреты (решение): `Secret` полностью исключены из scope. Агент не читает, не пишет, не наблюдает и не публикует `Secret`; в watcher-потоке и в логах не должно быть содержимого `Secret`. RBAC агента не должен включать доступ к `secrets`.
- Ресурсы MVP: Kubernetes workloads/services/configmaps (без `secrets`) и основные Istio CRD: `VirtualService`, `DestinationRule`, `Gateway`, `AuthorizationPolicy`.
- Kafka (решение): versioned JSON-сообщения; модель request/response — `request topic` для команд и `response topic` для результата каждой команды. `max.message.bytes` топиков = 10 МБ.
- Обработка (решение): получили сообщение из request-топика → **сразу коммитим offset** → выполняем команду → публикуем ответ в response-топик. Это at-most-once по исполнению: при падении агента после коммита команда теряется и автоматически не переисполняется; повтор инициирует клиент (для идемпотентных операций это безопасно). Dual-write/transactional-outbox не вводим.
- Порядок: для входящих команд строгий порядок не требуется; для watcher-потока порядок важен и должен обеспечиваться ключами сообщений/партиционированием по ресурсу или pod/log stream.
- Сбор логов (решение): запрос логов может охватывать множество подов и все контейнеры (current + previous). Агент выполняет **асинхронный** сбор всех запрошенных логов, раскладывает их по структуре `deployment/pod/container-<state>-<timestamp>.log`, упаковывает в zip, загружает в S3 и в ответе возвращает ключ файла (bucket/key), а не сами логи.
- Масштабирование: все pod могут читать Kafka через consumer group, но watcher/reconcile контур должен иметь отдельный leadership/coordination.
- CRD для команд в MVP не вводим: команды выполняются из Kafka напрямую, статусы уходят в response topic, аудит обеспечивается логами/метриками/Kafka retention.
- Клиент (решение): на стороне приложения предоставляется Java-библиотека в стиле fabric8, которая работает с kube-api/Istio через Kafka. Детали — в [java-client-design.md](java-client-design.md).

## Целевая схема

```mermaid
flowchart LR
  RequestTopic["Kafka request topic (10MB)"] --> ConsumerGroup["Agent consumer group (commit on receive)"]
  ConsumerGroup --> CommandRouter["Command router by type"]
  CommandRouter --> Handlers["Resource handlers"]
  Handlers --> DynamicClient["K8s dynamic client and RESTMapper"]
  DynamicClient --> KubeApi["Kubernetes and Istio API (SA + RBAC)"]
  Handlers --> ResponseTopic["Kafka response topic"]

  CommandRouter --> LogJob["Async log collector"]
  LogJob --> S3["Zip bundle -> S3"]
  S3 --> ResponseTopic

  WatchLeader["Watcher leader"] --> Informers["Informers (no Secrets)"]
  Informers --> WatchStream["Kafka watcher stream"]
  Informers --> Metrics["Prometheus metrics"]
```

## Контракт MVP

- Входящая команда должна содержать `schema_version`, `command_id`, `correlation_id`, `reply_topic`, `type`, `issuer` (только для аудита/трассировки, не для авторизации), `target`, `payload`, `idempotency_key`, `timeout`, `issued_at`.
- `target` должен явно задавать `group`, `version`, `kind`, `namespace`, `name`.
- `type` маршрутизируется на handler, например `k8s.apply`, `k8s.patch`, `k8s.delete`, `k8s.get`, `istio.apply`, `workload.scale`, а также высокоуровневые задания, например `logs.collect`.
- `logs.collect` должен принимать выборку подов (или label-selector + namespace), флаг сбора `current`/`previous` контейнеров и опциональные `sinceTime`/`tailLines`/`limitBytes`; результат — ссылка на S3-объект, а не содержимое логов.
- Результат должен содержать `command_id`, `correlation_id`, `status`, `reason`, `resource_ref`, `observed_generation/resource_version` где применимо, `started_at`, `finished_at`, `error`; для `logs.collect` дополнительно `s3_bucket`, `s3_key`, `byte_size`, `file_count`, `partial_errors`.
- Watcher stream должен публиковать resource events/state changes по allow-list ресурсов (`Secret` исключён полностью); ключ сообщения должен сохранять порядок внутри одного ресурса или log stream.

## Этапы реализации

1. Обновить архитектурный документ [design-draft.md](design-draft.md): заменить открытые вопросы на принятые решения и добавить MVP-схему, контракт сообщений, security model и scaling model.
2. Инициализировать Go-проект и структуру пакетов: `cmd/agent`, `internal/kafka`, `internal/command`, `internal/handlers`, `internal/k8s`, `internal/logs`, `internal/s3`, `internal/watch`, `internal/result`, `deploy`.
3. Реализовать versioned JSON-модели команд/результатов, валидацию envelope и маршрутизацию по `type`.
4. Реализовать Kafka request consumer group на `segmentio/kafka-go` и response publisher: commit offset сразу после получения, затем выполнение и публикация ответа в `reply_topic`/response-топик (at-most-once, без DLQ).
5. Реализовать in-cluster Kubernetes client: dynamic client, RESTMapper discovery cache (с reload на `NoMatchError`), server-side apply/patch/delete helpers и базовые проверки namespace/resource. RBAC без доступа к `secrets`.
6. Реализовать handlers для MVP-ресурсов Kubernetes и Istio (`k8s.apply/patch/delete/get`, `workload.scale`, `istio.apply`). `workload.scale` — только абсолютное число реплик (идемпотентность).
7. Реализовать асинхронный сборщик логов (`logs.collect`): fan-out по подам × контейнерам × {current, previous}, раскладка в `deployment/pod/container-<state>-<timestamp>.log`, zip, загрузка в S3, ответ с ключом объекта; backpressure и лимиты размера на сбор.
8. Реализовать watcher-контур: informers для allow-list ресурсов (без `Secret`), публикация state/resource events, отдельный ordered key strategy для watcher stream.
9. Добавить deployment manifests: `ServiceAccount`, минимальный `Role/ClusterRole` (без `secrets`), `Deployment`, конфигурация (Kafka, S3), probes, metrics endpoint.
10. Добавить тесты: unit-тесты валидации/роутинга, handler tests с fake dynamic client, Kafka boundaries через интерфейсы, log-collector tests (layout/zip/S3 через интерфейс), watcher ordering tests для key strategy.

## Проверка готовности MVP

- Команда из request-топика валидируется, выполняется against Kubernetes/Istio API и публикует ответ в response-топик; границей доступа выступает `ServiceAccount` + `RBAC`.
- Дубликат команды не приводит к неконтролируемому повторному эффекту для идемпотентных операций (повтор инициирует клиент по таймауту).
- `logs.collect` асинхронно собирает логи по множеству подов/контейнеров (current + previous), упаковывает в zip, кладёт в S3 и возвращает ключ объекта.
- Watcher публикует события с сохранением порядка в пределах выбранного ключа; `Secret` отсутствует во всех потоках.
- RBAC агента не включает `secrets` и не шире фактически поддерживаемого набора операций MVP.
