# Драфт: Java-клиент к kube-api через Kafka (fabric8-style)

> Статус: черновик для обсуждения. Кода пока нет — фиксируем архитектуру клиента.
> Связанные документы: [design-draft.md](design-draft.md), [mvp-plan.md](mvp-plan.md).

## Задача

Библиотека на стороне приложения, которая даёт разработчику привычный fabric8-подобный API
(`KubernetesClient`/`IstioClient`), но **транспорт идёт не по HTTP к kube-apiserver, а через Kafka**
(request/response топики) до in-cluster агента. Агент уже исполняет операции своим
`ServiceAccount` + `RBAC`. У приложения **нет** kube-credentials — только доступ к Kafka.

Граница безопасности — `ServiceAccount` агента + `RBAC` (см. [mvp-plan.md](mvp-plan.md)).
Это намеренно делает агента шлюзом «доступ к kube-api по Kafka», ограниченным правами SA.

## Что взять из fabric8 (рекомендация)

Проверено на fabric8 kubernetes-client 7.7.0 (середина 2026):

| Компонент fabric8 | Берём? | Зачем / комментарий |
| --- | --- | --- |
| Модель ресурсов (`io.fabric8:kubernetes-client-api`, POJO `Pod`, `Deployment`, `Service`, `ConfigMap`, builders) | **Да** | Не переписываем POJO/билдеры руками; берём готовые типизированные модели и fluent-builders. |
| Istio-модель и DSL (`io.fabric8:istio-model`, `io.fabric8:istio-client`) | **Да** | `VirtualService`, `DestinationRule`, `Gateway`, `AuthorizationPolicy` уже есть, включая `serverSideApply`. |
| Сериализация (`io.fabric8.kubernetes.client.utils.Serialization` / `KubernetesSerialization`, Jackson) | **Да** | Единый JSON-формат payload с агентом, без расхождений схем. |
| **Подключаемый транспорт через SPI** `io.fabric8.kubernetes.client.http.HttpClient.Factory` (регистрация через `META-INF/services`) | **Да (ключевое)** | Позволяет подменить HTTP-бэкенд на Kafka и переиспользовать **весь** DSL `KubernetesClient`/`IstioClient` поверх Kafka. |
| Сетевой слой (TLS/auth/`Config`/прокси, okhttp/vertx/jetty/jdk бэкенды) | **Нет** | Нерелевантно: kube-credentials и TLS живут на агенте, не на клиенте. Наш `Config` — заглушка (фиктивный masterUrl). |
| WebSocket-фичи (`exec`/`attach`/`port-forward`/follow-logs) | **Нет в MVP** | Стриминг по WebSocket поверх Kafka — отдельная большая задача; в MVP кидаем `UnsupportedOperationException`. |
| Informers/watch (`SharedInformer`) | **Отложить** | Требует стриминга событий; в MVP клиент — request/response. См. фазу 2. |

Вывод: переиспользуем модель + сериализацию + DSL, а **меняем только транспорт** через штатную точку расширения fabric8 (`HttpClient.Factory`). Это и есть «fabric8, но через Kafka».

## Два варианта реализации транспорта

### Вариант 1 — HTTP-туннель поверх Kafka (через `HttpClient` SPI) — рекомендуемый

Реализуем `KafkaHttpClientFactory implements io.fabric8.kubernetes.client.http.HttpClient.Factory`
и `KafkaHttpClient`, который:

1. Сериализует исходящий fabric8 `HttpRequest` (method, path+query, нужные headers, body bytes)
   в конверт команды и шлёт в **request-топик**.
2. Ждёт ответ из **response-топика** по `correlation_id` (через `CompletableFuture`).
3. Восстанавливает `HttpResponse` (status, headers, body) для fabric8.

Регистрируем фабрику в `META-INF/services/io.fabric8.kubernetes.client.http.HttpClient$Factory`.
После этого `new KubernetesClientBuilder().build()` и `istio-client` работают **поверх Kafka без изменений вызывающего кода**:

```java
KubernetesClient client = new KubernetesClientBuilder()
    .withConfig(kafkaTunnelConfig())   // фиктивный masterUrl, реальная авторизация — на агенте
    .build();

client.apps().deployments().inNamespace("payments")
    .withName("api").scale(3);          // уходит командой в Kafka, агент исполняет
```

На стороне **агента** запрос трактуется как HTTP-вызов (verb + path + body) и проксируется
в in-cluster apiserver его `ServiceAccount`. Агент по сути — обратный прокси к kube-apiserver,
фронтированный Kafka и ограниченный RBAC.

- ➕ Максимальное переиспользование fabric8: весь DSL, Istio, типы, билдеры — «бесплатно».
- ➕ Минимум собственного протокола; новые kube-API работают без правок контракта.
- ➕ Хорошо ложится на решение «доступ = SA + RBAC» (прозрачный шлюз).
- ➖ На агенте нужен слой «HTTP-over-Kafka → apiserver» (разбор path/verb, проксирование).
- ➖ Watch/streaming и WebSocket-фичи — сложные, переносим в фазу 2.
- ➖ Латентность = round-trip через Kafka на каждый вызов.

### Вариант 2 — типизированный командный DSL поверх Kafka

Свой fluent-DSL, который строит командный конверт (`type` + `target` GVK + `payload`
= сериализованная fabric8-модель) и шлёт в Kafka; ответ — наш result-конверт.

- ➕ Полный контроль над набором операций; естественно ложатся высокоуровневые задания.
- ➕ Проще на стороне агента (нет эмуляции HTTP).
- ➖ Нужно вручную воспроизводить эргономику fabric8-DSL (много работы и риск расхождений).
- ➖ Каждую новую операцию добавляем руками.

### Рекомендация

**Гибрид с упором на Вариант 1:**

- Generic CRUD/apply/patch/delete/get/(list) — через **HTTP-туннель** (Вариант 1): переиспользуем
  fabric8-DSL целиком.
- Высокоуровневые задания, которые **не являются** простым kube-API вызовом
  (в первую очередь `logs.collect` → zip → S3 → ключ), — через **отдельный типизированный командный канал**
  (Вариант 2-стиль), с собственным fluent-API в нашей библиотеке.

То есть в одном клиенте две «дорожки» поверх тех же request/response топиков:
`type=k8s.api` (туннель) и `type=logs.collect` / будущие задания (типизированные команды).

## Протокол request/reply поверх Kafka

- Топики: `k8s.commands.request` + `reply_topic` на инстанс/приложение; `max.message.bytes = 10 МБ`.
- **Заголовки Kafka-сообщения (решение):** `correlation_id` и `reply_topic` передаются в **headers**
  Kafka-записи (классический request-reply pattern); агент отвечает в `reply_topic` из заголовка.
- Тело команды (JSON, согласовано с [mvp-plan.md](mvp-plan.md)):
  `schema_version`, `command_id`, `type`, `issuer` (аудит), `target`, `payload`,
  `idempotency_key`, `timeout`, `issued_at`.
- **Корреляция на клиенте:** in-memory `Map<correlation_id, CompletableFuture<Response>>`; фоновый
  consumer на `reply_topic` завершает future по `correlation_id` из заголовка; по истечении `timeout`
  future падает с таймаутом.
- **Доставка ответа (решение):** `reply_topic` на инстанс/приложение — агент отвечает в топик из заголовка.
- **Семантика обработки** (решение из mvp-plan): агент коммитит offset сразу при получении, затем исполняет
  и публикует ответ → at-most-once. Если ответа нет за `timeout`, клиент считает операцию неуспешной;
  повторный вызов — ответственность приложения (для идемпотентных операций безопасен).

## Усечение ответов (response shaping)

Чтобы не гонять по Kafka лишние данные «перегруженных» объектов, **агент усекает ответы перед отправкой**
(настройки — в конфиге агента):

- по умолчанию срезаются `metadata.managedFields` и аннотация
  `kubectl.kubernetes.io/last-applied-configuration` (главные источники «жира»);
- опционально (по конфигу) — `status` и другие неиспользуемые секции;
- поля, нужные клиенту/fabric8 (`resourceVersion`, `uid` и т.п.), **сохраняются**;
- для записи (`apply`/`patch`/`scale`) ответ можно усекать до `status + resourceVersion`;
- дополнительно: compression продюсера (`zstd`/`lz4`) и пагинация `LIST` (`limit`/`continue`);
- для чисто метаданных — `Accept: application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1`.

Важно: при Варианте 1 (туннель) усечение делает агента не «прозрачным», а «фильтрующим» прокси —
поэтому набор срезаемых полей задаётся конфигом и не должен ломать `resourceVersion`/optimistic concurrency.

```mermaid
sequenceDiagram
  participant App as App (Java client)
  participant Req as request topic
  participant Agent as in-cluster agent
  participant K8s as kube/Istio API
  participant Res as reply topic
  App->>Req: command {command_id, reply_topic, type, payload}
  Agent->>Req: poll + commit offset
  Agent->>K8s: execute via SA + RBAC
  K8s-->>Agent: result / status
  Agent->>Res: response {correlation_id, status, ...}
  Res-->>App: complete CompletableFuture by correlation_id
```

## Поток `logs.collect` (логи → zip → S3 → ключ)

- **Запрос:** выборка подов (явный список или label-selector + namespace), контейнеры = все
  (`current` + `previous`), опционально `sinceTime`/`tailLines`/`limitBytes`, и **креды S3**
  (endpoint S3 берётся из конфига агента; креды передаются в запросе).
- **Агент (асинхронно):** fan-out чтений логов по `pod × container × {current, previous}`
  (`previous` — terminated-логи, в fabric8 это `.terminated()` / REST `previous=true`).
  Раскладка во временную структуру:

  ```text
  <deployment>/<pod>/<container>-current-<timestamp>.log
  <deployment>/<pod>/<container>-previous-<timestamp>.log
  ```

  затем zip → загрузка в S3 (endpoint из конфига агента, креды из запроса; ключ напр. `logs/<yyyy>/<mm>/<dd>/<command_id>.zip`).
- **Ответ:** `status`, `s3_bucket`, `s3_key`, `byte_size`, `file_count`, `partial_errors[]`
  (часть подов могла не отдать логи — собираем best-effort и сообщаем об ошибках). Сами логи в Kafka не уходят.
- **Клиентский API:** отдельный билдер, напр.:

```java
LogBundleRef ref = client.logs()
    .inNamespace("payments")
    .selectByLabel("app", "api")
    .allContainers()
    .includePrevious(true)
    .sinceTime(Instant.now().minus(Duration.ofHours(1)))
    .collect();          // -> {bucket, key}; клиент скачивает zip из S3 сам
```

- Поскольку задание может быть долгим, у `logs.collect` отдельный (больший) `timeout`. Опция на будущее —
  двухфазный ответ: немедленный `accepted` + последующее `completed` с ключом (вынесено за MVP).

## Структура модулей (Maven, предложение)

```text
usmc-k8s-client/                 (parent pom)
  client-api/            // публичные интерфейсы, конверт команд/ответов, модели результатов
  client-transport-kafka/// Kafka producer/consumer, request/reply, корреляция, reply-topic
  client-fabric8/        // HttpClient.Factory SPI (KafkaHttpClient) + сборка KubernetesClient/IstioClient
  client-logs/           // высокоуровневый logs.collect builder + LogBundleRef
  examples/
```

Ключевые зависимости: `io.fabric8:kubernetes-client-api`, `io.fabric8:istio-client`,
`org.apache.kafka:kafka-clients`. Целевая Java — 11+ (как у fabric8).

## Принятые решения

1. **Транспорт generic-операций:** гибрид — HTTP-туннель (Вариант 1) для CRUD + типизированные команды
   для заданий, **с усечением ответов на агенте** (настройки усечения — в конфиге агента). Как это
   работает — в [java-client-transport-options.md](java-client-transport-options.md).
2. **Доставка ответов:** `reply_topic` и `correlation_id` передаются в **заголовках** запроса; агент
   отвечает в `reply_topic` из заголовка.
3. **Allow-list:** нужен. Поверх `RBAC` агент применяет конфигурируемый allow-list (verb × resource/GVK +
   допустимые типы команд); туннель — «фильтрующий» прокси, а не прозрачный.
4. **S3:** endpoint S3 — в конфиге агента (под него настраивается egress); креды S3 передаются в запросе
   `logs.collect`. Тип хранилища (AWS S3 / MinIO) — S3-совместимый по endpoint из конфига.
5. **Версия fabric8:** фиксируем `io.fabric8` = **7.7.0** (модель, istio-client, сериализация, HttpClient SPI).

## Остаётся открытым

- **Watch/informers на клиенте:** нужны ли в обозримой перспективе (тянет за собой стриминг событий и
  WebSocket), или клиент остаётся request/response? По умолчанию (рекомендация) — **откладываем в фазу 2**.
- **Безопасность кред S3 в сообщении:** креды летят в Kafka-payload → request-топик должен быть с
  ACL/шифрованием, креды короткоживущие и не должны попадать в логи агента. Presigned URL как
  альтернатива **не подходит**: egress дописывает свои заголовки, не входящие в подпись presigned-запроса,
  и S3 отвечает `SignatureDoesNotMatch`; поэтому возвращаем ключ объекта, клиент скачивает своими read-кредами.
```

