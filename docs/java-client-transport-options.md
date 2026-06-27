# Транспорт generic-операций Java-клиента: как это работает

> Статус: пояснение к открытому вопросу №1 из [java-client-design.md](java-client-design.md).
> Связанные документы: [java-client-design.md](java-client-design.md), [mvp-plan.md](mvp-plan.md), [design-draft.md](design-draft.md).

Документ детально объясняет, **как generic-операции (создать/изменить/удалить/получить ресурс)
доезжают от Java-приложения до kube-apiserver через Kafka**. Есть два принципиально разных способа;
разница в том, на каком «уровне» мы разрезаем стек fabric8.

## Сначала — как работает обычный fabric8 (без Kafka)

Когда вы пишете:

```java
client.apps().deployments().inNamespace("payments").withName("api").scale(3);
```

внутри fabric8 происходит конвейер:

1. **DSL** (`deployments().inNamespace().withName()`) — строит описание операции.
2. **Сериализация** — модель/патч превращается в JSON.
3. **Построение HTTP-запроса** — получается конкретный HTTP-вызов, например:
   `PATCH /apis/apps/v1/namespaces/payments/deployments/api/scale` с телом `{"spec":{"replicas":3}}`.
4. **HttpClient** — этот запрос реально уходит по сети на `https://<apiserver>`.
5. Ответ (status + JSON) поднимается обратно по стеку и десериализуется в POJO.

Ключевой факт: шаг 4 (HttpClient) у fabric8 **подключаемый через SPI**
(`io.fabric8.kubernetes.client.http.HttpClient.Factory`). Можно подменить именно его,
оставив шаги 1–3 и 5 нетронутыми.

## Вариант 1 — «HTTP-туннель поверх Kafka» (рекомендуемый)

Идея: **режем стек на шаге 4**. Мы пишем свой `HttpClient`, который не ходит по сети,
а заворачивает готовый HTTP-запрос в Kafka-сообщение.

### На стороне клиента (Java, в приложении)

Шаги 1–3 остаются стандартными fabric8 (вы используете обычный `KubernetesClient`/`IstioClient`).
Дальше вместо сетевого вызова наш `KafkaHttpClient`:

- берёт уже сформированный fabric8 `HttpRequest` — это просто `{method, path, query, headers, body}`,
  например `PATCH /apis/apps/v1/namespaces/payments/deployments/api/scale` + тело;
- кладёт его в конверт команды и шлёт в **request-топик**. `correlation_id` и `reply_topic` идут в
  **заголовках** Kafka-записи, тело — в JSON:

```text
Kafka headers: correlation_id=uuid-1, reply_topic=app-42.responses
```

```json
{
  "command_id": "uuid-1",
  "type": "k8s.api",
  "http": {
    "method": "PATCH",
    "path": "/apis/apps/v1/namespaces/payments/deployments/api/scale",
    "headers": {"Content-Type": "application/merge-patch+json"},
    "body": "{\"spec\":{\"replicas\":3}}"
  }
}
```

- создаёт `CompletableFuture` и кладёт в `Map<correlation_id, future>`;
- фоновый consumer на response-топике дожидается ответа с тем же `correlation_id`,
  восстанавливает из него `HttpResponse` (status + body) и **отдаёт обратно в fabric8**,
  как будто это пришло по сети.

Регистрируется это одной строкой в
`META-INF/services/io.fabric8.kubernetes.client.http.HttpClient$Factory` —
fabric8 сам подхватывает нашу фабрику с classpath.

### На стороне агента (Go, in-cluster)

Агент получает сообщение `type=k8s.api`, видит внутри готовый HTTP-запрос и **просто проигрывает его
против настоящего kube-apiserver** своим `ServiceAccount`'ом (это буквально REST-вызов
`PATCH /apis/.../scale`). Затем заворачивает HTTP-ответ apiserver'а в response-конверт и шлёт в `reply_topic`.

То есть агент здесь — **обратный прокси к kube-apiserver, фронтированный Kafka**.
Он почти ничего не знает про конкретные ресурсы; он гоняет HTTP-запросы.

> Принятые уточнения (см. [java-client-design.md](java-client-design.md)): прокси не «прозрачный», а
> **фильтрующий** — перед проксированием агент проверяет операцию против **allow-list** (verb × resource/GVK),
> а ответ **усекает** перед отправкой в Kafka (срез `managedFields`/`last-applied-configuration`, опц.
> `status`), чтобы не гонять «перегруженные» объекты. Это лечит избыточный трафик, оставляя весь fabric8-DSL.

### Поток целиком

```text
[Ваш код] → fabric8 DSL → JSON → HTTP-запрос → KafkaHttpClient → request topic
                                                                       │
                                                                  [Агент в кластере]
                                                                       │  тот же HTTP-запрос
                                                                       ▼
                                                              kube-apiserver (SA+RBAC)
                                                                       │
[Ваш код] ← POJO ← JSON ← HttpResponse ← KafkaHttpClient ← response topic ◄── ответ
```

### Что это даёт

- ➕ **Весь fabric8-DSL работает без изменений** — любые ресурсы, Istio, билдеры, типы.
  Вы пишете обычный fabric8-код, а он «магически» ходит через Kafka.
- ➕ Новые kube-API/CRD работают **без правок протокола** — это же просто другой path.
- ➕ Идеально ложится на «доступ = SA + RBAC»: агент прозрачен, граница — права его SA.
- ➖ На агенте нужен слой «HTTP-over-Kafka → apiserver» (разбор method/path, проксирование).
- ➖ `watch` (стриминг событий) и WebSocket-операции (`exec`, `port-forward`, follow-logs) —
  сложные, в MVP откладываем.
- ➖ Латентность: каждый вызов = round-trip через Kafka.

## Вариант 2 — «типизированный командный DSL» (не рекомендуемый как основной)

Идея: **режем стек выше — на шаге 1**. Мы НЕ используем `KubernetesClient` fabric8 для отправки.
Вместо этого пишем свой fluent-API, который строит **наш** командный конверт с явным типом операции
и целью (GVK), а payload — это сериализованная fabric8-модель.

```java
// наш собственный API, не fabric8 KubernetesClient
client.command()
    .type("workload.scale")
    .target("apps", "v1", "Deployment", "payments", "api")
    .payload(Map.of("replicas", 3))
    .execute();
```

Сообщение в Kafka выглядит «семантически», а не как HTTP:

```json
{
  "type": "workload.scale",
  "target": {"group":"apps","version":"v1","kind":"Deployment","namespace":"payments","name":"api"},
  "payload": {"replicas": 3}
}
```

Агент здесь не прокси, а **диспетчер по `type`**: для `workload.scale` вызывает свой обработчик,
который сам решает, что сделать с dynamic client.

- ➕ Полный контроль над разрешённым набором операций (естественный allow-list).
- ➕ Проще агент (нет эмуляции HTTP).
- ➖ **Нужно вручную воспроизводить эргономику fabric8** — каждую операцию
  (`apply/patch/delete/get/list/...` для каждого вида ресурса) добавлять руками.
  Это много работы и риск, что API будет «не как fabric8».
- ➖ Каждый новый ресурс/операция — правка протокола и кода с обеих сторон.

## Чем отличаются по сути

| | Вариант 1 (HTTP-туннель) | Вариант 2 (командный DSL) |
|---|---|---|
| Где «режем» fabric8 | На транспорте (HttpClient) | На уровне DSL (не используем клиент fabric8 для отправки) |
| Что в Kafka-сообщении | HTTP-запрос (method+path+body) | Семантическая команда (type+target+payload) |
| Роль агента | Прозрачный прокси к apiserver | Диспетчер своих обработчиков по `type` |
| API для разработчика | Настоящий fabric8 `KubernetesClient` | Наш собственный DSL |
| Переиспользование fabric8 | Максимальное (DSL целиком) | Только модель + сериализация |
| Набор операций | Любой, что поддерживает fabric8 | Только то, что мы явно реализовали |

## Почему рекомендуется гибрид

Generic-операции (CRUD/apply/patch/delete/get) лучше пустить по **Варианту 1** — мы почти бесплатно
получаем весь fabric8 и не тратим силы на переписывание DSL.

Но `logs.collect` (сбор по множеству подов → zip → S3 → ключ) — это **не один kube-API вызов**,
его нельзя выразить как «проксированный HTTP-запрос». Это оркестровка из многих чтений логов
плюс упаковка и загрузка в S3. Такие высокоуровневые задания естественно делать по **Варианту 2** —
отдельным типом команды со своим обработчиком на агенте.

Поэтому: generic — туннель (Вариант 1), задания — типизированные команды (Вариант 2),
в одной и той же паре request/response-топиков, разделение по полю `type`.
