# Аудит безопасности сервиса agent (uamc-k8s-agent)

**Дата:** 2026-07-10  
**Объект:** компоненты `ingress`, `egress`, `agent-service` (и монолитный режим `AGENT_COMPONENT=all`)  
**Метод:** повторный анализ исходного кода, манифестов deploy и policy после первичного обзора

---

## Краткое резюме

С момента первичного аудита внедрены значимые меры: ingress блокирует `/internal/*`, internal API требует bearer token, policy engine усилен (Kind обязателен, wildcard namespace запрещён), добавлен Kafka CommandGuard (issuer/reply topic), feature flags через `features.yaml`, hardening контейнеров.

**Остаются открытыми:** избыточные RBAC на ingress/egress, обход namespace-allowlist для cluster-scoped list, проброс HTTP-заголовков в apiserver, опциональная auth cache/metrics в base deploy, отсутствие валидации `output_topic`, мёртвая конфигурация `LEADER_ONLY_COMMANDS`.

| Приоритет | Кол-во | Примеры |
|-----------|--------|---------|
| Высокий | 3 | SA token на ingress, plaintext Kafka (base), header forwarding |
| Средний | 6 | Cluster-list bypass, cache без token, internal API без Kafka guard |
| Низкий | 5 | LEADER_ONLY_COMMANDS, placeholder secrets, широкий egress NP |

---

## Архитектура и границы доверия

```
[Core Client] --Kafka--> [egress] --HTTP Bearer--> [agent-service leader] --> kube-apiserver
                              |                           |
                         CommandGuard                  policy engine
                         (issuer, reply)              + features.yaml
[Client pod] --HTTP--> [ingress :8080] --proxy--> [agent-service :8081]
                       (whitelist paths)
```

**Предполагаемая модель доверия:**

1. Команды поступают только от доверенных Kafka-продюсеров с валидным `issuer`.
2. Ответы уходят только в разрешённые `reply_topic`.
3. HTTP ingress — read-only доступ к cache/metrics для мониторинга.
4. YAML policy + RBAC ограничивают scope операций в кластере.

---

## Статус находок первичного аудита

| # | Находка (первичный аудит) | Статус | Комментарий |
|---|---------------------------|--------|-------------|
| 1 | Неаутентифицированный `/internal/v1/commands` через ingress | **ИСПРАВЛЕНО** | `ingressProxyAllowed()` блокирует `/internal/*`; тест `auth_test.go` |
| 2 | Обход policy на cluster-scoped путях | **ЧАСТИЧНО** | Kind теперь обязателен; cluster-list для allowed GVK всё ещё без namespace check |
| 3 | Избыточный SA `uamcsa` на ingress/egress | **ОТКРЫТО** | Все три deployment используют один SA + ClusterRoleBinding watch |
| 4 | Kafka как слабая граница доверия | **ЧАСТИЧНО** | CommandGuard + issuer/reply allowlist; base deploy — plaintext Kafka |
| 5 | Опциональный bearer token | **ЧАСТИЧНО** | Internal token обязателен; cache token опционален в base |
| 6 | Проброс заголовков в apiserver | **ОТКРЫТО** | Без изменений |
| 7 | Мёртвая конфигурация `LEADER_ONLY_COMMANDS` | **ОТКРЫТО** | Объявлена, не используется |
| 8 | Отсутствие securityContext | **ИСПРАВЛЕНО** | runAsNonRoot, readOnlyRootFilesystem, drop ALL, seccomp |

---

## Исправленные уязвимости (детали)

### F-FIX-01. Ingress блокирует internal API

**Файлы:** `internal/httpapi/auth.go:22-35`, `internal/httpapi/proxy.go:41-46`

Ingress пропускает только `/metrics`, `/v1/cache`, `/v1/cache/*`. Путь `/internal/v1/commands` возвращает 404.

```go
func ingressProxyAllowed(path string) bool {
    if strings.HasPrefix(path, "/internal/") {
        return false
    }
    // ...
}
```

### F-FIX-02. Internal API требует bearer token

**Файлы:** `internal/httpapi/internal.go:25-26`, `internal/httpapi/auth.go:8-18`, `internal/config/config.go:214-215`

- Пустой `HTTP_INTERNAL_BEARER_TOKEN` → API отключён (401).
- Startup validation: token **обязателен** для `agent-service` и `egress`.
- Bridge передаёт token: `internal/bridge/client.go:50-52`.

### F-FIX-03. Policy engine: обязательный Kind

**Файл:** `internal/policy/engine.go:207-214`

Пути без определяемого Kind отклоняются. Ранее `GET /api/v1/namespaces` обходил GVK-allowlist.

### F-FIX-04. Kafka CommandGuard

**Файлы:** `internal/kafka/guard.go`, `deploy/base/policy/policy.yaml:46-53`

Перед выполнением проверяются: `issuer`, `reply_topic`, `command_type`.

```yaml
allowed_issuers:
  - core-client
  - mock-core
allowed_reply_topics:
  - core-client.dev.responses
allowed_reply_topic_prefixes:
  - core-client.
```

### F-FIX-05. Feature flags

**Файл:** `deploy/base/policy/features.yaml`

9 capability groups (cluster_inventory, workload_manage, istio_manage, rbac_inspect, logs_collect, logs_stream, watch_events, health_report, cache). Каждая группа маппится на отдельный ClusterRole (см. `docs/rbac-features-capacity.md`).

### F-FIX-06. Container hardening

**Файлы:** `deploy/base/deployment-*.yaml`

```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 65532
  seccompProfile:
    type: RuntimeDefault
containers:
  - securityContext:
      allowPrivilegeEscalation: false
      readOnlyRootFilesystem: true
      capabilities:
        drop: [ALL]
```

---

## Открытые уязвимости

### F-01. Избыточный ServiceAccount на ingress/egress — **Высокий**

**Файлы:**
- `deploy/base/deployment-ingress.yaml:21`
- `deploy/base/deployment-egress.yaml:21`
- `deploy/base/clusterrole.yaml`
- `deploy/base/uamcsa-clusterrolebinding-watch.yaml`

Все компоненты используют `serviceAccountName: uamcsa`. Ingress — HTTP reverse proxy, K8s API не вызывает. Egress — Kafka consumer + HTTP bridge.

**RBAC `uamcsa`:**
- Namespace RoleBinding → ClusterRole `k8s-agent`: write Deployments, Istio CRDs, logs, leases, RBAC read.
- ClusterRoleBinding `uamcsa-watch` → cluster-wide watch Namespace/Pod/Deployment/DeploymentConfig.

**Сценарий атаки:**
1. RCE/SSRF в ingress pod.
2. Чтение `/var/run/secrets/kubernetes.io/serviceaccount/token`.
3. Cluster-wide watch всех Namespace/Pod/Deployment **без прохождения policy**.

**Рекомендация:** ingress — `automountServiceAccountToken: false` или отдельный SA без RBAC; egress — SA только для leases; agent-service — feature-scoped RBAC.

---

### F-02. Cluster-scoped list обходит namespace allowlist — **Средний**

**Файл:** `internal/policy/engine.go:197-201`

```go
if req.Namespace != "" {
    if err := e.allowNamespace(req.Namespace); err != nil {
        return err
    }
}
```

При пустом `Namespace` проверка namespace **пропускается**.

**Примеры проходящих запросов при `allowed_namespaces: [uamc-agent]`:**
- `GET /api/v1/namespaces` — **by design** (feature `cluster_inventory`)
- `GET /apis/apps/v1/deployments` — list Deployments **во всех** namespace
- `GET /apis/rbac.authorization.k8s.io/v1/rolebindings` — cluster-wide RBAC enumeration

Policy пропускает, если GVK в allowlist. Итог зависит от RBAC SA — при ClusterRoleBinding это полный cluster access.

**Рекомендация:** явный deny/allow для cluster-scoped list per GVK; prefer RoleBinding over ClusterRoleBinding.

---

### F-03. Проброс произвольных HTTP-заголовков в apiserver — **Высокий**

**Файлы:** `internal/k8s/client.go:98-99`, `internal/handlers/api/handler.go`

```go
for k, v := range headers {
    req.Header.Set(k, v)
}
```

Нет фильтрации `Impersonate-User`, `Impersonate-Group`, `Authorization`.

**Сценарий атаки:** атакующий с доступом к Kafka (valid issuer) отправляет `k8s.api` с impersonation headers. Текущий SA не имеет `impersonate` verb, но при расширении RBAC это станет критичным. Также риск неожиданного поведения apiserver.

**Рекомендация:** allowlist (`Accept`, `Content-Type`, `If-Match`); denylist impersonation/auth headers.

---

### F-04. Internal API не применяет Kafka guard — **Средний**

**Файл:** `internal/httpapi/internal.go:49`

Egress валидирует issuer/reply через `CommandGuard` перед bridge. Agent-service на `/internal/v1/commands` вызывает `router.Handle()` напрямую — **issuer и reply_topic не перепроверяются**.

**Сценарий атаки:** утечка `HTTP_INTERNAL_BEARER_TOKEN` (из pod egress/agent-service) → POST команд с произвольным issuer, минуя Kafka guard.

**Рекомендация:** применить CommandGuard на internal API path (defense in depth).

---

### F-05. Cache и metrics без auth в base deploy — **Средний**

**Файлы:** `internal/httpapi/server.go:123-135`, `internal/httpapi/auth.go:27-28`

| Endpoint | Auth (base) | Auth (prod overlay) |
|----------|---------------|---------------------|
| `/internal/v1/commands` | Bearer required | Bearer required |
| `/v1/cache` | **Опционально** | Bearer via secret |
| `/metrics` | **Нет** | **Нет** |
| `/healthz`, `/readyz` | Нет | Нет |

Ingress проксирует `/v1/cache` и `/metrics` на leader.

**Сценарий атаки:** pod в namespace с меткой `k8s-agent-access: "true"` → `GET /v1/cache/...` через ingress → чтение in-memory cache (операционные данные, feature flags).

**Рекомендация:** fail-closed при пустом `HTTP_BEARER_TOKEN`; закрыть `/metrics` от ingress или добавить auth.

---

### F-06. Kafka: plaintext в base, output_topic не валидируется — **Средний**

**Файлы:**
- `deploy/base/deployment-egress.yaml` — `host.docker.internal:9092` без TLS
- `internal/watch/payload.go`, `internal/logstream/manager.go`, `internal/healthreport/manager.go`

**Проблемы:**
1. Base/dev: Kafka PLAINTEXT → injection при слабых broker ACL.
2. `output_topic` для watch/logs/health **не проверяется** policy → exfil в произвольный topic при наличии Kafka ACL.
3. Prefix `core-client.` для reply topics — любой topic под префиксом разрешён.

**Рекомендация:** mTLS/SASL в prod (частично в prod overlay); валидация `output_topic` по allowlist.

---

### F-07. RBAC не следует за feature flags — **Средний**

**Файлы:** `deploy/base/policy/features.yaml`, `deploy/base/clusterrole.yaml`

Отключение feature в `features.yaml` блокирует handler в software, но SA `uamcsa` **сохраняет** все RBAC bindings. Defense-in-depth нарушен.

**Рекомендация:** split ClusterRole per feature; RoleBinding только для enabled features.

---

### F-08. Placeholder secrets в prod overlay — **Средний (операционный)**

**Файл:** `deploy/overlays/prod/kustomization.yaml`

```yaml
token=change-me-in-production
token=change-me-internal-in-production
```

Риск деплоя с дефолтными значениями.

---

### F-09. Мёртвая конфигурация LEADER_ONLY_COMMANDS — **Низкий**

**Файл:** `internal/config/config.go:71,168`

Переменная объявлена, **нигде не используется**. Leader enforcement через leader election + `state.IsLeader()`.

Оператор может ошибочно полагать, что `LEADER_ONLY_COMMANDS=false` отключает leader gating.

---

### F-10. NetworkPolicy egress без destination scope — **Низкий**

**Файл:** `deploy/base/networkpolicy.yaml:105-114`

Agent-service egress на `:443`, `:9092`, `:9000` — **любой** host. Приемлемо для dev; prod — сузить до apiserver/Kafka/S3 CIDR.

---

### F-11. KAFKA_COMMIT_ON_RECEIVE=true — **Низкий**

**Файл:** `internal/kafka/processor.go:58-61`

Commit до обработки → at-most-once; при сбое команда теряется. DoS/data-loss, не privilege escalation.

---

### F-12. automountServiceAccountToken на ingress — **Низкий**

Ingress не нуждается в K8s API token, но SA token монтируется по умолчанию.

---

## Матрица атак (актуальная)

| Вектор | Что получает атакующий | Сложность | Статус |
|--------|------------------------|-----------|--------|
| Pod в `k8s-agent-access` NS → ingress → `/internal/v1/commands` | — | — | **Закрыт** (404) |
| Pod в `k8s-agent-access` NS → ingress → `/v1/cache` | Чтение cache | Низкая | **Открыт** (base) |
| Write access к Kafka request topic | k8s-команды через policy | Средняя | Частично (guard) |
| Утечка internal bearer token | Команды без Kafka guard | Средняя | **Открыт** |
| Компромисс ingress pod | SA token → cluster watch | Средняя | **Открыт** |
| Cluster-scoped k8s.api list | Enumeration cross-namespace | Низкая* | **Открыт** |
| Impersonate headers в k8s.api | Зависит от RBAC | Средняя | **Открыт** |

\* при наличии любого пути к командам

---

## Положительные практики

1. **Разделение компонентов:** egress (Kafka) → bridge → agent-service (K8s API).
2. **Internal token enforced at startup** для sensitive components.
3. **Feature flags** с маппингом на RBAC roles.
4. **Watch namespace scoping** для namespaced kinds.
5. **Secrets denied** по умолчанию (`deny_secrets: true`).
6. **Тесты:** `auth_test.go`, `guard_test.go`, policy engine tests.
7. **Distroless nonroot** образ.
8. **`features-minimal.yaml`** — минимальный профиль для prod (не default).

---

## Рекомендации по приоритету

| # | Действие | Effort | Impact |
|---|----------|--------|--------|
| 1 | Split ServiceAccounts: ingress без token, egress leases-only, agent-service feature-scoped | High | Critical |
| 2 | Header allowlist на apiserver proxy | Low | High |
| 3 | CommandGuard на internal API path | Low | Medium |
| 4 | Deny cluster-list paths или explicit allow per GVK | Medium | Medium |
| 5 | Валидация `output_topic` для watch/logs/health | Medium | Medium |
| 6 | Require `HTTP_BEARER_TOKEN` в base (fail-closed) | Low | Medium |
| 7 | Default `features-minimal.yaml` в prod overlay | Low | Medium |
| 8 | Удалить или реализовать `LEADER_ONLY_COMMANDS` | Low | Low |
| 9 | `automountServiceAccountToken: false` на ingress | Low | Low |
| 10 | Replace placeholder secrets в prod | Low | Medium |

---

## Индекс ключевых файлов

| Область | Пути |
|---------|------|
| HTTP auth/proxy | `internal/httpapi/auth.go`, `internal.go`, `proxy.go`, `server.go` |
| Policy/features | `internal/policy/engine.go`, `path.go`, `deploy/base/policy/features.yaml`, `policy.yaml` |
| Kafka/bridge | `internal/kafka/guard.go`, `processor.go`, `internal/bridge/client.go` |
| K8s proxy | `internal/k8s/client.go`, `internal/handlers/api/handler.go` |
| Config | `internal/config/config.go`, `cmd/agent/main.go` |
| Deploy/RBAC | `deploy/base/deployment-*.yaml`, `clusterrole.yaml`, `clusterrole-watch.yaml`, `networkpolicy.yaml`, `uamcsa-*.yaml` |
| Документация RBAC | `docs/rbac-features-capacity.md` |

---

## История ревизий

| Дата | Автор | Изменения |
|------|-------|-----------|
| 2026-07-09 | Security review (initial) | Первичный аудит; критические находки по ingress/internal API |
| 2026-07-10 | Security review (re-audit) | Повторная проверка; зафиксированы исправления и оставшиеся риски |
