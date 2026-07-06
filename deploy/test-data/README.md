# Локальные тестовые манифесты Kubernetes

Демо-workload'ы и RBAC для ручного E2E в kind-кластере.

## Применение

```bash
kubectl apply -k deploy/test-data
# или:
make seed-test-data
```

Policy namespace allow-list для агента (local overlay):

```bash
kubectl apply -k deploy/overlays/local
```

## Файлы

| Файл | Содержимое |
| --- | --- |
| `kustomization.yaml` | Kustomize entrypoint |
| `namespaces.yaml` | Namespace `payments`, `catalog`, `demo` |
| `default.yaml` | Deployment `web`, `api`; Pod `logger-a`, `logger-b` |
| `payments.yaml` | Deployment `billing-api`; Pod `logger` |
| `catalog.yaml` | Deployment `products`, `indexer`; Pod `logger` |
| `demo.yaml` | Deployment `worker`; Pod `logger` |
| `uamcsa-rolebindings.yaml` | RoleBinding `uamcsa-agent` → SA `uamcsa` (ns `k8s-agent`) |
| `policy/namespaces.yaml` | Allow-list namespace'ов для local overlay |

> **Синхронизация:** `deploy/overlays/local/policy/namespaces.yaml` должен совпадать с `policy/namespaces.yaml` (kustomize overlay не может ссылаться на файлы вне своей директории).

## Метки

Все объекты: `app.kubernetes.io/part-of: test-data`.

Pod'ы с `app=test` — для шаблона `logs-collect` в mock-core UI.

Deployment'ы на busybox пишут логи в stdout (для `logs.collect` / `kubectl logs`).

## Связанные артефакты

- JSON-команды для mock-core UI: [`test/fixtures/`](../../test/fixtures/)
- Local overlay агента: [`deploy/overlays/local/`](../overlays/local/)
