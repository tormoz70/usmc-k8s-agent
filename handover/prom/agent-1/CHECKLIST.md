# Чеклист выката — Agent 1 (v1)

## До apply

- [ ] Выбран **только** Agent 1 (Agent 2 не ставится в этот кластер)
- [ ] `CLUSTER_ID` уникален в Core / не занят другим агентом
- [ ] `KAFKA_BROKERS` указывают на ПРОМ-брокеры
- [ ] Образ агента: registry + tag/digest (не `k8s-agent:dev`)
- [ ] Kafka mTLS: ca.crt, tls.crt, tls.key подставлены (не `.placeholder`)
- [ ] `k8s-agent-http-token` и `k8s-agent-internal-token` — боевые значения
- [ ] `deploy/base/policy/namespaces.yaml` — allow-list площадки
- [ ] RBAC / NetworkPolicy согласованы с security-командой
- [ ] `kubectl kustomize .` без ошибок; манифест просмотрен

## После apply

- [ ] Pods `ingress`, `egress`, `agent-service` Ready в `uamc-agent`
- [ ] **Нет** DaemonSet `logs-node-agent`
- [ ] Leader election: один leader у agent-service
- [ ] REGISTER принят Core: `agent_implementation=v1`, `logs_backend=api`
- [ ] Smoke: `k8s.api` list namespaces/pods
- [ ] Smoke: `logs.collect` (GetLogs) → объект в S3 / ответ completed
- [ ] Повторный REGISTER того же `CLUSTER_ID` → `AgentAlreadyRegistered`

## Откат

- [ ] `kubectl delete -k .` либо откат на предыдущий image tag
- [ ] Core: deregister / очистка записи кластера (по runbook Core)
