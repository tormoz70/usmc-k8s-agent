# Чеклист выката — Agent 2 (v2)

## До apply

- [ ] Выбран **только** Agent 2 (Agent 1 не ставится в этот кластер)
- [ ] `CLUSTER_ID` уникален в Core
- [ ] `KAFKA_BROKERS` — ПРОМ
- [ ] Образ (Deployments **и** DaemonSet) — registry + tag/digest
- [ ] Kafka mTLS файлы и HTTP/internal tokens — боевые
- [ ] Policy namespaces allow-list заполнен
- [ ] Согласованы hostPath `/var/log/pods`, PSA/PSP, SELinux
- [ ] Порт 8083 между agent-service и logs-node разрешён
- [ ] `kubectl kustomize .` — в манифесте есть DaemonSet `logs-node-agent`

## После apply

- [ ] Deployments Ready: ingress, egress, agent-service
- [ ] DaemonSet `logs-node-agent`: Desired == Ready на всех (нужных) нодах
- [ ] REGISTER: `agent_implementation=v2`, `logs_backend=nodelocal`
- [ ] Smoke: `k8s.api`
- [ ] Smoke: `logs.collect` / `logs.stream` через nodelocal (не GetLogs)
- [ ] Повторный REGISTER → `AgentAlreadyRegistered`

## Откат

- [ ] `kubectl delete -k .` (удалит и DaemonSet)
- [ ] Проверить, что hostPath mounts сняты; Core deregister
