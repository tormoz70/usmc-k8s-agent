# Сравнение Agent 1 vs Agent 2 (кратко для приёмки)

| | Agent 1 | Agent 2 |
|--|---------|---------|
| Папка | `handover/prom/agent-1` | `handover/prom/agent-2` |
| `AGENT_IMPLEMENTATION` | `v1` | `v2` |
| `LOGS_BACKEND` | `api` | `nodelocal` |
| Workloads | ingress, egress, agent-service | + DaemonSet `logs-node-agent` |
| Логи | GetLogs API | `/var/log/pods` на ноде |
| hostPath | нет | да |
| Порт node agent | — | `8083` |
| REGISTER | `logs_backend=api` | `logs_backend=nodelocal` |
| Kafka-команды | одинаковые | одинаковые |
| Namespace | `uamc-agent` | `uamc-agent` |
| Совместная установка | **запрещена** | **запрещена** |

Подробнее: `docs/agent-v1-v2.md` в каждой папке.
