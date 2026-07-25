# Agent deployment variants
#
# | Overlay | Namespace | CLUSTER_ID | Implementation | Notes |
# |---------|-----------|------------|----------------|-------|
# | `overlays/local` | `uamc-agent` | `local` | v1 (default) | existing local kind |
# | `overlays/test-v1` | `uamc-agent-v1` | `test-v1` | v1 GetLogs | Kafka topic `k8s.commands.request.v1` |
# | `overlays/test-v2` | `uamc-agent-v2` | `test-v2` | v2 DaemonSet | Kafka topic `k8s.commands.request.v2` + components/logs-node |
# | `overlays/prod-v1` | `uamc-agent` | (cluster) | v1 | **PROD: pick one** (extends overlays/prod) |
# | `overlays/prod-v2` | `uamc-agent` | (cluster) | v2 | **PROD: pick one** + logs-node component |
# | `overlays/profiles/balanced` | `uamc-agent` | — | resource profile | replicas=1, QPS 30/60, logs jobs=5 |
# | `overlays/profiles/lean` | `uamc-agent` | — | resource profile | replicas=1, features-minimal, QPS 20/40 |
# | `overlays/prod` | `uamc-agent` | (cluster) | legacy base for prod-v1/v2 | kept for compatibility |
#
# ## Resource profiles
#
# ```bash
# kubectl apply -k deploy/overlays/profiles/balanced
# kubectl apply -k deploy/overlays/profiles/lean
# ```
#
# Or use mock-core-ui → **Resources** → Apply profile (ha | balanced | lean).
#
# ## Test: both agents in one cluster
#
# ```bash
# kubectl apply -k deploy/overlays/test-v1
# kubectl apply -k deploy/overlays/test-v2
# ```
#
# In mock-core-ui header, select Agent 1 / Agent 2 (topics …request.v1 / …request.v2).
# For the default local overlay (`uamc-agent` + `k8s.commands.request`) keep **Local agent**.
# Wrong target → scenarios hang on "Running …" until reply timeout (~45s).
# See docs/local-test-contour.md §5.2.0 and docs/agent-v1-v2.md.
#
# ## PROD: exactly one agent per cluster
#
# Install **either** `prod-v1` **or** `prod-v2`, never both in the same namespace.
# Core registration rejects a second agent for the same `cluster_id`
# (`reason=AgentAlreadyRegistered`).
#
# ```bash
# kubectl apply -k deploy/overlays/prod-v1   # OR
# kubectl apply -k deploy/overlays/prod-v2
# ```
#
# DaemonSet for v2 lives in [components/logs-node](components/logs-node).
