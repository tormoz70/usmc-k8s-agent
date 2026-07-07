#!/usr/bin/env bash
# Chaos smoke: kill leader pod and verify agent.lifecycle + new leader.
set -euo pipefail

NS="${NAMESPACE:-k8s-agent}"
DEPLOY="${DEPLOYMENT:-k8s-agent}"

echo "Current leader pod:"
LEADER=$(kubectl get pods -n "$NS" -l app=k8s-agent,k8s-agent/leader=true -o jsonpath='{.items[0].metadata.name}')
echo "  $LEADER"

echo "Start mock-core listener in another terminal:"
echo "  mock-core -listen -topic agent.lifecycle"
echo "Deleting leader pod $LEADER ..."
kubectl delete pod -n "$NS" "$LEADER" --wait=false

echo "Waiting for new leader label..."
for i in $(seq 1 60); do
  NEW=$(kubectl get pods -n "$NS" -l app=k8s-agent,k8s-agent/leader=true -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [[ -n "$NEW" && "$NEW" != "$LEADER" ]]; then
    echo "New leader: $NEW"
    echo "Verify agent.lifecycle event and replay cache.put from core."
    exit 0
  fi
  sleep 2
done

echo "Timed out waiting for new leader"
exit 1
