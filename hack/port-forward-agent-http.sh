#!/usr/bin/env bash
# Expose k8s-agent ingress HTTP on localhost:8080 for mock-core-ui REST scenarios.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$ROOT/hack/.agent-http-pf.pid"
LOG_FILE="$ROOT/hack/.agent-http-pf.log"
NS=uamc-agent
SVC=svc/k8s-agent-http
LOCAL_PORT=8080

if [[ -f "$PID_FILE" ]]; then
  old=$(cat "$PID_FILE" || true)
  if [[ -n "${old:-}" ]] && kill -0 "$old" 2>/dev/null; then
    echo "Stopping previous port-forward (pid $old)..."
    kill "$old" 2>/dev/null || true
  fi
  rm -f "$PID_FILE"
fi
pkill -f "port-forward.*k8s-agent-http" 2>/dev/null || true

echo "Starting kubectl port-forward $SVC -> localhost:${LOCAL_PORT} ..."
nohup kubectl port-forward -n "$NS" "$SVC" "${LOCAL_PORT}:8080" >"$LOG_FILE" 2>&1 &
echo $! >"$PID_FILE"

deadline=$((SECONDS + 20))
while (( SECONDS < deadline )); do
  if (echo >/dev/tcp/localhost/"$LOCAL_PORT") 2>/dev/null; then
    echo "Agent HTTP ready: http://localhost:${LOCAL_PORT}/healthz (pid $(cat "$PID_FILE"))"
    exit 0
  fi
  sleep 1
done

echo "Timeout waiting for localhost:${LOCAL_PORT}. Log:" >&2
cat "$LOG_FILE" >&2 || true
exit 1
