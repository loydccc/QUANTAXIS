#!/usr/bin/env bash
set -euo pipefail

# Minimal regression check to avoid "service won't start" refactor bugs.
# 1) Import-time sanity (catches missing modules / circular imports)
python3 -c "import api.app" >/dev/null
python3 -c "import api.signals; import api.signals_impl; import api.core; import api.security; import api.state" >/dev/null

echo "[OK] python imports"

# Optional: run health check if requested
if [[ "${1:-}" == "--health" ]]; then
  PORT="${QUANTAXIS_API_PORT:-8000}"
  HOST="${QUANTAXIS_API_HOST:-127.0.0.1}"
  URL="http://${HOST}:${PORT}/health"
  echo "[INFO] starting uvicorn for health check on ${URL}"
  # start server in background
  python3 -m uvicorn api.app:app --host "$HOST" --port "$PORT" --log-level warning &
  PID=$!
  trap 'kill "$PID" >/dev/null 2>&1 || true' EXIT
  # wait briefly
  for i in {1..30}; do
    if curl -fsS "$URL" >/dev/null 2>&1; then
      echo "[OK] /health"
      exit 0
    fi
    sleep 0.2
  done
  echo "[FAIL] /health did not respond" >&2
  exit 1
fi
