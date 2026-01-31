#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

PORT=${PORT:-8000}
HOST=${HOST:-127.0.0.1}

# Optional: protect execution endpoints with a token
# export QUANTAXIS_API_TOKEN="your-secret"

python3 -c "import fastapi, uvicorn" >/dev/null 2>&1 || {
  echo "fastapi/uvicorn not installed. Install: pip3 install fastapi uvicorn" >&2
  exit 1
}

exec python3 -m uvicorn api.app:app --host "$HOST" --port "$PORT"
