#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

PORT=${PORT:-8000}
HOST=${HOST:-127.0.0.1}

# -----------------------------------------------------------------------------
# Defaults for running the API on the host machine (uvicorn on macOS/Linux).
#
# In Docker Compose, other services can reach Mongo via hostname "mongodb".
# But when you run this API on the host, "mongodb" is NOT resolvable; you must
# use the published port (usually 127.0.0.1:27017).
#
# We therefore default to host-friendly settings *only if* the user did not set
# them already.
# -----------------------------------------------------------------------------
: "${MONGODB_HOST:=127.0.0.1}"
: "${MONGODB_PORT:=27017}"
: "${MONGODB_DATABASE:=quantaxis}"
: "${MONGODB_USER:=quantaxis}"
: "${MONGODB_PASSWORD:=quantaxis}"
export MONGODB_HOST MONGODB_PORT MONGODB_DATABASE MONGODB_USER MONGODB_PASSWORD

# Optional: protect execution endpoints with a token
# export QUANTAXIS_API_TOKEN="your-secret"

python3 -c "import fastapi, uvicorn" >/dev/null 2>&1 || {
  echo "fastapi/uvicorn not installed. Install: pip3 install fastapi uvicorn" >&2
  exit 1
}

exec python3 -m uvicorn api.app:app --host "$HOST" --port "$PORT"
