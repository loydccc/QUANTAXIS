#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

CONTAINER=quantaxis-core

if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "[fetch] container ${CONTAINER} not running; start first with ./scripts/up.sh" >&2
  exit 1
fi

START=${1:-}
END=${2:-}
LIMIT=${3:-200}
CODES=${4:-}

if [[ -z "$START" || -z "$END" ]]; then
  echo "Usage: $0 <START_YYYYMMDD> <END_YYYYMMDD> [LIMIT] [CODES]" >&2
  echo "  CODES example: 000001.SZ,600000.SH" >&2
  exit 2
fi

echo "[fetch] start=${START} end=${END} limit=${LIMIT} codes=${CODES:-<auto>}"

# If token is in repo-local .env, export it for this shell (docker exec -e uses caller env)
if [[ -z "${TUSHARE_TOKEN:-}" && -f .env ]]; then
  export TUSHARE_TOKEN
  TUSHARE_TOKEN=$(python3 - <<'PY'
import os
from pathlib import Path
for line in Path('.env').read_text(encoding='utf-8').splitlines():
    if line.startswith('TUSHARE_TOKEN='):
        print(line.split('=',1)[1].strip())
        break
PY
)
fi

if [[ -z "${TUSHARE_TOKEN:-}" ]]; then
  echo "[fetch] ERROR: missing TUSHARE_TOKEN (set env or put into .env)" >&2
  exit 3
fi

EXTRA_ARGS=()
if [[ -n "${CODES}" ]]; then
  EXTRA_ARGS+=(--codes "${CODES}")
fi

if ((${#EXTRA_ARGS[@]})); then
  docker exec \
    -e TUSHARE_TOKEN \
    -e MONGODB_HOST -e MONGODB_PORT -e MONGODB_DATABASE -e MONGODB_USER -e MONGODB_PASSWORD \
    -e MONGO_ROOT_USER -e MONGO_ROOT_PASSWORD \
    ${CONTAINER} \
    python /app/scripts/fetch_tushare_stock_day.py --start "${START}" --end "${END}" --limit "${LIMIT}" "${EXTRA_ARGS[@]}"
else
  docker exec \
    -e TUSHARE_TOKEN \
    -e MONGODB_HOST -e MONGODB_PORT -e MONGODB_DATABASE -e MONGODB_USER -e MONGODB_PASSWORD \
    -e MONGO_ROOT_USER -e MONGO_ROOT_PASSWORD \
    ${CONTAINER} \
    python /app/scripts/fetch_tushare_stock_day.py --start "${START}" --end "${END}" --limit "${LIMIT}"
fi
