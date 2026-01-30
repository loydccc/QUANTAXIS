#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

CONTAINER=quantaxis-core

if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "[fetch-tdx] container ${CONTAINER} not running; start first with ./scripts/up.sh" >&2
  exit 1
fi

START=${1:-}
END=${2:-}
LIMIT=${3:-200}
CODES=${4:-}

if [[ -z "$START" || -z "$END" ]]; then
  echo "Usage: $0 <START_YYYYMMDD> <END_YYYYMMDD> [LIMIT] [CODES]" >&2
  echo "  CODES example: 000001,600000" >&2
  exit 2
fi

echo "[fetch-tdx] start=${START} end=${END} limit=${LIMIT} codes=${CODES:-<auto>}"

EXTRA_ARGS=()
if [[ -n "${CODES}" ]]; then
  EXTRA_ARGS+=(--codes "${CODES}")
fi

docker exec \
  -e MONGODB_HOST -e MONGODB_PORT -e MONGODB_DATABASE -e MONGODB_USER -e MONGODB_PASSWORD \
  -e MONGO_ROOT_USER -e MONGO_ROOT_PASSWORD \
  -e TDX_IP -e TDX_PORT \
  ${CONTAINER} \
  timeout 60s python /app/scripts/fetch_tdx_stock_day.py --start "${START}" --end "${END}" --limit "${LIMIT}" "${EXTRA_ARGS[@]}"
