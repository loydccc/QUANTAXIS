#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Usage:
#   export TDX_IP=...; export TDX_PORT=...
#   ./scripts/fetch_hot_themes_tdx.sh 20240101 20240131 "AI,机器人,航天,军工,电力,变压器,半导体,存储" 10

START=${1:-}
END=${2:-}
KEYWORDS=${3:-}
TOP_BOARDS=${4:-10}

if [[ -z "$START" || -z "$END" || -z "$KEYWORDS" ]]; then
  echo "Usage: $0 <START_YYYYMMDD> <END_YYYYMMDD> <KEYWORDS_CSV> [TOP_BOARDS]" >&2
  exit 2
fi

CONTAINER=quantaxis-core
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "[themes] container ${CONTAINER} not running; start first with ./scripts/up.sh" >&2
  exit 1
fi

# Generate codes inside container (akshare)
CODES=$(docker exec ${CONTAINER} python /app/scripts/select_theme_codes.py --keywords "$KEYWORDS" --top "$TOP_BOARDS" --print-codes)

if [[ -z "$CODES" ]]; then
  echo "[themes] no codes matched for keywords: $KEYWORDS" >&2
  exit 3
fi

echo "[themes] matched codes: $(echo "$CODES" | tr ',' '\n' | wc -l | tr -d ' ')"

export TDX_IP TDX_PORT
./scripts/fetch_tdx_stock_day.sh "$START" "$END" 999999 "$CODES"
./scripts/verify_data.sh
