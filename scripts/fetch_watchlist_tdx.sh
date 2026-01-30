#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Mid/low-frequency: fetch daily bars for a curated seed universe.
# Usage:
#   export TDX_IP=...; export TDX_PORT=...
#   ./scripts/fetch_watchlist_tdx.sh 20230101 20241231 all
#   ./scripts/fetch_watchlist_tdx.sh 20240101 20240131 AI_算力_服务器_数据中心

START=${1:-}
END=${2:-}
THEME=${3:-all}

if [[ -z "$START" || -z "$END" ]]; then
  echo "Usage: $0 <START_YYYYMMDD> <END_YYYYMMDD> [THEME|all]" >&2
  exit 2
fi

CONTAINER=quantaxis-core
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "[watchlist] container ${CONTAINER} not running; start first with ./scripts/up.sh" >&2
  exit 1
fi

# Generate comma-separated codes from JSON
CODES=$(python3 - <<'PY'
import json
from pathlib import Path
import sys

start = sys.argv[1]
end = sys.argv[2]
theme = sys.argv[3]

p = Path('watchlists/themes_seed_cn.json')
obj = json.loads(p.read_text(encoding='utf-8'))

codes=set()
for t in obj['themes']:
  if theme=='all' or t['theme']==theme:
    for c in t['seed_codes']:
      codes.add(str(c).zfill(6))

print(','.join(sorted(codes)))
PY
"$START" "$END" "$THEME")

if [[ -z "$CODES" ]]; then
  echo "[watchlist] No codes found for theme=$THEME" >&2
  exit 3
fi

echo "[watchlist] theme=$THEME codes=$(echo "$CODES" | tr ',' '\n' | wc -l | tr -d ' ') range=$START..$END"

# Feed into the existing TDX fetcher
./scripts/fetch_tdx_stock_day.sh "$START" "$END" 999999 "$CODES"
./scripts/verify_data.sh
