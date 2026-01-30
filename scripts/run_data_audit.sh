#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Run data audit inside container for a given theme/universe.
# Usage:
#   ./scripts/run_data_audit.sh 20190101 20241231 all

CONTAINER=quantaxis-core
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "[audit] container ${CONTAINER} not running; start first with ./scripts/up.sh" >&2
  exit 1
fi

START=${1:-20190101}
END=${2:-20241231}
THEME=${3:-all}

# Build codes list from watchlist
CODES=$(python3 -c "import json; from pathlib import Path; import sys; theme=sys.argv[1]; obj=json.loads(Path('watchlists/themes_seed_cn.json').read_text(encoding='utf-8')); codes=set();
for t in obj['themes']:
  if theme=='all' or t['theme']==theme:
    for c in t['seed_codes']:
      codes.add(str(c).zfill(6));
print(','.join(sorted(codes)))" "$THEME")

TS=$(date +%Y%m%d-%H%M%S)
RUN_ID="${TS}_audit_theme=${THEME}_${START}-${END}"
OUTDIR_HOST="output/reports/${RUN_ID}"
mkdir -p "$OUTDIR_HOST"

docker cp scripts/data_audit.py ${CONTAINER}:/tmp/data_audit.py

docker exec ${CONTAINER} python /tmp/data_audit.py \
  --codes "$CODES" --start "$START" --end "$END" --outdir /tmp/output | tee "${OUTDIR_HOST}/audit_console.txt"

docker cp ${CONTAINER}:/tmp/output/data_audit.csv "${OUTDIR_HOST}/data_audit.csv"
docker cp ${CONTAINER}:/tmp/output/data_audit_summary.json "${OUTDIR_HOST}/data_audit_summary.json"

echo "[audit] wrote ${OUTDIR_HOST}/data_audit.csv + data_audit_summary.json"
