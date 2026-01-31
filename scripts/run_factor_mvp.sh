#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

CONTAINER=quantaxis-core
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "[factor] container ${CONTAINER} not running; start first with ./scripts/up.sh" >&2
  exit 1
fi

START=${1:-20190101}
END=${2:-20241231}
THEME=${3:-all}

TS=$(date +%Y%m%d-%H%M%S)
RUN_ID="${TS}_factor_mvp_theme=${THEME}_${START}-${END}"
OUTDIR_HOST="output/reports/${RUN_ID}"
mkdir -p "$OUTDIR_HOST"

docker cp factors/factor_mvp.py ${CONTAINER}:/tmp/factor_mvp.py
# Put module into a temporary package path
# We will run by injecting PYTHONPATH=/tmp and placing factors module under /tmp/factors

docker exec ${CONTAINER} bash -lc 'mkdir -p /tmp/factors && cp /tmp/factor_mvp.py /tmp/factors/factor_mvp.py && touch /tmp/factors/__init__.py'

docker cp scripts/run_factor_mvp.py ${CONTAINER}:/tmp/run_factor_mvp.py

docker exec ${CONTAINER} bash -lc "PYTHONPATH=/tmp python /tmp/run_factor_mvp.py --start $START --end $END --theme '$THEME' --outdir /tmp/output" | tee "$OUTDIR_HOST/console.txt"

docker cp ${CONTAINER}:/tmp/output/factor_values.parquet "$OUTDIR_HOST/factor_values.parquet"
docker cp ${CONTAINER}:/tmp/output/factor_zscore.parquet "$OUTDIR_HOST/factor_zscore.parquet"
docker cp ${CONTAINER}:/tmp/output/factor_meta.json "$OUTDIR_HOST/factor_meta.json"

echo "[factor] wrote ${OUTDIR_HOST}/factor_*.parquet + factor_meta.json"
