#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p output

CONTAINER=quantaxis-core
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "[baseline] container ${CONTAINER} not running; start first with ./scripts/up.sh" >&2
  exit 1
fi

START=${1:-20190101}
END=${2:-20241231}
THEME=${3:-all}
LOOKBACK=${4:-60}
TOPK=${5:-10}
COST_BPS=${6:-10}

cat > /tmp/qa_baseline.py <<'PY'
# placeholder; will be overwritten by docker cp from repo script
PY

# Copy the repo script into container
# (We do docker cp to avoid depending on bind mounts)
docker cp scripts/backtest_baseline.py ${CONTAINER}:/tmp/backtest_baseline.py

# Run inside container
# Note: Mongo creds are already in container env; root creds are also present via compose.
docker exec ${CONTAINER} python /tmp/backtest_baseline.py \
  --start "$START" --end "$END" --theme "$THEME" \
  --lookback "$LOOKBACK" --top "$TOPK" --cost-bps "$COST_BPS" \
  --outdir /tmp/output | tee output/baseline_console.txt

# Copy artifacts back
rm -f output/baseline_metrics.json output/baseline_equity.csv output/baseline_positions.csv || true

docker cp ${CONTAINER}:/tmp/output/baseline_metrics.json output/baseline_metrics.json
docker cp ${CONTAINER}:/tmp/output/baseline_equity.csv output/baseline_equity.csv
docker cp ${CONTAINER}:/tmp/output/baseline_positions.csv output/baseline_positions.csv

echo "[baseline] wrote output/baseline_metrics.json + baseline_equity.csv + baseline_positions.csv"
