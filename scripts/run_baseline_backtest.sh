#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Run baseline backtests and write versioned reports under output/reports/<run_id>/
# Usage:
#   ./scripts/run_baseline_backtest.sh 20190101 20241231 all xsec_momentum_weekly_topk 60 10 60 10
#   ./scripts/run_baseline_backtest.sh 20190101 20241231 all ts_ma_weekly 60 10 60 10

CONTAINER=quantaxis-core
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "[baseline] container ${CONTAINER} not running; start first with ./scripts/up.sh" >&2
  exit 1
fi

START=${1:-20190101}
END=${2:-20241231}
THEME=${3:-all}
STRATEGY=${4:-xsec_momentum_weekly_topk}
LOOKBACK=${5:-60}
TOPK=${6:-10}
MA=${7:-60}
COST_BPS=${8:-10}
MIN_BARS=${9:-252}

TS=$(date +%Y%m%d-%H%M%S)
RUN_ID="${TS}_${STRATEGY}_theme=${THEME}_lb=${LOOKBACK}_top=${TOPK}_ma=${MA}_cost=${COST_BPS}_minbars=${MIN_BARS}_${START}-${END}"
OUTDIR_HOST="output/reports/${RUN_ID}"
mkdir -p "$OUTDIR_HOST"

# Copy script into container

docker cp scripts/backtest_baseline.py ${CONTAINER}:/tmp/backtest_baseline.py

docker exec ${CONTAINER} python /tmp/backtest_baseline.py \
  --start "$START" --end "$END" --theme "$THEME" --strategy "$STRATEGY" \
  --lookback "$LOOKBACK" --top "$TOPK" --ma "$MA" --cost-bps "$COST_BPS" --min-bars "$MIN_BARS" \
  --outdir /tmp/output | tee "${OUTDIR_HOST}/console.txt"

# Copy artifacts back

docker cp ${CONTAINER}:/tmp/output/metrics.json "${OUTDIR_HOST}/metrics.json"
docker cp ${CONTAINER}:/tmp/output/equity.csv "${OUTDIR_HOST}/equity.csv"
docker cp ${CONTAINER}:/tmp/output/positions.csv "${OUTDIR_HOST}/positions.csv"

echo "[baseline] wrote ${OUTDIR_HOST}/metrics.json + equity.csv + positions.csv"
