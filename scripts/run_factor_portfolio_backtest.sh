#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

CONTAINER=quantaxis-core
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "[factor-bt] container ${CONTAINER} not running; start first with ./scripts/up.sh" >&2
  exit 1
fi

START=${1:-20190101}
END=${2:-20241231}
THEME=${3:-all}
FACTOR=${4:-mom_60}
REBALANCE=${5:-weekly}
TOPK=${6:-10}
COST_BPS=${7:-10}

LATEST_FACTOR=$(ls -t output/reports 2>/dev/null | grep "factor_mvp_theme=${THEME}_" | head -n 1 || true)
if [ -z "${LATEST_FACTOR}" ]; then
  echo "[factor-bt] no factor_mvp report found for theme=${THEME}; run ./scripts/run_factor_mvp.sh first" >&2
  exit 1
fi

TS=$(date +%Y%m%d-%H%M%S)
RUN_ID="${TS}_factor_bt_theme=${THEME}_fac=${FACTOR}_reb=${REBALANCE}_top=${TOPK}_cost=${COST_BPS}_${START}-${END}"
OUTDIR_HOST="output/reports/${RUN_ID}"
mkdir -p "${OUTDIR_HOST}"

# stage input parquet and script
FACTOR_PARQUET_HOST="output/reports/${LATEST_FACTOR}/factor_zscore.parquet"
docker cp "${FACTOR_PARQUET_HOST}" ${CONTAINER}:/tmp/factor_zscore.parquet

docker cp scripts/backtest_factor_portfolio.py ${CONTAINER}:/tmp/backtest_factor_portfolio.py

docker exec ${CONTAINER} bash -lc "python /tmp/backtest_factor_portfolio.py --start ${START} --end ${END} --theme '${THEME}' --factor-parquet /tmp/factor_zscore.parquet --factor '${FACTOR}' --rebalance ${REBALANCE} --topk ${TOPK} --cost-bps ${COST_BPS} --outdir /tmp/output" | tee "${OUTDIR_HOST}/console.txt"

docker cp ${CONTAINER}:/tmp/output/metrics.json "${OUTDIR_HOST}/metrics.json"
docker cp ${CONTAINER}:/tmp/output/equity.csv "${OUTDIR_HOST}/equity.csv"
docker cp ${CONTAINER}:/tmp/output/positions.csv "${OUTDIR_HOST}/positions.csv"

python3 scripts/render_report_summary.py --report "${OUTDIR_HOST}" >/dev/null 2>&1 || true
python3 scripts/render_report_manifest.py --report "${OUTDIR_HOST}" >/dev/null 2>&1 || true

echo "[factor-bt] wrote ${OUTDIR_HOST}/{metrics.json,equity.csv,positions.csv,summary.md,manifest.json}"
