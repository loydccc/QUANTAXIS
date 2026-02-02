#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Run baseline backtests and write versioned reports under output/reports/<run_id>/
# Usage:
#   ./scripts/run_baseline_backtest.sh 20190101 20241231 all xsec_momentum_weekly_topk 60 10 60 10 252
#   ./scripts/run_baseline_backtest.sh 20190101 20241231 all ts_ma_weekly 60 10 60 10 252
#   ./scripts/run_baseline_backtest.sh 20190101 20241231 all xsec_momentum_weekly_invvol 60 10 60 10 252 20 0.10

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
VOL_WINDOW=${10:-20}
MAX_WEIGHT=${11:-0.10}

TS=$(date +%Y%m%d-%H%M%S)
RUN_ID="${TS}_${STRATEGY}_theme=${THEME}_lb=${LOOKBACK}_top=${TOPK}_ma=${MA}_cost=${COST_BPS}_minbars=${MIN_BARS}_vw=${VOL_WINDOW}_mw=${MAX_WEIGHT}_${START}-${END}"
OUTDIR_HOST="output/reports/${RUN_ID}"
mkdir -p "$OUTDIR_HOST"

# Copy script into container

docker cp scripts/backtest_baseline.py ${CONTAINER}:/tmp/backtest_baseline.py

# Snapshot provenance (required when triggered via run_from_cfg/API)
DATA_VERSION_ID=${QUANTAXIS_DATA_VERSION_ID:-""}
MANIFEST_SHA256=${QUANTAXIS_MANIFEST_SHA256:-""}
REQUIRE_SNAPSHOT=${QUANTAXIS_REQUIRE_SNAPSHOT:-"0"}

SNAPSHOT_DIR_IN_CONTAINER=""
if [[ "$REQUIRE_SNAPSHOT" == "1" ]]; then
  if [[ -z "$DATA_VERSION_ID" || -z "$MANIFEST_SHA256" ]]; then
    echo "[baseline] missing QUANTAXIS_DATA_VERSION_ID or QUANTAXIS_MANIFEST_SHA256" >&2
    exit 2
  fi
  ASOF="${DATA_VERSION_ID#*@}"
  SRC_DIR="data/qa_cn_stock_daily/versions/${ASOF}"
  if [[ ! -f "${SRC_DIR}/bars.parquet" || ! -f "${SRC_DIR}/manifest.json" ]]; then
    echo "[baseline] snapshot not found: ${SRC_DIR} (run export_cn_stock_daily_snapshot.py first)" >&2
    exit 2
  fi
  SNAPSHOT_DIR_IN_CONTAINER="/tmp/qa_cn_stock_daily_snapshot"
  docker exec ${CONTAINER} rm -rf "${SNAPSHOT_DIR_IN_CONTAINER}" || true
  docker exec ${CONTAINER} mkdir -p "${SNAPSHOT_DIR_IN_CONTAINER}" || true
  docker cp "${SRC_DIR}/bars.parquet" ${CONTAINER}:"${SNAPSHOT_DIR_IN_CONTAINER}/bars.parquet"
  docker cp "${SRC_DIR}/manifest.json" ${CONTAINER}:"${SNAPSHOT_DIR_IN_CONTAINER}/manifest.json"
fi

docker exec ${CONTAINER} python /tmp/backtest_baseline.py \
  --start "$START" --end "$END" --theme "$THEME" --strategy "$STRATEGY" \
  --lookback "$LOOKBACK" --top "$TOPK" --ma "$MA" --cost-bps "$COST_BPS" --min-bars "$MIN_BARS" \
  --vol-window "$VOL_WINDOW" --max-weight "$MAX_WEIGHT" \
  --outdir /tmp/output \
  ${SNAPSHOT_DIR_IN_CONTAINER:+--snapshot-dir "$SNAPSHOT_DIR_IN_CONTAINER"} \
  ${DATA_VERSION_ID:+--data-version-id "$DATA_VERSION_ID"} \
  ${MANIFEST_SHA256:+--manifest-sha256 "$MANIFEST_SHA256"} \
  ${REQUIRE_SNAPSHOT:+--require-snapshot "$REQUIRE_SNAPSHOT"} \
  | tee "${OUTDIR_HOST}/console.txt"

# Copy artifacts back

docker cp ${CONTAINER}:/tmp/output/run.json "${OUTDIR_HOST}/run.json" || true

docker cp ${CONTAINER}:/tmp/output/metrics.json "${OUTDIR_HOST}/metrics.json"
docker cp ${CONTAINER}:/tmp/output/equity.csv "${OUTDIR_HOST}/equity.csv"
docker cp ${CONTAINER}:/tmp/output/positions.csv "${OUTDIR_HOST}/positions.csv"
docker cp ${CONTAINER}:/tmp/output/trades.csv "${OUTDIR_HOST}/trades.csv" || true

# Standard parquet artifacts
docker cp ${CONTAINER}:/tmp/output/equity_curve.parquet "${OUTDIR_HOST}/equity_curve.parquet" || true
docker cp ${CONTAINER}:/tmp/output/positions.parquet "${OUTDIR_HOST}/positions.parquet" || true
docker cp ${CONTAINER}:/tmp/output/returns.parquet "${OUTDIR_HOST}/returns.parquet" || true
docker cp ${CONTAINER}:/tmp/output/turnover.parquet "${OUTDIR_HOST}/turnover.parquet" || true
docker cp ${CONTAINER}:/tmp/output/trades.parquet "${OUTDIR_HOST}/trades.parquet" || true

# Render human-readable summary
python3 scripts/render_report_summary.py --report "${OUTDIR_HOST}" >/dev/null 2>&1 || true
python3 scripts/render_report_manifest.py --report "${OUTDIR_HOST}" >/dev/null 2>&1 || true
python3 scripts/build_artifact_manifest.py --dir "${OUTDIR_HOST}" >/dev/null 2>&1 || true

echo "[baseline] wrote ${OUTDIR_HOST}/metrics.json + equity.csv + positions.csv (+ parquet + summary.md + manifest.json + artifact_manifest.json)"
