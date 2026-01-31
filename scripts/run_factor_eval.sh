#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

CONTAINER=quantaxis-core
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "[factor-eval] container ${CONTAINER} not running; start first with ./scripts/up.sh" >&2
  exit 1
fi

START=${1:-20190101}
END=${2:-20241231}
THEME=${3:-all}
HORIZON=${4:-5}
QUANTILES=${5:-5}

LATEST_FACTOR=$(ls -t output/reports 2>/dev/null | grep "factor_mvp_theme=${THEME}_" | head -n 1 || true)
if [ -z "${LATEST_FACTOR}" ]; then
  echo "[factor-eval] no factor_mvp report found for theme=${THEME}; run ./scripts/run_factor_mvp.sh first" >&2
  exit 1
fi

TS=$(date +%Y%m%d-%H%M%S)
RUN_ID="${TS}_factor_eval_theme=${THEME}_h=${HORIZON}_q=${QUANTILES}_${START}-${END}"
OUTDIR_HOST="output/reports/${RUN_ID}"
mkdir -p "${OUTDIR_HOST}"

# stage inputs to container
FACTOR_PARQUET_HOST="output/reports/${LATEST_FACTOR}/factor_values.parquet"
docker cp "${FACTOR_PARQUET_HOST}" ${CONTAINER}:/tmp/factor_values.parquet

docker cp scripts/evaluate_factors.py ${CONTAINER}:/tmp/evaluate_factors.py

docker exec ${CONTAINER} bash -lc "python /tmp/evaluate_factors.py --start ${START} --end ${END} --theme '${THEME}' --factor-parquet /tmp/factor_values.parquet --horizon ${HORIZON} --quantiles ${QUANTILES} --outdir /tmp/output" | tee "${OUTDIR_HOST}/console.txt"

# copy outputs back
for f in metrics.json ic.csv quantile_returns.csv; do
  docker cp ${CONTAINER}:/tmp/output/${f} "${OUTDIR_HOST}/${f}"
done

python3 scripts/render_factor_summary.py --report "${OUTDIR_HOST}" >/dev/null 2>&1 || true
python3 scripts/render_report_manifest.py --report "${OUTDIR_HOST}" >/dev/null 2>&1 || true

echo "[factor-eval] wrote ${OUTDIR_HOST}/{metrics.json,ic.csv,quantile_returns.csv,summary.md,manifest.json}"
