#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Run the baseline suite and produce a comparison summary.
# Usage:
#   ./scripts/run_all_baselines.sh 20190101 20241231 all 800 10

START=${1:-20190101}
END=${2:-20241231}
THEME=${3:-all}
MIN_BARS=${4:-800}
COST_BPS=${5:-10}

# Strategy params (can be promoted to args later)
LOOKBACK=60
TOPK=10
MA=60
VOL_WINDOW=20
MAX_WEIGHT=0.10

./scripts/run_baseline_backtest.sh "$START" "$END" "$THEME" xsec_momentum_weekly_topk "$LOOKBACK" "$TOPK" "$MA" "$COST_BPS" "$MIN_BARS" "$VOL_WINDOW" "$MAX_WEIGHT"
./scripts/run_baseline_backtest.sh "$START" "$END" "$THEME" xsec_momentum_weekly_invvol "$LOOKBACK" "$TOPK" "$MA" "$COST_BPS" "$MIN_BARS" "$VOL_WINDOW" "$MAX_WEIGHT"
./scripts/run_baseline_backtest.sh "$START" "$END" "$THEME" ts_ma_weekly "$LOOKBACK" "$TOPK" "$MA" "$COST_BPS" "$MIN_BARS" "$VOL_WINDOW" "$MAX_WEIGHT"

# Build a comparison table from the latest 3 reports (they share the same timestamp prefix in practice)
python3 scripts/summarize_reports.py --latest 3 --out output/reports/latest_compare.csv --md output/reports/latest_compare.md

echo "[suite] wrote output/reports/latest_compare.csv and latest_compare.md"
