#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

OUT_JSON=${1:-/tmp/output/config_runs.jsonl}
mkdir -p "$(dirname "$OUT_JSON")"

for cfg in configs/*.json; do
  echo "[configs] running $cfg" >&2
  python3 scripts/run_from_cfg.py --config "$cfg" --result /tmp/output/last_run.json | tee /dev/stderr
  cat /tmp/output/last_run.json >> "$OUT_JSON"
  echo >> "$OUT_JSON"
done

echo "[configs] wrote $OUT_JSON"
