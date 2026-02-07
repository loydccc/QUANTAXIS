#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""T-3 shadow run helper.

Runs daily_pipeline end-to-end (ingest->validate->seal->HI->signal) and then
verifies required artifacts + assertions.

Outputs a compact JSON report to stdout and writes it to:
  output/reports/shadow_runs/<date>.json

Exit code:
- 0: PASS
- 2: FAIL

Usage:
  python3 scripts/shadow_run_day.py --date YYYY-MM-DD

Notes:
- This is SHADOW mode: it does not place orders.
- It expects Mongo credentials via env or daily_pipeline defaults.
"""

from __future__ import annotations

import argparse
import glob
import json
import math
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "output" / "reports" / "shadow_runs"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

MIN_POS = 6


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _sum_weights(positions: list[dict]) -> float:
    return float(sum(float(p.get("weight", 0.0)) for p in positions))


def _cash_weight(sig: dict) -> float:
    # canonical: meta.health.cash_weight
    h = (sig.get("meta", {}) or {}).get("health", {}) or {}
    if "cash_weight" in h and h["cash_weight"] is not None:
        return float(h["cash_weight"])
    # fallback: positions include CASH
    for p in sig.get("positions", []) or []:
        if str(p.get("code")).upper() == "CASH":
            return float(p.get("weight", 0.0))
    return 0.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--theme", default="a_ex_kcb_bse")
    ap.add_argument("--signal-theme", default="a_ex_kcb_bse")
    ap.add_argument("--signal-top-k", type=int, default=20)
    ap.add_argument("--skip-ingest", action="store_true", help="for local dry checks only")
    args = ap.parse_args()

    date = str(args.date)

    # 1) Run pipeline (full chain)
    cmd = [
        "python3",
        "scripts/daily_pipeline.py",
        "--date",
        date,
        "--theme",
        args.theme,
        "--run-hi",
        "--run-signal",
        "--signal-theme",
        args.signal_theme,
        "--signal-top-k",
        str(args.signal_top_k),
    ]
    if args.skip_ingest:
        cmd.append("--skip-ingest")

    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)

    pipeline_ok = proc.returncode == 0
    pipeline_tail = (proc.stdout or "").strip().splitlines()[-1] if (proc.stdout or "").strip() else ""
    pipeline_err_tail = (proc.stderr or "").strip().splitlines()[-1] if (proc.stderr or "").strip() else ""

    # 2) Required artifacts
    ops_path = ROOT / "output" / "reports" / "ops_data_status" / f"{date}.json"
    hi_path = ROOT / "output" / "reports" / "health_index" / "daily" / f"health_score_{date}.json"

    sig_glob = str(ROOT / "output" / "signals" / f"prod_signal_{date.replace('-', '')}_*.json")
    sig_paths = sorted([p for p in glob.glob(sig_glob) if not p.endswith(".status.json")])
    sig_path = Path(sig_paths[-1]) if sig_paths else None

    health_log_path = ROOT / "output" / "reports" / "health_index" / "health_signal_log.csv"

    missing = []
    if not ops_path.exists():
        missing.append(str(ops_path))
    if not hi_path.exists():
        missing.append(str(hi_path))
    if sig_path is None or (not sig_path.exists()):
        missing.append(sig_glob)
    if not health_log_path.exists():
        missing.append(str(health_log_path))

    report: dict = {
        "date": date,
        "pipeline": {
            "ok": pipeline_ok,
            "returncode": proc.returncode,
            "tail": pipeline_tail,
        "stderr_tail": pipeline_err_tail,
        },
        "artifacts": {
            "ops_data_status": str(ops_path),
            "health_cache": str(hi_path),
            "signal": str(sig_path) if sig_path else None,
            "health_signal_log": str(health_log_path),
            "missing": missing,
        },
        "assertions": {},
    }

    if missing:
        report["ok"] = False
        (REPORT_DIR / f"{date}.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False))
        raise SystemExit(2)

    ops = _read_json(ops_path)
    hi = _read_json(hi_path)
    sig = _read_json(sig_path)

    # A1 sealed_ok
    a1 = bool(ops.get("sealed_ok") is True)

    # A2 n_components_used >= 5
    a2 = int(hi.get("n_components_used") or 0) >= 5

    # A3 signal succeeded
    a3 = str(sig.get("status")) == "succeeded"

    # A4 funds conservation: positions already include CASH when needed
    positions = sig.get("positions", []) or []
    sw = _sum_weights(positions)
    cw = _cash_weight(sig)
    a4 = abs(sw - 1.0) <= 1e-9

    # A5 cash range
    a5 = (0.0 <= cw <= 0.6)

    # A6 sealed date
    sealed_date = (((sig.get("meta", {}) or {}).get("ops", {}) or {}).get("sealed_date"))
    a6 = (sealed_date == date)

    # A7 HI consistency
    mh = (sig.get("meta", {}) or {}).get("health", {}) or {}
    a7 = (
        (mh.get("health_missing") is False)
        and (mh.get("health_score") is not None)
        and (abs(float(mh.get("health_score")) - float(hi.get("health_score"))) <= 0.0)
    )

    # A8 position sanity
    any_neg = any(float(p.get("weight", 0.0)) < 0 for p in positions)
    a8 = (len(positions) >= MIN_POS) and (not any_neg)

    report["assertions"] = {
        "sealed_ok_true": a1,
        "health_components_ge_5": a2,
        "signal_succeeded": a3,
        "funds_conservation": a4,
        "cash_weight_in_range": a5,
        "meta_ops_sealed_date_match": a6,
        "meta_health_matches_cache": a7,
        "positions_sane": a8,
        "debug": {
            "sum_weights": sw,
            "cash_weight": cw,
            "sealed_date": sealed_date,
            "health_score_meta": mh.get("health_score"),
            "health_score_cache": hi.get("health_score"),
            "positions_n": len(positions),
        },
    }

    ok = all([a1, a2, a3, a4, a5, a6, a7, a8])
    report["ok"] = ok

    (REPORT_DIR / f"{date}.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))
    raise SystemExit(0 if ok else 2)


if __name__ == "__main__":
    main()
