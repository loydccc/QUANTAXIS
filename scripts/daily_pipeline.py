#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Daily pipeline v1: ingest -> validate -> seal -> (optional) HI -> (optional) signal.

Fixed ordering and degradation rules:
- Only sealed_ok=true allows HI cache + signal.
- If not sealed: do not compute HI, do not run new signal.

This is the minimal production-grade pipeline.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--theme", default="a_ex_kcb_bse")
    ap.add_argument("--run-hi", action="store_true")
    ap.add_argument("--run-signal", action="store_true")
    args = ap.parse_args()

    env = os.environ.copy()
    env.setdefault("MONGODB_HOST", "127.0.0.1")
    env.setdefault("MONGODB_PORT", "27017")
    env.setdefault("MONGODB_DATABASE", "quantaxis")
    env.setdefault("MONGODB_USER", "quantaxis")
    env.setdefault("MONGODB_PASSWORD", "quantaxis")

    # A) ingest
    subprocess.check_call(["python3", "scripts/ingest_daily_market_data.py", "--date", args.date, "--theme", args.theme], cwd=str(ROOT), env=env)

    # B) validate
    v = subprocess.run(["python3", "scripts/validate_daily_data.py", "--date", args.date], cwd=str(ROOT), env=env, capture_output=True, text=True)
    if v.returncode not in (0, 2):
        raise RuntimeError((v.stderr or v.stdout or "validate error")[-2000:])
    validate_json = (v.stdout or "").strip().splitlines()[-1]
    obj = json.loads(validate_json)

    # C) seal
    s = subprocess.run(["python3", "scripts/seal_trading_day.py", "--date", args.date, "--validate-json", validate_json], cwd=str(ROOT), env=env, capture_output=True, text=True)
    sealed_ok = s.returncode == 0

    if not sealed_ok:
        # degradation: do not run HI/signal
        print(json.dumps({"date": args.date, "sealed_ok": False, "action": "HOLD_PREV"}, ensure_ascii=False))
        raise SystemExit(2)

    # Optional: compute daily HI cache (single-day)
    if args.run_hi:
        subprocess.check_call(["python3", "scripts/health_index_daily_cache.py", "--date", args.date], cwd=str(ROOT), env=env)

    # Optional: run signal (weekly cadence still applies; end date can be set to today)
    if args.run_signal:
        # leave to operator/scheduler to provide exact cfg in production
        print(json.dumps({"date": args.date, "sealed_ok": True, "action": "SIGNAL_NOT_CONFIGURED_IN_PIPELINE"}, ensure_ascii=False))

    print(json.dumps({"date": args.date, "sealed_ok": True, "validate": obj.get("counts")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
