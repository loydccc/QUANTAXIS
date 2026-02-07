#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Ingest daily market data into Mongo.

Production v1 (minimal, auditable):
- Pull A-share daily bars for a single trading date into stock_day.
- Pull 510300 fund_daily for the same date into stock_day (code=510300).
- Upsert only; no deletions.

Data sources:
- Uses existing scripts:
  - scripts/fetch_tushare_stock_day.py
  - scripts/fetch_tushare_fund_daily.py

Env:
- TUSHARE_TOKEN (required)
- TUSHARE_HTTP_URL (optional)
- Mongo envs (defaults ok for local): MONGODB_HOST/PORT/USER/PASSWORD/DATABASE

Usage:
  python3 scripts/ingest_daily_market_data.py --date 2026-02-06 --theme a_ex_kcb_bse
"""

from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--theme", default="a_ex_kcb_bse")
    ap.add_argument("--sleep", type=float, default=0.6)
    args = ap.parse_args()

    d = args.date.replace("-", "")

    env = os.environ.copy()
    # For host runs
    env.setdefault("MONGODB_HOST", "127.0.0.1")
    env.setdefault("MONGODB_PORT", "27017")
    env.setdefault("MONGODB_DATABASE", "quantaxis")
    env.setdefault("MONGODB_USER", "quantaxis")
    env.setdefault("MONGODB_PASSWORD", "quantaxis")

    if not env.get("TUSHARE_TOKEN"):
        raise SystemExit("missing env TUSHARE_TOKEN")

    # 1) A-share daily bars for single day.
    cmd_stock = [
        "python3",
        "scripts/fetch_tushare_stock_day.py",
        "--from-stock-list",
        "--theme",
        str(args.theme),
        "--start",
        d,
        "--end",
        d,
        "--limit",
        "0",
        "--batch",
        "1",
        "--sleep",
        str(args.sleep),
    ]
    r1 = subprocess.run(cmd_stock, cwd=str(ROOT), env=env, capture_output=True, text=True)
    if r1.returncode != 0:
        raise RuntimeError((r1.stderr or r1.stdout or "stock ingest failed")[-2000:])

    # 2) 510300 fund_daily for the same day.
    cmd_etf = [
        "python3",
        "scripts/fetch_tushare_fund_daily.py",
        "--codes",
        "510300.SH",
        "--start",
        d,
        "--end",
        d,
        "--batch",
        "1",
        "--sleep",
        "0.2",
    ]
    r2 = subprocess.run(cmd_etf, cwd=str(ROOT), env=env, capture_output=True, text=True)
    if r2.returncode != 0:
        raise RuntimeError((r2.stderr or r2.stdout or "fund ingest failed")[-2000:])

    print("OK")


if __name__ == "__main__":
    main()
