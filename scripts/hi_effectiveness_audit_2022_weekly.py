#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Health Index effectiveness audit (2022 weekly signal days).

Produces the 2 required blocks:
1) Conservation/boundary checks over all weekly signal days in 2022-01-01..2022-10-31.
2) Exposure distribution checks over the same signal days.

This script is read-only w.r.t. strategy params; it just runs run_signal and inspects outputs.

Usage:
  MONGODB_HOST=127.0.0.1 ... python3 scripts/hi_effectiveness_audit_2022_weekly.py
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

# Ensure imports
import sys

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


def main():
    # Force local mongo
    os.environ.setdefault("MONGODB_HOST", "127.0.0.1")
    os.environ.setdefault("MONGODB_PORT", "27017")
    os.environ.setdefault("MONGODB_DATABASE", "quantaxis")
    os.environ.setdefault("MONGODB_USER", "quantaxis")
    os.environ.setdefault("MONGODB_PASSWORD", "quantaxis")

    start = "2022-01-01"
    end = "2022-10-31"
    theme = "a_ex_kcb_bse"

    from api.signals_impl import run_signal
    from ladder_audit_2022 import pick_rebalance_dates

    rebs = pick_rebalance_dates(start, end, theme)

    rows = []
    for d in rebs:
        sid = f"hi_week_{d.replace('-', '')}_{int(time.time()*1000)}"
        cfg = {
            "strategy": "hybrid_baseline_weekly_topk",
            "theme": theme,
            "rebalance": "weekly",
            "top_k": 20,
            "candidate_k": 100,
            "min_bars": 800,
            "liq_window": 20,
            "liq_min_ratio": 1.0,
            "hold_weeks": 2,
            "tranche_overlap": True,
            "ma_mode": "filter",
            "score_mode": "factor",
            # alpha weights (current)
            "score_w_ret_20d": -1.0,
            "score_w_ret_10d": -0.5,
            "score_w_ret_5d": -0.2,
            "score_w_ma_60d": 0.3,
            "score_w_vol_20d": -0.5,
            "score_w_liq_20d": 0.0,
            "execution_mode": "naive",
            "backup_k": 50,
            "min_weight": 0.04,
            # hard bottom-line
            "hard_dist_252h_min": -0.4,
            "hard_downvol_q": 0.70,
            # fallback asset
            "fallback_asset": "510300",
            "start": "2019-01-01",
            "end": d,
        }
        run_signal(sid, cfg)
        obj = json.loads((ROOT / "output" / "signals" / f"{sid}.json").read_text(encoding="utf-8"))
        h = (obj.get("meta", {}) or {}).get("health") or {}
        cash = float(h.get("cash_weight", 0.0))
        exposure = float(h.get("exposure", 1.0))
        hs = h.get("health_score")
        hs = float(hs) if hs is not None else float("nan")
        wsum = sum(float(x.get("weight", 0.0)) for x in obj.get("positions", []))
        err = abs((wsum) - 1.0)
        rows.append({"date": obj.get("as_of_date"), "sumw": wsum, "cash": cash, "err": err, "exposure": exposure, "health_score": hs})

    df = pd.DataFrame(rows).dropna(subset=["date"]).sort_values("date")

    # 1) conservation/boundaries
    max_err = float(df["err"].max()) if len(df) else float("nan")
    min_cash = float(df["cash"].min()) if len(df) else float("nan")
    max_cash = float(df["cash"].max()) if len(df) else float("nan")

    # 2) exposure dist
    at_floor = float((df["exposure"].round(10) == 0.4).mean()) if len(df) else float("nan")
    mean_exp = float(df["exposure"].mean()) if len(df) else float("nan")
    p10_exp = float(df["exposure"].quantile(0.1)) if len(df) else float("nan")

    out = {
        "n_signal_days": int(len(df)),
        "max_abs_sumw_minus_1": max_err,
        "min_cash_weight": min_cash,
        "max_cash_weight": max_cash,
        "pct_exposure_at_floor": at_floor,
        "mean_exposure": mean_exp,
        "p10_exposure": p10_exp,
    }
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
