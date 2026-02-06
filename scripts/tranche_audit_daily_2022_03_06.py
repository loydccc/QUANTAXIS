#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Daily-tranche audit (minimal truth check).

Goal (per user instruction):
- Do NOT change any strategy parameters.
- Add/consume audit fields only.
- Run daily (end date = each trading day) for a small window and output:
  1) min_tranche_effective_positions_after_min_weight
  2) fraction of tranche observations with effective_positions_after_min_weight < 6

This script checkpoints by reusing existing output/signals JSON if present.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pandas as pd
import pymongo

ROOT = Path(__file__).resolve().parents[1]
# Ensure repo root + scripts dir are importable when running as a script.
import sys

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

OUTDIR = ROOT / "output" / "reports" / "tranche_audit"
OUTDIR.mkdir(parents=True, exist_ok=True)

FALLBACK_ASSET = "510300"


def _mongo() -> pymongo.MongoClient:
    os.environ.setdefault("MONGODB_HOST", "127.0.0.1")
    os.environ.setdefault("MONGODB_PORT", "27017")
    os.environ.setdefault("MONGODB_DATABASE", "quantaxis")
    os.environ.setdefault("MONGODB_USER", "quantaxis")
    os.environ.setdefault("MONGODB_PASSWORD", "quantaxis")
    return pymongo.MongoClient(
        "mongodb://quantaxis:quantaxis@127.0.0.1:27017/quantaxis?authSource=admin",
        serverSelectionTimeoutMS=8000,
    )


def trading_days(theme: str, start: str, end: str) -> list[str]:
    """Get trading days by pulling a close panel index (fast enough for small window)."""
    c = _mongo()
    db = c["quantaxis"]
    from backtest_signal_walkforward import load_universe_from_mongo, fetch_panel_mixed_dates
    from backtest_baseline import detect_volume_field

    coll = db["stock_day"]
    codes = load_universe_from_mongo(db, theme)
    vol_field = detect_volume_field(coll)
    _o, _h, _l, close, _vol = fetch_panel_mixed_dates(coll, codes, start, end, volume_field=vol_field)
    idx = close.index.sort_values()
    idx = idx[(idx >= pd.to_datetime(start)) & (idx <= pd.to_datetime(end))]
    return [str(d.date()) for d in idx]


def load_signal_json(signal_id: str) -> dict:
    p = ROOT / "output" / "signals" / f"{signal_id}.json"
    return json.loads(p.read_text(encoding="utf-8"))


def status_path(signal_id: str) -> Path:
    return ROOT / "output" / "signals" / f"{signal_id}.status.json"


def main():
    start = os.getenv("START", "2022-03-01")
    end = os.getenv("END", "2022-06-30")
    theme = os.getenv("THEME", "a_ex_kcb_bse")

    cfg_base = {
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
        "fallback_asset": FALLBACK_ASSET,
        "start": "2019-01-01",
        "end": end,
    }

    days = trading_days(theme=theme, start=start, end=end)
    from api.signals_impl import run_signal

    tranche_obs = []

    sampled_days_for_assert = []

    AUDIT_VERSION = os.getenv("AUDIT_VERSION", "v2")

    for d in days:
        # Stable checkpoint id per day to support resume.
        # Version suffix allows re-running days when audit/meta schema changes.
        sid = f"daily_{d.replace('-', '')}_fb_{AUDIT_VERSION}"
        sp = status_path(sid)
        if sp.exists():
            st = json.loads(sp.read_text(encoding="utf-8"))
            if st.get("status") == "succeeded":
                obj = load_signal_json(sid)
            else:
                # failed/running from previous attempt; skip for now
                continue
        else:
            cfg = dict(cfg_base)
            cfg["end"] = d
            run_signal(sid, cfg)
            obj = load_signal_json(sid)

        ladder = (obj.get("meta", {}) or {}).get("ladder", {}) or {}
        runs = ladder.get("runs") or []
        for r in runs:
            ta = r.get("tranche_audit") or []
            for t in ta:
                tranche_obs.append(int(t.get("effective_positions_after_min_weight", 0)))

        # keep a few random days for post-run assertions
        sampled_days_for_assert.append(sid)

    # post-run assertions on 3 random days (hard requirements)
    import random

    random.seed(7)
    sampled_days_for_assert = [s for s in sampled_days_for_assert if status_path(s).exists()]
    sampled = random.sample(sampled_days_for_assert, min(3, len(sampled_days_for_assert)))
    for sid in sampled:
        obj = load_signal_json(sid)
        pos = obj.get("positions", [])
        n_pos = sum(1 for p in pos if str(p.get("code")) != FALLBACK_ASSET and float(p.get("weight", 0.0)) > 0)

        runs = (((obj.get("meta", {}) or {}).get("ladder", {}) or {}).get("runs") or [])
        if not runs:
            raise RuntimeError(f"missing meta.ladder.runs for {sid}")
        # the used level is meta.ladder.level_used; find matching run entry
        used = (((obj.get("meta", {}) or {}).get("ladder", {}) or {}).get("level_used"))
        run_used = None
        for r in runs:
            if r.get("level") == used:
                run_used = r
                break
        if run_used is None:
            run_used = runs[-1]

        tranche_audit = run_used.get("tranche_audit") or []
        cash = float(run_used.get("cash_weight_nofb", 0.0))
        # 1) tranche sum weights + cash == 1
        for t in tranche_audit:
            s = float(t.get("sum_weight_after_min_weight", 0.0))
            if abs((s + cash) - 1.0) > 1e-9:
                raise RuntimeError(f"assert fail sum_weight+cash!=1 for {sid}: {s}+{cash}")
        # 2) effective_positions_after_min_weight matches positions count
        eff = int(run_used.get("effective_positions_after_min_weight", 0))
        if abs(eff - n_pos) != 0:
            raise RuntimeError(f"assert fail eff_positions != n_positions for {sid}: eff={eff} n_pos={n_pos}")

    if not tranche_obs:
        print("NO_TRANCHE_OBS")
        return

    arr = pd.Series(tranche_obs, dtype="int")
    min_eff = int(arr.min())
    # Denominator MUST be tranche count, not trading-day count.
    n_tranches_total = int(len(arr))
    frac_lt6 = float((arr < 6).mean())

    p05 = float(arr.quantile(0.05))

    out = {
        "period": {"start": start, "end": end, "theme": theme},
        "num_tranches_total": n_tranches_total,
        "min_tranche_effective_positions_after_min_weight": min_eff,
        "frac_tranche_lt6": frac_lt6,
        "p05_tranche_effective_positions_after_min_weight": p05,
    }
    (OUTDIR / f"daily_tranche_{start}_{end}.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
