#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""OOS quickcheck (weekly) baseline vs HI for a date range.

Runs weekly signals, builds daily equity using stock+510300 closes, and compares
HI (exposure clip 0.4..1.0) vs baseline (health cache missing => exposure=1).

Outputs:
- output/reports/health_index/oos/{label}/equity_baseline.csv
- output/reports/health_index/oos/{label}/equity_hi.csv
- output/reports/health_index/oos/{label}/stats.json

Then prints the 3 required numbers (HI - baseline): mdd diff, worst5 diff, mean diff.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
import sys

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

OUTROOT = ROOT / "output" / "reports" / "health_index" / "oos"
OUTROOT.mkdir(parents=True, exist_ok=True)

FALLBACK = "510300"


def mdd(r: pd.Series) -> float:
    eq = (1.0 + r.fillna(0.0)).cumprod()
    dd = eq / eq.cummax() - 1.0
    return float(dd.min())


def worst5_mean(r: pd.Series) -> float:
    x = r.dropna().values
    if x.size == 0:
        return float("nan")
    thr = np.quantile(x, 0.05)
    return float(r[r <= thr].mean())


def build_close_panel(db, codes: list[str], start: str, end: str) -> pd.DataFrame:
    import pymongo

    coll = db["stock_day"]
    q = {"code": {"$in": codes}, "date": {"$gte": start, "$lte": end}}
    proj = {"_id": 0, "code": 1, "date": 1, "close": 1}
    rows = list(coll.find(q, proj))
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["code"] = df["code"].astype(str).str.zfill(6)
    close = df.pivot(index="date", columns="code", values="close").sort_index().apply(pd.to_numeric, errors="coerce")
    return close


def weights_by_day(runs: list[dict], close_idx: pd.DatetimeIndex) -> pd.DataFrame:
    # runs: [{rebalance_date, positions{code->w}}]
    by_reb = {pd.to_datetime(r["rebalance_date"]): r["positions"] for r in runs}
    reb_dates = sorted(by_reb.keys())
    cols = sorted({c for r in runs for c in r["positions"].keys() if c != "CASH"})
    wdf = pd.DataFrame(0.0, index=close_idx, columns=cols)
    for i, d in enumerate(reb_dates):
        start_i = d
        end_i = reb_dates[i + 1] if i + 1 < len(reb_dates) else close_idx.max() + pd.Timedelta(days=1)
        mask = (wdf.index >= start_i) & (wdf.index < end_i)
        for c, w in by_reb[d].items():
            if c == "CASH":
                continue
            if c in wdf.columns:
                wdf.loc[mask, c] = float(w)
    return wdf


def equity_from_close(close: pd.DataFrame, w: pd.DataFrame) -> pd.Series:
    w = w.reindex(close.index).ffill().fillna(0.0)
    w_eff = w.shift(1).fillna(0.0)
    dret = close.pct_change(fill_method=None).fillna(0.0)
    ret = (w_eff * dret).sum(axis=1)
    return (1.0 + ret).cumprod()


def run_range(label: str, start: str, end: str, *, use_hi: bool, health_cache_dir: str | None) -> tuple[pd.Series, pd.Series]:
    from api.signals_impl import run_signal
    from ladder_audit_2022 import pick_rebalance_dates
    import pymongo

    # mongo env
    os.environ.setdefault("MONGODB_HOST", "127.0.0.1")
    os.environ.setdefault("MONGODB_PORT", "27017")
    os.environ.setdefault("MONGODB_DATABASE", "quantaxis")
    os.environ.setdefault("MONGODB_USER", "quantaxis")
    os.environ.setdefault("MONGODB_PASSWORD", "quantaxis")

    # health cache
    if use_hi and health_cache_dir:
        os.environ["QUANTAXIS_HEALTH_CACHE_DIR"] = health_cache_dir
    else:
        # point to empty dir to force missing => exposure=1
        tmp = OUTROOT / "_empty_health_cache"
        tmp.mkdir(parents=True, exist_ok=True)
        os.environ["QUANTAXIS_HEALTH_CACHE_DIR"] = str(tmp)

    theme = "a_ex_kcb_bse"
    rebs = pick_rebalance_dates(start, end, theme)

    runs = []
    held_codes = set([FALLBACK])

    for d in rebs:
        sid = f"oos_{label}_{'hi' if use_hi else 'base'}_{d.replace('-', '')}_{int(time.time()*1000)}"
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
            "min_weight": 0.04,
            "hard_dist_252h_min": -0.4,
            "hard_downvol_q": 0.70,
            "fallback_asset": FALLBACK,
            "start": "2019-01-01",
            "end": d,
        }
        run_signal(sid, cfg)
        obj = json.loads((ROOT / "output" / "signals" / f"{sid}.json").read_text(encoding="utf-8"))
        pos = {str(p["code"]).zfill(6) if str(p["code"]).isdigit() else str(p["code"]): float(p["weight"]) for p in obj.get("positions", []) if float(p.get("weight", 0.0)) > 0}
        runs.append({"rebalance_date": str(obj.get("as_of_date")), "positions": pos})
        for c in pos.keys():
            if c not in {"CASH"}:
                held_codes.add(c)

    # build close panel for held codes
    c = pymongo.MongoClient(
        "mongodb://quantaxis:quantaxis@127.0.0.1:27017/quantaxis?authSource=admin",
        serverSelectionTimeoutMS=8000,
    )
    db = c["quantaxis"]
    close = build_close_panel(db, sorted({c for c in held_codes if c != "CASH"}), start, end)

    w = weights_by_day(runs, close.index)
    eq = equity_from_close(close, w)
    ret = eq.pct_change(fill_method=None).fillna(0.0)
    return eq, ret


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--health-cache-dir", required=True)
    args = ap.parse_args()

    outdir = OUTROOT / args.label
    outdir.mkdir(parents=True, exist_ok=True)

    eq_b, r_b = run_range(args.label, args.start, args.end, use_hi=False, health_cache_dir=None)
    eq_h, r_h = run_range(args.label, args.start, args.end, use_hi=True, health_cache_dir=args.health_cache_dir)

    pd.DataFrame({"date": eq_b.index.astype(str), "equity": eq_b.values}).to_csv(outdir / "equity_baseline.csv", index=False)
    pd.DataFrame({"date": eq_h.index.astype(str), "equity": eq_h.values}).to_csv(outdir / "equity_hi.csv", index=False)

    base = {"mdd": mdd(r_b), "worst5_mean": worst5_mean(r_b), "mean": float(r_b.mean())}
    hi = {"mdd": mdd(r_h), "worst5_mean": worst5_mean(r_h), "mean": float(r_h.mean())}

    diff = {"mdd": hi["mdd"] - base["mdd"], "worst5_mean": hi["worst5_mean"] - base["worst5_mean"], "mean": hi["mean"] - base["mean"]}
    stats = {"period": {"start": args.start, "end": args.end}, "baseline": base, "hi": hi, "hi_minus_baseline": diff}
    (outdir / "stats.json").write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(diff, ensure_ascii=False))


if __name__ == "__main__":
    main()
