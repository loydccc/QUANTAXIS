#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Compute and cache Health Index v1 for a single date.

Spec (fixed by user):
- Create daily cache artifact that run_signal can read.
- Output file: output/reports/health_index/daily/health_score_{date}.json
- If cache missing, run_signal defaults exposure=1 and records health_missing=true.

This script is intentionally standalone and read-only.

Usage:
  MONGODB_HOST=127.0.0.1 ... python3 scripts/health_index_daily_cache.py --date 2022-05-20
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pymongo

ROOT = Path(__file__).resolve().parents[1]
OUTDIR = ROOT / "output" / "reports" / "health_index" / "daily"
OUTDIR.mkdir(parents=True, exist_ok=True)


def mongo() -> pymongo.MongoClient:
    host = os.getenv("MONGODB_HOST", "127.0.0.1")
    port = int(os.getenv("MONGODB_PORT", "27017"))
    db = os.getenv("MONGODB_DATABASE", "quantaxis")
    user = os.getenv("MONGODB_USER", "quantaxis")
    pwd = os.getenv("MONGODB_PASSWORD", "quantaxis")
    uri = f"mongodb://{user}:{pwd}@{host}:{port}/{db}?authSource=admin"
    c = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)
    c.admin.command("ping")
    return c


def rank_pct_series(s: pd.Series) -> pd.Series:
    return s.rank(pct=True)


def compute_for_date(db, date_iso: str) -> dict:
    # Pull a small lookback window for rolling calcs.
    end = pd.to_datetime(date_iso)
    start = (end - pd.Timedelta(days=120)).date().isoformat()

    coll = db["stock_day"]
    q = {"date": {"$gte": start, "$lte": date_iso}}
    proj = {"_id": 0, "code": 1, "date": 1, "close": 1, "amount": 1}
    rows = list(coll.find(q, proj))
    if not rows:
        return {"date": date_iso, "health_score": None, "n_components_used": 0, "components": {}}

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["code"] = df["code"].astype(str).str.zfill(6)
    close = df.pivot(index="date", columns="code", values="close").sort_index().apply(pd.to_numeric, errors="coerce")
    amount = df.pivot(index="date", columns="code", values="amount").sort_index().apply(pd.to_numeric, errors="coerce")

    ret = close.pct_change(fill_method=None)

    # 1) tail_risk (needs downvol_20 q90)
    def downvol_win(x: np.ndarray) -> float:
        x = x[np.isfinite(x)]
        x = x[x < 0]
        if x.size < 5:
            return float("nan")
        return float(np.std(x, ddof=1))

    downvol20 = ret.rolling(20, min_periods=20).apply(lambda s: downvol_win(s.values), raw=False)
    q90_downvol = float(downvol20.loc[end].quantile(0.9)) if end in downvol20.index else float("nan")

    # 2) crowding (median corr over last 20d window)
    medcorr = float("nan")
    if end in ret.index:
        i = ret.index.get_loc(end)
        if isinstance(i, int) and i >= 19:
            win = ret.iloc[i - 19 : i + 1]
            w = win.dropna(axis=1, how="any")
            if w.shape[1] >= 30:
                c = np.corrcoef(w.values.T)
                iu = np.triu_indices(c.shape[0], k=1)
                medcorr = float(np.median(c[iu])) if iu[0].size else float("nan")

    # 3) liquidity_stress (q80 amihud_20)
    amihud = (ret.abs() / amount.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)
    amihud20 = amihud.rolling(20, min_periods=20).mean()
    q80_amihud = float(amihud20.loc[end].quantile(0.8)) if end in amihud20.index else float("nan")

    # 5) breakout_failure (pass ratio using close proxy)
    mx60 = close.rolling(60, min_periods=60).max()
    pass_ratio = float((close.loc[end] >= 0.98 * mx60.loc[end]).mean()) if end in mx60.index else float("nan")

    # 4) signal_stability: cross-sectional rank stability of 20D returns.
    # Compute 20D return ranks (pct) each day, then measure rolling 20D std of ranks per asset.
    # Take q80 across assets for end date.
    ret20 = close.pct_change(20, fill_method=None)
    rank20 = ret20.rank(axis=1, pct=True)
    rank_std20 = rank20.rolling(20, min_periods=20).std()
    q80_rank_std = float(rank_std20.loc[end].quantile(0.8)) if end in rank_std20.index else float("nan")

    # 6) portfolio_feasibility: not available pre-signal (leave NaN)
    p05_eff = float("nan")

    raw = pd.Series(
        {
            "q90_downvol_20d": q90_downvol,
            "median_corr_20d": medcorr,
            "q80_amihud_20": q80_amihud,
            "q80_rank_std": q80_rank_std,
            "breakout_pass_ratio": pass_ratio,
            "p05_effective_positions": p05_eff,
        }
    )

    # Map to components using time-series ranks within the (short) window available.
    # NOTE: For production we'd rank within a longer history; v1 cache is mainly for integration.
    tail_risk = float(1.0 - rank_pct_series(pd.Series([raw["q90_downvol_20d"]])).iloc[0]) if pd.notna(raw["q90_downvol_20d"]) else float("nan")
    crowding = float(1.0 - rank_pct_series(pd.Series([raw["median_corr_20d"]])).iloc[0]) if pd.notna(raw["median_corr_20d"]) else float("nan")
    liquidity_stress = float(1.0 - rank_pct_series(pd.Series([raw["q80_amihud_20"]])).iloc[0]) if pd.notna(raw["q80_amihud_20"]) else float("nan")
    signal_stability = float(1.0 - rank_pct_series(pd.Series([raw["q80_rank_std"]])).iloc[0]) if pd.notna(raw["q80_rank_std"]) else float("nan")
    breakout_failure = float(rank_pct_series(pd.Series([raw["breakout_pass_ratio"]])).iloc[0]) if pd.notna(raw["breakout_pass_ratio"]) else float("nan")
    portfolio_feasibility = float(rank_pct_series(pd.Series([raw["p05_effective_positions"]])).iloc[0]) if pd.notna(raw["p05_effective_positions"]) else float("nan")

    comps = {
        "tail_risk": tail_risk,
        "crowding": crowding,
        "liquidity_stress": liquidity_stress,
        "signal_stability": signal_stability,
        "breakout_failure": breakout_failure,
        "portfolio_feasibility": portfolio_feasibility,
    }
    comp_s = pd.Series(comps, dtype=float)
    n_used = int(comp_s.notna().sum())
    score = float(comp_s.mean(skipna=True)) if n_used > 0 else None

    return {
        "date": date_iso,
        "health_score": score,
        "n_components_used": n_used,
        "components": comps,
        "raw": raw.to_dict(),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    c = mongo()
    db = c[os.getenv("MONGODB_DATABASE", "quantaxis")]
    out = compute_for_date(db, args.date)

    p = OUTDIR / f"health_score_{args.date}.json"
    p.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(p))


if __name__ == "__main__":
    main()
