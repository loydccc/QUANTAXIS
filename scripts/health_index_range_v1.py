#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Health Index v1 (fixed spec) for an arbitrary date range.

Creates daily cache files and a range CSV.

Outputs:
- output/reports/health_index/ranges/health_score_{start}_{end}.csv
- output/reports/health_index/daily/health_score_{YYYY-MM-DD}.json

Spec notes:
- Components are 0..1 and mapped via time-series rank percentiles within the target range.
- health_score = mean(available components); records n_components_used.
- signal_stability and portfolio_feasibility are left as NaN if not available.

This script is read-only.
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
OUT_RANGES = ROOT / "output" / "reports" / "health_index" / "ranges"
OUT_RANGES.mkdir(parents=True, exist_ok=True)
OUT_DAILY = ROOT / "output" / "reports" / "health_index" / "daily"
OUT_DAILY.mkdir(parents=True, exist_ok=True)


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


def rank_pct(s: pd.Series) -> pd.Series:
    return s.rank(pct=True)


def winsor(s: pd.Series, lo=0.01, hi=0.99) -> pd.Series:
    if s.dropna().empty:
        return s
    return s.clip(lower=s.quantile(lo), upper=s.quantile(hi))


def load_panels(db, start: str, end: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    coll = db["stock_day"]
    q = {"date": {"$gte": start, "$lte": end}}
    proj = {"_id": 0, "code": 1, "date": 1, "close": 1, "amount": 1}
    rows = list(coll.find(q, proj))
    if not rows:
        raise RuntimeError("no stock_day rows")
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["code"] = df["code"].astype(str).str.zfill(6)
    close = df.pivot(index="date", columns="code", values="close").sort_index().apply(pd.to_numeric, errors="coerce")
    amount = df.pivot(index="date", columns="code", values="amount").sort_index().apply(pd.to_numeric, errors="coerce")
    return close, amount


def downside_vol_20(ret: pd.DataFrame) -> pd.DataFrame:
    def f(x: np.ndarray) -> float:
        x = x[np.isfinite(x)]
        x = x[x < 0]
        if x.size < 5:
            return float("nan")
        return float(np.std(x, ddof=1))

    return ret.rolling(20, min_periods=20).apply(lambda s: f(s.values), raw=False)


def amihud_20(ret: pd.DataFrame, amount: pd.DataFrame) -> pd.DataFrame:
    x = (ret.abs() / amount.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)
    return x.rolling(20, min_periods=20).mean()


def median_corr_20d(ret: pd.DataFrame) -> pd.Series:
    idx = ret.index
    out = []
    for i in range(len(idx)):
        if i < 19:
            out.append(np.nan)
            continue
        win = ret.iloc[i - 19 : i + 1]
        w = win.dropna(axis=1, how="any")
        if w.shape[1] < 30:
            out.append(np.nan)
            continue
        c = np.corrcoef(w.values.T)
        iu = np.triu_indices(c.shape[0], k=1)
        out.append(float(np.median(c[iu])) if iu[0].size else np.nan)
    return pd.Series(out, index=idx)


def breakout_pass_ratio(close: pd.DataFrame) -> pd.Series:
    mx60 = close.rolling(60, min_periods=60).max()
    cond = close >= (0.98 * mx60)
    return cond.mean(axis=1, skipna=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    start = str(args.start)
    end = str(args.end)

    c = mongo()
    db = c[os.getenv("MONGODB_DATABASE", "quantaxis")]

    # include lookback
    start_lb = (pd.to_datetime(start) - pd.Timedelta(days=120)).date().isoformat()
    close, amount = load_panels(db, start_lb, end)

    close_t = close.loc[(close.index >= pd.to_datetime(start)) & (close.index <= pd.to_datetime(end))]
    ret = close.pct_change(fill_method=None)
    ret_t = ret.loc[close_t.index]

    # components raw series
    q90_downvol = downside_vol_20(ret).loc[close_t.index].quantile(0.9, axis=1, numeric_only=True)
    medcorr = median_corr_20d(ret_t)
    q80_amihud = amihud_20(ret, amount).loc[close_t.index].quantile(0.8, axis=1, numeric_only=True)
    pass_ratio = breakout_pass_ratio(close).loc[close_t.index]

    # signal_stability / portfolio_feasibility not available here (NaN)
    q80_rank_std = pd.Series(np.nan, index=close_t.index)
    p05_eff = pd.Series(np.nan, index=close_t.index)

    tail_risk = 1.0 - rank_pct(winsor(q90_downvol))
    crowding = 1.0 - rank_pct(winsor(medcorr))
    liquidity_stress = 1.0 - rank_pct(winsor(q80_amihud))
    signal_stability = 1.0 - rank_pct(winsor(q80_rank_std))
    breakout_failure = rank_pct(winsor(pass_ratio))
    portfolio_feasibility = rank_pct(winsor(p05_eff))

    comp = pd.DataFrame(
        {
            "date": close_t.index.strftime("%Y-%m-%d"),
            "tail_risk": tail_risk.values,
            "crowding": crowding.values,
            "liquidity_stress": liquidity_stress.values,
            "signal_stability": signal_stability.values,
            "breakout_failure": breakout_failure.values,
            "portfolio_feasibility": portfolio_feasibility.values,
        }
    )

    comp_vals = comp.set_index("date")
    n_used = comp_vals.notna().sum(axis=1)
    score = comp_vals.mean(axis=1, skipna=True).clip(0.0, 1.0)

    out_csv = OUT_RANGES / f"health_score_{start}_{end}.csv"
    out_df = pd.DataFrame({"date": score.index, "health_score": score.values, "n_components_used": n_used.values})
    out_df.to_csv(out_csv, index=False)

    # write daily cache
    for d in score.index:
        p = OUT_DAILY / f"health_score_{d}.json"
        payload = {
            "date": d,
            "health_score": float(score.loc[d]) if pd.notna(score.loc[d]) else None,
            "n_components_used": int(n_used.loc[d]),
            "components": {k: (None if pd.isna(comp_vals.loc[d, k]) else float(comp_vals.loc[d, k])) for k in comp_vals.columns},
            "spec": "health_index_v1",
            "range": {"start": start, "end": end},
        }
        p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(str(out_csv))


if __name__ == "__main__":
    main()
