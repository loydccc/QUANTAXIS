#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""System Health Index (SHI) for QUANTAXIS.

User-spec (fixed): daily health_score in [0,1] from exactly 6 inputs
1) tail_risk
2) crowding
3) signal_stability
4) breakout_failure
5) liquidity_stress
6) portfolio_feasibility

Outputs (fixed):
- output/reports/health_index/health_score_2022.csv
- bucket comparison top30% vs bottom30%
- sample 10 low-score days with component snapshots

NOTE: This script is intentionally standalone and read-only (no strategy param changes).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import pymongo

ROOT = Path(__file__).resolve().parents[1]
OUTDIR = ROOT / "output" / "reports" / "health_index"
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


def _rank_pct_series(s: pd.Series) -> pd.Series:
    """0..1 percentile rank over time (ties average)."""
    return s.rank(pct=True)


def _winsor(s: pd.Series, lo=0.01, hi=0.99) -> pd.Series:
    a = s.copy()
    ql = a.quantile(lo)
    qh = a.quantile(hi)
    return a.clip(lower=ql, upper=qh)


def _load_panel_2022_from_mongo(db, start: str, end: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch close + amount panels for [start,end] (ISO dates) from stock_day.

    Uses a single date-range scan + pivot (fast enough for 2022).
    """
    coll = db["stock_day"]
    q = {"date": {"$gte": start, "$lte": end}}
    proj = {"_id": 0, "code": 1, "date": 1, "close": 1, "amount": 1}
    cur = coll.find(q, proj)
    rows = list(cur)
    if not rows:
        raise RuntimeError("no stock_day rows for range")
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["code"] = df["code"].astype(str).str.zfill(6)

    close = df.pivot(index="date", columns="code", values="close").sort_index()
    amount = df.pivot(index="date", columns="code", values="amount").sort_index()
    # numeric
    close = close.apply(pd.to_numeric, errors="coerce")
    amount = amount.apply(pd.to_numeric, errors="coerce")
    return close, amount


def _downside_vol_20(ret: pd.DataFrame) -> pd.DataFrame:
    """Per-code downside vol (std of negative returns) over 20d rolling window."""
    def f(x: np.ndarray) -> float:
        x = x[np.isfinite(x)]
        x = x[x < 0]
        if x.size < 5:
            return float("nan")
        return float(np.std(x, ddof=1))

    return ret.rolling(20, min_periods=20).apply(lambda s: f(s.values), raw=False)


def _amihud_20(ret: pd.DataFrame, amount: pd.DataFrame) -> pd.DataFrame:
    # amihud = mean(|ret|/amount)
    x = (ret.abs() / amount.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)
    return x.rolling(20, min_periods=20).mean()


def _breakout_pass_ratio(close: pd.DataFrame) -> pd.Series:
    # breakout_60 = close / max(high_60) - 1; we approximate using close as high proxy.
    # pass condition: breakout_60 > -0.02  <=> close >= 0.98 * max_60
    mx60 = close.rolling(60, min_periods=60).max()
    cond = close >= (0.98 * mx60)
    return cond.mean(axis=1, skipna=True)


def _median_corr_20d(ret: pd.DataFrame) -> pd.Series:
    """Median of upper-triangle correlation matrix over trailing 20d window.

    NOTE: O(N^2) per day; relies on N being manageable after NaN drop.
    """
    out = []
    idx = ret.index
    for i in range(len(idx)):
        if i < 19:
            out.append(np.nan)
            continue
        win = ret.iloc[i - 19 : i + 1]
        # require at least 30 names with full window
        w = win.dropna(axis=1, how="any")
        if w.shape[1] < 30:
            out.append(np.nan)
            continue
        c = np.corrcoef(w.values.T)
        iu = np.triu_indices(c.shape[0], k=1)
        med = float(np.median(c[iu])) if iu[0].size else np.nan
        out.append(med)
    return pd.Series(out, index=idx)


def _load_portfolio_feasibility_from_daily_signals(start: str, end: str) -> pd.Series:
    """Compute daily p05 effective_positions_after_min_weight from daily_* signals if present.

    If a day has no signal, returns NaN for that date.
    """
    sigdir = ROOT / "output" / "signals"
    # Accept any daily_YYYYMMDD_* status succeeded.
    # We scan JSON files directly.
    out: Dict[pd.Timestamp, float] = {}
    for p in sigdir.glob("daily_2022????_fb_*.json"):
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        asof = obj.get("as_of_date")
        if not (isinstance(asof, str) and start <= asof <= end):
            continue
        meta = obj.get("meta", {}) or {}
        ladder = meta.get("ladder", {}) or {}
        used = ladder.get("level_used")
        runs = ladder.get("runs") or []
        run_used = None
        for r in runs:
            if r.get("level") == used:
                run_used = r
                break
        if run_used is None and runs:
            run_used = runs[-1]
        ta = (run_used or {}).get("tranche_audit") or []
        vals = [int(t.get("effective_positions_after_min_weight", 0)) for t in ta]
        if not vals:
            continue
        out[pd.to_datetime(asof)] = float(pd.Series(vals).quantile(0.05))

    if not out:
        return pd.Series(dtype=float)
    s = pd.Series(out).sort_index()
    # ensure daily index coverage will be aligned later
    return s


def main():
    c = mongo()
    db = c[os.getenv("MONGODB_DATABASE", "quantaxis")]

    # Build panels (include lookback for 60d/20d windows)
    close, amount = _load_panel_2022_from_mongo(db, start="2021-10-01", end="2022-12-31")
    close_2022 = close.loc[(close.index >= "2022-01-01") & (close.index <= "2022-12-31")]
    amount_2022 = amount.reindex(close.index).loc[close_2022.index]

    ret = close.pct_change(fill_method=None)
    ret_2022 = ret.loc[close_2022.index]

    # --- components ---
    # 1) tail_risk: q90 downside vol
    downvol = _downside_vol_20(ret).loc[close_2022.index]
    q90_downvol = downvol.quantile(0.9, axis=1, numeric_only=True)

    # 2) crowding: median corr over 20d window
    medcorr20 = _median_corr_20d(ret_2022)

    # 3) liquidity_stress: q80 amihud_20
    amihud20 = _amihud_20(ret, amount).loc[close_2022.index]
    q80_amihud = amihud20.quantile(0.8, axis=1, numeric_only=True)

    # 5) breakout_failure: pass_ratio (higher is healthier)
    pass_ratio = _breakout_pass_ratio(close).loc[close_2022.index]

    # 6) portfolio_feasibility: p05 effective positions (if signals exist)
    p05_eff = _load_portfolio_feasibility_from_daily_signals("2022-01-01", "2022-12-31")
    p05_eff = p05_eff.reindex(close_2022.index)

    # 4) signal_stability: not yet available as rank_std; use NaN for v1 if missing.
    q80_rank_std = pd.Series(np.nan, index=close_2022.index)

    # Map to 0..1 components (time-series rank pct)
    tail_risk = 1.0 - _rank_pct_series(_winsor(q90_downvol))
    crowding = 1.0 - _rank_pct_series(_winsor(medcorr20))
    liquidity_stress = 1.0 - _rank_pct_series(_winsor(q80_amihud))
    signal_stability = 1.0 - _rank_pct_series(_winsor(q80_rank_std))
    breakout_failure = _rank_pct_series(_winsor(pass_ratio))
    portfolio_feasibility = _rank_pct_series(_winsor(p05_eff))

    comp = pd.DataFrame(
        {
            "date": close_2022.index.astype(str),
            "tail_risk": tail_risk.values,
            "crowding": crowding.values,
            "liquidity_stress": liquidity_stress.values,
            "signal_stability": signal_stability.values,
            "breakout_failure": breakout_failure.values,
            "portfolio_feasibility": portfolio_feasibility.values,
        }
    )
    comp.to_csv(OUTDIR / "health_components_2022.csv", index=False)

    # health_score = mean(available components)
    comp_vals = comp.set_index("date")
    n_used = comp_vals.notna().sum(axis=1)
    score = comp_vals.mean(axis=1, skipna=True)
    score = score.clip(lower=0.0, upper=1.0)

    score_df = pd.DataFrame(
        {
            "date": score.index,
            "health_score": score.values,
            "n_components_used": n_used.values,
        }
    )
    score_df.to_csv(OUTDIR / "health_score_2022.csv", index=False)

    # bucket comparison top30 vs bottom30 using SP500? user wants system performance; we proxy with 510300 daily ret.
    # Use 510300 close-to-close returns.
    try:
        etf = db["stock_day"].find({"code": "510300", "date": {"$gte": "2022-01-01", "$lte": "2022-12-31"}}, {"_id": 0, "date": 1, "close": 1})
        etf = pd.DataFrame(list(etf))
        etf["date"] = pd.to_datetime(etf["date"])
        etf = etf.dropna().drop_duplicates(subset=["date"]).set_index("date").sort_index()
        etf_ret = etf["close"].pct_change(fill_method=None).reindex(close_2022.index).fillna(0.0)
    except Exception:
        etf_ret = pd.Series(0.0, index=close_2022.index)

    s = score_df.set_index(pd.to_datetime(score_df["date"]))["health_score"]
    q30 = s.quantile(0.30)
    q70 = s.quantile(0.70)
    top = etf_ret[s >= q70]
    bot = etf_ret[s <= q30]

    def mdd(r: pd.Series) -> float:
        eq = (1.0 + r.fillna(0.0)).cumprod()
        dd = eq / eq.cummax() - 1.0
        return float(dd.min())

    def worst5(r: pd.Series) -> float:
        x = r.dropna().values
        if x.size == 0:
            return float("nan")
        thr = np.quantile(x, 0.05)
        return float(r[r <= thr].mean())

    buckets = pd.DataFrame(
        [
            {"bucket": "top30", "avg_ret": float(top.mean()), "mdd": mdd(top), "worst5_mean": worst5(top), "n": int(top.shape[0])},
            {"bucket": "bottom30", "avg_ret": float(bot.mean()), "mdd": mdd(bot), "worst5_mean": worst5(bot), "n": int(bot.shape[0])},
        ]
    )
    buckets.to_csv(OUTDIR / "health_buckets_2022.csv", index=False)

    # low10 cases
    low10 = s.nsmallest(10)
    cases = comp.set_index(pd.to_datetime(comp["date"]))
    rows = []
    for d, sc in low10.items():
        row = {"date": str(d.date()), "health_score": float(sc)}
        for k in ["tail_risk", "crowding", "liquidity_stress", "signal_stability", "breakout_failure", "portfolio_feasibility"]:
            row[k] = float(cases.loc[d, k]) if d in cases.index and pd.notna(cases.loc[d, k]) else float("nan")
        # ladder level (weekly known; daily unknown) -> None
        row["ladder_level"] = None
        row["ret_510300"] = float(etf_ret.loc[d]) if d in etf_ret.index else float("nan")
        rows.append(row)

    pd.DataFrame(rows).to_csv(OUTDIR / "health_low10_cases_2022.csv", index=False)

    print(json.dumps({"outdir": str(OUTDIR), "n_days": int(len(close_2022.index))}, ensure_ascii=False))


if __name__ == "__main__":
    main()
