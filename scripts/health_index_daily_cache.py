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
from typing import Dict, List

import numpy as np
import pandas as pd
import pymongo

ROOT = Path(__file__).resolve().parents[1]
OUTDIR = ROOT / "output" / "reports" / "health_index" / "daily"
OUTDIR.mkdir(parents=True, exist_ok=True)

RANK_WINDOW = int(os.getenv("QUANTAXIS_HEALTH_RANK_WINDOW", "252"))
MIN_HISTORY = int(os.getenv("QUANTAXIS_HEALTH_MIN_HISTORY", "60"))
RAW_KEYS = (
    "q90_downvol_20d",
    "median_corr_20d",
    "q80_amihud_20",
    "q80_rank_std",
    "breakout_pass_ratio",
    "p05_effective_positions",
)


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


def _safe_float(x):
    try:
        v = float(x)
    except Exception:
        return None
    if not np.isfinite(v):
        return None
    return v


def _cache_date(path: Path):
    name = path.stem
    if not name.startswith("health_score_"):
        return None
    d = name[len("health_score_") :]
    try:
        return pd.to_datetime(d, format="%Y-%m-%d", errors="raise")
    except Exception:
        return None


def _load_history_raw(end_date_iso: str, limit: int) -> Dict[str, List[float]]:
    """Load historical raw metric values from daily cache files.

    We use cached raw values to form a rolling history distribution for percentile mapping.
    This avoids expensive full-range recomputation inside single-day cache jobs.
    """
    end_ts = pd.to_datetime(end_date_iso)
    hist: Dict[str, List[float]] = {k: [] for k in RAW_KEYS}

    for p in sorted(OUTDIR.glob("health_score_????-??-??.json")):
        d = _cache_date(p)
        if d is None or d >= end_ts:
            continue
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        raw = obj.get("raw") or {}
        if not isinstance(raw, dict):
            continue
        for k in RAW_KEYS:
            v = _safe_float(raw.get(k))
            if v is not None:
                hist[k].append(v)

    if limit > 0:
        for k in RAW_KEYS:
            if len(hist[k]) > limit:
                hist[k] = hist[k][-limit:]
    return hist


def _series_history(s: pd.Series, end_ts: pd.Timestamp, limit: int) -> List[float]:
    """Extract finite history values from a time series before end_ts."""
    if s is None or len(s) == 0:
        return []
    x = s.copy()
    x = x[x.index < end_ts]
    x = pd.to_numeric(x, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if x.empty:
        return []
    if limit > 0:
        x = x.tail(limit)
    return [float(v) for v in x.values]


def _rank_with_history(current: float, history: List[float], min_history: int):
    """Return (rank_pct, hist_n, used_neutral_fallback).

    If there is not enough history, return neutral 0.5 to avoid unstable cliff behavior.
    """
    cv = _safe_float(current)
    hv = [float(x) for x in history if _safe_float(x) is not None]
    n = len(hv)
    if cv is None:
        return float("nan"), n, False
    if n < max(1, int(min_history)):
        return 0.5, n, True
    s = pd.Series(hv + [cv], dtype=float)
    rk = float(s.rank(pct=True, method="average").iloc[-1])
    if not np.isfinite(rk):
        return 0.5, n, True
    return rk, n, False


def compute_for_date(db, date_iso: str) -> dict:
    # Pull enough history to support rolling metrics + ranking window.
    end = pd.to_datetime(date_iso)
    hist_days = max(120, int(RANK_WINDOW * 2))
    start = (end - pd.Timedelta(days=hist_days)).date().isoformat()

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
    q90_downvol_s = downvol20.quantile(0.9, axis=1, numeric_only=True)
    q90_downvol = float(q90_downvol_s.loc[end]) if end in q90_downvol_s.index else float("nan")

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
    q80_amihud_s = amihud20.quantile(0.8, axis=1, numeric_only=True)
    q80_amihud = float(q80_amihud_s.loc[end]) if end in q80_amihud_s.index else float("nan")

    # 5) breakout_failure (pass ratio using close proxy)
    mx60 = close.rolling(60, min_periods=60).max()
    pass_ratio_s = (close >= 0.98 * mx60).mean(axis=1, skipna=True)
    pass_ratio = float(pass_ratio_s.loc[end]) if end in pass_ratio_s.index else float("nan")

    # 4) signal_stability: cross-sectional rank stability of 20D returns.
    # Compute 20D return ranks (pct) each day, then measure rolling 20D std of ranks per asset.
    # Take q80 across assets for end date.
    ret20 = close.pct_change(20, fill_method=None)
    rank20 = ret20.rank(axis=1, pct=True)
    rank_std20 = rank20.rolling(20, min_periods=20).std()
    q80_rank_std_s = rank_std20.quantile(0.8, axis=1, numeric_only=True)
    q80_rank_std = float(q80_rank_std_s.loc[end]) if end in q80_rank_std_s.index else float("nan")

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

    # Map raw metrics to components via rolling history percentile.
    # This avoids pathological 0/1 collapse from single-point ranking.
    hist = _load_history_raw(date_iso, limit=max(1, int(RANK_WINDOW)))
    # Prefer live-computed series history when available; fallback to cache history.
    hist["q90_downvol_20d"] = _series_history(q90_downvol_s, end, max(1, int(RANK_WINDOW))) or hist["q90_downvol_20d"]
    hist["q80_amihud_20"] = _series_history(q80_amihud_s, end, max(1, int(RANK_WINDOW))) or hist["q80_amihud_20"]
    hist["q80_rank_std"] = _series_history(q80_rank_std_s, end, max(1, int(RANK_WINDOW))) or hist["q80_rank_std"]
    hist["breakout_pass_ratio"] = _series_history(pass_ratio_s, end, max(1, int(RANK_WINDOW))) or hist["breakout_pass_ratio"]

    rk_tail, n_tail, fb_tail = _rank_with_history(raw["q90_downvol_20d"], hist["q90_downvol_20d"], MIN_HISTORY)
    rk_crowd, n_crowd, fb_crowd = _rank_with_history(raw["median_corr_20d"], hist["median_corr_20d"], MIN_HISTORY)
    rk_liq, n_liq, fb_liq = _rank_with_history(raw["q80_amihud_20"], hist["q80_amihud_20"], MIN_HISTORY)
    rk_stab, n_stab, fb_stab = _rank_with_history(raw["q80_rank_std"], hist["q80_rank_std"], MIN_HISTORY)
    rk_break, n_break, fb_break = _rank_with_history(raw["breakout_pass_ratio"], hist["breakout_pass_ratio"], MIN_HISTORY)
    rk_p05, n_p05, fb_p05 = _rank_with_history(raw["p05_effective_positions"], hist["p05_effective_positions"], MIN_HISTORY)

    tail_risk = float(1.0 - rk_tail) if np.isfinite(rk_tail) else float("nan")
    crowding = float(1.0 - rk_crowd) if np.isfinite(rk_crowd) else float("nan")
    liquidity_stress = float(1.0 - rk_liq) if np.isfinite(rk_liq) else float("nan")
    signal_stability = float(1.0 - rk_stab) if np.isfinite(rk_stab) else float("nan")
    breakout_failure = float(rk_break) if np.isfinite(rk_break) else float("nan")
    portfolio_feasibility = float(rk_p05) if np.isfinite(rk_p05) else float("nan")

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
        "ranking": {
            "method": "rolling_cache_percentile",
            "rank_window": int(RANK_WINDOW),
            "min_history": int(MIN_HISTORY),
            "history_n": {
                "q90_downvol_20d": int(n_tail),
                "median_corr_20d": int(n_crowd),
                "q80_amihud_20": int(n_liq),
                "q80_rank_std": int(n_stab),
                "breakout_pass_ratio": int(n_break),
                "p05_effective_positions": int(n_p05),
            },
            "neutral_fallback_used": {
                "q90_downvol_20d": bool(fb_tail),
                "median_corr_20d": bool(fb_crowd),
                "q80_amihud_20": bool(fb_liq),
                "q80_rank_std": bool(fb_stab),
                "breakout_pass_ratio": bool(fb_break),
                "p05_effective_positions": bool(fb_p05),
            },
        },
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
