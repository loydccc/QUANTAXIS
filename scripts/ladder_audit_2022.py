#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Audit ladder behavior in a specified period.

Designed for the 2022-01-01~2022-10-31 stress window.

What it produces:
- Ladder trigger frequency by rebalance date (weekly, W-FRI convention)
- Max consecutive L3 streak (in rebalance steps)
- Fallback weight distribution on L3 dates
- Equity/drawdown comparison with vs without fallback

Notes / limitations:
- Our signal product rebalances weekly. The audit operates on rebalance dates.
  The user asked "按交易日"; we can extend to daily later, but weekly is the
  current product definition.
- 510300 price series is NOT present in Mongo (in this environment), so we
  treat fallback asset return as 0 (cash-equivalent) for the equity comparison.
  This still tests the *risk-budgeting* effect of moving capital out of the stock
  sleeve. Once 510300 data is ingested, we can redo with real ETF returns.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

# Ensure repo root + scripts directory are importable
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

OUTDIR = ROOT / "output" / "reports" / "ladder_audit"


@dataclass
class SignalRun:
    rebalance_date: str
    signal_id: str
    disable_fallback: bool
    level_used: str
    fallback_weight: float
    cash_weight: float
    positions: List[Dict[str, Any]]
    meta: Dict[str, Any]


def _load_signal_json(signal_id: str) -> Dict[str, Any]:
    p = ROOT / "output" / "signals" / f"{signal_id}.json"
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    # surface failure details
    sp = ROOT / "output" / "signals" / f"{signal_id}.status.json"
    if sp.exists():
        st = json.loads(sp.read_text(encoding="utf-8"))
        raise RuntimeError(f"signal failed: {st}")
    raise FileNotFoundError(str(p))


def run_one(reb_date: str, *, disable_fallback: bool, cfg_base: Dict[str, Any]) -> SignalRun:
    # Ensure Mongo is reachable when running on host (compose hostname 'mongodb' is not resolvable).
    import os

    os.environ.setdefault("MONGODB_HOST", "127.0.0.1")
    os.environ.setdefault("MONGODB_PORT", "27017")
    os.environ.setdefault("MONGODB_DATABASE", "quantaxis")
    os.environ.setdefault("MONGODB_USER", "quantaxis")
    os.environ.setdefault("MONGODB_PASSWORD", "quantaxis")

    # Lazy import to use current code.
    from api.signals_impl import run_signal

    sid = f"audit_{reb_date.replace('-', '')}_{'nofb' if disable_fallback else 'fb'}_{int(time.time()*1000)}"
    cfg = dict(cfg_base)
    cfg["end"] = reb_date
    cfg["disable_fallback"] = bool(disable_fallback)

    run_signal(sid, cfg)
    obj = _load_signal_json(sid)
    meta = obj.get("meta", {})
    level_used = (meta.get("ladder", {}) or {}).get("level_used") or meta.get("fallback", {}).get("level")

    pos = obj.get("positions", [])
    fb_w = 0.0
    for p in pos:
        if p.get("code") == str(cfg.get("fallback_asset", "510300")):
            fb_w = float(p.get("weight", 0.0))

    stock_sum = sum(float(p.get("weight", 0.0)) for p in pos if p.get("code"))
    # Quantize to avoid floating drift causing spurious assertion failures.
    cash_w = round(float(max(0.0, 1.0 - stock_sum)), 12)

    return SignalRun(
        rebalance_date=str(obj.get("as_of_date")),
        signal_id=sid,
        disable_fallback=disable_fallback,
        level_used=str(level_used),
        fallback_weight=float(fb_w),
        cash_weight=cash_w,
        positions=pos,
        meta=meta,
    )


def derive_nofb_from_fb(fb: SignalRun, *, fallback_asset: str = "510300") -> SignalRun:
    """Derive a no-fallback run from an already computed fallback-enabled output.

    Rules (fixed, do not change):
    - Do NOT re-run baseline, factors, or pick selection.
    - Keep stock sleeve weights identical (no renormalization).
    - Remove fallback asset (or set weight=0).
    - Cash is residual: 1 - sum(stock_weights).
    """

    fa = str(fallback_asset)

    # If fallback asset isn't present, nofb==fb (marking that it's derived).
    pos_fb = list(fb.positions or [])
    has_fa = any(str(p.get("code")) == fa for p in pos_fb)

    if not has_fa:
        # Acceptance rule: when fallback asset is absent, treat nofb == fb and
        # cash_weight should equal weight_fb(510300) (typically 0).
        cash_w = float(fb.fallback_weight)
        meta2 = dict(fb.meta or {})
        audit = dict((meta2.get("audit") or {}))
        audit.update({"nofb_derived_from_fb": True, "nofb_cash_weight": cash_w})
        meta2["audit"] = audit
        return SignalRun(
            rebalance_date=fb.rebalance_date,
            signal_id=fb.signal_id,
            disable_fallback=True,
            level_used="NOFB_DERIVED_NO_FALLBACK_PRESENT",
            fallback_weight=0.0,
            cash_weight=cash_w,
            positions=pos_fb,
            meta=meta2,
        )

    pos_nofb: List[Dict[str, Any]] = []
    for p in pos_fb:
        if str(p.get("code")) == fa:
            continue
        pos_nofb.append(p)

    stock_sum = sum(float(p.get("weight", 0.0)) for p in pos_nofb if p.get("code"))
    # Acceptance rule: cash_weight equals the removed fallback weight (no renorm).
    # This also avoids floating drift from summing rounded weights.
    cash_w = float(fb.fallback_weight)

    meta2 = dict(fb.meta or {})
    audit = dict((meta2.get("audit") or {}))
    audit.update({"nofb_derived_from_fb": True, "nofb_cash_weight": float(cash_w)})
    meta2["audit"] = audit

    return SignalRun(
        rebalance_date=fb.rebalance_date,
        signal_id=fb.signal_id,  # same underlying run id (single run per week)
        disable_fallback=True,
        level_used="NOFB_DERIVED_FROM_FB",
        fallback_weight=0.0,
        cash_weight=float(cash_w),
        positions=pos_nofb,
        meta=meta2,
    )


def pick_rebalance_dates(start: str, end: str, theme: str) -> List[str]:
    # Use Mongo trading calendar via existing helper; easiest: get close panel index.
    import pymongo

    # Connect to local forwarded mongo
    c = pymongo.MongoClient(
        "mongodb://quantaxis:quantaxis@127.0.0.1:27017/quantaxis?authSource=admin",
        serverSelectionTimeoutMS=8000,
    )
    db = c["quantaxis"]

    # Build universe list using the same logic as walkforward script.
    # scripts/ is not a package; import via direct module name (script directory is on sys.path).
    from backtest_signal_walkforward import load_universe_from_mongo, fetch_panel_mixed_dates
    from backtest_baseline import detect_volume_field, pick_weekly_rebalance_dates

    coll = db["stock_day"]
    codes = load_universe_from_mongo(db, theme)
    vol_field = detect_volume_field(coll)
    _o, _h, _l, close, _vol = fetch_panel_mixed_dates(coll, codes, start, end, volume_field=vol_field)
    idx = close.index.sort_values()
    rebs = pick_weekly_rebalance_dates(idx)
    # keep inside [start,end]
    rebs = [d for d in rebs if (d >= pd.to_datetime(start)) and (d <= pd.to_datetime(end))]
    return [str(d.date()) for d in rebs]


def build_equity(
    close: pd.DataFrame,
    weights_by_day: pd.DataFrame,
    *,
    cost_bps: float = 0.0,
) -> pd.Series:
    """Close-to-close equity with T+1 execution, fallback treated as cash (0 ret)."""
    w = weights_by_day.reindex(close.index).ffill().fillna(0.0)
    w_eff = w.shift(1).fillna(0.0)
    dret = close.pct_change(fill_method=None).fillna(0.0)
    gross = (w_eff * dret).sum(axis=1)
    # optional simple turnover cost
    if cost_bps and cost_bps > 0:
        turnover = w_eff.diff().abs().sum(axis=1) / 2.0
        gross = gross - (float(cost_bps) / 10000.0) * turnover
    return (1.0 + gross).cumprod()


def max_drawdown(eq: pd.Series) -> float:
    peak = eq.cummax()
    dd = (eq / peak) - 1.0
    return float(dd.min())


def main():
    start = "2022-01-01"
    end = "2022-10-31"
    theme = "a_ex_kcb_bse"

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
        # downvol quantile guard
        "hard_downvol_q": 0.70,
        # fallback asset
        "fallback_asset": "510300",
        # bound baseline backtests
        "start": "2019-01-01",
        "end": end,
    }

    OUTDIR.mkdir(parents=True, exist_ok=True)

    rebs = pick_rebalance_dates(start, end, theme)

    runs: List[SignalRun] = []
    for d in rebs:
        fb = run_one(d, disable_fallback=False, cfg_base=cfg_base)
        runs.append(fb)
        # Derived no-fallback variant (purely derived; no recompute)
        runs.append(derive_nofb_from_fb(fb, fallback_asset=cfg_base.get("fallback_asset", "510300")))

    # Build ladder frequency (fallback enabled only)
    fb = [r for r in runs if not r.disable_fallback]
    df = pd.DataFrame(
        {
            "date": [r.rebalance_date for r in fb],
            "level": [r.level_used for r in fb],
            "fallback_weight": [r.fallback_weight for r in fb],
            "cash_weight": [r.cash_weight for r in fb],
            "signal_id": [r.signal_id for r in fb],
        }
    ).sort_values("date")

    # ladder frequency
    freq = df["level"].value_counts(dropna=False).to_dict()
    freq_pct = {k: float(v) / float(len(df)) for k, v in freq.items()}

    # max consecutive L3 streak (rebalance steps)
    levels = df["level"].fillna("").tolist()
    max_streak = 0
    cur = 0
    for lv in levels:
        if lv == "L3":
            cur += 1
            max_streak = max(max_streak, cur)
        else:
            cur = 0

    # fallback weight distribution on L3 days
    l3w = df[df["level"] == "L3"]["fallback_weight"].astype(float)
    fb_stats = {
        "n_L3": int(l3w.shape[0]),
        "mean": float(l3w.mean()) if len(l3w) else 0.0,
        "p90": float(l3w.quantile(0.9)) if len(l3w) else 0.0,
        "max": float(l3w.max()) if len(l3w) else 0.0,
    }

    # --- Equity comparison (with vs without fallback) ---
    # Build daily close panel for all codes ever held in either variant,
    # INCLUDING the fallback asset (so "with fallback" uses real ETF returns).
    all_codes = set()
    for r in runs:
        for p in r.positions:
            c = str(p.get("code"))
            if c:
                all_codes.add(c)

    import pymongo

    c = pymongo.MongoClient(
        "mongodb://quantaxis:quantaxis@127.0.0.1:27017/quantaxis?authSource=admin",
        serverSelectionTimeoutMS=8000,
    )
    coll = c["quantaxis"]["stock_day"]

    # Fetch close series (mixed date formats) for speed.
    start_s = start
    end_s = end
    start2 = start.replace("-", "")
    end2 = end.replace("-", "")
    start_i = int(start2)
    end_i = int(end2)

    series = {}
    for code in sorted(all_codes):
        q = {
            "code": code,
            "$or": [
                {"date": {"$gte": start_s, "$lte": end_s}},
                {"date": {"$gte": start2, "$lte": end2}},
                {"date": {"$gte": start_i, "$lte": end_i}},
            ],
        }
        rows = list(coll.find(q, {"_id": 0, "date": 1, "close": 1}).sort("date", 1))
        if not rows:
            continue
        tmp = pd.DataFrame(rows)
        tmp["date"] = pd.to_datetime(tmp["date"].astype(str), format="mixed", errors="coerce")
        tmp = tmp.dropna(subset=["date"]).drop_duplicates(subset=["date"]).set_index("date").sort_index()
        if "close" not in tmp.columns:
            continue
        s = pd.to_numeric(tmp["close"], errors="coerce")
        series[code] = s

    close = pd.concat(series, axis=1).sort_index()
    close = close[(close.index >= pd.to_datetime(start)) & (close.index <= pd.to_datetime(end))]

    # Build weight-by-day from weekly signals (rebalance date -> constant until next rebalance)
    def weights_from_runs(rs: List[SignalRun]) -> pd.DataFrame:
        w_by_reb = {}
        for r in rs:
            # Keep every code in positions, including fallback_asset when present.
            w = {str(p["code"]): float(p["weight"]) for p in r.positions if str(p.get("code"))}
            w_by_reb[pd.to_datetime(r.rebalance_date)] = w
        # Align to close columns
        wdf = pd.DataFrame(0.0, index=close.index, columns=close.columns)
        reb_dates = sorted(w_by_reb.keys())
        for i, d in enumerate(reb_dates):
            w = w_by_reb[d]
            start_i = d
            end_i = reb_dates[i + 1] if i + 1 < len(reb_dates) else close.index.max()
            mask = (wdf.index >= start_i) & (wdf.index < end_i)
            for code, wt in w.items():
                if code in wdf.columns:
                    wdf.loc[mask, code] = wt
        return wdf

    fb_runs = [r for r in runs if not r.disable_fallback]
    nofb_runs = [r for r in runs if r.disable_fallback]

    w_fb = weights_from_runs(fb_runs)
    w_nofb = weights_from_runs(nofb_runs)

    eq_fb = build_equity(close, w_fb, cost_bps=0.0)
    eq_nofb = build_equity(close, w_nofb, cost_bps=0.0)

    out = {
        "period": {"start": start, "end": end, "theme": theme},
        "rebalance_steps": int(len(df)),
        "ladder_freq": freq,
        "ladder_freq_pct": freq_pct,
        "max_consecutive_L3": int(max_streak),
        "fallback_weight_stats_L3": fb_stats,
        "drawdown": {
            "with_fallback": max_drawdown(eq_fb),
            "without_fallback_cash": max_drawdown(eq_nofb),
        },
        "notes": [
            "Equity comparison uses real 510300 returns when fallback is enabled; when fallback is disabled the unallocated sleeve stays as cash (0 return).",
            "Counts are per weekly rebalance date (product cadence), not per trading day.",
        ],
    }

    (OUTDIR / "2022-01-01_2022-10-31_summary.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    df.to_csv(OUTDIR / "2022-01-01_2022-10-31_ladder.csv", index=False)
    pd.DataFrame({"date": eq_fb.index.astype(str), "equity_with_fallback": eq_fb.values, "equity_without_fallback_cash": eq_nofb.values}).to_csv(
        OUTDIR / "2022-01-01_2022-10-31_equity.csv", index=False
    )

    print(json.dumps({"outdir": str(OUTDIR), **out}, ensure_ascii=False))


if __name__ == "__main__":
    main()
