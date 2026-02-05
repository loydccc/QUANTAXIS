#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Ladder audit for 2022-01-01 ~ 2022-10-31.

Implements the strict ladder spec (L0->L1->L2->L3) on weekly rebalance dates
and produces the 4 required report blocks.

Important notes:
- Product cadence is weekly rebalance. We compute ladder level on each rebalance
  date, then forward-fill that level across trading days until next rebalance
  to get "按交易日" frequency and L3 streak in trading days.
- 510300 is not present in this Mongo dataset; for PnL we treat fallback leg as
  cash proxy (0 return). This still tests the risk-budgeting behavior.
"""

from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pymongo

ROOT = Path(__file__).resolve().parents[1]
OUTDIR = ROOT / "output" / "reports" / "ladder_audit"
OUTDIR.mkdir(parents=True, exist_ok=True)

# Fixed params (write-hard)
MIN_POS = 6
N_GUARD = 12
DOWNVOL_Q = 0.70
S_STRUCT_FLOOR_L1 = 0.25
W_CAP_MULT_L2 = 0.5
FALLBACK_ASSET = "510300"

DIST_252H_MIN = -0.40

START = "2022-01-01"
END = "2022-10-31"
THEME = "a_ex_kcb_bse"

# Strategy params (align current system defaults)
TOP_K = 20
CANDIDATE_K = 100
LOOKBACK = 60
MA_WIN = 60
MA_MODE = "filter"
MIN_BARS = 800
LIQ_WINDOW = 20
LIQ_MIN_RATIO = 1.0
HOLD_WEEKS = 2
TRANCHE_OVERLAP = True

# Alpha weights (current)
W_RET_20D = -1.0
W_RET_10D = -0.5
W_RET_5D = -0.2
W_MA_60D = 0.3
W_VOL_20D = -0.5
W_LIQ_20D = 0.0


def mongo() -> pymongo.MongoClient:
    # Host-run friendly
    host = "127.0.0.1"
    port = 27017
    dbn = "quantaxis"
    user = "quantaxis"
    password = "quantaxis"
    uri = f"mongodb://{user}:{password}@{host}:{port}/{dbn}?authSource=admin"
    c = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)
    c.admin.command("ping")
    return c


def load_universe(db, theme: str) -> List[str]:
    theme = (theme or "all").strip()

    def _is_a_ex_kcb_bse(code: str) -> bool:
        if not code or len(code) != 6 or not code.isdigit():
            return False
        if code.startswith("688"):
            return False
        if code.startswith(("8", "4")):
            return False
        return code.startswith(("600", "601", "603", "605", "000", "001", "002", "003", "300", "301"))

    codes: set[str] = set()
    coll = db.get_collection("stock_list")
    try:
        n = coll.estimated_document_count()
    except Exception:
        n = 0

    if n and n > 0:
        for doc in coll.find({}, {"_id": 0, "code": 1, "ts_code": 1}):
            c = doc.get("code")
            if not c and doc.get("ts_code"):
                c = str(doc.get("ts_code")).split(".")[0]
            if c:
                codes.add(str(c).zfill(6))
    else:
        for c in db["stock_day"].distinct("code"):
            if c:
                codes.add(str(c).zfill(6))

    if theme == "a_ex_kcb_bse":
        out = sorted([c for c in codes if _is_a_ex_kcb_bse(c)])
    else:
        out = sorted(codes)

    if not out:
        raise RuntimeError("empty universe")
    return out


def fetch_ohlc_batch(coll, codes: List[str], start: str, end: str) -> pd.DataFrame:
    """Fetch OHLC(+amount) in one query; return long df with date parsed."""
    start2 = start.replace("-", "")
    end2 = end.replace("-", "")
    start_i = int(start2)
    end_i = int(end2)

    proj = {"_id": 0, "code": 1, "date": 1, "open": 1, "high": 1, "low": 1, "close": 1, "amount": 1}
    q = {
        "code": {"$in": codes},
        "$or": [
            {"date": {"$gte": start, "$lte": end}},
            {"date": {"$gte": start2, "$lte": end2}},
            {"date": {"$gte": start_i, "$lte": end_i}},
        ],
    }
    cur = coll.find(q, proj, no_cursor_timeout=True)
    rows = list(cur)
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("no data fetched")
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["date"] = pd.to_datetime(df["date"].astype(str), format="mixed", errors="coerce")
    df = df.dropna(subset=["date", "code"])
    for c in ["open", "high", "low", "close", "amount"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.sort_values(["code", "date"]).drop_duplicates(subset=["code", "date"], keep="last")
    return df


def to_panel(df: pd.DataFrame, col: str) -> pd.DataFrame:
    piv = df.pivot(index="date", columns="code", values=col)
    piv = piv.sort_index()
    return piv


def pick_weekly_rebalance_dates(idx: pd.DatetimeIndex) -> List[pd.Timestamp]:
    # align to W-FRI like existing scripts
    di = pd.DatetimeIndex(idx)
    weeks = di.to_period("W-FRI")
    # pick last trading day in each period
    out = di.to_series().groupby(weeks).max().tolist()
    return [pd.Timestamp(x) for x in out]


def zscore_cs(s: pd.Series) -> pd.Series:
    s = s.replace([np.inf, -np.inf], np.nan)
    m = float(s.mean())
    sd = float(s.std(ddof=1))
    if not np.isfinite(sd) or sd <= 1e-12:
        return s * 0.0
    return (s - m) / sd


def sbreak(x: float) -> float:
    # breakout score S_b (fixed)
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return 0.2
    if x >= -0.02:
        return 1.0
    if x < -0.08:
        return 0.2
    return 0.4 + (x - (-0.08)) * (1.0 - 0.4) / ((-0.02) - (-0.08))


def sdraw(x: float) -> float:
    # drawdown score S_d (fixed)
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return 0.1
    if x >= -0.30:
        return 1.0
    if x < -0.45:
        return 0.1
    return 0.3 + (x - (-0.45)) * (1.0 - 0.3) / ((-0.30) - (-0.45))


def apply_min_weight(port: Dict[str, float], min_weight: float) -> Dict[str, float]:
    if min_weight <= 0:
        return dict(port)
    return {c: w for c, w in port.items() if float(w) >= float(min_weight)}


def normalize(port: Dict[str, float]) -> Dict[str, float]:
    s = sum(max(0.0, float(w)) for w in port.values())
    if s <= 0:
        return {}
    return {c: float(w) / s for c, w in port.items() if float(w) > 0}


def l2_cap(port: Dict[str, float], cap: float) -> Dict[str, float]:
    if not port:
        return {}
    clipped = {c: min(float(w), float(cap)) for c, w in port.items()}
    s_clip = sum(clipped.values())
    if s_clip <= 0:
        return {}
    if s_clip < 1.0:
        under = {c: w for c, w in clipped.items() if w < cap - 1e-12}
        if under:
            add = 1.0 - s_clip
            s_under = sum(under.values())
            for c in list(under.keys()):
                clipped[c] = clipped[c] + add * (under[c] / s_under if s_under > 0 else 1.0 / len(under))
    return normalize(clipped)


def build_signal_for_date(
    d: pd.Timestamp,
    *,
    o: pd.DataFrame,
    h: pd.DataFrame,
    l: pd.DataFrame,
    close: pd.DataFrame,
    amount: Optional[pd.DataFrame],
    prev_picks: List[str],
    min_weight: float,
    enable_fallback: bool,
) -> Tuple[Dict[str, float], str, float, Dict]:
    """Return (portfolio weights including fallback asset optionally, ladder_level_used, fallback_weight, audit_meta)."""

    cols = list(close.columns)

    # eligibility
    bars = close.loc[:d].notna().sum(axis=0)
    elig = bars >= int(MIN_BARS)

    # liquidity window
    if LIQ_WINDOW and LIQ_WINDOW > 0:
        win = int(LIQ_WINDOW)
        need = int(np.floor(win * float(LIQ_MIN_RATIO) + 1e-9))
        close_ok = close.notna().rolling(win, min_periods=win).sum() >= need
        elig = elig & close_ok.loc[d]
        if amount is not None:
            amt_ok = (amount.fillna(0.0) > 0).rolling(win, min_periods=win).sum() >= need
            elig = elig & amt_ok.loc[d]

    elig_codes = [c for c in cols if bool(elig.get(c, False))]
    if not elig_codes:
        # force L3
        port = {FALLBACK_ASSET: 1.0} if enable_fallback else {}
        level_used = "L3" if enable_fallback else "L2"
        audit = {"date": str(d.date()), "level_used": level_used, "fallback_weight": float(port.get(FALLBACK_ASSET, 0.0)), "ladder_runs": [], "reason": "empty_elig"}
        return port, level_used, float(port.get(FALLBACK_ASSET, 0.0)), audit

    # candidate pool
    mom = (close / close.shift(int(LOOKBACK)) - 1.0).loc[d, elig_codes].replace([np.inf, -np.inf], np.nan).dropna()
    mom = mom.sort_values(ascending=False)
    mom_top = mom.index.tolist()[: max(int(TOP_K), int(CANDIDATE_K))]

    ma = close.rolling(int(MA_WIN)).mean()
    ma_long = close.loc[d, elig_codes] > ma.loc[d, elig_codes]
    ma_set = set([c for c in elig_codes if bool(ma_long.get(c, False))])

    if MA_MODE == "filter":
        cand = set(mom_top) & ma_set
        if len(cand) < max(3, min(5, int(TOP_K))):
            cand = set(ma_set)
    else:
        cand = set(mom_top) | ma_set

    if not cand:
        port = {FALLBACK_ASSET: 1.0} if enable_fallback else {}
        level_used = "L3" if enable_fallback else "L2"
        audit = {"date": str(d.date()), "level_used": level_used, "fallback_weight": float(port.get(FALLBACK_ASSET, 0.0)), "ladder_runs": [], "reason": "empty_candidates"}
        return port, level_used, float(port.get(FALLBACK_ASSET, 0.0)), audit

    # factor values on candidates
    cand = sorted(cand)

    # dist_252h hard
    hh = h.loc[:d, cand].tail(252)
    hi252 = hh.max(axis=0)
    dist_252h = close.loc[d, cand] / hi252 - 1.0
    cand2 = [c for c in cand if float(dist_252h.get(c, -999.0)) >= float(DIST_252H_MIN)]

    if not cand2:
        port = {FALLBACK_ASSET: 1.0} if enable_fallback else {}
        level_used = "L3" if enable_fallback else "L2"
        audit = {"date": str(d.date()), "level_used": level_used, "fallback_weight": float(port.get(FALLBACK_ASSET, 0.0)), "ladder_runs": [], "reason": "dist_hard_all"}
        return port, level_used, float(port.get(FALLBACK_ASSET, 0.0)), audit

    # downvol (20d neg ret)
    ret = close.pct_change(fill_method=None)
    tail = ret.loc[:d, cand2].tail(20)
    downvol = tail.where(tail < 0).std(axis=0, ddof=1)

    # downvol rank pct (0 low risk .. 1 high risk)
    dv_series = downvol.replace([np.inf, -np.inf], np.nan)
    dv_rank = dv_series.rank(pct=True)
    s_down = 1.0 - dv_rank

    # breakout/drawdown
    h60 = h.loc[:d, cand2].tail(60)
    c60 = close.loc[:d, cand2].tail(60)
    breakout_60 = close.loc[d, cand2] / h60.max(axis=0) - 1.0
    drawdown_60 = close.loc[d, cand2] / c60.max(axis=0) - 1.0
    s_struct = pd.Series({c: sbreak(float(breakout_60.get(c, np.nan))) * sdraw(float(drawdown_60.get(c, np.nan))) for c in cand2})

    # alpha score components
    ret5 = close.loc[d, cand2] / close.shift(5).loc[d, cand2] - 1.0
    ret10 = close.loc[d, cand2] / close.shift(10).loc[d, cand2] - 1.0
    ret20 = close.loc[d, cand2] / close.shift(20).loc[d, cand2] - 1.0
    ma60 = close.loc[d, cand2] / close.rolling(60).mean().loc[d, cand2] - 1.0
    vol20 = ret.loc[:d, cand2].tail(20).std(axis=0, ddof=1)
    liq20 = amount.loc[:d, cand2].tail(20).mean(axis=0) if amount is not None else pd.Series(0.0, index=cand2)

    z = {
        "ret_20d": zscore_cs(ret20).reindex(cand2).fillna(0.0),
        "ret_10d": zscore_cs(ret10).reindex(cand2).fillna(0.0),
        "ret_5d": zscore_cs(ret5).reindex(cand2).fillna(0.0),
        "ma_60d": zscore_cs(ma60).reindex(cand2).fillna(0.0),
        "vol_20d": zscore_cs(vol20).reindex(cand2).fillna(0.0),
        "liq_20d": zscore_cs(liq20).reindex(cand2).fillna(0.0),
    }
    alpha = (
        W_RET_20D * z["ret_20d"]
        + W_RET_10D * z["ret_10d"]
        + W_RET_5D * z["ret_5d"]
        + W_MA_60D * z["ma_60d"]
        + W_VOL_20D * z["vol_20d"]
        + W_LIQ_20D * z["liq_20d"]
    )

    ladder_runs = []
    picks_base: List[str] = []

    def eval_level(level: str) -> Tuple[Dict[str, float], float, Dict]:
        cands = list(cand2)
        downvol_mode = "penalty_only"
        # downvol hard cut only at L0 and when pool >= N_GUARD
        if level == "L0" and len(cands) >= N_GUARD:
            thr = float(downvol.quantile(DOWNVOL_Q))
            cands = [c for c in cands if float(downvol.get(c, 0.0)) <= thr]
            downvol_mode = "hard+penalty"
        # L1: struct floor
        sst = s_struct.copy()
        if level == "L1":
            sst = sst.clip(lower=S_STRUCT_FLOOR_L1)
        # gated score
        if level in {"L0", "L1"}:
            gscore = alpha.reindex(cands).fillna(0.0) * sst.reindex(cands).fillna(0.0) * s_down.reindex(cands).fillna(0.5)
            order = gscore.sort_values(ascending=False)
        else:
            # L2: struct only affects ordering (secondary key)
            gscore = alpha.reindex(cands).fillna(0.0) * s_down.reindex(cands).fillna(0.5)
            tmp = pd.DataFrame({"g": gscore, "s": s_struct.reindex(cands).fillna(0.0)})
            tmp = tmp.sort_values(["g", "s"], ascending=False)
            order = tmp["g"]

        picks = list(order.index[:TOP_K])
        # record base picks for tranche overlap across weeks (independent of ladder outcome)
        nonlocal picks_base
        if level == "L0":
            picks_base = list(picks)

        # 2-tranche overlap
        if TRANCHE_OVERLAP and HOLD_WEEKS > 1:
            final = sorted(set(picks) | set(prev_picks))
            # averaged weights between current + prev
            curr_w = {c: 1.0 / len(picks) for c in picks} if picks else {}
            prev_w = {c: 1.0 / len(prev_picks) for c in prev_picks} if prev_picks else {}
            port = {c: 0.5 * curr_w.get(c, 0.0) + 0.5 * prev_w.get(c, 0.0) for c in final}
        else:
            port = {c: 1.0 / len(picks) for c in picks} if picks else {}

        # min_weight filter (do not renorm here)
        port2 = apply_min_weight(port, min_weight)

        # L2 cap
        cap = None
        if level == "L2" and port2:
            cap = (1.0 / float(TOP_K)) * float(W_CAP_MULT_L2)
            port2 = l2_cap(normalize(port2), cap=cap)  # cap requires normalized stock sleeve
            # after cap, apply min_weight again without renorm (risk budget)
            port2 = apply_min_weight(port2, min_weight)

        eff_n = len(port2)
        info = {
            "level": level,
            "candidates_after_dist": int(len(cand2)),
            "candidates_after_downvol": int(len(cands)),
            "downvol_mode": downvol_mode,
            "effective_positions": int(eff_n),
            "l1_s_struct_floor": (S_STRUCT_FLOOR_L1 if level == "L1" else None),
            "l2_cap": cap,
        }
        return port2, float(sum(port2.values())), info

    level_used = "L3"
    port_final: Dict[str, float] = {}
    sumw = 0.0
    last_p: Dict[str, float] = {}
    last_sw: float = 0.0

    for level in ["L0", "L1", "L2"]:
        p, sw, info = eval_level(level)
        ladder_runs.append(info)
        last_p, last_sw = p, sw
        if len(p) >= MIN_POS:
            level_used = level
            port_final = p
            sumw = sw
            break
    else:
        # after L2 still < MIN_POS -> keep the L2 sleeve (do NOT drop it); then L3 may add fallback.
        level_used = "L3"
        port_final = dict(last_p)
        sumw = float(last_sw)

    fallback_w = 0.0
    if enable_fallback and level_used == "L3":
        fallback_w = max(0.0, 1.0 - float(sumw))
        if sumw <= 1e-12:
            fallback_w = 1.0
        port_final = dict(port_final)
        port_final[FALLBACK_ASSET] = port_final.get(FALLBACK_ASSET, 0.0) + fallback_w

    # normalize total (stocks + fallback) to 1
    port_final = normalize(port_final)

    audit = {
        "date": str(d.date()),
        "level_used": level_used,
        "fallback_weight": float(port_final.get(FALLBACK_ASSET, 0.0)),
        "ladder_runs": ladder_runs,
        "picks_base": picks_base,
    }
    return port_final, level_used, float(port_final.get(FALLBACK_ASSET, 0.0)), audit


def backtest_daily(close: pd.DataFrame, w_daily: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    w_eff = w_daily.shift(1).fillna(0.0)
    dret = close.pct_change(fill_method=None).fillna(0.0)
    net = (w_eff * dret).sum(axis=1)
    eq = (1.0 + net).cumprod()
    return eq, net


def max_dd(eq: pd.Series) -> float:
    peak = eq.cummax()
    dd = eq / peak - 1.0
    return float(dd.min())


def dd_duration(eq: pd.Series) -> int:
    peak = eq.cummax()
    under = eq < peak
    # longest consecutive under-water days
    m = 0
    c = 0
    for v in under.values:
        if v:
            c += 1
            m = max(m, c)
        else:
            c = 0
    return int(m)


def main():
    t0 = time.time()
    client = mongo()
    db = client["quantaxis"]
    coll = db["stock_day"]

    codes = load_universe(db, THEME)
    # For speed/memory, we only need codes that have data in the period.
    # Keep universe list but data fetch will naturally drop missing.

    # Fetch a longer history so eligibility/min_bars and rolling windows are valid.
    hist_start = "2019-01-01"

    df = fetch_ohlc_batch(coll, codes, hist_start, END)
    close = to_panel(df, "close")
    high = to_panel(df, "high")
    low = to_panel(df, "low")
    open_ = to_panel(df, "open")
    amt = to_panel(df, "amount") if "amount" in df.columns else None

    # align panels
    idx = close.index
    for p in [high, low, open_]:
        p.reindex(index=idx)
    if amt is not None:
        amt = amt.reindex(index=idx)

    rebs_all = pick_weekly_rebalance_dates(idx)
    rebs = [d for d in rebs_all if (d >= pd.to_datetime(START)) and (d <= pd.to_datetime(END))]

    # Run weekly signals, then expand to daily weights
    audits_fb = []
    audits_nofb = []

    prev_picks: List[str] = []
    prev_picks2: List[str] = []

    w_by_reb_fb = {}
    w_by_reb_nofb = {}

    for d in rebs:
        port_fb, lvl_fb, fbw, audit_fb = build_signal_for_date(
            d,
            o=open_,
            h=high,
            l=low,
            close=close,
            amount=amt,
            prev_picks=prev_picks,
            min_weight=0.04,
            enable_fallback=True,
        )
        audits_fb.append(audit_fb)
        w_by_reb_fb[d] = port_fb
        # update prev picks for tranche overlap using base picks (not affected by ladder/L3)
        prev_picks = list(audit_fb.get("picks_base") or [])

        port_nf, lvl_nf, fbw2, audit_nf = build_signal_for_date(
            d,
            o=open_,
            h=high,
            l=low,
            close=close,
            amount=amt,
            prev_picks=prev_picks2,
            min_weight=0.04,
            enable_fallback=False,
        )
        # For "disable L3" comparison, force full investment in stocks by renormalizing.
        port_nf = normalize({c: w for c, w in port_nf.items() if c != FALLBACK_ASSET})
        audits_nofb.append({**audit_nf, "fallback_weight": 0.0})
        w_by_reb_nofb[d] = port_nf
        prev_picks2 = list(audit_nf.get("picks_base") or [])

    # Build daily weight matrices (stocks only; fallback treated as cash proxy)
    cols = list(close.columns)
    w_fb = pd.DataFrame(0.0, index=idx, columns=cols)
    w_nf = pd.DataFrame(0.0, index=idx, columns=cols)

    idx_eval = idx[(idx >= pd.to_datetime(START)) & (idx <= pd.to_datetime(END))]

    reb_dates = sorted(w_by_reb_fb.keys())
    for i, d in enumerate(reb_dates):
        end = reb_dates[i + 1] if i + 1 < len(reb_dates) else (idx.max() + pd.Timedelta(days=1))
        mask = (idx >= d) & (idx < end)
        for c, w in w_by_reb_fb[d].items():
            if c in w_fb.columns:
                w_fb.loc[mask, c] = float(w)
        for c, w in w_by_reb_nofb[d].items():
            if c in w_nf.columns:
                w_nf.loc[mask, c] = float(w)

    eq_fb, net_fb = backtest_daily(close.loc[idx_eval], w_fb.loc[idx_eval])
    eq_nf, net_nf = backtest_daily(close.loc[idx_eval], w_nf.loc[idx_eval])

    # Expand ladder level to trading days (forward fill weekly level)
    lvl_series = pd.Series(index=idx_eval, dtype=object)
    fbw_series = pd.Series(index=idx_eval, dtype=float)

    for i, d in enumerate(reb_dates):
        end = reb_dates[i + 1] if i + 1 < len(reb_dates) else (idx.max() + pd.Timedelta(days=1))
        mask = (idx_eval >= d) & (idx_eval < end)
        lvl = audits_fb[i].get("level_used")
        fbw = float(audits_fb[i].get("fallback_weight", 0.0))
        lvl_series.loc[mask] = lvl
        fbw_series.loc[mask] = fbw

    # 1) ladder freq by trading day
    freq = lvl_series.value_counts(dropna=False).to_dict()
    freq_pct = {k: float(v) / float(len(lvl_series.dropna())) for k, v in freq.items()}

    # consecutive L3 in trading days
    max_streak = 0
    cur = 0
    for lv in lvl_series.fillna("").tolist():
        if lv == "L3":
            cur += 1
            max_streak = max(max_streak, cur)
        else:
            cur = 0

    # 2) fallback_weight dist on L3 days (trading days)
    l3w = fbw_series[lvl_series == "L3"].dropna()
    fb_stats = {
        "mean": float(l3w.mean()) if len(l3w) else 0.0,
        "p90": float(l3w.quantile(0.9)) if len(l3w) else 0.0,
        "max": float(l3w.max()) if len(l3w) else 0.0,
        "n_days": int(l3w.shape[0]),
    }

    # 3) drawdown + recovery duration + tail
    dd = {
        "with_fallback_cash_proxy": {
            "max_drawdown": max_dd(eq_fb),
            "max_dd_duration_days": dd_duration(eq_fb),
            "p05_daily_return": float(net_fb.quantile(0.05)),
        },
        "without_fallback": {
            "max_drawdown": max_dd(eq_nf),
            "max_dd_duration_days": dd_duration(eq_nf),
            "p05_daily_return": float(net_nf.quantile(0.05)),
        },
    }

    # 4) sample L3 dates (weekly) for chain inspection
    l3_weeks = [a for a in audits_fb if a["level_used"] == "L3"]
    sample = l3_weeks[:20]

    summary = {
        "period": {"start": START, "end": END, "theme": THEME},
        "trading_days": int(len(idx_eval)),
        "rebalance_steps": int(len(rebs)),
        "ladder_freq": freq,
        "ladder_freq_pct": freq_pct,
        "max_consecutive_L3_trading_days": int(max_streak),
        "fallback_weight_stats_L3_trading_days": fb_stats,
        "drawdown_compare": dd,
        "l3_samples_weekly": sample,
        "notes": [
            "Fallback leg (510300) treated as cash proxy (0 return) due to missing 510300 in Mongo stock_day.",
            "Ladder frequency is computed per trading day by forward-filling weekly level.",
        ],
        "elapsed_sec": float(time.time() - t0),
    }

    (OUTDIR / "2022-01-01_2022-10-31_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(audits_fb).to_json(OUTDIR / "2022-01-01_2022-10-31_weekly_audits_fb.json", force_ascii=False, indent=2)
    pd.DataFrame({"date": idx_eval.astype(str), "level": lvl_series.values, "fallback_weight": fbw_series.values}).to_csv(
        OUTDIR / "2022-01-01_2022-10-31_daily_levels.csv", index=False
    )
    pd.DataFrame({"date": eq_fb.index.astype(str), "equity_with_fallback_cash_proxy": eq_fb.values, "equity_without_fallback": eq_nf.values}).to_csv(
        OUTDIR / "2022-01-01_2022-10-31_equity.csv", index=False
    )

    print(json.dumps({"outdir": str(OUTDIR), **summary}, ensure_ascii=False))


if __name__ == "__main__":
    main()
