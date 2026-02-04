#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Walk-forward backtest for the Mode C (Signals) weekly selector.

This script backtests the *signal selection logic itself* week by week,
without calling the API.

It mirrors the current product shape:
- weekly rebalance
- top_k equal-weight selections
- optional 2-week hold via 2-tranche overlap
- candidate pool from cross-sectional momentum + optional MA context
- factor scoring (z-scored cross-sectionally) to rank candidates

Outputs (outdir):
- metrics.json
- equity.csv
- positions.csv
- weekly_returns.csv

Notes:
- Uses close-to-close returns with T+1 execution (weights shift by 1 trading day).
- Uses Mongo stock_day as data source (supports mixed date formats).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pymongo

# Reuse proven helpers from baseline backtests.
# scripts/ is not a Python package, so add it to sys.path.
import sys

_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from backtest_baseline import (  # type: ignore
    MongoCfg,
    detect_volume_field,
    get_mongo_cfg,
    mongo_client,
    pick_weekly_rebalance_dates,
    perf_stats,
)


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _canonical_json(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def zscore_cs(x: pd.Series) -> pd.Series:
    x = x.replace([np.inf, -np.inf], np.nan).dropna()
    if x.empty:
        return x
    sd = float(x.std(ddof=1))
    if sd <= 0 or np.isnan(sd):
        return x * 0.0
    return (x - float(x.mean())) / sd


def fetch_panel_mixed_dates(
    coll: pymongo.collection.Collection,
    codes: List[str],
    start: str,
    end: str,
    volume_field: Optional[str],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, Optional[pd.DataFrame]]:
    """Fetch OHLC (and optional volume/amount) panels from Mongo.

    Supports mixed `date` formats:
    - YYYY-MM-DD
    - YYYYMMDD (string)
    - YYYYMMDD (int)
    """

    open_series: Dict[str, pd.Series] = {}
    high_series: Dict[str, pd.Series] = {}
    low_series: Dict[str, pd.Series] = {}
    close_series: Dict[str, pd.Series] = {}
    vol_series: Dict[str, pd.Series] = {} if volume_field else {}

    proj = {"_id": 0, "date": 1, "open": 1, "high": 1, "low": 1, "close": 1}
    if volume_field:
        proj[volume_field] = 1

    start2 = start.replace("-", "")
    end2 = end.replace("-", "")

    # Also query int range; some collections store ints.
    start_i = int(start2)
    end_i = int(end2)

    for code in codes:
        q = {
            "code": code,
            "$or": [
                {"date": {"$gte": start, "$lte": end}},
                {"date": {"$gte": start2, "$lte": end2}},
                {"date": {"$gte": start_i, "$lte": end_i}},
            ],
        }
        rows = list(coll.find(q, proj).sort("date", 1))
        if not rows:
            continue
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"].astype(str), format="mixed", errors="coerce")
        df = df.dropna(subset=["date"]).drop_duplicates(subset=["date"]).set_index("date").sort_index()

        if "open" in df.columns:
            open_series[code] = pd.to_numeric(df["open"], errors="coerce")
        if "high" in df.columns:
            high_series[code] = pd.to_numeric(df["high"], errors="coerce")
        if "low" in df.columns:
            low_series[code] = pd.to_numeric(df["low"], errors="coerce")
        if "close" in df.columns:
            close_series[code] = pd.to_numeric(df["close"], errors="coerce")

        if volume_field and volume_field in df.columns:
            vol_series[code] = pd.to_numeric(df[volume_field], errors="coerce")

    if not close_series:
        raise RuntimeError("no data found for selected universe")

    open_panel = pd.concat(open_series, axis=1).sort_index() if open_series else pd.DataFrame(index=pd.DatetimeIndex([]))
    high_panel = pd.concat(high_series, axis=1).sort_index() if high_series else pd.DataFrame(index=pd.DatetimeIndex([]))
    low_panel = pd.concat(low_series, axis=1).sort_index() if low_series else pd.DataFrame(index=pd.DatetimeIndex([]))
    close_panel = pd.concat(close_series, axis=1).sort_index()

    # align indices
    idx = close_panel.index
    open_panel = open_panel.reindex(index=idx)
    high_panel = high_panel.reindex(index=idx)
    low_panel = low_panel.reindex(index=idx)

    vol_panel = None
    if volume_field and vol_series:
        vol_panel = pd.concat(vol_series, axis=1).sort_index().reindex(index=idx)

    return open_panel, high_panel, low_panel, close_panel, vol_panel


def load_universe_from_mongo(db, theme: str) -> List[str]:
    """Universe selection consistent with other scripts (hs10/cyb20/a_ex_kcb_bse)."""

    theme = (theme or "all").strip()

    def _is_hs10(code: str) -> bool:
        if not code or len(code) != 6 or not code.isdigit():
            return False
        if code.startswith(("300", "301", "688")):
            return False
        if code.startswith(("8", "4")):
            return False
        return code.startswith(("600", "601", "603", "605", "000", "001", "002", "003"))

    def _is_cyb20(code: str) -> bool:
        return bool(code) and len(code) == 6 and code.isdigit() and code.startswith(("300", "301"))

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

    if theme in {"hs10", "cn_hs10", "a_hs10"}:
        out = sorted([c for c in codes if _is_hs10(c)])
    elif theme in {"cyb20", "cn_cyb20", "a_cyb20"}:
        out = sorted([c for c in codes if _is_cyb20(c)])
    elif theme in {"a_ex_kcb_bse", "cn_a_ex_kcb_bse", "a_no_kcb_bse"}:
        out = sorted([c for c in codes if _is_a_ex_kcb_bse(c)])
    else:
        # fallback: treat as "all" (use derived codes)
        out = sorted(codes)

    if not out:
        raise RuntimeError(f"empty universe for theme={theme}")
    return out


def build_weights_on_rebalance(
    o: pd.DataFrame,
    h: pd.DataFrame,
    l: pd.DataFrame,
    close: pd.DataFrame,
    vol: Optional[pd.DataFrame],
    reb_dates: List[pd.Timestamp],
    *,
    lookback: int,
    top_k: int,
    candidate_k: int,
    ma_window: int,
    ma_mode: str,
    hold_weeks: int,
    tranche_overlap: bool,
    liq_window: int,
    liq_min_ratio: float,
    liq_min_quantile: Optional[float],
    vol_max_quantile: Optional[float],
    max_abs_ret_1d: Optional[float],
    limit_move_mode: str,
    limit_touch_eps: float,
    limit_pct: float,
    limit_price_eps: float,
    min_bars: int,
    score_weights_up: Dict[str, float],
    score_weights_down: Optional[Dict[str, float]],
    regime_switch: bool,
    regime_mode: str,
    regime_threshold: float,
    regime_cash: bool,
    cash_up: float,
    cash_side: float,
    cash_down: float,
    side_band: float,
    backup_k: int,
    fac_windows: Dict[str, int],
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """Return weights_on_rebalance (index=close.index, columns=close.columns)."""

    cols = list(close.columns)
    weights = pd.DataFrame(0.0, index=close.index, columns=cols, dtype=float)

    # Precompute eligibility masks for speed.
    close_ok_cum = close.notna().cumsum(axis=0)

    close_ok_roll = None
    vol_ok_roll = None
    if liq_window and liq_window > 0:
        win = int(liq_window)
        ratio = float(max(0.0, min(1.0, liq_min_ratio)))
        need = int(np.floor(win * ratio + 1e-9))
        close_ok_roll = close.notna().rolling(win, min_periods=win).sum() >= need
        if vol is not None:
            vol_ok_roll = (vol.fillna(0.0) > 0).rolling(win, min_periods=win).sum() >= need

    # Indicators
    mom_ret = close / close.shift(int(lookback)) - 1.0
    ma = close.rolling(int(ma_window)).mean()
    ma_long = close > ma

    daily_ret = close.pct_change(fill_method=None)
    # OHLC panels for execution feasibility
    o = o.reindex(index=close.index)
    h = h.reindex(index=close.index)
    l = l.reindex(index=close.index)

    # Factor components (computed cross-sectionally at each rebalance date)
    ret10 = close / close.shift(int(fac_windows["ret_10d"])) - 1.0
    ret20 = close / close.shift(int(fac_windows["ret_20d"])) - 1.0
    vol20 = daily_ret.rolling(int(fac_windows["vol_20d"])) .std(ddof=1)
    liq20 = None
    if vol is not None:
        liq20 = vol.rolling(int(fac_windows["liq_20d"])) .mean()

    # Store picks for tranche overlap
    picks_by_reb: List[List[str]] = []

    # Track previous rebalance weights (stocks only, already scaled by gross exposure)
    prev_wrow = pd.Series(0.0, index=cols)

    # Counters
    stats = {"rebalance_count": 0, "empty_candidates": 0, "regime_up": 0, "regime_down": 0, "regime_side": 0}

    for i, d in enumerate(reb_dates):
        if d not in close.index:
            continue

        # Eligibility at date d
        elig = (close_ok_cum.loc[d] >= int(min_bars))

        if close_ok_roll is not None:
            elig = elig & close_ok_roll.loc[d]
        if vol_ok_roll is not None:
            elig = elig & vol_ok_roll.loc[d]

        elig_codes = [c for c in cols if bool(elig.get(c, False))]

        # Limit-up/down proxy filter (optional): uses close-to-close 1d return at rebalance date.
        # NOTE: this is only for candidate filtering. For execution realism prefer limit_move_mode="freeze".
        if limit_move_mode == "filter" and max_abs_ret_1d is not None and elig_codes:
            thr = float(max_abs_ret_1d)
            r1 = daily_ret.loc[d, elig_codes].replace([np.inf, -np.inf], np.nan).dropna()
            if not r1.empty:
                elig_codes = [c for c in elig_codes if abs(float(r1.get(c, 0.0))) <= thr]
        if not elig_codes:
            picks_by_reb.append([])
            stats["empty_candidates"] += 1
            continue

        # Momentum ranking for candidate pool
        mom = mom_ret.loc[d, elig_codes].replace([np.inf, -np.inf], np.nan).dropna()
        mom = mom.sort_values(ascending=False)
        mom_top = mom.index.tolist()[: max(int(top_k), int(candidate_k))]

        ma_set = set([c for c in elig_codes if bool(ma_long.loc[d, c])])

        if ma_mode == "filter":
            candidates = set(mom_top) & ma_set
            if len(candidates) < max(3, min(5, int(top_k))):
                candidates = set(ma_set)
        else:
            candidates = set(mom_top) | ma_set

        if not candidates:
            picks_by_reb.append([])
            stats["empty_candidates"] += 1
            continue

        cand = sorted(list(candidates))

        # Optional liquidity quantile filter on candidates (cross-sectional, at rebalance date)
        if liq_min_quantile is not None:
            q = float(liq_min_quantile)
            q = max(0.0, min(1.0, q))
            if q > 0 and liq20 is not None:
                lraw = liq20.loc[d, cand].replace([np.inf, -np.inf], np.nan).dropna()
                if not lraw.empty:
                    thr = float(lraw.quantile(q))
                    cand = [c for c in cand if float(lraw.get(c, -np.inf)) >= thr]

        # Optional volatility max-quantile filter (drop the most volatile tail)
        if vol_max_quantile is not None:
            q = float(vol_max_quantile)
            q = max(0.0, min(1.0, q))
            if q < 1.0:
                vraw = vol20.loc[d, cand].replace([np.inf, -np.inf], np.nan).dropna()
                if not vraw.empty:
                    thr = float(vraw.quantile(q))
                    cand = [c for c in cand if float(vraw.get(c, np.inf)) <= thr]

        # Build factor score on candidates
        r10 = ret10.loc[d, cand]
        r20 = ret20.loc[d, cand]
        v20 = vol20.loc[d, cand]
        l20 = liq20.loc[d, cand] if liq20 is not None else pd.Series(0.0, index=cand)

        zr10 = zscore_cs(r10)
        zr20 = zscore_cs(r20)
        zv20 = zscore_cs(v20)
        zliq = zscore_cs(l20)

        # Fill missing candidate values to 0 after zscore computation
        zr10 = zr10.reindex(cand).fillna(0.0)
        zr20 = zr20.reindex(cand).fillna(0.0)
        zv20 = zv20.reindex(cand).fillna(0.0)
        zliq = zliq.reindex(cand).fillna(0.0)

        # Regime switch: pick score weights (and optional cash weight) for this rebalance date
        w = score_weights_up
        cash_w = 0.0

        if regime_mode == "breadth_ma":
            # breadth = fraction of eligible universe above MA at date d
            b = float(ma_long.loc[d, elig_codes].mean()) if len(elig_codes) else 0.0
            delta = b - 0.5
            # 3-state regime: UP / SIDE / DOWN
            if abs(delta) < float(side_band):
                regime = "SIDE"
            elif delta >= 0:
                regime = "UP"
            else:
                regime = "DOWN"
        else:
            regime = "UP"

        if regime_switch and score_weights_down is not None:
            if regime == "UP":
                stats["regime_up"] += 1
                w = score_weights_up
            elif regime == "DOWN":
                stats["regime_down"] += 1
                w = score_weights_down
            else:
                stats["regime_side"] += 1
                # In SIDE, keep the UP weights for now (we can add side-weights later)
                w = score_weights_up

        if regime_cash:
            if regime == "UP":
                cash_w = float(cash_up)
            elif regime == "DOWN":
                cash_w = float(cash_down)
            else:
                cash_w = float(cash_side)
            cash_w = max(0.0, min(1.0, cash_w))

        score = (
            float(w.get("ret_20d", 0.0)) * zr20
            + float(w.get("ret_10d", 0.0)) * zr10
            + float(w.get("vol_20d", 0.0)) * zv20
            + float(w.get("liq_20d", 0.0)) * zliq
        )

        # Stable tie-break: score desc, then momentum rank (if present), then code
        mom_rank = {c: j for j, c in enumerate(mom_top, start=1)}

        def sk(code: str):
            return (-float(score.get(code, 0.0)), mom_rank.get(code, 10**9), code)

        ranked = sorted(cand, key=sk)
        picks = ranked[: int(top_k)]
        backups = ranked[int(top_k) : int(top_k) + max(0, int(backup_k))]
        picks_by_reb.append(picks)
        stats["rebalance_count"] += 1

        # Tranche overlap (2-week hold) -> merge current and previous tranche
        if not tranche_overlap or int(hold_weeks) <= 1:
            final = picks
            tranche_n = 1
        else:
            # MVP: 2 tranches
            prev = picks_by_reb[i - 1] if i - 1 >= 0 else []
            tranche_n = 2
            final = []
            # merge by equal weight (dedupe)
            final = sorted(set(picks) | set(prev))

        if not final:
            continue

        # Build final weights at rebalance date (stocks only), then apply gross exposure.
        wrow = pd.Series(0.0, index=cols)
        if tranche_n == 1:
            wgt = 1.0 / len(final)
            wrow.loc[final] = wgt
        else:
            # tranche weights averaged
            curr_w = {c: 1.0 / len(picks) for c in picks} if picks else {}
            prev = picks_by_reb[i - 1] if i - 1 >= 0 else []
            prev_w = {c: 1.0 / len(prev) for c in prev} if prev else {}
            for c in set(curr_w) | set(prev_w):
                wrow.loc[c] = 0.5 * curr_w.get(c, 0.0) + 0.5 * prev_w.get(c, 0.0)

        gross = 1.0 - cash_w
        if gross < 0:
            gross = 0.0
        if gross > 1:
            gross = 1.0
        wrow = wrow * gross

        # Execution feasibility approximation: freeze trades for limit-move names on rebalance dates.
        # If a name is "blocked", we keep its previous weight and rescale the remaining tradable sleeve.
        if limit_move_mode == "freeze" and gross > 0:
            # More realistic A-share limit-touch detection.
            # Approximate daily price limit by prev_close * (1±limit_pct).
            # Freeze only in the trade direction:
            # - buy attempt blocked if close is at/near up-limit
            # - sell attempt blocked if close is at/near down-limit
            eps_touch = float(limit_touch_eps)
            eps_price = float(limit_price_eps)
            lpct = float(limit_pct)

            c_today = close.loc[d, cols]
            hi_today = h.loc[d, cols]
            lo_today = l.loc[d, cols]
            c_prev = close.shift(1).loc[d, cols]

            blocked = set()

            for c in cols:
                prev_w = float(prev_wrow.get(c, 0.0))
                tgt_w = float(wrow.get(c, 0.0))
                if abs(tgt_w - prev_w) < 1e-15:
                    continue

                cc = c_today.get(c)
                hh = hi_today.get(c)
                ll = lo_today.get(c)
                pc = c_prev.get(c)
                if pd.isna(cc) or pd.isna(hh) or pd.isna(ll) or pd.isna(pc):
                    continue

                # First require close to be at extreme (one-price style)
                at_high = abs(float(cc) - float(hh)) <= eps_touch
                at_low = abs(float(cc) - float(ll)) <= eps_touch

                up_lim = float(pc) * (1.0 + lpct)
                dn_lim = float(pc) * (1.0 - lpct)

                if tgt_w > prev_w:
                    # buy attempt: block if at/near up-limit
                    if at_high and abs(float(cc) - up_lim) <= eps_price:
                        blocked.add(c)
                else:
                    # sell attempt: block if at/near down-limit
                    if at_low and abs(float(cc) - dn_lim) <= eps_price:
                        blocked.add(c)

            if blocked:
                prev_w = prev_wrow.reindex(cols).fillna(0.0)
                tgt_w = wrow.copy()

                blocked = sorted(list(blocked))
                nonblocked = [c for c in cols if c not in set(blocked)]

                # Split blocked into buy-blocked vs sell-blocked
                buy_blocked = [c for c in blocked if float(tgt_w.get(c, 0.0)) > float(prev_w.get(c, 0.0))]
                sell_blocked = [c for c in blocked if float(tgt_w.get(c, 0.0)) < float(prev_w.get(c, 0.0))]

                # For blocked names, keep previous weights (can't trade in that direction)
                w_new = tgt_w.copy()
                for c in buy_blocked + sell_blocked:
                    w_new.loc[c] = float(prev_w.get(c, 0.0))

                # Compute how much gross exposure is left to allocate to tradable sleeve
                sum_fixed = float(w_new.loc[blocked].sum()) if blocked else 0.0
                target_tradable = max(0.0, gross - sum_fixed)

                # Try to allocate tradable sleeve using ranked backups if buys were blocked
                # Only allocate to names that are NOT blocked and are in our ranked list.
                tradable_set = set(nonblocked)

                # Current tradable allocations (from w_new)
                sum_tradable = float(w_new.loc[nonblocked].sum()) if nonblocked else 0.0

                # If buys were blocked, we may need to fill missing exposure with backups
                if buy_blocked and target_tradable > 0:
                    held = set([c for c in cols if float(w_new.get(c, 0.0)) > 0])
                    # choose backup names not held and tradable
                    add = []
                    for c in backups:
                        if c in tradable_set and c not in held:
                            add.append(c)
                        if len(add) >= len(buy_blocked):
                            break
                    if add:
                        # give them equal provisional weights; will be rescaled below
                        for c in add:
                            w_new.loc[c] = w_new.get(c, 0.0) + 1.0

                # Rescale tradable sleeve to target_tradable
                sum_tradable2 = float(w_new.loc[nonblocked].sum()) if nonblocked else 0.0
                if sum_tradable2 > 0:
                    w_new.loc[nonblocked] = w_new.loc[nonblocked] * (target_tradable / sum_tradable2)
                else:
                    # If nothing tradable, renormalize fixed sleeve to gross
                    if sum_fixed > 0:
                        w_new.loc[blocked] = w_new.loc[blocked] * (gross / sum_fixed)

                wrow = w_new

        weights.loc[d] = wrow
        prev_wrow = wrow.copy()

    return weights, stats


def backtest_close_to_close(
    close: pd.DataFrame,
    weights_on_rebalance: pd.DataFrame,
    cost_bps: float,
    *,
    liq20: Optional[pd.DataFrame] = None,
    impact_k: float = 0.0,
    impact_floor: float = 0.0,
):
    """Close-to-close backtest with T+1 execution.

    Cost model:
    - base linear cost: cost_bps * turnover
    - optional impact: (impact_k / sqrt(liq_20d)) * turnover
      where liq_20d is a rolling average of the chosen liquidity field.

    Notes:
    - This is still an approximation; it is meant to prevent the research loop
      from selecting unrealistic high-impact portfolios.
    """

    # forward-fill weights; then shift for T+1 execution
    w = weights_on_rebalance.reindex(close.index).ffill().fillna(0.0)
    w_eff = w.shift(1).fillna(0.0)

    daily_ret = close.pct_change(fill_method=None).fillna(0.0)
    gross = (w_eff * daily_ret).sum(axis=1)

    turnover = w_eff.diff().abs().sum(axis=1) / 2.0

    base_cost = (float(cost_bps) / 10000.0) * turnover

    imp_cost = 0.0
    if impact_k and impact_k > 0 and liq20 is not None:
        # portfolio liquidity proxy = sum_i w_i * liq_i
        liq20a = liq20.reindex(close.index).ffill()
        port_liq = (w_eff * liq20a).sum(axis=1)
        floor = float(impact_floor) if impact_floor else 0.0
        if floor > 0:
            port_liq = port_liq.clip(lower=floor)
        # if liquidity is missing, assume very high impact (small liq)
        port_liq = port_liq.fillna(floor if floor > 0 else 1.0)
        imp_bps = float(impact_k) / np.sqrt(port_liq)
        imp_cost = (imp_bps / 10000.0) * turnover

    net = gross - base_cost - imp_cost

    equity = (1.0 + net).cumprod()
    return equity, w_eff, turnover, net


def weekly_returns_from_net(net: pd.Series) -> pd.DataFrame:
    # group by W-FRI and compound
    di = pd.DatetimeIndex(net.index)
    g = net.groupby(di.to_period("W-FRI"))
    wk = g.apply(lambda x: float((1.0 + x).prod() - 1.0))
    df = wk.reset_index()
    df.columns = ["week", "weekly_ret"]
    df["week"] = df["week"].astype(str)
    df["win"] = df["weekly_ret"] > 0
    return df


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--theme", default="a_ex_kcb_bse")

    ap.add_argument("--topk", type=int, default=50)
    ap.add_argument("--candidate-k", type=int, default=250)

    ap.add_argument("--lookback", type=int, default=60)
    ap.add_argument("--ma", type=int, default=60)
    ap.add_argument("--ma-mode", choices=["filter", "boost"], default="boost")

    ap.add_argument("--min-bars", type=int, default=800)
    ap.add_argument("--liq-window", type=int, default=20)
    ap.add_argument("--liq-min-ratio", type=float, default=1.0)
    ap.add_argument(
        "--liq-min-quantile",
        type=float,
        default=None,
        help="Optional cross-sectional liquidity filter on rebalance dates. Example: 0.2 keeps top 80% by liq_20d.",
    )
    ap.add_argument(
        "--vol-max-quantile",
        type=float,
        default=None,
        help="Optional cross-sectional volatility filter on rebalance dates. Example: 0.9 keeps the lowest 90% by vol_20d (drops top 10%).",
    )

    ap.add_argument("--hold-weeks", type=int, default=2)
    ap.add_argument("--tranche-overlap", action="store_true", default=True)

    ap.add_argument("--cost-bps", type=float, default=10.0)
    ap.add_argument(
        "--impact-k",
        type=float,
        default=0.0,
        help="Optional liquidity-based impact cost coefficient. Effective cost adds impact_k / sqrt(liq_20d).",
    )
    ap.add_argument(
        "--impact-floor",
        type=float,
        default=0.0,
        help="Floor for liq_20d in impact model to avoid blow-ups (same units as liq field).",
    )
    ap.add_argument(
        "--max-abs-ret-1d",
        type=float,
        default=None,
        help="(Legacy) Limit-move threshold on rebalance date (close-to-close 1d return). Used when --limit-move-mode == filter.",
    )
    ap.add_argument(
        "--limit-pct",
        type=float,
        default=0.10,
        help="Price limit percentage used for limit-touch detection in freeze mode (default 10%).",
    )
    ap.add_argument(
        "--limit-price-eps",
        type=float,
        default=1e-3,
        help="Tolerance for comparing prices to limit price (in price units, not pct).",
    )
    ap.add_argument(
        "--limit-move-mode",
        choices=["none", "filter", "freeze"],
        default="none",
        help="How to handle limit-up/down on rebalance dates: none|filter(candidates by 1d ret)|freeze(trades by OHLC touch).",
    )
    ap.add_argument(
        "--limit-touch-eps",
        type=float,
        default=1e-6,
        help="Epsilon for OHLC comparisons when detecting limit-touch (close==high or close==low).",
    )
    ap.add_argument(
        "--backup-k",
        type=int,
        default=150,
        help="Backup candidate list size used when limit-move freeze blocks buys (top_k + backup_k ranked list).",
    )

    # factor windows
    ap.add_argument("--fac-ret-10d", type=int, default=10)
    ap.add_argument("--fac-ret-20d", type=int, default=20)
    ap.add_argument("--fac-vol-20d", type=int, default=20)
    ap.add_argument("--fac-liq-20d", type=int, default=20)

    # score weights
    ap.add_argument("--w-ret-20d", type=float, default=1.0)
    ap.add_argument("--w-ret-10d", type=float, default=0.5)
    ap.add_argument("--w-vol-20d", type=float, default=-0.5)
    ap.add_argument("--w-liq-20d", type=float, default=0.2)

    # Regime switch (optional): pick weights based on market state
    ap.add_argument("--regime-switch", action="store_true", default=False, help="Enable simple regime switch on rebalance dates")
    ap.add_argument("--regime-mode", choices=["breadth_ma"], default="breadth_ma")
    ap.add_argument("--regime-threshold", type=float, default=0.5, help="For breadth_ma: fraction of universe above MA to consider regime=up")

    # Optional cash overlay driven by regime (0 cash return assumed)
    ap.add_argument("--regime-cash", action="store_true", default=False, help="Enable cash allocation by regime")
    ap.add_argument("--cash-up", type=float, default=0.0)
    ap.add_argument("--cash-side", type=float, default=0.3)
    ap.add_argument("--cash-down", type=float, default=0.7)
    ap.add_argument("--side-band", type=float, default=0.1, help="For breadth_ma: SIDE regime when |breadth-0.5| < side_band")

    # Down-regime weights (only used when --regime-switch is enabled)
    ap.add_argument("--down-w-ret-20d", type=float, default=-1.0)
    ap.add_argument("--down-w-ret-10d", type=float, default=-0.6)
    ap.add_argument("--down-w-vol-20d", type=float, default=-0.3)
    ap.add_argument("--down-w-liq-20d", type=float, default=0.0)

    ap.add_argument("--outdir", default="")

    args = ap.parse_args(argv)

    start = args.start
    end = args.end

    cfg: MongoCfg = get_mongo_cfg()
    # For local mac runs, mongodb is usually reachable at 127.0.0.1 even if compose uses hostname.
    host = os.getenv("MONGODB_HOST", cfg.host)
    if host == "mongodb":
        os.environ["MONGODB_HOST"] = "127.0.0.1"
    client = mongo_client(get_mongo_cfg())
    db = client[get_mongo_cfg().db]
    coll = db["stock_day"]

    codes = load_universe_from_mongo(db, args.theme)

    volume_field = detect_volume_field(coll)
    o_raw, h_raw, l_raw, close_raw, vol_raw = fetch_panel_mixed_dates(coll, codes, start, end, volume_field=volume_field)
    close_raw = close_raw.sort_index()

    # Keep only universe columns we actually have data for
    cols = [c for c in codes if c in close_raw.columns]
    o = o_raw.reindex(index=close_raw.index)[cols]
    h = h_raw.reindex(index=close_raw.index)[cols]
    l = l_raw.reindex(index=close_raw.index)[cols]
    close = close_raw[cols]
    vol = vol_raw.reindex(index=close_raw.index)[cols] if (vol_raw is not None) else None

    reb_dates = pick_weekly_rebalance_dates(close.index)

    score_weights_up = {
        "ret_20d": float(args.w_ret_20d),
        "ret_10d": float(args.w_ret_10d),
        "vol_20d": float(args.w_vol_20d),
        "liq_20d": float(args.w_liq_20d),
    }

    score_weights_down = {
        "ret_20d": float(args.down_w_ret_20d),
        "ret_10d": float(args.down_w_ret_10d),
        "vol_20d": float(args.down_w_vol_20d),
        "liq_20d": float(args.down_w_liq_20d),
    } if bool(args.regime_switch) else None

    fac_windows = {
        "ret_10d": int(args.fac_ret_10d),
        "ret_20d": int(args.fac_ret_20d),
        "vol_20d": int(args.fac_vol_20d),
        "liq_20d": int(args.fac_liq_20d),
    }

    weights_on_reb, st = build_weights_on_rebalance(
        o,
        h,
        l,
        close,
        vol,
        reb_dates,
        lookback=int(args.lookback),
        top_k=int(args.topk),
        candidate_k=int(args.candidate_k),
        ma_window=int(args.ma),
        ma_mode=str(args.ma_mode),
        hold_weeks=int(args.hold_weeks),
        tranche_overlap=bool(args.tranche_overlap),
        liq_window=int(args.liq_window),
        liq_min_ratio=float(args.liq_min_ratio),
        liq_min_quantile=(None if args.liq_min_quantile is None else float(args.liq_min_quantile)),
        vol_max_quantile=(None if args.vol_max_quantile is None else float(args.vol_max_quantile)),
        max_abs_ret_1d=(None if args.max_abs_ret_1d is None else float(args.max_abs_ret_1d)),
        limit_move_mode=str(args.limit_move_mode),
        limit_touch_eps=float(args.limit_touch_eps),
        limit_pct=float(args.limit_pct),
        limit_price_eps=float(args.limit_price_eps),
        min_bars=int(args.min_bars),
        score_weights_up=score_weights_up,
        score_weights_down=score_weights_down,
        regime_switch=bool(args.regime_switch),
        regime_mode=str(args.regime_mode),
        regime_threshold=float(args.regime_threshold),
        regime_cash=bool(args.regime_cash),
        cash_up=float(args.cash_up),
        cash_side=float(args.cash_side),
        cash_down=float(args.cash_down),
        side_band=float(args.side_band),
        backup_k=int(args.backup_k),
        fac_windows=fac_windows,
    )

    # rolling liquidity for impact model
    liq20 = None
    if vol is not None:
        liq20 = vol.rolling(int(args.fac_liq_20d)).mean()

    equity, positions, turnover, net = backtest_close_to_close(
        close,
        weights_on_reb,
        cost_bps=float(args.cost_bps),
        liq20=liq20,
        impact_k=float(args.impact_k),
        impact_floor=float(args.impact_floor),
    )
    wk = weekly_returns_from_net(net)

    stats = perf_stats(equity, net, turnover)
    win_rate_weekly = float(wk["win"].mean()) if len(wk) else 0.0

    meta = {
        "strategy": "signal_walkforward",
        "theme": args.theme,
        "start": start,
        "end": end,
        "rebalance": "weekly",
        "top_k": int(args.topk),
        "candidate_k": int(args.candidate_k),
        "lookback": int(args.lookback),
        "ma": int(args.ma),
        "ma_mode": str(args.ma_mode),
        "min_bars": int(args.min_bars),
        "liq_window": int(args.liq_window),
        "liq_min_ratio": float(args.liq_min_ratio),
        "liq_min_quantile": (None if args.liq_min_quantile is None else float(args.liq_min_quantile)),
        "vol_max_quantile": (None if args.vol_max_quantile is None else float(args.vol_max_quantile)),
        "hold_weeks": int(args.hold_weeks),
        "tranche_overlap": bool(args.tranche_overlap),
        "cost_bps": float(args.cost_bps),
        "impact_k": float(args.impact_k),
        "impact_floor": float(args.impact_floor),
        "max_abs_ret_1d": (None if args.max_abs_ret_1d is None else float(args.max_abs_ret_1d)),
        "limit_move_mode": str(args.limit_move_mode),
        "limit_touch_eps": float(args.limit_touch_eps),
        "backup_k": int(args.backup_k),
        "limit_pct": float(args.limit_pct),
        "limit_price_eps": float(args.limit_price_eps),
        "score_weights_up": score_weights_up,
        "score_weights_down": score_weights_down,
        "regime_switch": bool(args.regime_switch),
        "regime_mode": str(args.regime_mode),
        "regime_threshold": float(args.regime_threshold),
        "regime_cash": bool(args.regime_cash),
        "cash_up": float(args.cash_up),
        "cash_side": float(args.cash_side),
        "cash_down": float(args.cash_down),
        "side_band": float(args.side_band),
        "factor_windows": fac_windows,
        "liq_field": volume_field,
        "generated_at": int(time.time()),
        "internal": st,
        "universe_size": int(close.shape[1]),
    }
    meta["config_signature"] = _sha256_bytes(_canonical_json(meta))

    # outdir
    if args.outdir:
        outdir = Path(args.outdir)
    else:
        run_id = _sha256_bytes(_canonical_json(meta))[:12]
        outdir = Path("output/reports/signal_walkforward") / run_id

    outdir.mkdir(parents=True, exist_ok=True)

    (outdir / "metrics.json").write_text(
        json.dumps({**stats, "weekly_win_rate": win_rate_weekly, **meta}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    pd.DataFrame({"date": equity.index, "equity": equity.values}).to_csv(outdir / "equity.csv", index=False)
    positions.reset_index().rename(columns={"index": "date"}).to_csv(outdir / "positions.csv", index=False)
    wk.to_csv(outdir / "weekly_returns.csv", index=False)

    print(json.dumps({"outdir": str(outdir), "metrics": stats, "weekly_win_rate": win_rate_weekly}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
