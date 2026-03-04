#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Signals implementation (Mode C).

Extracted from api/app.py to keep the FastAPI app module small.

This module owns:
- signal cfg validation
- signal generation (run_signal)
- factor scoring helpers used by the signal generator

NOTE: This is an MVP; keep changes incremental and behavior-compatible.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import HTTPException

from api.core import ROOT, SIGNALS_DIR
from api.security import validate_cfg_envelope
from api.state import job_sem

HEALTH_CACHE_DIR_DEFAULT = str(ROOT / "output" / "reports" / "health_index" / "daily")


# --- Factor score config (versioned in signal meta) ---
FAC_WINDOWS = {
    "ret_5d": int(os.getenv("QUANTAXIS_FAC_RET_5D", "5")),
    "ret_10d": int(os.getenv("QUANTAXIS_FAC_RET_10D", "10")),
    "ret_20d": int(os.getenv("QUANTAXIS_FAC_RET_20D", "20")),
    "ma_60d": int(os.getenv("QUANTAXIS_FAC_MA_60D", "60")),
    "vol_20d": int(os.getenv("QUANTAXIS_FAC_VOL_20D", "20")),
    "liq_20d": int(os.getenv("QUANTAXIS_FAC_LIQ_20D", "20")),
}
FAC_WEIGHTS = {
    # Default weights remain momentum-ish; we override per request cfg.
    "ret_20d": float(os.getenv("QUANTAXIS_SCORE_W_RET_20D", "1.0")),
    "ret_10d": float(os.getenv("QUANTAXIS_SCORE_W_RET_10D", "0.5")),
    "ret_5d": float(os.getenv("QUANTAXIS_SCORE_W_RET_5D", "0.2")),
    # ma_60d is a trend/quality proxy: (close/MA60 - 1). Positive means above trend.
    "ma_60d": float(os.getenv("QUANTAXIS_SCORE_W_MA_60D", "0.2")),
    "vol_20d": float(os.getenv("QUANTAXIS_SCORE_W_VOL_20D", "-0.5")),
    "liq_20d": float(os.getenv("QUANTAXIS_SCORE_W_LIQ_20D", "0.2")),
}

# --- Hard threshold filters (tradability/risk) ---
HARD_VOL_20D_MAX = float(os.getenv("QUANTAXIS_HARD_VOL_20D_MAX", "0"))
HARD_LIQ_20D_MIN = float(os.getenv("QUANTAXIS_HARD_LIQ_20D_MIN", "0"))

# --- Hard quality/shape filters (stability) ---
HARD_DIST_252H_MIN = float(os.getenv("QUANTAXIS_HARD_DIST_252H_MIN", "-0.4"))
HARD_BREAKOUT_60_MIN = float(os.getenv("QUANTAXIS_HARD_BREAKOUT_60_MIN", "-0.02"))
HARD_DRAWDOWN_60_MIN = float(os.getenv("QUANTAXIS_HARD_DRAWDOWN_60_MIN", "-0.30"))
HARD_DOWNVOL_Q = float(os.getenv("QUANTAXIS_HARD_DOWNVOL_Q", "0.70"))  # keep <= q-quantile

# --- Ladder fixed params (risk budget; do not tweak dynamically) ---
MIN_POS = 6
N_GUARD = 12
DOWNVOL_Q = 0.70
S_STRUCT_FLOOR_L1 = 0.25
W_CAP_MULT_L2 = 0.5
FALLBACK_ASSET_DEFAULT = "510300"

# Job timeouts/log tail
API_JOB_TIMEOUT_SEC = int(os.getenv("QUANTAXIS_API_JOB_TIMEOUT_SEC", "3600"))
API_LOG_TAIL = int(os.getenv("QUANTAXIS_API_LOG_TAIL", "2000"))


_baseline_strategy_re = re.compile(r"^(xsec_momentum_weekly_topk|ts_ma_weekly|hybrid_baseline_weekly_topk)$")
_theme_re = re.compile(r"^[A-Za-z0-9_.-]{1,60}$")
_sha256_hex_re = re.compile(r"^[0-9a-fA-F]{64}$")


def validate_signal_cfg(cfg: Dict[str, Any]) -> None:
    """Validate /signals/run cfg."""
    validate_cfg_envelope(cfg)

    strategy = cfg.get("strategy")
    theme = cfg.get("theme", "all")
    top_k = cfg.get("top_k", 10)
    rebalance = cfg.get("rebalance", "weekly")
    hold_weeks = cfg.get("hold_weeks", 2)
    tranche_overlap = cfg.get("tranche_overlap", True)
    liq_window = cfg.get("liq_window", 20)
    liq_min_ratio = cfg.get("liq_min_ratio", 1.0)
    ma_mode = cfg.get("ma_mode", "filter")
    score_mode = cfg.get("score_mode", "baseline")
    min_weight = float(cfg.get("min_weight", 0.0))
    weight_mode = str(cfg.get("weight_mode", "score")).lower()
    score_temp = float(cfg.get("score_temp", 0.35))
    min_trade_weight = float(cfg.get("min_trade_weight", 0.005))
    max_name_weight = float(cfg.get("max_name_weight", (1.0 / float(max(1, MIN_POS)))))
    rebalance_trigger_mode = str(cfg.get("rebalance_trigger_mode", "event_only"))
    rebalance_drift_min_turnover_2way = float(cfg.get("rebalance_drift_min_turnover_2way", 0.03))
    rebalance_drift_max_cost_bps = float(cfg.get("rebalance_drift_max_cost_bps", 25.0))

    # Execution realism (optional)
    execution_mode = str(cfg.get("execution_mode", "naive"))  # naive|realistic|shadow
    backup_k = int(cfg.get("backup_k", 150))
    limit_tiering = bool(cfg.get("limit_tiering", True))
    limit_pct = float(cfg.get("limit_pct", 0.10))
    limit_price_eps_bps = float(cfg.get("limit_price_eps_bps", 5.0))
    limit_touch_mode = str(cfg.get("limit_touch_mode", "hl"))  # hl|close
    limit_touch_eps = float(cfg.get("limit_touch_eps", 1e-6))
    aum_cny = float(cfg.get("aum_cny", 20_000_000.0))
    adv_participation_max = float(cfg.get("adv_participation_max", 0.10))
    impact_k = float(cfg.get("impact_k", 0.01))
    impact_alpha = float(cfg.get("impact_alpha", 0.70))
    impact_liq_floor = float(cfg.get("impact_liq_floor", 1_000_000.0))
    impact_cost_budget_bps = float(cfg.get("impact_cost_budget_bps", 25.0))
    fee_bps = float(cfg.get("fee_bps", 8.0))

    if not isinstance(strategy, str) or not _baseline_strategy_re.match(strategy):
        raise HTTPException(status_code=400, detail="bad strategy")
    if not isinstance(cfg.get("data_version_id"), str) or not cfg.get("data_version_id"):
        raise HTTPException(status_code=400, detail="missing data_version_id")
    msha = cfg.get("manifest_sha256")
    if not isinstance(msha, str) or not _sha256_hex_re.match(msha):
        raise HTTPException(status_code=400, detail="missing or bad manifest_sha256")
    if not isinstance(theme, str) or not _theme_re.match(theme):
        raise HTTPException(status_code=400, detail="bad theme")
    if rebalance != "weekly":
        raise HTTPException(status_code=400, detail="only weekly rebalance supported in MVP")
    if not isinstance(top_k, int) or top_k <= 0 or top_k > 200:
        raise HTTPException(status_code=400, detail="bad top_k")
    if not isinstance(hold_weeks, int) or hold_weeks < 1 or hold_weeks > 8:
        raise HTTPException(status_code=400, detail="bad hold_weeks")
    if not isinstance(tranche_overlap, bool):
        raise HTTPException(status_code=400, detail="bad tranche_overlap")

    if not isinstance(liq_window, int) or liq_window < 0 or liq_window > 252:
        raise HTTPException(status_code=400, detail="bad liq_window")
    try:
        liq_min_ratio_f = float(liq_min_ratio)
    except Exception:
        raise HTTPException(status_code=400, detail="bad liq_min_ratio")
    if liq_min_ratio_f <= 0 or liq_min_ratio_f > 1.0:
        raise HTTPException(status_code=400, detail="bad liq_min_ratio")

    if ma_mode not in {"filter", "boost"}:
        raise HTTPException(status_code=400, detail="bad ma_mode (filter|boost)")

    if score_mode not in {"baseline", "factor"}:
        raise HTTPException(status_code=400, detail="bad score_mode (baseline|factor)")

    if not (0.0 <= float(min_weight) <= 1.0):
        raise HTTPException(status_code=400, detail="bad min_weight (0..1)")
    if weight_mode not in {"equal", "score"}:
        raise HTTPException(status_code=400, detail="bad weight_mode (equal|score)")
    if not (0.01 <= float(score_temp) <= 5.0):
        raise HTTPException(status_code=400, detail="bad score_temp (0.01..5.0)")
    if not (0.0 <= float(min_trade_weight) <= 0.2):
        raise HTTPException(status_code=400, detail="bad min_trade_weight (0..0.2)")
    if not (0.01 <= float(max_name_weight) <= 1.0):
        raise HTTPException(status_code=400, detail="bad max_name_weight (0.01..1.0)")
    if rebalance_trigger_mode not in {"event_only", "event_or_drift"}:
        raise HTTPException(status_code=400, detail="bad rebalance_trigger_mode (event_only|event_or_drift)")
    if not (0.0 <= rebalance_drift_min_turnover_2way <= 1.0):
        raise HTTPException(status_code=400, detail="bad rebalance_drift_min_turnover_2way (0..1)")
    if not (0.0 <= rebalance_drift_max_cost_bps <= 2000.0):
        raise HTTPException(status_code=400, detail="bad rebalance_drift_max_cost_bps (0..2000)")

    if execution_mode not in {"naive", "realistic", "shadow"}:
        raise HTTPException(status_code=400, detail="bad execution_mode (naive|realistic|shadow)")
    if not isinstance(backup_k, int) or backup_k < 0 or backup_k > 500:
        raise HTTPException(status_code=400, detail="bad backup_k")
    if not (0.0 < limit_pct < 0.5):
        raise HTTPException(status_code=400, detail="bad limit_pct")
    if not (0.0 <= limit_price_eps_bps <= 1000.0):
        raise HTTPException(status_code=400, detail="bad limit_price_eps_bps")
    if limit_touch_mode not in {"hl", "close"}:
        raise HTTPException(status_code=400, detail="bad limit_touch_mode (hl|close)")
    if not (0.0 <= limit_touch_eps <= 0.5):
        raise HTTPException(status_code=400, detail="bad limit_touch_eps")
    if not (0.0 < aum_cny <= 1e13):
        raise HTTPException(status_code=400, detail="bad aum_cny (0..1e13)")
    if not (0.0 <= adv_participation_max <= 1.0):
        raise HTTPException(status_code=400, detail="bad adv_participation_max (0..1)")
    if not (0.0 <= impact_k <= 10.0):
        raise HTTPException(status_code=400, detail="bad impact_k (0..10)")
    if not (0.1 <= impact_alpha <= 3.0):
        raise HTTPException(status_code=400, detail="bad impact_alpha (0.1..3)")
    if not (0.0 <= impact_liq_floor <= 1e13):
        raise HTTPException(status_code=400, detail="bad impact_liq_floor (0..1e13)")
    if not (0.0 <= impact_cost_budget_bps <= 2000.0):
        raise HTTPException(status_code=400, detail="bad impact_cost_budget_bps (0..2000)")
    if not (0.0 <= fee_bps <= 2000.0):
        raise HTTPException(status_code=400, detail="bad fee_bps (0..2000)")


# ---- helpers copied from api/app.py (signals section) ----

def _write_signal_csv(path: Path, positions: list[dict]) -> None:
    lines = ["code,weight,rank,score"]
    for p in positions:
        lines.append(f"{p['code']},{p['weight']},{p['rank']},{p['score']}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_factors_csv(path: Path, positions: list[dict]) -> None:
    cols = [
        "code",
        "weight",
        "rank",
        "score",
        "fac_ret_5d",
        "fac_ret_10d",
        "fac_ret_20d",
        "fac_ma_60d",
        "fac_vol_20d",
        "fac_liq_20d",
        "z_ret_5d",
        "z_ret_10d",
        "z_ret_20d",
        "z_ma_60d",
        "z_vol_20d",
        "z_liq_20d",
    ]
    lines = [",".join(cols)]
    for p in positions:
        row = [str(p.get(c, "")) for c in cols]
        lines.append(",".join(row))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _positions_to_portfolio(positions: list[dict]) -> Dict[str, float]:
    return {p["code"]: float(p["weight"]) for p in positions}


def _portfolio_to_positions(
    port: Dict[str, float],
    scores: Optional[Dict[str, float]] = None,
    factors: Optional[Dict[str, Dict[str, float]]] = None,
    zfactors: Optional[Dict[str, Dict[str, float]]] = None,
    *,
    normalize: bool = True,
) -> list[dict]:
    s = sum(max(0.0, float(w)) for w in port.values())
    if s <= 0:
        return []
    if normalize:
        items = [(c, float(w) / s) for c, w in port.items() if float(w) > 0]
    else:
        items = [(c, float(w)) for c, w in port.items() if float(w) > 0]

    def sk(x):
        c, w = x
        sc = (scores or {}).get(c, 0.0)
        return (-w, -sc, c)

    items.sort(key=sk)
    out = []
    for i, (c, w) in enumerate(items, start=1):
        row = {"code": c, "weight": round(w, 10), "rank": i, "score": round((scores or {}).get(c, 0.0), 6)}
        if factors and c in factors:
            row.update({f"fac_{k}": factors[c].get(k) for k in ["ret_5d", "ret_10d", "ret_20d", "ma_60d", "vol_20d", "liq_20d"]})
        if zfactors and c in zfactors:
            row.update({f"z_{k}": round(zfactors[c].get(k, 0.0), 6) for k in ["ret_5d", "ret_10d", "ret_20d", "ma_60d", "vol_20d", "liq_20d"]})
        out.append(row)
    return out


def _zscore(s: Dict[str, float]) -> Dict[str, float]:
    finite_vals: list[float] = []
    for v in s.values():
        try:
            fv = float(v)
        except Exception:
            continue
        if math.isfinite(fv):
            finite_vals.append(fv)

    if not finite_vals:
        return {k: 0.0 for k in s.keys()}

    m = sum(finite_vals) / len(finite_vals)
    var = sum((v - m) ** 2 for v in finite_vals) / max(1, (len(finite_vals) - 1))
    sd = math.sqrt(var) if var > 0 else 0.0
    if sd <= 0:
        return {k: 0.0 for k in s.keys()}

    out: Dict[str, float] = {}
    for k, v in s.items():
        try:
            fv = float(v)
        except Exception:
            out[k] = 0.0
            continue
        out[k] = ((fv - m) / sd) if math.isfinite(fv) else 0.0
    return out


def _compute_factors_for_codes(as_of_date: str, codes: list[str], cfg: Dict[str, Any]):
    """Compute factor pack for codes at as_of_date from Mongo stock_day.

    Returns:
      fac: {code -> factor dict}
      liq_field: detected liquidity field name or None
      reasons: {code -> reason string} for codes that were requested but did not get factors
               (audit-only; does not affect trading logic)
    """
    import pandas as pd
    import pymongo

    host = os.getenv("MONGODB_HOST", os.getenv("MONGO_HOST", "mongodb"))
    port = int(os.getenv("MONGODB_PORT", os.getenv("MONGO_PORT", "27017")))
    dbn = os.getenv("MONGODB_DATABASE", os.getenv("MONGO_DATABASE", "quantaxis"))

    user = os.getenv("MONGODB_USER", os.getenv("MONGO_USER", "quantaxis"))
    password = os.getenv("MONGODB_PASSWORD", os.getenv("MONGO_PASSWORD", "quantaxis"))

    root_user = os.getenv("MONGO_ROOT_USER", "root")
    root_password = os.getenv("MONGO_ROOT_PASSWORD", "root")

    uris = [
        f"mongodb://{user}:{password}@{host}:{port}/{dbn}?authSource=admin",
        f"mongodb://{root_user}:{root_password}@{host}:{port}/{dbn}?authSource=admin",
        f"mongodb://{host}:{port}/{dbn}",
    ]
    last = None
    client = None
    for uri in uris:
        try:
            c = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)
            c.admin.command("ping")
            client = c
            break
        except Exception as e:
            last = e
    if client is None:
        raise RuntimeError(f"mongo connect failed: {last!r}")

    coll = client[dbn]["stock_day"]

    sample = coll.find_one({}, {"_id": 0, "volume": 1, "vol": 1, "amount": 1, "money": 1})
    liq_field = None
    if sample:
        for k in ["amount", "volume", "vol", "money"]:
            if k in sample and sample.get(k) is not None:
                liq_field = k
                break

    ret5 = int(cfg.get("fac_ret_5d", FAC_WINDOWS["ret_5d"]))
    ret10 = int(cfg.get("fac_ret_10d", FAC_WINDOWS["ret_10d"]))
    ret20 = int(cfg.get("fac_ret_20d", FAC_WINDOWS["ret_20d"]))
    maw = int(cfg.get("fac_ma_60d", FAC_WINDOWS["ma_60d"]))
    volw = int(cfg.get("fac_vol_20d", FAC_WINDOWS["vol_20d"]))
    liqw = int(cfg.get("fac_liq_20d", FAC_WINDOWS["liq_20d"]))

    end_dt = pd.to_datetime(as_of_date)
    # Need enough history for 252d high + 60d breakout/drawdown + vol + MA.
    # Use calendar days buffer (trading days < calendar days).
    start_dt = end_dt - pd.Timedelta(days=520)

    proj = {"_id": 0, "date": 1, "close": 1, "high": 1, "low": 1}
    if liq_field:
        proj[liq_field] = 1

    # Dates are normalized to ISO strings (YYYY-MM-DD) by migration.
    start_s = str(start_dt.date())
    end_s = str(end_dt.date())

    fac: Dict[str, Dict[str, float]] = {}
    reasons: Dict[str, str] = {}
    for code in codes:
        code6 = str(code).zfill(6)
        q = {
            "code": code6,
            "date": {"$gte": start_s, "$lte": end_s},
        }
        rows = list(coll.find(q, proj).sort("date", 1))
        if not rows:
            reasons[code6] = "no_rows"
            continue
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).drop_duplicates(subset=["date"]).set_index("date").sort_index()
        if "close" not in df.columns:
            reasons[code6] = "missing_close"
            continue

        close = pd.to_numeric(df["close"], errors="coerce").dropna()
        close = close.loc[close.index <= end_dt]

        high = pd.to_numeric(df.get("high"), errors="coerce")
        low = pd.to_numeric(df.get("low"), errors="coerce")
        if high is not None:
            high = high.loc[high.index <= end_dt]
        if low is not None:
            low = low.loc[low.index <= end_dt]

        need = max(ret20 + 1, volw + 1, maw + 1, 252 + 1, 60 + 1)
        if close.shape[0] < need:
            # Window not full: still return a factor record with NaNs so the code remains
            # score-valid, but its alpha contribution will be neutral/penalized downstream.
            reasons[code6] = "window_not_full"
            fac[code6] = {
                "ret_5d": float("nan"),
                "ret_10d": float("nan"),
                "ret_20d": float("nan"),
                "ma_60d": float("nan"),
                "vol_20d": float("nan"),
                "liq_20d": float("nan"),
                "dist_252h": float("nan"),
                "breakout_60": float("nan"),
                "drawdown_60": float("nan"),
                "downvol_20d": float("nan"),
            }
            continue

        c_end = float(close.iloc[-1])
        r5 = float(c_end / float(close.iloc[-1 - ret5]) - 1.0) if close.shape[0] > ret5 else 0.0
        r10 = float(c_end / float(close.iloc[-1 - ret10]) - 1.0) if close.shape[0] > ret10 else 0.0
        r20 = float(c_end / float(close.iloc[-1 - ret20]) - 1.0) if close.shape[0] > ret20 else 0.0

        # trend proxy: close / MA(maw) - 1
        ma60 = float(close.tail(maw).mean()) if close.shape[0] >= maw else float(close.mean())
        ma_dist = float(c_end / ma60 - 1.0) if ma60 else 0.0

        # 52w high distance (quality bottom-line filter)
        # Use HIGH when available; fallback to close.
        hh = high.dropna() if high is not None else close
        if hh is None or hh.dropna().shape[0] < 252:
            dist_252h = float("nan")
        else:
            hi_252 = float(hh.tail(252).max())
            dist_252h = float(c_end / hi_252 - 1.0) if hi_252 else float("nan")

        # breakout/drawdown structure
        h60 = (high.dropna() if high is not None else close).tail(60)
        c60 = close.tail(60)
        breakout_60 = float(c_end / float(h60.max()) - 1.0) if h60.shape[0] >= 20 and float(h60.max()) else float("nan")
        drawdown_60 = float(c_end / float(c60.max()) - 1.0) if c60.shape[0] >= 20 and float(c60.max()) else float("nan")

        ret = close.pct_change().dropna()
        v20 = float(ret.tail(volw).std()) if ret.shape[0] >= volw else float(ret.std())

        # downside vol: std of negative daily returns (last volw days)
        tail = ret.tail(volw)
        neg = tail[tail < 0]
        downvol_20d = float(neg.std()) if neg.shape[0] >= 5 else float(tail.std())

        liq = 0.0
        if liq_field and liq_field in df.columns:
            series = pd.to_numeric(df[liq_field], errors="coerce").fillna(0.0)
            series = series.loc[series.index <= end_dt]
            liq = float(series.tail(liqw).mean()) if series.shape[0] >= 1 else 0.0

        fac[code6] = {
            "ret_5d": r5,
            "ret_10d": r10,
            "ret_20d": r20,
            "ma_60d": ma_dist,
            "vol_20d": v20,
            "liq_20d": liq,
            "dist_252h": dist_252h,
            "breakout_60": breakout_60,
            "drawdown_60": drawdown_60,
            "downvol_20d": downvol_20d,
        }

    # Keep reasons for requested codes that were not fully ready.
    return fac, liq_field, reasons


# ---- baseline runner helpers (used for signal generation) ----


def _extract_last_tranches_from_positions_csv(path: Path, n: int = 2) -> list[dict]:
    import pandas as pd

    df = pd.read_csv(path)
    if df.shape[0] < 2:
        raise RuntimeError("positions.csv too short")
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    changed = (df.diff().abs().sum(axis=1) > 1e-12)
    nonzero = (df.abs().sum(axis=1) > 0)
    eff_dates = [d for d in df.index[changed & nonzero]]
    if not eff_dates:
        eff_dates = [df.index[nonzero][-1]]
    eff_dates = eff_dates[-n:]
    out = []
    for d in eff_dates[::-1]:
        idx = df.index.get_loc(d)
        reb = df.index[idx - 1] if isinstance(idx, int) and idx > 0 else d
        row = df.loc[d]
        weights = {c: float(row[c]) for c in df.columns if pd.notna(row[c]) and float(row[c]) > 0}
        out.append({"rebalance_date": str(reb.date()), "effective_date": str(d.date()), "weights": weights})
    return out


def _extract_latest_positions_from_positions_csv(path: Path):
    import csv

    with path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)

    if not rows:
        raise RuntimeError("positions.csv is empty")

    last_row = None
    for r in reversed(rows):
        try:
            if any((c and float(c) != 0.0) for c in r[1:]):
                last_row = r
                break
        except Exception:
            continue
    if last_row is None:
        last_row = rows[-1]

    as_of_date = last_row[0]
    weights = []
    for i in range(1, len(header)):
        c = last_row[i]
        try:
            w = float(c) if c else 0.0
        except Exception:
            w = 0.0
        if w > 0:
            weights.append((header[i], w))

    weights.sort(key=lambda x: x[1], reverse=True)
    return as_of_date, weights


def _run_baseline_backtest_to_workdir(workdir: Path, cfg: Dict[str, Any], strategy: str):
    theme = str(cfg.get("theme", "all"))
    top_k = int(cfg.get("top_k", 10))
    lookback = int(cfg.get("lookback", 60))
    ma = int(cfg.get("ma", 60))
    min_bars = int(cfg.get("min_bars", 800))
    liq_window = int(cfg.get("liq_window", 20))
    liq_min_ratio = float(cfg.get("liq_min_ratio", 1.0))
    cost_bps = float(cfg.get("cost_bps", 10.0))
    start = str(cfg.get("start", "2019-01-01"))
    end = str(cfg.get("end", "2099-12-31"))
    snapshot_dir = str(cfg.get("snapshot_dir", "") or "")
    data_version_id = str(cfg.get("data_version_id", "") or "")
    manifest_sha256 = str(cfg.get("manifest_sha256", "") or "")
    require_snapshot = str(cfg.get("require_snapshot", "") or "")

    cmd = [
        "python3",
        "scripts/backtest_baseline.py",
        "--start",
        start,
        "--end",
        end,
        "--theme",
        theme,
        "--strategy",
        strategy,
        "--lookback",
        str(lookback),
        "--top",
        str(top_k),
        "--ma",
        str(ma),
        "--min-bars",
        str(min_bars),
        "--liq-window",
        str(liq_window),
        "--liq-min-ratio",
        str(liq_min_ratio),
        "--cost-bps",
        str(cost_bps),
        "--outdir",
        str(workdir),
    ]
    if snapshot_dir:
        cmd += ["--snapshot-dir", snapshot_dir]
    if data_version_id:
        cmd += ["--data-version-id", data_version_id]
    if manifest_sha256:
        cmd += ["--manifest-sha256", manifest_sha256]
    if require_snapshot:
        cmd += ["--require-snapshot", require_snapshot]

    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, timeout=max(1, API_JOB_TIMEOUT_SEC))
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or "run failed")[-API_LOG_TAIL:])

    positions_csv = workdir / "positions.csv"
    metrics_json = workdir / "metrics.json"
    if not positions_csv.exists() or not metrics_json.exists():
        raise RuntimeError("missing baseline outputs (positions.csv/metrics.json)")

    as_of_date, weights = _extract_latest_positions_from_positions_csv(positions_csv)
    picks = [c for c, _w in weights]
    stats = json.loads(metrics_json.read_text(encoding="utf-8"))
    return as_of_date, picks, stats


def _run_baseline_backtest_to_workdir_with_fallback(workdir: Path, cfg: Dict[str, Any], strategy: str):
    def _is_empty_universe_err(msg: str) -> bool:
        return "no eligible codes after filters" in (msg or "")

    attempts: list[Dict[str, Any]] = []
    base = dict(cfg)
    ladder: list[Dict[str, Any]] = [
        {},
        {"liq_window": 0},
        {"liq_window": 0, "min_bars": min(int(base.get("min_bars", 800)), 250)},
        {"liq_window": 0, "min_bars": min(int(base.get("min_bars", 800)), 120)},
    ]

    last_err: Exception | None = None
    for i, patch in enumerate(ladder, start=1):
        cfg_try = dict(base)
        cfg_try.update(patch)
        try:
            as_of_date, picks, stats = _run_baseline_backtest_to_workdir(workdir, cfg_try, strategy)
            stats = dict(stats)
            stats["auto_relax"] = attempts
            return as_of_date, picks, stats, cfg_try
        except Exception as e:
            last_err = e
            msg = str(e)
            attempts.append({"attempt": i, "patch": patch, "error": (msg[-400:] if msg else repr(e))})
            if not _is_empty_universe_err(msg):
                raise
            continue

    if last_err:
        raise last_err
    raise RuntimeError("run failed")


# ---- execution realism helpers ----


def _limit_pct_for(code: str, base: float, tiering: bool) -> float:
    if tiering and str(code).startswith(("300", "301")):
        return 0.20
    return float(base)


def _near_bps(a: float, b: float, eps_bps: float) -> bool:
    if b == 0:
        return abs(a - b) <= 1e-6
    return abs(a - b) <= abs(b) * (float(eps_bps) / 10000.0)


def _clip(x: float, lo: float, hi: float) -> float:
    return float(max(lo, min(hi, float(x))))


def _load_health_score(date_iso: str) -> tuple[float | None, str | None]:
    """Load health_score for date from the daily cache.

    Spec:
    - run_signal only reads today's cache.
    - if missing => exposure=1 and meta.health_missing=true.

    Returns (score, source_path). score=None when missing.
    """

    cache_dir = str(os.getenv("QUANTAXIS_HEALTH_CACHE_DIR", HEALTH_CACHE_DIR_DEFAULT)).strip()
    if not cache_dir:
        return None, None

    p = Path(cache_dir) / f"health_score_{date_iso}.json"
    if not p.exists() or p.stat().st_size == 0:
        return None, str(p)

    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
        sc = obj.get("health_score")
        return (float(sc) if sc is not None else None), str(p)
    except Exception:
        return None, str(p)


# -------------------------
# main signal generator
# -------------------------


def run_signal(signal_id: str, cfg: Dict[str, Any]) -> None:
    """Generate a weekly topK signal as JSON+CSV under output/signals/."""

    status_path = SIGNALS_DIR / f"{signal_id}.status.json"
    status_path.write_text(
        json.dumps({"signal_id": signal_id, "status": "running", "started_at": int(time.time())}, ensure_ascii=False),
        encoding="utf-8",
    )

    workdir = SIGNALS_DIR / f"{signal_id}.work"
    workdir.mkdir(parents=True, exist_ok=True)

    strategy = str(cfg.get("strategy"))
    theme = str(cfg.get("theme", "all"))
    top_k = int(cfg.get("top_k", 10))
    lookback = int(cfg.get("lookback", 60))
    ma = int(cfg.get("ma", 60))
    min_bars = int(cfg.get("min_bars", 800))
    liq_window = int(cfg.get("liq_window", 20))
    liq_min_ratio = float(cfg.get("liq_min_ratio", 1.0))
    ma_mode = str(cfg.get("ma_mode", "filter"))
    score_mode = str(cfg.get("score_mode", "baseline"))
    min_weight = float(cfg.get("min_weight", 0.0))
    weight_mode = str(cfg.get("weight_mode", "score")).lower()
    score_temp = float(cfg.get("score_temp", 0.35))
    min_trade_weight = float(cfg.get("min_trade_weight", 0.005))
    max_name_weight = float(cfg.get("max_name_weight", (1.0 / float(max(1, MIN_POS)))))
    rebalance_trigger_mode = str(cfg.get("rebalance_trigger_mode", "event_only"))
    rebalance_drift_min_turnover_2way = float(cfg.get("rebalance_drift_min_turnover_2way", 0.03))
    rebalance_drift_max_cost_bps_cfg = cfg.get("rebalance_drift_max_cost_bps", None)
    hold_weeks = int(cfg.get("hold_weeks", 2))
    tranche_overlap = bool(cfg.get("tranche_overlap", True))
    data_version_id = str(cfg.get("data_version_id", "") or "")
    manifest_sha256 = str(cfg.get("manifest_sha256", "") or "")

    # Execution realism
    execution_mode = str(cfg.get("execution_mode", "naive"))
    backup_k = int(cfg.get("backup_k", 150))
    limit_tiering = bool(cfg.get("limit_tiering", True))
    limit_pct = float(cfg.get("limit_pct", 0.10))
    limit_price_eps_bps = float(cfg.get("limit_price_eps_bps", 5.0))
    limit_touch_mode = str(cfg.get("limit_touch_mode", "hl"))
    limit_touch_eps = float(cfg.get("limit_touch_eps", 1e-6))
    aum_cny = float(cfg.get("aum_cny", 20_000_000.0))
    adv_participation_max = float(cfg.get("adv_participation_max", 0.10))
    impact_k = float(cfg.get("impact_k", 0.01))
    impact_alpha = float(cfg.get("impact_alpha", 0.70))
    impact_liq_floor = float(cfg.get("impact_liq_floor", 1_000_000.0))
    impact_cost_budget_bps = float(cfg.get("impact_cost_budget_bps", 25.0))
    fee_bps = float(cfg.get("fee_bps", 8.0))
    rebalance_drift_max_cost_bps = (
        float(impact_cost_budget_bps) if rebalance_drift_max_cost_bps_cfg is None else float(rebalance_drift_max_cost_bps_cfg)
    )

    cfg_used: Dict[str, Any] = dict(cfg)

    hard_vol_20d_max = float(cfg.get("hard_vol_20d_max", HARD_VOL_20D_MAX))
    hard_liq_20d_min = float(cfg.get("hard_liq_20d_min", HARD_LIQ_20D_MIN))

    # quality/shape filters (dist stays hard; breakout/drawdown become gating)
    hard_dist_252h_min = float(cfg.get("hard_dist_252h_min", HARD_DIST_252H_MIN))
    hard_breakout_60_min = float(cfg.get("hard_breakout_60_min", HARD_BREAKOUT_60_MIN))
    hard_drawdown_60_min = float(cfg.get("hard_drawdown_60_min", HARD_DRAWDOWN_60_MIN))
    hard_downvol_q = float(cfg.get("hard_downvol_q", HARD_DOWNVOL_Q))

    # Fallback ladder + substitute asset (L3 only)
    fallback_asset = str(cfg.get("fallback_asset", FALLBACK_ASSET_DEFAULT))
    disable_fallback = bool(cfg.get("disable_fallback", False))
    strategy_key_legacy = f"{strategy}:{theme}:{top_k}:{lookback}:{ma}:{ma_mode}:{score_mode}:{hold_weeks}:{int(tranche_overlap)}"

    # Include execution-sensitive knobs to avoid comparing signals from different parameter regimes.
    strategy_key = ":".join(
        [
            str(strategy),
            str(theme),
            str(top_k),
            str(lookback),
            str(ma),
            str(ma_mode),
            str(score_mode),
            str(hold_weeks),
            str(int(tranche_overlap)),
            str(weight_mode),
            f"{float(score_temp):.6f}",
            f"{float(max_name_weight):.6f}",
            f"{float(min_trade_weight):.6f}",
            str(rebalance_trigger_mode),
            f"{float(rebalance_drift_min_turnover_2way):.6f}",
            f"{float(rebalance_drift_max_cost_bps):.6f}",
            str(execution_mode),
            f"{float(adv_participation_max):.6f}",
            f"{float(impact_k):.6f}",
            f"{float(impact_alpha):.6f}",
            f"{float(impact_cost_budget_bps):.6f}",
        ]
    )

    def _compat_prev_meta(m: dict) -> bool:
        # Transitional compatibility: allow one-hop fallback from legacy strategy_key
        # only when critical execution knobs are equivalent.
        try:
            wm = str(m.get("weight_mode", "score")).lower()
            st = float(m.get("score_temp", 0.35))
            mnw = float(m.get("max_name_weight", (1.0 / float(max(1, MIN_POS)))))
            mtw = float(m.get("min_trade_weight", 0.005))
            rtm = str(m.get("rebalance_trigger_mode", "event_only"))
            rtt = float(m.get("rebalance_drift_min_turnover_2way", 0.03))
            rcb = float(m.get("rebalance_drift_max_cost_bps", 25.0))
            em = str(m.get("execution_mode", "naive"))
            adv = float(m.get("adv_participation_max", 0.10))
            ik = float(m.get("impact_k", 0.01))
            ia = float(m.get("impact_alpha", 0.70))
            ib = float(m.get("impact_cost_budget_bps", 25.0))
        except Exception:
            return False
        return (
            wm == str(weight_mode)
            and abs(st - float(score_temp)) <= 1e-12
            and abs(mnw - float(max_name_weight)) <= 1e-12
            and abs(mtw - float(min_trade_weight)) <= 1e-12
            and rtm == str(rebalance_trigger_mode)
            and abs(rtt - float(rebalance_drift_min_turnover_2way)) <= 1e-12
            and abs(rcb - float(rebalance_drift_max_cost_bps)) <= 1e-12
            and em == str(execution_mode)
            and abs(adv - float(adv_participation_max)) <= 1e-12
            and abs(ik - float(impact_k)) <= 1e-12
            and abs(ia - float(impact_alpha)) <= 1e-12
            and abs(ib - float(impact_cost_budget_bps)) <= 1e-12
        )

    def _load_prev_signal_for_strategy(sk: str, sealed_date: str) -> dict | None:
        import glob

        candidates = []
        for path in glob.glob(str(SIGNALS_DIR / "*.json")):
            if path.endswith(".status.json"):
                continue
            try:
                obj = json.loads(Path(path).read_text(encoding="utf-8"))
            except Exception:
                continue
            if obj.get("status") != "succeeded":
                continue
            m = obj.get("meta", {}) or {}
            mkey = str(m.get("strategy_key") or "")
            if mkey != sk:
                if not (mkey == strategy_key_legacy and _compat_prev_meta(m)):
                    continue
            ops = (m.get("ops", {}) or {})
            sd = ops.get("sealed_date")
            if not isinstance(sd, str):
                continue
            if sd >= sealed_date:
                continue
            candidates.append((sd, int(obj.get("generated_at") or 0), obj))
        if not candidates:
            return None
        candidates.sort(key=lambda x: (x[0], x[1]))
        return candidates[-1][2]

    def _non_cash_weights(sig_obj: dict) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for p in (sig_obj.get("positions") or []):
            c = str(p.get("code", "")).upper()
            if c == "CASH":
                continue
            try:
                w = float(p.get("weight", 0.0) or 0.0)
            except Exception:
                w = 0.0
            if w > 0:
                out[str(p.get("code"))] = w
        s = float(sum(max(0.0, float(w)) for w in out.values()))
        if s > 0:
            out = {c: float(w) / s for c, w in out.items()}
        return out

    def _normalize_nonneg_port(wmap: Dict[str, float]) -> Dict[str, float]:
        s = float(sum(max(0.0, float(v)) for v in wmap.values()))
        if s <= 0:
            return {}
        return {str(c): (max(0.0, float(v)) / s) for c, v in wmap.items() if float(v) > 1e-12}

    def _turnover_2way(prev_w: Dict[str, float], curr_w: Dict[str, float]) -> float:
        codes = set(prev_w.keys()) | set(curr_w.keys())
        return float(0.5 * sum(abs(float(curr_w.get(c, 0.0) - prev_w.get(c, 0.0))) for c in codes))

    def _liq20_map(asof: str, codes: set[str]) -> Dict[str, float]:
        if not codes:
            return {}
        out: Dict[str, float] = {}
        try:
            fac_u, _lf, _reasons = _compute_factors_for_codes(asof, list(codes), cfg)
            for c in codes:
                c6 = str(c).zfill(6)
                fp = fac_u.get(c6) or fac_u.get(str(c)) or {}
                try:
                    lv = float(fp.get("liq_20d", float("nan")))
                except Exception:
                    lv = float("nan")
                if math.isfinite(lv) and lv > 0:
                    out[str(c)] = lv
        except Exception:
            return {}
        return out

    def _estimate_trade_cost_bps(prev_w: Dict[str, float], curr_w: Dict[str, float], liq_map: Dict[str, float]) -> Dict[str, float]:
        liq_vals = [float(v) for v in liq_map.values() if math.isfinite(float(v)) and float(v) > 0]
        liq_proxy = float(sorted(liq_vals)[len(liq_vals) // 2]) if liq_vals else float(max(impact_liq_floor, 1.0))
        fee_rate = float(max(0.0, fee_bps) / 10000.0)

        traded_notional_cny = 0.0
        fee_cost_cny = 0.0
        impact_cost_cny = 0.0
        for c in set(prev_w.keys()) | set(curr_w.keys()):
            dw = float(curr_w.get(c, 0.0) - prev_w.get(c, 0.0))
            t_cny = abs(dw) * float(aum_cny)
            if t_cny <= 0:
                continue
            liq_cny = float(liq_map.get(c, liq_proxy))
            liq_eff = max(float(impact_liq_floor), liq_cny, 1.0)
            participation = float(t_cny / liq_eff)
            imp_rate = float(max(0.0, impact_k) * (participation ** float(max(0.1, impact_alpha))))

            traded_notional_cny += t_cny
            fee_cost_cny += fee_rate * t_cny
            impact_cost_cny += imp_rate * t_cny

        total_cost_cny = fee_cost_cny + impact_cost_cny
        total_cost_bps = (total_cost_cny / float(aum_cny)) * 10000.0 if aum_cny > 0 else 0.0
        impact_cost_bps = (impact_cost_cny / float(aum_cny)) * 10000.0 if aum_cny > 0 else 0.0
        fee_cost_bps = (fee_cost_cny / float(aum_cny)) * 10000.0 if aum_cny > 0 else 0.0
        return {
            "traded_notional_cny": float(traded_notional_cny),
            "fee_cost_cny": float(fee_cost_cny),
            "impact_cost_cny": float(impact_cost_cny),
            "total_cost_cny": float(total_cost_cny),
            "fee_cost_bps": float(fee_cost_bps),
            "impact_cost_bps": float(impact_cost_bps),
            "total_cost_bps": float(total_cost_bps),
        }

    def _apply_execution_realism(prev_w: Dict[str, float], target_w: Dict[str, float], asof: str) -> tuple[Dict[str, float], Dict[str, Any]]:
        codes = set(prev_w.keys()) | set(target_w.keys())
        if not codes:
            return target_w, {"enabled": False, "reason": "empty_codes"}

        liq_map = _liq20_map(asof, codes)
        liq_vals = [float(v) for v in liq_map.values() if math.isfinite(float(v)) and float(v) > 0]
        liq_proxy = float(sorted(liq_vals)[len(liq_vals) // 2]) if liq_vals else float(max(impact_liq_floor, 1.0))

        filled: Dict[str, float] = {}
        n_full = 0
        n_partial = 0
        n_blocked = 0
        fill_ratio_sum = 0.0
        fill_ratio_n = 0
        for c in codes:
            wp = float(prev_w.get(c, 0.0))
            wt = float(target_w.get(c, 0.0))
            dw = wt - wp
            if abs(dw) <= 1e-12:
                w_exec = wp
                fill = 1.0
            else:
                t_cny = abs(dw) * float(aum_cny)
                liq_cny = float(liq_map.get(c, liq_proxy))
                cap_cny = float(max(0.0, adv_participation_max) * max(0.0, liq_cny))
                if t_cny <= 0:
                    fill = 1.0
                elif cap_cny <= 0:
                    fill = 0.0
                else:
                    fill = min(1.0, float(cap_cny / t_cny))
                w_exec = wp + dw * fill
                if fill <= 1e-12:
                    n_blocked += 1
                elif fill < 1.0 - 1e-12:
                    n_partial += 1
                else:
                    n_full += 1

            fill_ratio_sum += float(fill)
            fill_ratio_n += 1
            if w_exec > 1e-12:
                filled[c] = float(w_exec)

        # If clipping leaves gross stock weight >1, scale down to keep long-only no-leverage.
        s_fill = float(sum(max(0.0, float(w)) for w in filled.values()))
        if s_fill > 1.0 + 1e-12:
            filled = {c: (float(w) / s_fill) for c, w in filled.items() if float(w) > 1e-12}

        cost0 = _estimate_trade_cost_bps(prev_w, filled, liq_map)
        budget_scale = 1.0
        if float(impact_cost_budget_bps) > 0 and cost0["total_cost_bps"] > float(impact_cost_budget_bps) and prev_w:
            budget_scale = max(0.0, float(impact_cost_budget_bps) / float(cost0["total_cost_bps"]))
            scaled: Dict[str, float] = {}
            for c in codes:
                wp = float(prev_w.get(c, 0.0))
                wf = float(filled.get(c, 0.0))
                ws = wp + (wf - wp) * budget_scale
                if ws > 1e-12:
                    scaled[c] = ws
            s_sc = float(sum(max(0.0, float(w)) for w in scaled.values()))
            if s_sc > 1.0 + 1e-12:
                scaled = {c: (float(w) / s_sc) for c, w in scaled.items() if float(w) > 1e-12}
            filled = scaled

        cost1 = _estimate_trade_cost_bps(prev_w, filled, liq_map)
        executed_turnover_2way = 0.5 * sum(abs(float(filled.get(c, 0.0) - prev_w.get(c, 0.0))) for c in codes)

        meta = {
            "enabled": True,
            "mode": execution_mode,
            "aum_cny": float(aum_cny),
            "adv_participation_max": float(adv_participation_max),
            "impact_k": float(impact_k),
            "impact_alpha": float(impact_alpha),
            "impact_liq_floor": float(impact_liq_floor),
            "impact_cost_budget_bps": float(impact_cost_budget_bps),
            "fee_bps": float(fee_bps),
            "codes_n": int(len(codes)),
            "full_fill_n": int(n_full),
            "partial_fill_n": int(n_partial),
            "blocked_fill_n": int(n_blocked),
            "avg_fill_ratio": float(fill_ratio_sum / max(1, fill_ratio_n)),
            "budget_scale": float(budget_scale),
            "executed_turnover_2way": float(executed_turnover_2way),
            "cost_estimate": cost1,
        }
        return filled, meta

    try:
        if strategy != "hybrid_baseline_weekly_topk":
            raise RuntimeError("signals_impl currently supports only hybrid_baseline_weekly_topk")

        mom_dir = workdir / "mom"
        mom_dir.mkdir(parents=True, exist_ok=True)
        mom_date, mom_picks, mom_stats, cfg_used = _run_baseline_backtest_to_workdir_with_fallback(
            mom_dir, cfg, "xsec_momentum_weekly_topk"
        )

        ma_dir = workdir / "ma"
        ma_dir.mkdir(parents=True, exist_ok=True)
        ma_date, ma_picks, ma_stats, cfg_used = _run_baseline_backtest_to_workdir_with_fallback(
            ma_dir, cfg_used, "ts_ma_weekly"
        )

        import pandas as pd

        mom_tr = _extract_last_tranches_from_positions_csv(mom_dir / "positions.csv", n=max(hold_weeks, 1))
        ma_tr = _extract_last_tranches_from_positions_csv(ma_dir / "positions.csv", n=max(hold_weeks, 1))

        # Ladder execution (strict order): L0 -> L1 -> L2 -> L3
        ladder_runs = []
        fallback_level = "L0"
        fallback_trigger_reason = None

        n_tr = 1 if not tranche_overlap or hold_weeks <= 1 else min(2, hold_weeks)
        scale = 1.0 / n_tr

        tranche_objs = []
        hard_filter_stats = []
        final_port: Dict[str, float] = {}
        final_scores: Dict[str, float] = {}

        for ladder_level in ["L0", "L1", "L2"]:
            fallback_level = ladder_level
            tranche_objs = []
            tranche_contribs = []
            hard_filter_stats = []
            final_port = {}
            final_scores = {}
            score_mass: Dict[str, float] = {}
            weight_mass: Dict[str, float] = {}

            for t in range(n_tr):
                mom_w = mom_tr[t]["weights"] if t < len(mom_tr) else {}
                ma_w = ma_tr[t]["weights"] if t < len(ma_tr) else {}

                mom_sorted = sorted(mom_w.items(), key=lambda x: (-x[1], x[0]))
                cand_k = int(cfg.get("candidate_k", (top_k * 5 if score_mode == "factor" else top_k)))
                cand_k = max(top_k, min(500, cand_k))
                mom_top = [c for c, _w in mom_sorted][:cand_k]
                ma_set = set([c for c, w in ma_w.items() if w > 0])

                scores: Dict[str, float] = {}
                mom_rank: Dict[str, int] = {}
                for i, code in enumerate(mom_top, start=1):
                    mom_rank[code] = i
                    scores[code] = float(top_k - i + 1)
                if ma_mode == "boost":
                    for code in ma_set:
                        scores[code] = scores.get(code, 0.0) + 1.0

                factor_pack: Dict[str, Dict[str, float]] = {}
                if score_mode == "factor":
                    factor_pack, _liq_field, factor_reasons = _compute_factors_for_codes(
                        mom_tr[t]["rebalance_date"], list(set(mom_top) | ma_set), cfg
                    )
                    r5 = {c: factor_pack[c]["ret_5d"] for c in factor_pack}
                    r10 = {c: factor_pack[c]["ret_10d"] for c in factor_pack}
                    r20 = {c: factor_pack[c]["ret_20d"] for c in factor_pack}
                    ma60 = {c: factor_pack[c]["ma_60d"] for c in factor_pack}
                    v20 = {c: factor_pack[c]["vol_20d"] for c in factor_pack}
                    liq = {c: factor_pack[c]["liq_20d"] for c in factor_pack}
                    zr5, zr10, zr20, zma60, zv20, zliq = (
                        _zscore(r5),
                        _zscore(r10),
                        _zscore(r20),
                        _zscore(ma60),
                        _zscore(v20),
                        _zscore(liq),
                    )
                    w = {
                        "ret_20d": float(cfg.get("score_w_ret_20d", FAC_WEIGHTS["ret_20d"])),
                        "ret_10d": float(cfg.get("score_w_ret_10d", FAC_WEIGHTS["ret_10d"])),
                        "ret_5d": float(cfg.get("score_w_ret_5d", FAC_WEIGHTS["ret_5d"])),
                        "ma_60d": float(cfg.get("score_w_ma_60d", FAC_WEIGHTS["ma_60d"])),
                        "vol_20d": float(cfg.get("score_w_vol_20d", FAC_WEIGHTS["vol_20d"])),
                        "liq_20d": float(cfg.get("score_w_liq_20d", FAC_WEIGHTS["liq_20d"])),
                    }
                    for c in factor_pack:
                        scores[c] = (
                            w["ret_20d"] * zr20.get(c, 0.0)
                            + w["ret_10d"] * zr10.get(c, 0.0)
                            + w["ret_5d"] * zr5.get(c, 0.0)
                            + w["ma_60d"] * zma60.get(c, 0.0)
                            + w["vol_20d"] * zv20.get(c, 0.0)
                            + w["liq_20d"] * zliq.get(c, 0.0)
                        )

                if ma_mode == "filter":
                    candidates = set(mom_top) & ma_set
                    if len(candidates) < max(3, min(5, top_k)):
                        candidates = set(ma_set)
                else:
                    candidates = set(mom_top) | ma_set

                hard_stats = {
                    "before": len(candidates),
                    "after": len(candidates),
                    "vol_20d_max": hard_vol_20d_max,
                    "liq_20d_min": hard_liq_20d_min,
                    "dist_252h_min": hard_dist_252h_min,
                    "breakout_60_min": hard_breakout_60_min,
                    "drawdown_60_min": hard_drawdown_60_min,
                    "downvol_q": hard_downvol_q,
                }
                if score_mode == "factor" and factor_pack:
                    # --- Audit breakdown counts (do not affect trading logic) ---
                    # N3: score_valid (factors exist)
                    valid = {c for c in candidates if c in factor_pack}
                    hard_stats["N3_after_score_valid"] = int(len(valid))

                    # top1 invalid reason (highest momentum rank among invalid)
                    inv = [c for c in candidates if c not in factor_pack]
                    if inv:
                        inv_sorted = sorted(inv, key=lambda c: (mom_rank.get(c, 10**9), c))
                        c0 = str(inv_sorted[0]).zfill(6)
                        hard_stats["top1_invalid_reason"] = str((factor_reasons or {}).get(c0, "unknown"))

                    def _ok(code: str) -> bool:
                        fp = factor_pack.get(code)
                        # If factors are missing entirely, treat as score-invalid (cannot apply hard filters).
                        if not fp:
                            return False
                        # If vol/liq are missing (NaN), do not hard-fail eligibility.
                        v = fp.get("vol_20d", float("nan"))
                        l = fp.get("liq_20d", float("nan"))
                        try:
                            v = float(v)
                        except Exception:
                            v = float("nan")
                        try:
                            l = float(l)
                        except Exception:
                            l = float("nan")
                        if hard_vol_20d_max > 0 and v == v and v > hard_vol_20d_max:
                            return False
                        if hard_liq_20d_min > 0 and l == l and l < hard_liq_20d_min:
                            return False
                        # survival bottom-line filter (ONLY hard quality filter)
                        # IMPORTANT: if dist_252h is not computable (e.g., window not full),
                        # do not hard-fail eligibility here; treat as "unknown" and let it
                        # affect alpha quality only.
                        d = fp.get("dist_252h")
                        try:
                            d = float(d)
                        except Exception:
                            d = float("nan")
                        if d == d:  # not NaN
                            if d < hard_dist_252h_min:
                                return False
                        return True

                    candidates = {c for c in candidates if _ok(c)}
                    hard_stats["N1_after_dist"] = int(len(candidates))

                    # downside-vol quantile hard filter (conditional)
                    if candidates and 0.0 < hard_downvol_q < 1.0:
                        dvs = [float(factor_pack[c].get("downvol_20d", 0.0)) for c in candidates if c in factor_pack]
                        dvs = [x for x in dvs if x == x]  # drop NaN
                        # Only apply hard cut when pool is large enough (avoid emptying)
                        # and only at L0 (L1 disables hard downvol cut).
                        if ladder_level == "L0" and len(dvs) >= N_GUARD:
                            dvs.sort()
                            idx = int(round((len(dvs) - 1) * hard_downvol_q))
                            thr = float(dvs[idx])
                            hard_stats["downvol_thr"] = thr
                            candidates = {c for c in candidates if float(factor_pack[c].get("downvol_20d", 0.0)) <= thr}

                    hard_stats["N2_after_downvol_hard"] = int(len(candidates))
                    hard_stats["after"] = len(candidates)

                hard_filter_stats.append({"tranche": t, **hard_stats})

                # Structure gating (soft): do not kill candidates; just downweight score.
                # breakout score S_b
                def _sb(x: float) -> float:
                    if x is None or x != x:
                        return 0.2
                    if x >= -0.02:
                        return 1.0
                    if x < -0.08:
                        return 0.2
                    # linear from 0.4..1.0 over [-0.08, -0.02)
                    return 0.4 + (x - (-0.08)) * (1.0 - 0.4) / ((-0.02) - (-0.08))

                # drawdown score S_d
                def _sd(x: float) -> float:
                    if x is None or x != x:
                        return 0.1
                    if x >= -0.30:
                        return 1.0
                    if x < -0.45:
                        return 0.1
                    # linear from 0.3..1.0 over [-0.45, -0.30)
                    return 0.3 + (x - (-0.45)) * (1.0 - 0.3) / ((-0.30) - (-0.45))

                # Downvol penalty (always available; hard cut already applied conditionally)
                downvol_rank = {}
                if score_mode == "factor" and factor_pack and candidates:
                    dvs = [(c, float(factor_pack.get(c, {}).get("downvol_20d", 0.0))) for c in candidates]
                    dvs = [(c, v) for c, v in dvs if v == v]
                    dvs.sort(key=lambda x: x[1])  # low risk first
                    n = len(dvs)
                    for i, (c, _v) in enumerate(dvs):
                        downvol_rank[c] = (i / max(1, n - 1))  # 0..1

                def gated_score(code: str) -> float:
                    sc = float(scores.get(code, 0.0))
                    if score_mode != "factor" or not factor_pack:
                        return sc
                    fp = factor_pack.get(code) or {}
                    sb = _sb(float(fp.get("breakout_60", float('nan'))))
                    sd = _sd(float(fp.get("drawdown_60", float('nan'))))
                    s_struct = sb * sd
                    if ladder_level == "L1":
                        s_struct = max(s_struct, float(S_STRUCT_FLOOR_L1))

                    # downvol penalty: 1 - rank_pct(downvol)
                    s_down = 1.0 - float(downvol_rank.get(code, 0.5))

                    if ladder_level == "L2":
                        # Structure affects sorting only (not multiplicative).
                        return sc * s_down

                    return sc * s_struct * s_down

                def sort_key(code: str):
                    if ladder_level == "L2" and score_mode == "factor" and factor_pack:
                        fp = factor_pack.get(code) or {}
                        sb = _sb(float(fp.get("breakout_60", float('nan'))))
                        sd = _sd(float(fp.get("drawdown_60", float('nan'))))
                        s_struct = sb * sd
                        # Primary: gated_score (no struct), Secondary: struct
                        return (-gated_score(code), -s_struct, mom_rank.get(code, 10**9), code)
                    return (-gated_score(code), mom_rank.get(code, 10**9), code)

                ranked = sorted(candidates, key=sort_key)
                picks = ranked[:top_k]
                backups = ranked[top_k : top_k + max(0, backup_k)]

                # Weight concentration BEFORE applying min_weight:
                # If we use tranche_overlap (n_tr=2), equal-weight top_k can fall below min_weight
                # (e.g., 0.5*(1/20)=0.025 < 0.04) and get wiped out.
                # We keep selection top_k unchanged, but allocate weights only to the head.
                k_alloc = min(int(top_k), max(int(MIN_POS) * 2, 12))
                k_alloc = max(1, min(int(top_k), int(k_alloc)))

                # Execution realism (BUY-side): block new buys at up-limit and fill with backups
                if execution_mode in {"realistic", "shadow"} and backups:
                    prev_set = set()
                    if t - 1 >= 0 and t - 1 < len(tranche_objs):
                        prev_set = set(tranche_objs[t - 1].get("picks") or [])

                    blocked_buys = set()
                    asof = pd.to_datetime(mom_tr[t]["rebalance_date"]) if t < len(mom_tr) else pd.to_datetime(mom_date)

                    # connect mongo
                    import pymongo

                    host = os.getenv("MONGODB_HOST", os.getenv("MONGO_HOST", "mongodb"))
                    port = int(os.getenv("MONGODB_PORT", os.getenv("MONGO_PORT", "27017")))
                    dbn = os.getenv("MONGODB_DATABASE", os.getenv("MONGO_DATABASE", "quantaxis"))
                    user = os.getenv("MONGODB_USER", os.getenv("MONGO_USER", "quantaxis"))
                    password = os.getenv("MONGODB_PASSWORD", os.getenv("MONGO_PASSWORD", "quantaxis"))
                    root_user = os.getenv("MONGO_ROOT_USER", "root")
                    root_password = os.getenv("MONGO_ROOT_PASSWORD", "root")

                    uris = [
                        f"mongodb://{user}:{password}@{host}:{port}/{dbn}?authSource=admin",
                        f"mongodb://{root_user}:{root_password}@{host}:{port}/{dbn}?authSource=admin",
                        f"mongodb://{host}:{port}/{dbn}",
                    ]
                    client = None
                    for uri in uris:
                        try:
                            c = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)
                            c.admin.command("ping")
                            client = c
                            break
                        except Exception:
                            pass

                    if client is not None:
                        coll = client[dbn]["stock_day"]
                        buy_list = [c for c in picks if c not in prev_set]
                        if buy_list:
                            end = asof
                            start = asof - pd.Timedelta(days=10)
                            start_s = str(start.date())
                            end_s = str(end.date())
                            start2 = start.strftime("%Y%m%d")
                            end2 = end.strftime("%Y%m%d")

                            for code in buy_list:
                                code6 = str(code).zfill(6)
                                q = {
                                    "code": code6,
                                    "$or": [
                                        {"date": {"$gte": start_s, "$lte": end_s}},
                                        {"date": {"$gte": start2, "$lte": end2}},
                                    ],
                                }
                                rows = list(coll.find(q, {"_id": 0, "date": 1, "high": 1, "close": 1}).sort("date", 1))
                                if len(rows) < 2:
                                    continue
                                df = pd.DataFrame(rows)
                                df["date"] = pd.to_datetime(df["date"].astype(str), format="mixed", errors="coerce")
                                df = df.dropna(subset=["date"]).sort_values("date")
                                df = df[df["date"] <= asof]
                                if df.shape[0] < 2:
                                    continue
                                today = df.iloc[-1]
                                prev = df.iloc[-2]
                                pc = float(prev.get("close", 0.0) or 0.0)
                                hh = float(today.get("high", 0.0) or 0.0)
                                cc = float(today.get("close", 0.0) or 0.0)

                                lp = _limit_pct_for(code6, base=limit_pct, tiering=limit_tiering)
                                up_lim = pc * (1.0 + lp)
                                if limit_touch_mode == "hl":
                                    touch_up = hh >= up_lim and _near_bps(hh, up_lim, limit_price_eps_bps)
                                else:
                                    touch_up = abs(cc - hh) <= float(limit_touch_eps) and _near_bps(cc, up_lim, limit_price_eps_bps)
                                if touch_up:
                                    blocked_buys.add(code)

                    if blocked_buys:
                        keep = [c for c in picks if c not in blocked_buys]
                        held = set(prev_set) | set(keep)
                        add = []
                        for c in backups:
                            if c not in held:
                                add.append(c)
                            if len(add) >= len(blocked_buys):
                                break
                        picks = keep + add

                picks_alloc = picks[:k_alloc]

                def _apply_weight_cap(weights: Dict[str, float], cap_in: float) -> Dict[str, float]:
                    if not weights:
                        return {}
                    n = len(weights)
                    cap = max(float(cap_in), 1.0 / float(max(1, n)))
                    w = {c: max(0.0, float(v)) for c, v in weights.items()}
                    s0 = float(sum(w.values()))
                    if s0 <= 0:
                        return {}
                    w = {c: (v / s0) for c, v in w.items()}
                    for _ in range(8):
                        clipped = {c: min(v, cap) for c, v in w.items()}
                        s = float(sum(clipped.values()))
                        if s <= 0:
                            return {}
                        if abs(s - 1.0) <= 1e-12:
                            return clipped
                        if s < 1.0:
                            under = {c: v for c, v in clipped.items() if v < cap - 1e-12}
                            if not under:
                                return {c: (v / s) for c, v in clipped.items()}
                            add = 1.0 - s
                            su = float(sum(under.values()))
                            if su <= 0:
                                add_each = add / float(len(under))
                                for c in under:
                                    clipped[c] = clipped[c] + add_each
                            else:
                                for c in under:
                                    clipped[c] = clipped[c] + add * (under[c] / su)
                            w = clipped
                            continue
                        w = {c: (v / s) for c, v in clipped.items()}
                    sf = float(sum(w.values()))
                    return {c: (v / sf) for c, v in w.items()} if sf > 0 else {}

                def _alloc_weights(codes: list[str]) -> Dict[str, float]:
                    if not codes:
                        return {}
                    if weight_mode == "equal":
                        ew = 1.0 / float(len(codes))
                        return _apply_weight_cap({c: ew for c in codes}, max_name_weight)

                    # Score-driven allocation: softmax over gated scores.
                    vals = []
                    for c in codes:
                        v = float(gated_score(c))
                        if not math.isfinite(v):
                            v = 0.0
                        vals.append(v)

                    if len(set(round(v, 12) for v in vals)) <= 1:
                        ew = 1.0 / float(len(codes))
                        return _apply_weight_cap({c: ew for c in codes}, max_name_weight)

                    # Standardize cross-sectional scores first so score_temp is stable
                    # across dates/levels and not dominated by raw score magnitude drift.
                    vm = float(sum(vals) / len(vals))
                    vv = float(sum((v - vm) ** 2 for v in vals) / max(1, len(vals) - 1))
                    vsd = math.sqrt(vv) if vv > 0 else 0.0
                    if vsd > 1e-12:
                        vals = [max(-6.0, min(6.0, (v - vm) / vsd)) for v in vals]
                    else:
                        vals = [0.0 for _ in vals]

                    t = max(0.01, float(score_temp))
                    vmax = max(vals)
                    exps = [math.exp((v - vmax) / t) for v in vals]
                    s = float(sum(exps))
                    if s <= 0:
                        ew = 1.0 / float(len(codes))
                        return _apply_weight_cap({c: ew for c in codes}, max_name_weight)
                    return _apply_weight_cap({c: float(exps[i] / s) for i, c in enumerate(codes)}, max_name_weight)

                tranche_port = _alloc_weights(picks_alloc)

                contrib: Dict[str, float] = {}
                for c, w in tranche_port.items():
                    dw = scale * w
                    final_port[c] = final_port.get(c, 0.0) + dw
                    contrib[c] = contrib.get(c, 0.0) + dw
                    # Keep score information as weighted average across tranche contributions.
                    # Use the same gated score as ranking/allocation to keep exported score aligned.
                    gsc = float(gated_score(c))
                    if not math.isfinite(gsc):
                        gsc = 0.0
                    score_mass[c] = score_mass.get(c, 0.0) + dw * gsc
                    weight_mass[c] = weight_mass.get(c, 0.0) + dw

                tranche_contribs.append(contrib)
                tranche_objs.append(
                    {
                        "rebalance_date": mom_tr[t]["rebalance_date"],
                        "effective_date": mom_tr[t]["effective_date"],
                        "picks": picks,
                        "picks_alloc": picks_alloc,
                        "k_alloc": int(k_alloc),
                    }
                )

            if weight_mass:
                final_scores = {
                    c: (float(score_mass.get(c, 0.0)) / float(wm))
                    for c, wm in weight_mass.items()
                    if float(wm) > 1e-12
                }

            # --- Ladder evaluation (use effective positions after min_weight) ---
            eff_port = dict(final_port)
            if min_weight and min_weight > 0:
                eff_port = {c: w for c, w in eff_port.items() if float(w) >= float(min_weight)}

            # L2: risk downtier (cap single-name max weight)
            cap_stats = None
            if ladder_level == "L2" and eff_port:
                cap = (1.0 / float(top_k)) * float(W_CAP_MULT_L2)
                # clip
                clipped = {c: min(float(w), cap) for c, w in eff_port.items()}
                s_clip = sum(clipped.values())
                # redistribute remaining weight to under-cap names proportionally
                if s_clip > 0 and s_clip < 1.0:
                    under = {c: w for c, w in clipped.items() if w < cap - 1e-12}
                    if under:
                        s_under = sum(under.values())
                        add = 1.0 - s_clip
                        for c in list(under.keys()):
                            clipped[c] = clipped[c] + add * (under[c] / s_under if s_under > 0 else 1.0 / len(under))
                # renormalize
                s2 = sum(max(0.0, float(w)) for w in clipped.values())
                eff_port = {c: float(w) / s2 for c, w in clipped.items()} if s2 > 0 else {}
                cap_stats = {"cap": cap, "n": len(eff_port)}

            eff_n = len(eff_port)
            sum_w_after = float(sum(float(w) for w in eff_port.values()))
            cash_w_nofb = float(max(0.0, 1.0 - sum_w_after))

            # --- Per-tranche audit after min_weight (no effect on trading logic) ---
            # NOTE: tranche_contribs sums to ~scale per tranche (e.g. 0.5) by construction.
            # The audit requirement wants sum_weight_after_min_weight to reflect the *portfolio*
            # (after min_weight) so that: sum_weight_after_min_weight + cash_weight_nofb == 1.
            tranche_audit = []
            for i, contrib in enumerate(tranche_contribs):
                kept = {c: float(w) for c, w in (contrib or {}).items() if c in eff_port and float(w) >= float(min_weight or 0.0)}
                tranche_audit.append(
                    {
                        "tranche": int(i),
                        "effective_positions_after_min_weight": int(len(kept)),
                        "sum_weight_after_min_weight": float(sum_w_after),
                    }
                )

            ladder_runs.append(
                {
                    "level": ladder_level,
                    "effective_positions": eff_n,
                    "effective_positions_after_min_weight": int(eff_n),
                    "sum_weight_after_min_weight": float(sum_w_after),
                    "cash_weight_nofb": float(cash_w_nofb),
                    "tranche_audit": tranche_audit,
                    "min_weight": min_weight,
                    "downvol_mode": "hard+penalty" if ladder_level == "L0" else ("penalty_only" if ladder_level == "L1" else "penalty_only"),
                    "s_struct_floor": float(S_STRUCT_FLOOR_L1) if ladder_level == "L1" else None,
                    "l2_cap": cap_stats,
                    "hard_filter_stats": hard_filter_stats,
                }
            )

            if eff_n >= MIN_POS:
                fallback_level = ladder_level
                break

        as_of_date = tranche_objs[0]["rebalance_date"] if tranche_objs else (mom_date or ma_date)

        factors = None
        zfactors = None
        liq_field_detected = None
        if score_mode == "factor" and final_port:
            factors, liq_field_detected, _final_factor_reasons = _compute_factors_for_codes(as_of_date, list(final_port.keys()), cfg)
            r5 = {c: factors[c]["ret_5d"] for c in factors}
            r10 = {c: factors[c]["ret_10d"] for c in factors}
            r20 = {c: factors[c]["ret_20d"] for c in factors}
            ma60 = {c: factors[c]["ma_60d"] for c in factors}
            v20 = {c: factors[c]["vol_20d"] for c in factors}
            liq = {c: factors[c]["liq_20d"] for c in factors}
            zr5, zr10, zr20, zma60, zv20, zliq = (
                _zscore(r5),
                _zscore(r10),
                _zscore(r20),
                _zscore(ma60),
                _zscore(v20),
                _zscore(liq),
            )
            zfactors = {
                c: {
                    "ret_5d": zr5.get(c, 0.0),
                    "ret_10d": zr10.get(c, 0.0),
                    "ret_20d": zr20.get(c, 0.0),
                    "ma_60d": zma60.get(c, 0.0),
                    "vol_20d": zv20.get(c, 0.0),
                    "liq_20d": zliq.get(c, 0.0),
                }
                for c in factors
            }

        # Optional: drop tiny weights (e.g., tranche-only names).
        # NOTE: do NOT renormalize yet; we may intentionally leave some weight unallocated
        # and route it to the fallback asset (L3).
        if min_weight and min_weight > 0:
            final_port = {c: w for c, w in final_port.items() if float(w) >= float(min_weight)}

        # Hard protection: disallow inconsistent state where we have positive weight budget
        # but end up with zero effective positions after min_weight. In that case, force
        # the sleeve to cash (nofb) or full fallback (fb).
        s_stock = sum(max(0.0, float(w)) for w in final_port.values())
        if len(final_port) == 0 and s_stock > 1e-12:
            # This should be impossible, but guard anyway.
            final_port = {}

        if len(final_port) == 0:
            if not disable_fallback:
                fallback_level = "L3"
                fallback_trigger_reason = "empty_after_min_weight"
                final_port[fallback_asset] = 1.0
                final_scores[fallback_asset] = 0.0
            else:
                # no positions; remaining sleeve is cash
                fallback_trigger_reason = "empty_after_min_weight_nofb"

        # Fallback ladder (L3): if after L2 we still have too few names, park remaining weight in a substitute asset.
        eff_n = len(final_port)
        if (not disable_fallback) and fallback_level == "L2" and eff_n < MIN_POS:
            fallback_level = "L3"
            fallback_trigger_reason = "candidates<6 after L2"
            s = sum(max(0.0, float(w)) for w in final_port.values())
            fallback_weight = max(0.0, 1.0 - float(s))
            # If everything got filtered out, allocate 100% to fallback asset.
            if s <= 1e-12:
                fallback_weight = 1.0
            if fallback_weight > 1e-12:
                final_port[fallback_asset] = final_port.get(fallback_asset, 0.0) + fallback_weight
                final_scores[fallback_asset] = 0.0

        # Final normalize
        s = sum(max(0.0, float(w)) for w in final_port.values())
        if s > 0:
            final_port = {c: float(w) / s for c, w in final_port.items() if float(w) > 0}

        execution_realism_meta = None
        turnover_trigger_meta = None

        # Turnover trigger:
        # - event_only: only trade when a new rebalance date appears.
        # - event_or_drift: allow extra rebalance only when both drift and estimated cost gates pass.
        # Then apply deadband + execution realism.
        if final_port:
            try:
                sealed_date_for_turnover = str(cfg.get("sealed_date") or as_of_date)
                prev_sig = _load_prev_signal_for_strategy(strategy_key, sealed_date_for_turnover)
                if prev_sig is not None:
                    prev_asof = prev_sig.get("as_of_date")
                    is_new_rebalance = bool(str(prev_asof) != str(as_of_date))
                    prev_port = _non_cash_weights(prev_sig)
                    target_port = _normalize_nonneg_port(final_port)
                    exec_realism_on = str(execution_mode) in {"realistic", "shadow"}

                    raw_turnover_2way = _turnover_2way(prev_port, target_port)
                    raw_est_total_cost_bps = None
                    if exec_realism_on and (prev_port or target_port):
                        liq_map0 = _liq20_map(as_of_date, set(prev_port.keys()) | set(target_port.keys()))
                        raw_est_total_cost_bps = float(_estimate_trade_cost_bps(prev_port, target_port, liq_map0)["total_cost_bps"])

                    drift_gate = bool(raw_turnover_2way >= float(rebalance_drift_min_turnover_2way))
                    cost_gate = bool(
                        (raw_est_total_cost_bps is None) or (raw_est_total_cost_bps <= float(rebalance_drift_max_cost_bps))
                    )
                    should_trade = bool(is_new_rebalance)
                    trigger_reason = "new_rebalance" if is_new_rebalance else "hold_no_trigger"

                    if (not should_trade) and str(rebalance_trigger_mode) == "event_or_drift" and drift_gate and cost_gate:
                        should_trade = True
                        trigger_reason = "drift_and_cost"

                    if should_trade:
                        if min_trade_weight > 0:
                            adjusted: Dict[str, float] = {}
                            for c in sorted(set(prev_port.keys()) | set(target_port.keys())):
                                w_prev = float(prev_port.get(c, 0.0))
                                w_new = float(target_port.get(c, 0.0))
                                if abs(w_new - w_prev) < float(min_trade_weight):
                                    w_new = w_prev
                                if w_new > 1e-12:
                                    adjusted[c] = w_new
                            target_port = _normalize_nonneg_port(adjusted)

                        post_deadband_turnover_2way = _turnover_2way(prev_port, target_port)
                        if (not is_new_rebalance) and str(rebalance_trigger_mode) == "event_or_drift":
                            if post_deadband_turnover_2way < float(rebalance_drift_min_turnover_2way):
                                should_trade = False
                                trigger_reason = "drift_below_threshold_after_deadband"

                        if should_trade:
                            if exec_realism_on and prev_port:
                                final_port, execution_realism_meta = _apply_execution_realism(prev_port, target_port, as_of_date)
                            else:
                                final_port = target_port
                        else:
                            final_port = prev_port
                    else:
                        final_port = prev_port

                    turnover_trigger_meta = {
                        "mode": str(rebalance_trigger_mode),
                        "is_new_rebalance": bool(is_new_rebalance),
                        "trigger_reason": trigger_reason,
                        "raw_turnover_2way": float(raw_turnover_2way),
                        "raw_est_total_cost_bps": (None if raw_est_total_cost_bps is None else float(raw_est_total_cost_bps)),
                        "drift_min_turnover_2way": float(rebalance_drift_min_turnover_2way),
                        "drift_max_cost_bps": float(rebalance_drift_max_cost_bps),
                        "drift_gate": bool(drift_gate),
                        "cost_gate": bool(cost_gate),
                        "trade_applied": bool(should_trade),
                    }
                else:
                    turnover_trigger_meta = {
                        "mode": str(rebalance_trigger_mode),
                        "is_new_rebalance": None,
                        "trigger_reason": "no_prev_signal",
                        "raw_turnover_2way": None,
                        "raw_est_total_cost_bps": None,
                        "drift_min_turnover_2way": float(rebalance_drift_min_turnover_2way),
                        "drift_max_cost_bps": float(rebalance_drift_max_cost_bps),
                        "drift_gate": None,
                        "cost_gate": None,
                        "trade_applied": None,
                    }
            except Exception:
                pass

        # --- Health Index v1 integration (ONLY affects overall exposure via cash) ---
        # Does NOT change selection, ranking, factor computation, ladder thresholds, or weights' relative structure.
        health_date = cfg.get("health_date") or as_of_date
        health_score, health_path = _load_health_score(health_date)
        health_missing = health_score is None
        # If missing, default to full exposure (per spec).
        exposure = _clip(health_score if health_score is not None else 1.0, 0.4, 1.0)

        if exposure < 1.0:
            final_port = {c: float(w) * exposure for c, w in final_port.items()}
        cash_weight = float(max(0.0, 1.0 - sum(max(0.0, float(w)) for w in final_port.values())))

        # Build positions without renormalizing, then append cash.
        positions = _portfolio_to_positions(final_port, scores=final_scores, factors=factors, zfactors=zfactors, normalize=False)
        if cash_weight > 1e-12:
            positions.append({"code": "CASH", "weight": round(float(cash_weight), 10), "rank": 10**9, "score": 0.0})

        stats = mom_stats
        sealed_date_for_ops = cfg.get("sealed_date") or as_of_date

        meta_base = {
            "strategy": strategy,
            "theme": theme,
            "top_k": top_k,
            "rebalance": "weekly",
            "strategy_key": strategy_key,
            "picks_base": list(mom_picks) if isinstance(mom_picks, list) else None,
            "min_bars": int(cfg_used.get("min_bars", min_bars)),
            "liq_window": int(cfg_used.get("liq_window", liq_window)),
            "liq_min_ratio": float(cfg_used.get("liq_min_ratio", liq_min_ratio)),
            "lookback": lookback,
            "ma": ma,
            "ma_mode": ma_mode,
            "score_mode": score_mode,
            "score_w_ret_20d": float(cfg.get("score_w_ret_20d", FAC_WEIGHTS["ret_20d"])),
            "score_w_ret_10d": float(cfg.get("score_w_ret_10d", FAC_WEIGHTS["ret_10d"])),
            "score_w_ret_5d": float(cfg.get("score_w_ret_5d", FAC_WEIGHTS["ret_5d"])),
            "score_w_ma_60d": float(cfg.get("score_w_ma_60d", FAC_WEIGHTS["ma_60d"])),
            "score_w_vol_20d": float(cfg.get("score_w_vol_20d", FAC_WEIGHTS["vol_20d"])),
            "score_w_liq_20d": float(cfg.get("score_w_liq_20d", FAC_WEIGHTS["liq_20d"])),
            "min_weight": min_weight,
            "weight_mode": weight_mode,
            "score_temp": float(score_temp),
            "min_trade_weight": float(min_trade_weight),
            "max_name_weight": float(max_name_weight),
            "rebalance_trigger_mode": str(rebalance_trigger_mode),
            "rebalance_drift_min_turnover_2way": float(rebalance_drift_min_turnover_2way),
            "rebalance_drift_max_cost_bps": float(rebalance_drift_max_cost_bps),
            "turnover_trigger": turnover_trigger_meta,
            "execution_mode": execution_mode,
            "execution_realism": execution_realism_meta,
            "backup_k": backup_k,
            "limit_tiering": limit_tiering,
            "limit_pct": limit_pct,
            "limit_price_eps_bps": limit_price_eps_bps,
            "limit_touch_mode": limit_touch_mode,
            "limit_touch_eps": limit_touch_eps,
            "aum_cny": float(aum_cny),
            "adv_participation_max": float(adv_participation_max),
            "impact_k": float(impact_k),
            "impact_alpha": float(impact_alpha),
            "impact_liq_floor": float(impact_liq_floor),
            "impact_cost_budget_bps": float(impact_cost_budget_bps),
            "fee_bps": float(fee_bps),
            "auto_relax": stats.get("auto_relax") if isinstance(stats, dict) else None,
            "hard_filters": {
                "vol_20d_max": hard_vol_20d_max,
                "liq_20d_min": hard_liq_20d_min,
                "dist_252h_min": hard_dist_252h_min,
                "breakout_60_min": hard_breakout_60_min,
                "drawdown_60_min": hard_drawdown_60_min,
                "downvol_q": hard_downvol_q,
            },
            "hard_filter_stats": hard_filter_stats,
            "ladder": {
                "level_used": fallback_level,
                "runs": ladder_runs,
            },
            "fallback": {
                "level": fallback_level,
                "trigger_reason": fallback_trigger_reason,
                "asset": fallback_asset,
                "disabled": disable_fallback,
            },
            "health": {
                "health_score": health_score,
                "exposure": float(exposure),
                "cash_weight": float(cash_weight),
                "path": health_path,
                "health_missing": bool(health_missing),
            },
            "ops": {
                "sealed_date": sealed_date_for_ops,
                "sealed_ok": True,
            },
            "data_version_id": data_version_id,
            "manifest_sha256": manifest_sha256,
            "hold_weeks": hold_weeks,
            "tranche_overlap": tranche_overlap,
            "tranches": tranche_objs,
            "liq_field_detected": liq_field_detected,
        }

        meta_sig = json.dumps(meta_base, sort_keys=True, ensure_ascii=False)
        config_signature = hashlib.sha256(meta_sig.encode("utf-8")).hexdigest()

        # Append minimal daily health log: date, health_score, exposure, level_used
        try:
            log_dir = ROOT / "output" / "reports" / "health_index"
            log_dir.mkdir(parents=True, exist_ok=True)
            log_path = log_dir / "health_signal_log.csv"
            line = f"{health_date},{'' if health_score is None else health_score},{exposure},{fallback_level}\n"
            if not log_path.exists():
                log_path.write_text("date,health_score,exposure,level_used\n" + line, encoding="utf-8")
            else:
                with log_path.open("a", encoding="utf-8") as f:
                    f.write(line)
        except Exception:
            pass

        # --- Turnover attribution + hold smoothing (meta-only; does not affect positions) ---
        def _weights_from_signal_meta(sig_obj: dict) -> dict:
            pos = (sig_obj.get("positions") or [])
            w = {}
            for p in pos:
                c = str(p.get("code"))
                w[c] = float(p.get("weight", 0.0) or 0.0)
            # Ensure CASH present as pseudo-asset
            mh = (sig_obj.get("meta", {}) or {}).get("health", {}) or {}
            cw = mh.get("cash_weight")
            if cw is None:
                cw = max(0.0, 1.0 - sum(max(0.0, float(x)) for k, x in w.items() if k.upper() != "CASH"))
            w["CASH"] = float(cw)
            # Normalize tiny float drift
            s2 = sum(float(x) for x in w.values())
            if s2 > 0:
                for k in list(w.keys()):
                    w[k] = float(w[k]) / s2
            return w

        def _load_prev_signal(strategy_key: str, sealed_date: str) -> dict | None:
            import glob

            candidates = []
            for path in glob.glob(str(SIGNALS_DIR / "*.json")):
                if path.endswith(".status.json"):
                    continue
                try:
                    obj = json.loads(Path(path).read_text(encoding="utf-8"))
                except Exception:
                    continue
                if obj.get("status") != "succeeded":
                    continue
                m = obj.get("meta", {}) or {}
                mkey = str(m.get("strategy_key") or "")
                if mkey != str(strategy_key):
                    if not (mkey == strategy_key_legacy and _compat_prev_meta(m)):
                        continue
                ops = (m.get("ops", {}) or {})
                sd = ops.get("sealed_date")
                if not isinstance(sd, str):
                    continue
                if sd >= sealed_date:
                    continue
                candidates.append((sd, int(obj.get("generated_at") or 0), obj))
            if not candidates:
                return None
            candidates.sort(key=lambda x: (x[0], x[1]))
            return candidates[-1][2]

        def _is_exposure_scale(prev_w_nc: dict, curr_w_nc: dict, eps: float = 1e-6) -> bool:
            sp = sum(prev_w_nc.values())
            sc = sum(curr_w_nc.values())
            if sp <= 1e-12 or sc <= 1e-12:
                return False
            k = sc / sp
            codes = set(prev_w_nc.keys()) | set(curr_w_nc.keys())
            for c in codes:
                if abs(curr_w_nc.get(c, 0.0) - k * prev_w_nc.get(c, 0.0)) > eps:
                    return False
            return True

        turnover_attrib = None
        hold_smoothing = None
        try:
            strategy_key = meta_base.get("strategy_key")
            prev_sig = _load_prev_signal(strategy_key, str(sealed_date_for_ops)) if strategy_key else None
            if prev_sig is None:
                turnover_attrib = {
                    "prev_signal_id": None,
                    "prev_as_of_date": None,
                    "reason": "no_prev_signal",
                    "entered": [],
                    "exited": [],
                    "kept": [],
                    "turnover_buy": 0.0,
                    "turnover_sell": 0.0,
                    "turnover_2way": 0.0,
                    "turnover_1way": 0.0,
                    "is_new_rebalance": True,
                }
            else:
                prev_w = _weights_from_signal_meta(prev_sig)
                curr_stub = {"positions": positions, "meta": meta_base}
                curr_w = _weights_from_signal_meta(curr_stub)

                prev_asof = prev_sig.get("as_of_date")
                is_new_rebalance = bool(str(prev_asof) != str(as_of_date))

                entered = []
                exited = []
                kept = []

                codes = set(prev_w.keys()) | set(curr_w.keys())
                for code in sorted(codes):
                    wp = float(prev_w.get(code, 0.0))
                    wc = float(curr_w.get(code, 0.0))
                    fb_asset = ((meta_base.get("fallback", {}) or {}).get("asset"))
                    curr_l3 = str(((meta_base.get("fallback", {}) or {}).get("level") or "")) == "L3"
                    prev_l3 = str(((prev_sig.get("meta", {}) or {}).get("fallback", {}) or {}).get("level") or "") == "L3"
                    is_fb_leg = (
                        fb_asset is not None
                        and str(code) == str(fb_asset)
                        and (curr_l3 or prev_l3)
                    )

                    if wp <= 0 and wc > 0:
                        if code.upper() != "CASH":
                            entered.append({"code": code, "new_weight": wc, "reason": "fallback_leg" if is_fb_leg else "rank_gain"})
                    elif wp > 0 and wc <= 0:
                        if code.upper() != "CASH":
                            exited.append({"code": code, "old_weight": wp, "reason": "fallback_leg" if is_fb_leg else "rank_change"})
                    elif wp > 0 and wc > 0:
                        # reason
                        if code.upper() == "CASH":
                            r = "exposure_scale"
                        elif is_new_rebalance:
                            r = "rebalance"
                        else:
                            prev_nc = {k: v for k, v in prev_w.items() if k.upper() != "CASH"}
                            curr_nc = {k: v for k, v in curr_w.items() if k.upper() != "CASH"}
                            r = "exposure_scale" if _is_exposure_scale(prev_nc, curr_nc) else "score_change"
                        kept.append({"code": code, "old_weight": wp, "new_weight": wc, "reason": r})

                diffs = {c: curr_w.get(c, 0.0) - prev_w.get(c, 0.0) for c in codes}
                turnover_buy = float(sum(max(d, 0.0) for d in diffs.values()))
                turnover_sell = float(sum(max(-d, 0.0) for d in diffs.values()))
                turnover_2way = float(0.5 * sum(abs(d) for d in diffs.values()))

                turnover_attrib = {
                    "prev_signal_id": prev_sig.get("signal_id"),
                    "prev_as_of_date": prev_asof,
                    "is_new_rebalance": is_new_rebalance,
                    "entered": entered,
                    "exited": exited,
                    "kept": kept,
                    "turnover_buy": turnover_buy,
                    "turnover_sell": turnover_sell,
                    "turnover_2way": turnover_2way,
                    "turnover_1way": turnover_buy,
                }

            # stale/hold smoothing
            current_picks = set((meta_base.get("picks_base") or [])[: int(top_k)])
            fb_asset = ((meta_base.get("fallback", {}) or {}).get("asset"))
            pos_nc = [p for p in (positions or []) if str(p.get("code", "")).upper() != "CASH"]
            denom = float(sum(float(p.get("weight", 0.0) or 0.0) for p in pos_nc))
            stale = [
                (str(p.get("code")), float(p.get("weight", 0.0) or 0.0))
                for p in pos_nc
                if (str(p.get("code")) not in current_picks) and (fb_asset is None or str(p.get("code")) != str(fb_asset))
            ]
            stale_weight = float(sum(w for _c, w in stale))
            stale_sorted = sorted(stale, key=lambda x: -x[1])[:5]
            hold_smoothing = {
                "stale_weight_ratio": 0.0 if denom <= 0 else float(stale_weight / denom),
                "n_stale_codes": int(len(stale)),
                "stale_top5": [{"code": c, "weight": w} for c, w in stale_sorted],
            }
        except Exception:
            turnover_attrib = None
            hold_smoothing = None

        meta_base["turnover_attrib"] = turnover_attrib
        meta_base["hold_smoothing"] = hold_smoothing

        signal_obj: Dict[str, Any] = {
            "signal_id": signal_id,
            "status": "succeeded",
            "generated_at": int(time.time()),
            "as_of_date": as_of_date,
            "strategy": strategy,
            "rebalance": "weekly",
            "theme": theme,
            "top_k": top_k,
            "positions": positions,
            "meta": {
                "config_signature": config_signature,
                "universe_fingerprint": stats.get("universe_fingerprint"),
                "universe_size": stats.get("universe_size"),
                **meta_base,
            },
        }

        out_json = SIGNALS_DIR / f"{signal_id}.json"
        out_csv = SIGNALS_DIR / f"{signal_id}.csv"
        out_fcsv = SIGNALS_DIR / f"{signal_id}_factors.csv"
        out_json.write_text(json.dumps(signal_obj, indent=2, ensure_ascii=False), encoding="utf-8")
        _write_signal_csv(out_csv, positions)
        _write_factors_csv(out_fcsv, positions)

        status_path.write_text(
            json.dumps({"signal_id": signal_id, "status": "succeeded", "finished_at": int(time.time())}, ensure_ascii=False),
            encoding="utf-8",
        )

    except Exception as e:
        status_path.write_text(
            json.dumps({"signal_id": signal_id, "status": "failed", "finished_at": int(time.time()), "error": repr(e)}, ensure_ascii=False),
            encoding="utf-8",
        )

    finally:
        try:
            job_sem.release()
        except Exception:
            pass
