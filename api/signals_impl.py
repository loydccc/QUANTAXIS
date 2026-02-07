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

    # Execution realism (optional)
    execution_mode = str(cfg.get("execution_mode", "naive"))  # naive|realistic
    backup_k = int(cfg.get("backup_k", 150))
    limit_tiering = bool(cfg.get("limit_tiering", True))
    limit_pct = float(cfg.get("limit_pct", 0.10))
    limit_price_eps_bps = float(cfg.get("limit_price_eps_bps", 5.0))
    limit_touch_mode = str(cfg.get("limit_touch_mode", "hl"))  # hl|close
    limit_touch_eps = float(cfg.get("limit_touch_eps", 1e-6))

    if not isinstance(strategy, str) or not _baseline_strategy_re.match(strategy):
        raise HTTPException(status_code=400, detail="bad strategy")
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

    if execution_mode not in {"naive", "realistic"}:
        raise HTTPException(status_code=400, detail="bad execution_mode (naive|realistic)")
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
    vals = [v for v in s.values() if v is not None]
    if not vals:
        return {k: 0.0 for k in s.keys()}
    import math

    m = sum(vals) / len(vals)
    var = sum((v - m) ** 2 for v in vals) / max(1, (len(vals) - 1))
    sd = math.sqrt(var) if var > 0 else 0.0
    if sd <= 0:
        return {k: 0.0 for k in s.keys()}
    return {k: (v - m) / sd for k, v in s.items()}


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
    hold_weeks = int(cfg.get("hold_weeks", 2))
    tranche_overlap = bool(cfg.get("tranche_overlap", True))

    # Execution realism
    execution_mode = str(cfg.get("execution_mode", "naive"))
    backup_k = int(cfg.get("backup_k", 150))
    limit_tiering = bool(cfg.get("limit_tiering", True))
    limit_pct = float(cfg.get("limit_pct", 0.10))
    limit_price_eps_bps = float(cfg.get("limit_price_eps_bps", 5.0))
    limit_touch_mode = str(cfg.get("limit_touch_mode", "hl"))
    limit_touch_eps = float(cfg.get("limit_touch_eps", 1e-6))

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
                picks_alloc = picks[:k_alloc]

                # Execution realism (BUY-side): block new buys at up-limit and fill with backups
                if execution_mode == "realistic" and backups:
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

                tranche_port = {c: (1.0 / len(picks_alloc) if picks_alloc else 0.0) for c in picks_alloc}

                contrib: Dict[str, float] = {}
                for c, w in tranche_port.items():
                    dw = scale * w
                    final_port[c] = final_port.get(c, 0.0) + dw
                    contrib[c] = contrib.get(c, 0.0) + dw
                    final_scores[c] = max(final_scores.get(c, 0.0), scores.get(c, 0.0))

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

        # --- Health Index v1 integration (ONLY affects overall exposure via cash) ---
        # Does NOT change selection, ranking, factor computation, ladder thresholds, or weights' relative structure.
        health_score, health_path = _load_health_score(as_of_date)
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
        meta_base = {
            "strategy": strategy,
            "theme": theme,
            "top_k": top_k,
            "rebalance": "weekly",
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
            "execution_mode": execution_mode,
            "backup_k": backup_k,
            "limit_tiering": limit_tiering,
            "limit_pct": limit_pct,
            "limit_price_eps_bps": limit_price_eps_bps,
            "limit_touch_mode": limit_touch_mode,
            "limit_touch_eps": limit_touch_eps,
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
            line = f"{as_of_date},{'' if health_score is None else health_score},{exposure},{fallback_level}\n"
            if not log_path.exists():
                log_path.write_text("date,health_score,exposure,level_used\n" + line, encoding="utf-8")
            else:
                with log_path.open("a", encoding="utf-8") as f:
                    f.write(line)
        except Exception:
            pass

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
