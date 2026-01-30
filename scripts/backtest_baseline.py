#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Baseline backtests (mid/low-frequency) for the CN theme seed universe.

Supports strategies:
1) xsec_momentum_weekly_topk: cross-sectional momentum, weekly rebalance, long-only top K equal-weight.
2) xsec_momentum_weekly_invvol: momentum top K, weighted by inverse trailing vol (risk-aware), with max weight cap.
3) ts_ma_weekly: time-series MA filter per-asset, weekly rebalance, long-only.

Data source:
- MongoDB collection: stock_day
- Fields: code, date (YYYY-MM-DD), close

Outputs (written to outdir):
- metrics.json
- equity.csv
- positions.csv

Notes:
- Uses close-to-close returns.
- Rebalance on last available trading day of each ISO week.
- Signals use only information up to rebalance date.
- Positions are applied starting next trading day (T+1) to avoid look-ahead.
- Simple linear cost model: cost_bps * turnover (one-way) applied on rebalance-effective days.
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


@dataclass
class MongoCfg:
    host: str
    port: int
    db: str
    user: str
    password: str
    root_user: str
    root_password: str


def get_mongo_cfg() -> MongoCfg:
    return MongoCfg(
        host=os.getenv("MONGODB_HOST", "mongodb"),
        port=int(os.getenv("MONGODB_PORT", "27017")),
        db=os.getenv("MONGODB_DATABASE", "quantaxis"),
        user=os.getenv("MONGODB_USER", "quantaxis"),
        password=os.getenv("MONGODB_PASSWORD", "quantaxis"),
        root_user=os.getenv("MONGO_ROOT_USER", "root"),
        root_password=os.getenv("MONGO_ROOT_PASSWORD", "root"),
    )


def mongo_client(cfg: MongoCfg) -> pymongo.MongoClient:
    uris = [
        f"mongodb://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.db}?authSource=admin",
        f"mongodb://{cfg.root_user}:{cfg.root_password}@{cfg.host}:{cfg.port}/{cfg.db}?authSource=admin",
    ]
    last = None
    for uri in uris:
        try:
            c = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)
            c.admin.command("ping")
            return c
        except Exception as e:
            last = e
    raise last  # type: ignore[misc]


def load_universe(theme: str) -> List[str]:
    obj = json.loads(Path("watchlists/themes_seed_cn.json").read_text(encoding="utf-8"))
    codes = set()
    for t in obj["themes"]:
        if theme == "all" or t["theme"] == theme:
            for c in t["seed_codes"]:
                codes.add(str(c).zfill(6))
    return sorted(codes)


def fetch_close_panel(
    coll: pymongo.collection.Collection,
    codes: List[str],
    start: str,
    end: str,
) -> pd.DataFrame:
    series = {}
    for code in codes:
        cursor = coll.find(
            {"code": code, "date": {"$gte": start, "$lte": end}},
            {"_id": 0, "date": 1, "close": 1},
        ).sort("date", 1)
        rows = list(cursor)
        if not rows:
            continue
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"])
        df = df.dropna(subset=["close"]).drop_duplicates(subset=["date"]).set_index("date")
        series[code] = df["close"].astype(float)

    if not series:
        raise RuntimeError("no data found for selected universe")

    panel = pd.concat(series, axis=1).sort_index()
    return panel


def pick_weekly_rebalance_dates(index: pd.DatetimeIndex) -> List[pd.Timestamp]:
    iso = index.isocalendar()
    df = pd.DataFrame({"year": iso.year.values, "week": iso.week.values}, index=index)
    last_dates = df.groupby(["year", "week"], sort=True).apply(lambda x: x.index.max(), include_groups=False)
    return list(pd.to_datetime(last_dates.values))


def compute_weights_xsec_mom(
    close: pd.DataFrame,
    rebalance_dates: List[pd.Timestamp],
    lookback: int,
    top_k: int,
) -> pd.DataFrame:
    weights = pd.DataFrame(index=close.index, columns=close.columns, dtype=float)

    for d in rebalance_dates:
        if d not in close.index:
            continue
        loc = close.index.get_loc(d)
        if isinstance(loc, slice):
            loc = loc.stop - 1
        if loc < lookback:
            continue

        window = close.iloc[loc - lookback : loc + 1]
        mom = window.iloc[-1] / window.iloc[0] - 1.0
        mom = mom.dropna()
        if mom.empty:
            continue
        winners = mom.sort_values(ascending=False).head(top_k).index
        w = pd.Series(0.0, index=close.columns)
        w.loc[winners] = 1.0 / len(winners)
        weights.loc[d] = w

    return weights


def _cap_and_normalize(w: pd.Series, max_weight: float) -> pd.Series:
    w = w.clip(lower=0.0)
    if w.sum() <= 0:
        return w * 0.0
    w = w / w.sum()
    if max_weight is None or max_weight <= 0 or max_weight >= 1:
        return w

    w = w.copy()
    for _ in range(10):
        over = w > max_weight
        if not over.any():
            break
        excess = (w[over] - max_weight).sum()
        w[over] = max_weight
        under = w < max_weight
        if under.sum() == 0:
            break
        w[under] = w[under] + excess * (w[under] / w[under].sum())
    if w.sum() > 0:
        w = w / w.sum()
    return w


def compute_weights_xsec_mom_invvol(
    close: pd.DataFrame,
    rebalance_dates: List[pd.Timestamp],
    lookback: int,
    top_k: int,
    vol_window: int,
    max_weight: float,
) -> pd.DataFrame:
    # Pick winners by momentum; weight by inverse vol.
    weights = pd.DataFrame(index=close.index, columns=close.columns, dtype=float)
    ret = close.pct_change(fill_method=None)
    vol = ret.rolling(vol_window).std()

    for d in rebalance_dates:
        if d not in close.index:
            continue
        loc = close.index.get_loc(d)
        if isinstance(loc, slice):
            loc = loc.stop - 1
        if loc < max(lookback, vol_window):
            continue

        window = close.iloc[loc - lookback : loc + 1]
        mom = window.iloc[-1] / window.iloc[0] - 1.0
        mom = mom.dropna()
        if mom.empty:
            continue
        winners = mom.sort_values(ascending=False).head(top_k).index

        inv = (1.0 / (vol.loc[d, winners].replace(0.0, np.nan))).replace([np.inf, -np.inf], np.nan).dropna()
        if inv.empty:
            w = pd.Series(0.0, index=close.columns)
        else:
            w_sub = _cap_and_normalize(inv, max_weight=max_weight)
            w = pd.Series(0.0, index=close.columns)
            w.loc[w_sub.index] = w_sub.values
        weights.loc[d] = w

    return weights


def compute_weights_ts_ma(
    close: pd.DataFrame,
    rebalance_dates: List[pd.Timestamp],
    ma_window: int,
) -> pd.DataFrame:
    weights = pd.DataFrame(index=close.index, columns=close.columns, dtype=float)

    ma = close.rolling(ma_window).mean()

    for d in rebalance_dates:
        if d not in close.index:
            continue
        # signal at date d, effective next day
        sig = (close.loc[d] > ma.loc[d]).astype(float)
        sig = sig.replace([np.inf, -np.inf], np.nan).fillna(0.0)
        if sig.sum() <= 0:
            w = pd.Series(0.0, index=close.columns)
        else:
            w = sig / sig.sum()
        weights.loc[d] = w

    return weights


def backtest_close_to_close(
    close: pd.DataFrame,
    weights_on_rebalance: pd.DataFrame,
    cost_bps: float,
) -> Tuple[pd.Series, pd.DataFrame, pd.Series, pd.Series]:
    w = weights_on_rebalance.reindex(close.index).ffill().fillna(0.0)
    w_eff = w.shift(1).fillna(0.0)

    daily_ret = close.pct_change(fill_method=None).fillna(0.0)
    gross = (w_eff * daily_ret).sum(axis=1)

    turnover = w_eff.diff().abs().sum(axis=1) / 2.0
    cost = (cost_bps / 10000.0) * turnover
    net = gross - cost

    equity = (1.0 + net).cumprod()
    return equity, w_eff, turnover, net


def perf_stats(equity: pd.Series, net_ret: pd.Series, turnover: pd.Series) -> Dict:
    n = len(net_ret)
    ann = 252
    cagr = float(equity.iloc[-1] ** (ann / max(n, 1)) - 1.0) if n > 1 else 0.0
    vol = float(net_ret.std() * np.sqrt(ann))
    sharpe = float((net_ret.mean() * ann) / (net_ret.std() * np.sqrt(ann) + 1e-12))
    peak = equity.cummax()
    dd = equity / peak - 1.0
    max_dd = float(dd.min())

    avg_turnover = float(turnover.mean())
    turnover_annual = float(turnover.sum() / (n / ann)) if n > 0 else 0.0

    return {
        "bars": int(n),
        "cagr": cagr,
        "vol": vol,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "final_equity": float(equity.iloc[-1]),
        "avg_daily_turnover": avg_turnover,
        "annual_turnover": turnover_annual,
    }


def norm_date(s: str) -> str:
    s = s.strip()
    if "-" in s:
        return s
    if len(s) == 8:
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    raise ValueError(f"bad date: {s}")


def universe_fingerprint(codes: List[str], cfg: Dict) -> str:
    payload = {
        "codes": codes,
        "cfg": cfg,
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def audit_from_close_panel(close: pd.DataFrame) -> Dict:
    # basic coverage diagnostics
    bars_per_code = close.notna().sum(axis=0).astype(int)
    return {
        "universe_size_raw": int(close.shape[1]),
        "bars_min": int(bars_per_code.min()) if len(bars_per_code) else 0,
        "bars_max": int(bars_per_code.max()) if len(bars_per_code) else 0,
        "panel_missing_ratio": float(close.isna().sum().sum() / max(close.size, 1)),
    }


def write_outputs(outdir: Path, equity: pd.Series, positions: pd.DataFrame, stats: Dict) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "metrics.json").write_text(json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8")
    pd.DataFrame({"date": equity.index.strftime("%Y-%m-%d"), "equity": equity.values}).to_csv(
        outdir / "equity.csv", index=False
    )
    pos = positions.copy()
    pos.insert(0, "date", pos.index.strftime("%Y-%m-%d"))
    pos.to_csv(outdir / "positions.csv", index=False)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--theme", default="all")
    ap.add_argument(
        "--strategy",
        default="xsec_momentum_weekly_topk",
        choices=["xsec_momentum_weekly_topk", "xsec_momentum_weekly_invvol", "ts_ma_weekly"],
    )
    ap.add_argument("--lookback", type=int, default=60)
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--ma", type=int, default=60)
    ap.add_argument("--vol-window", type=int, default=20, help="trailing trading days for vol weighting")
    ap.add_argument("--max-weight", type=float, default=0.10, help="max single-asset weight cap")
    ap.add_argument("--cost-bps", type=float, default=10.0)
    ap.add_argument("--min-bars", type=int, default=252, help="min non-NaN close bars required per code")
    ap.add_argument("--outdir", default="/tmp/output")
    args = ap.parse_args(argv)

    start = norm_date(args.start)
    end = norm_date(args.end)

    codes = load_universe(args.theme)
    cfg = get_mongo_cfg()
    client = mongo_client(cfg)
    db = client[cfg.db]
    coll = db["stock_day"]

    close_raw = fetch_close_panel(coll, codes, start, end).sort_index()

    # Universe eligibility: require enough bars to support indicators/weekly rebalance.
    bars_per_code = close_raw.notna().sum(axis=0).astype(int)
    eligible = bars_per_code[bars_per_code >= int(args.min_bars)].index.tolist()
    dropped = sorted(set(close_raw.columns) - set(eligible))

    close = close_raw[eligible]

    if close.shape[1] == 0:
        raise RuntimeError(f"no eligible codes after min-bars filter={args.min_bars}")

    reb_dates = pick_weekly_rebalance_dates(close.index)

    if args.strategy == "xsec_momentum_weekly_topk":
        weights = compute_weights_xsec_mom(close, reb_dates, lookback=args.lookback, top_k=args.top)
    elif args.strategy == "xsec_momentum_weekly_invvol":
        weights = compute_weights_xsec_mom_invvol(
            close,
            reb_dates,
            lookback=args.lookback,
            top_k=args.top,
            vol_window=args.vol_window,
            max_weight=args.max_weight,
        )
    else:
        weights = compute_weights_ts_ma(close, reb_dates, ma_window=args.ma)

    equity, positions, turnover, net_ret = backtest_close_to_close(close, weights, cost_bps=args.cost_bps)

    stats = perf_stats(equity, net_ret, turnover)
    run_cfg = {
        "strategy": args.strategy,
        "theme": args.theme,
        "start": start,
        "end": end,
        "min_bars": int(args.min_bars),
        "cost_bps": float(args.cost_bps),
        "params": {
            "lookback": int(args.lookback),
            "top": int(args.top),
            "ma": int(args.ma),
            "vol_window": int(args.vol_window),
            "max_weight": float(args.max_weight),
        },
        "data": {"collection": "stock_day", "price": "close", "adjustment": "none"},
    }

    stats.update(
        {
            **run_cfg,
            "universe_size": int(close.shape[1]),
            "universe_size_raw": int(close_raw.shape[1]),
            "universe_dropped": dropped,
            "universe_fingerprint": universe_fingerprint(sorted(close.columns.tolist()), run_cfg),
            "data_audit": audit_from_close_panel(close),
            "start_effective": str(close.index.min().date()),
            "end_effective": str(close.index.max().date()),
            "generated_at": int(time.time()),
        }
    )

    outdir = Path(args.outdir)
    write_outputs(outdir, equity, positions, stats)
    print(json.dumps(stats, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
