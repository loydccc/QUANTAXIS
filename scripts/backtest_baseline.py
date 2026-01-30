#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Baseline backtest (mid/low-frequency) for the CN theme seed universe.

Strategy v1: cross-sectional momentum (weekly rebalance, long-only top K).

Data source:
- MongoDB collection: stock_day
- Fields: code, date (YYYY-MM-DD), close (and optionally open/high/low/vol/amount)

Outputs:
- /tmp/output/baseline_metrics.json
- /tmp/output/baseline_equity.csv
- /tmp/output/baseline_positions.csv

Notes:
- Uses close-to-close returns.
- Rebalance on last available trading day of each ISO week.
- Signal uses trailing lookback trading days ending at rebalance date.
- Positions are applied starting next trading day (T+1) to avoid look-ahead.
- Simple linear cost model: cost_bps * turnover (one-way) applied on rebalance days.
"""

from __future__ import annotations

import argparse
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
    # Panel: index=date, columns=code, values=close
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
    # forward-fill within reasonable gaps (suspensions) could be handled later; for now keep NaNs.
    return panel


def pick_weekly_rebalance_dates(index: pd.DatetimeIndex) -> List[pd.Timestamp]:
    # Last trading day of each ISO week
    # Use ISO year/week; pick last trading day within each group.
    iso = index.isocalendar()
    df = pd.DataFrame({"year": iso.year.values, "week": iso.week.values}, index=index)
    last_dates = df.groupby(["year", "week"], sort=True).apply(lambda x: x.index.max(), include_groups=False)
    return list(pd.to_datetime(last_dates.values))


def compute_momentum_weights(
    close: pd.DataFrame,
    rebalance_dates: List[pd.Timestamp],
    lookback: int,
    top_k: int,
) -> pd.DataFrame:
    # weights on rebalance date (effective next trading day)
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


def backtest_close_to_close(
    close: pd.DataFrame,
    weights_on_rebalance: pd.DataFrame,
    rebalance_dates: List[pd.Timestamp],
    cost_bps: float,
) -> Tuple[pd.Series, pd.DataFrame, pd.Series]:
    # Apply T+1: shift weights by 1 trading day
    w = weights_on_rebalance.reindex(close.index).ffill().fillna(0.0)
    w_eff = w.shift(1).fillna(0.0)

    daily_ret = close.pct_change(fill_method=None).fillna(0.0)
    gross = (w_eff * daily_ret).sum(axis=1)

    # Turnover computed on effective weights changes (on rebalance effective day)
    turnover = w_eff.diff().abs().sum(axis=1) / 2.0
    # Apply cost on days when weights change (turnover>0)
    cost = (cost_bps / 10000.0) * turnover
    net = gross - cost

    equity = (1.0 + net).cumprod()

    # positions dataframe for output
    positions = w_eff.copy()
    return equity, positions, turnover


def perf_stats(equity: pd.Series, turnover: pd.Series) -> Dict:
    ret = equity.pct_change().fillna(0.0)
    n = len(ret)
    ann = 252
    cagr = float(equity.iloc[-1] ** (ann / max(n, 1)) - 1.0) if n > 1 else 0.0
    vol = float(ret.std() * np.sqrt(ann))
    sharpe = float((ret.mean() * ann) / (ret.std() * np.sqrt(ann) + 1e-12))
    peak = equity.cummax()
    dd = equity / peak - 1.0
    max_dd = float(dd.min())

    # turnover stats
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


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYYMMDD or YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYYMMDD or YYYY-MM-DD")
    ap.add_argument("--theme", default="all", help="theme name from themes_seed_cn.json or 'all'")
    ap.add_argument("--lookback", type=int, default=60, help="lookback trading days for momentum")
    ap.add_argument("--top", type=int, default=10, help="top K winners")
    ap.add_argument("--cost-bps", type=float, default=10.0, help="one-way turnover cost in bps")
    ap.add_argument("--outdir", default="/tmp/output", help="output directory")
    args = ap.parse_args(argv)

    def norm_date(s: str) -> str:
        s = s.strip()
        if "-" in s:
            return s
        if len(s) == 8:
            return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
        raise ValueError(f"bad date: {s}")

    start = norm_date(args.start)
    end = norm_date(args.end)

    codes = load_universe(args.theme)
    cfg = get_mongo_cfg()
    client = mongo_client(cfg)
    db = client[cfg.db]
    coll = db["stock_day"]

    close = fetch_close_panel(coll, codes, start, end)
    close = close.sort_index()

    reb_dates = pick_weekly_rebalance_dates(close.index)

    weights = compute_momentum_weights(close, reb_dates, lookback=args.lookback, top_k=args.top)
    equity, positions, turnover = backtest_close_to_close(close, weights, reb_dates, cost_bps=args.cost_bps)

    stats = perf_stats(equity, turnover)
    stats.update(
        {
            "strategy": "xsec_momentum_weekly_topk",
            "theme": args.theme,
            "universe_size": int(close.shape[1]),
            "start": str(close.index.min().date()),
            "end": str(close.index.max().date()),
            "lookback": args.lookback,
            "top": args.top,
            "cost_bps": args.cost_bps,
            "generated_at": int(time.time()),
        }
    )

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    (outdir / "baseline_metrics.json").write_text(json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8")

    equity_df = pd.DataFrame({"date": equity.index.strftime("%Y-%m-%d"), "equity": equity.values})
    equity_df.to_csv(outdir / "baseline_equity.csv", index=False)

    pos_df = positions.copy()
    pos_df.insert(0, "date", pos_df.index.strftime("%Y-%m-%d"))
    pos_df.to_csv(outdir / "baseline_positions.csv", index=False)

    print(json.dumps(stats, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
