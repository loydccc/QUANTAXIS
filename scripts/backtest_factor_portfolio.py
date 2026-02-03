#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Backtest a simple factor-based cross-sectional portfolio.

Inputs:
- factor parquet (long): date, code, factor columns (either raw or zscored)
- close panel from Mongo stock_day

Logic (MVP):
- Rebalance on daily/weekly/monthly dates
- Select topK by factor (descending) OR top quantile
- Equal-weight the selected names
- Close-to-close returns with T+1 execution (weights shift by 1 day)
- Transaction cost: cost_bps * turnover

Outputs:
- metrics.json
- equity.csv
- positions.csv

Notes:
- This is an MVP for productization; future enhancements can add constraints,
  vol targeting, cash, limit-up/down handling, suspensions, etc.
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


def universe_fingerprint(codes: List[str]) -> str:
    s = "\n".join(sorted(codes)).encode("utf-8")
    return hashlib.sha256(s).hexdigest()


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


def norm_date(s: str) -> str:
    s = s.strip()
    if "-" in s:
        return s
    if len(s) == 8:
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    raise ValueError(s)


def load_universe(theme: str) -> List[str]:
    """Return the base universe by theme.

    Supports special themes derived from Mongo:
    - hs10: 沪深主板 10%（排除创业板/科创板/北交所/新三板）
    - cyb20: 创业板 20%（300/301）
    - a_ex_kcb_bse: 沪深主板 + 创业板（仅排除科创板 688 与北交所/新三板）
    """

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

    if theme in {"hs10", "cn_hs10", "a_hs10"} or theme in {"cyb20", "cn_cyb20", "a_cyb20"} or theme in {"a_ex_kcb_bse", "cn_a_ex_kcb_bse", "a_no_kcb_bse"}:
        cfg = get_mongo_cfg()
        client = mongo_client(cfg)
        db = client[cfg.db]
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
        if theme.startswith("hs"):
            return sorted([c for c in codes if _is_hs10(c)])
        if theme.startswith("cy"):
            return sorted([c for c in codes if _is_cyb20(c)])
        return sorted([c for c in codes if _is_a_ex_kcb_bse(c)])

    obj = json.loads(Path("watchlists/themes_seed_cn.json").read_text(encoding="utf-8"))
    codes = set()
    for t in obj["themes"]:
        if theme == "all" or t["theme"] == theme:
            for c in t["seed_codes"]:
                codes.add(str(c).zfill(6))
    return sorted(codes)


def fetch_close_panel(coll, codes: List[str], start: str, end: str) -> pd.DataFrame:
    series = {}
    for code in codes:
        cur = coll.find(
            {"code": code, "date": {"$gte": start, "$lte": end}},
            {"_id": 0, "date": 1, "close": 1},
        ).sort("date", 1)
        rows = list(cur)
        if not rows:
            continue
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"])
        df = df.dropna(subset=["close"]).drop_duplicates(subset=["date"]).set_index("date")
        series[code] = df["close"].astype(float)
    if not series:
        raise RuntimeError("no data")
    return pd.concat(series, axis=1).sort_index()


def rebalance_dates(index: pd.DatetimeIndex, freq: str) -> List[pd.Timestamp]:
    dates = list(index)
    if freq == "daily":
        return dates
    di = pd.DatetimeIndex(dates)
    if freq == "weekly":
        return di.to_series(index=di).groupby(di.to_period("W-FRI")).max().sort_values().tolist()
    if freq == "monthly":
        return di.to_series(index=di).groupby(di.to_period("M")).max().sort_values().tolist()
    raise ValueError(freq)


def backtest_close_to_close(
    close: pd.DataFrame,
    weights_on_rebalance: pd.DataFrame,
    cost_bps: float,
) -> Tuple[pd.Series, pd.DataFrame, pd.Series, pd.Series]:
    # weights set on rebalance dates; forward-fill; then shift for T+1 execution
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


def compute_weights(
    fac_long: pd.DataFrame,
    close: pd.DataFrame,
    reb_dates: List[pd.Timestamp],
    factor: str,
    topk: int,
    quantile: Optional[float],
    direction: str,
) -> pd.DataFrame:
    weights = pd.DataFrame(index=close.index, columns=close.columns, dtype=float)

    for d in reb_dates:
        if d not in close.index:
            continue
        try:
            x = fac_long.xs(d, level=0)[factor]
        except Exception:
            continue

        x = x.replace([np.inf, -np.inf], np.nan).dropna()
        if x.empty:
            continue

        asc = True if direction == "long_low" else False

        if quantile is not None:
            # quantile: for long_high use top tail, for long_low use bottom tail
            if direction == "long_low":
                thr = x.quantile(1.0 - quantile)
                sel = x[x <= thr].sort_values(ascending=asc).index
            else:
                thr = x.quantile(quantile)
                sel = x[x >= thr].sort_values(ascending=asc).index
        else:
            sel = x.sort_values(ascending=asc).head(topk).index

        if len(sel) == 0:
            continue

        w = pd.Series(0.0, index=close.columns)
        w.loc[sel] = 1.0 / len(sel)
        weights.loc[d] = w

    return weights


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--theme", default="all")
    ap.add_argument("--factor-parquet", required=True)
    ap.add_argument("--factor", required=True)
    ap.add_argument("--rebalance", choices=["daily", "weekly", "monthly"], default="weekly")
    ap.add_argument("--direction", choices=["long_high", "long_low"], default="long_high")
    ap.add_argument("--topk", type=int, default=10)
    ap.add_argument("--quantile", type=float, default=None, help="if set (0-1), select tail quantile instead of topk")
    ap.add_argument("--cost-bps", type=float, default=10.0)
    ap.add_argument("--outdir", default="/tmp/output")
    args = ap.parse_args(argv)

    start = norm_date(args.start)
    end = norm_date(args.end)

    fac = pd.read_parquet(args.factor_parquet)
    fac["date"] = pd.to_datetime(fac["date"])
    fac_long = fac.set_index(["date", "code"]).sort_index()

    cfg = get_mongo_cfg()
    client = mongo_client(cfg)
    coll = client[cfg.db]["stock_day"]

    codes = load_universe(args.theme)
    close = fetch_close_panel(coll, codes, start, end)

    reb_dates = rebalance_dates(close.index, args.rebalance)
    w_on = compute_weights(
        fac_long,
        close,
        reb_dates,
        factor=args.factor,
        topk=int(args.topk),
        quantile=args.quantile,
        direction=args.direction,
    )

    equity, w_eff, turnover, net = backtest_close_to_close(close, w_on, cost_bps=float(args.cost_bps))
    stats = perf_stats(equity, net, turnover)

    metrics = {
        **stats,
        "strategy": "factor_portfolio",
        "theme": args.theme,
        "start": start,
        "end": end,
        "start_effective": str(equity.index.min().date()),
        "end_effective": str(equity.index.max().date()),
        "rebalance": args.rebalance,
        "factor": args.factor,
        "direction": args.direction,
        "topk": int(args.topk),
        "quantile": args.quantile,
        "cost_bps": float(args.cost_bps),
        "data": {"collection": "stock_day", "price": "close", "adjustment": "none"},
        "generated_at": int(time.time()),
        "universe_size": int(close.shape[1]),
        "universe_fingerprint": universe_fingerprint(codes),
    }

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "metrics.json").write_text(json.dumps(metrics, indent=2, ensure_ascii=False), encoding="utf-8")

    pd.DataFrame({"date": equity.index, "equity": equity.values}).to_csv(outdir / "equity.csv", index=False)
    w_eff.reset_index().rename(columns={"index": "date"}).to_csv(outdir / "positions.csv", index=False)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
