#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Generate a factor diagnostic report (RankIC/ICIR, decile spread, decay, correlations).

This is the A-step of the optimization framework: *measure factors before tuning weights*.

Universe:
- theme supports curated seeds and special themes (hs10/cyb20/a_ex_kcb_bse).

Data:
- Reads Mongo quantaxis.stock_day (expects fields: code, date, close, amount/vol/money).
- Handles mixed date formats (YYYYMMDD and YYYY-MM-DD).

Outputs:
- output/reports/factor_reports/<run_id>/report.json
- output/reports/factor_reports/<run_id>/report.csv

Metrics:
- Spearman RankIC between factor(t) and forward return over horizons (5d, 10d, 20d).
- ICIR (mean(IC) / std(IC)) by overall + by year.
- Decile spread: mean fwd return of top decile minus bottom decile (per date, then averaged).
- Factor correlations (cross-sectional, averaged over dates).

Note:
- This script is designed to be self-contained and reproducible. It does not alter the DB.
"""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pymongo

ROOT = Path(__file__).resolve().parents[1]
OUT_ROOT = ROOT / "output" / "reports" / "factor_reports"


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
    # Reuse the same theme logic as backtest_baseline.py
    import importlib.util

    p = ROOT / "scripts" / "backtest_baseline.py"
    spec = importlib.util.spec_from_file_location("_qa_bt", str(p))
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore
    return list(mod.load_universe(theme))


def detect_liq_field(coll: pymongo.collection.Collection) -> Optional[str]:
    sample = coll.find_one({}, {"_id": 0, "amount": 1, "money": 1, "vol": 1, "volume": 1})
    if not sample:
        return None
    for k in ["amount", "money", "volume", "vol"]:
        if k in sample and sample.get(k) is not None:
            return k
    return None


def _norm_date_str(x: str) -> str:
    x = str(x)
    if "-" in x:
        return x.replace("-", "")
    return x


def fetch_long_panel(
    coll: pymongo.collection.Collection,
    codes: List[str],
    start: str,
    end: str,
    liq_field: Optional[str],
) -> pd.DataFrame:
    """Fetch (date, code, close, liq) long-form dataframe."""
    start1 = start
    end1 = end
    start2 = start.replace("-", "")
    end2 = end.replace("-", "")

    proj = {"_id": 0, "code": 1, "date": 1, "close": 1}
    if liq_field:
        proj[liq_field] = 1

    cur = coll.find(
        {
            "code": {"$in": codes},
            "$or": [
                {"date": {"$gte": start1, "$lte": end1}},
                {"date": {"$gte": start2, "$lte": end2}},
            ],
        },
        proj,
        no_cursor_timeout=True,
    )

    rows = []
    for r in cur:
        rows.append(r)
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("no data fetched")

    # normalize
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["date"] = pd.to_datetime(df["date"], format="mixed", errors="coerce")
    df = df.dropna(subset=["date", "code", "close"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"]).copy()

    if liq_field and liq_field in df.columns:
        df["liq"] = pd.to_numeric(df[liq_field], errors="coerce").fillna(0.0)
    else:
        df["liq"] = 0.0

    df = df[["date", "code", "close", "liq"]]
    df = df.sort_values(["code", "date"]).drop_duplicates(subset=["code", "date"], keep="last")
    return df


def add_factors(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("code", sort=False)
    df = df.copy()
    ret = g["close"].pct_change(fill_method=None)

    # factors at t (computed using history up to t)
    df["ret_10d"] = g["close"].pct_change(10, fill_method=None)
    df["ret_20d"] = g["close"].pct_change(20, fill_method=None)
    df["vol_20d"] = ret.rolling(20).std().reset_index(level=0, drop=True)
    df["liq_20d"] = g["liq"].rolling(20).mean().reset_index(level=0, drop=True)

    return df


def add_forward_returns(df: pd.DataFrame, horizons: List[int]) -> pd.DataFrame:
    g = df.groupby("code", sort=False)
    df = df.copy()
    for h in horizons:
        df[f"fwd_{h}d"] = g["close"].shift(-h) / df["close"] - 1.0
    return df


def _rankic_for_date(sub: pd.DataFrame, fac: str, target: str) -> float:
    x = sub[fac]
    y = sub[target]
    m = x.notna() & y.notna()
    if m.sum() < 30:
        return np.nan
    xr = x[m].rank(method="average")
    yr = y[m].rank(method="average")
    return float(xr.corr(yr))


def compute_rankic_series(df: pd.DataFrame, factors: List[str], targets: List[str]) -> pd.DataFrame:
    out_rows = []
    for (d, sub) in df.groupby("date", sort=True):
        row = {"date": d}
        for fac in factors:
            for tgt in targets:
                row[f"ic_{fac}__{tgt}"] = _rankic_for_date(sub, fac, tgt)
        out_rows.append(row)
    out = pd.DataFrame(out_rows).set_index("date").sort_index()
    return out


def decile_spread(df: pd.DataFrame, fac: str, tgt: str, q: int = 10) -> pd.Series:
    def _one(sub: pd.DataFrame) -> float:
        x = sub[fac]
        y = sub[tgt]
        m = x.notna() & y.notna()
        if m.sum() < 200:
            return np.nan
        sub2 = sub.loc[m, [fac, tgt]].copy()
        # rank into deciles
        sub2["bin"] = pd.qcut(sub2[fac].rank(method="first"), q, labels=False, duplicates="drop")
        top = sub2[sub2["bin"] == sub2["bin"].max()][tgt].mean()
        bot = sub2[sub2["bin"] == sub2["bin"].min()][tgt].mean()
        return float(top - bot)

    return df.groupby("date", sort=True).apply(_one)


def summarize_ic(ic: pd.Series) -> Dict[str, float]:
    ic = ic.dropna()
    if ic.empty:
        return {"mean": float("nan"), "std": float("nan"), "icir": float("nan"), "n": 0}
    mu = float(ic.mean())
    sd = float(ic.std(ddof=1))
    icir = float(mu / sd) if sd > 1e-12 else float("nan")
    return {"mean": mu, "std": sd, "icir": icir, "n": int(ic.shape[0])}


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--theme", default="a_ex_kcb_bse")
    ap.add_argument("--start", default="2019-01-01")
    ap.add_argument("--end", default="2026-02-04")
    ap.add_argument("--min-bars", type=int, default=800)
    ap.add_argument("--liq-window", type=int, default=20)
    ap.add_argument("--liq-min-ratio", type=float, default=1.0)
    ap.add_argument("--max-codes", type=int, default=0, help="0 = all")
    ap.add_argument("--outdir", default=None)
    args = ap.parse_args(argv)

    run_id = uuid.uuid4().hex[:12]
    outdir = Path(args.outdir) if args.outdir else (OUT_ROOT / run_id)
    outdir.mkdir(parents=True, exist_ok=True)

    theme = str(args.theme)
    codes = load_universe(theme)
    if args.max_codes and int(args.max_codes) > 0:
        codes = codes[: int(args.max_codes)]

    cfg = get_mongo_cfg()
    client = mongo_client(cfg)
    db = client[cfg.db]
    coll = db["stock_day"]

    liq_field = detect_liq_field(coll)

    df = fetch_long_panel(coll, codes, str(args.start), str(args.end), liq_field)

    # eligibility filters
    bars = df.groupby("code")["close"].count()
    eligible = bars[bars >= int(args.min_bars)].index
    df = df[df["code"].isin(eligible)].copy()

    # liquidity recent window filter (optional)
    win = int(args.liq_window)
    ratio = max(0.0, min(1.0, float(args.liq_min_ratio)))
    if win > 0:
        # compute last win rows per code up to end
        df["_rn"] = df.groupby("code").cumcount()
        # pick last date per code to get window slice via tail
        def _ok(sub: pd.DataFrame) -> bool:
            sub = sub.tail(win)
            close_ok = int(sub["close"].notna().sum()) >= int(win * ratio)
            liq_ok = int((sub["liq"].fillna(0.0) > 0).sum()) >= int(win * ratio)
            return bool(close_ok and liq_ok)

        ok_codes = [c for c, sub in df.groupby("code", sort=False) if _ok(sub)]
        df = df[df["code"].isin(ok_codes)].copy()

    # compute factors + fwd returns
    horizons = [5, 10, 20]
    df = add_factors(df)
    df = add_forward_returns(df, horizons)

    factors = ["ret_10d", "ret_20d", "vol_20d", "liq_20d"]
    targets = [f"fwd_{h}d" for h in horizons]

    # keep rows where at least one factor + one target exists
    df = df.dropna(subset=["close"])  # already

    ic_df = compute_rankic_series(df, factors, targets)

    # summary
    summary = {
        "run_id": run_id,
        "generated_at": int(time.time()),
        "theme": theme,
        "start": str(args.start),
        "end": str(args.end),
        "min_bars": int(args.min_bars),
        "liq_window": int(args.liq_window),
        "liq_min_ratio": float(args.liq_min_ratio),
        "universe_size_raw": int(len(codes)),
        "universe_size_eligible": int(df["code"].nunique()),
        "liq_field_detected": liq_field,
        "metrics": {},
    }

    metrics_rows = []

    # overall + by year
    for fac in factors:
        for tgt in targets:
            s = ic_df[f"ic_{fac}__{tgt}"]
            key = f"{fac}__{tgt}"
            overall = summarize_ic(s)
            summary["metrics"][key] = {"overall": overall, "by_year": {}}

            # by year
            for y, ss in s.groupby(s.index.year):
                summary["metrics"][key]["by_year"][str(int(y))] = summarize_ic(ss)

            metrics_rows.append(
                {
                    "factor": fac,
                    "target": tgt,
                    **{f"overall_{k}": v for k, v in overall.items()},
                }
            )

    # decile spreads (top-bottom)
    spreads = {}
    for fac in factors:
        for tgt in targets:
            sp = decile_spread(df, fac, tgt)
            spreads[f"spread_{fac}__{tgt}"] = summarize_ic(sp)  # treat like a time series
    summary["decile_spreads"] = spreads

    # factor correlations (average cross-sectional corr per date)
    corr_rows = []
    for d, sub in df.groupby("date"):
        sub2 = sub[["ret_10d", "ret_20d", "vol_20d", "liq_20d"]].copy()
        if sub2.dropna().shape[0] < 200:
            continue
        corr = sub2.corr(method="spearman")
        corr["date"] = d
        corr_rows.append(corr)
    if corr_rows:
        # average matrix
        mats = [c.drop(columns=["date"]) if "date" in c.columns else c for c in corr_rows]
        summary["factor_corr_spearman_avg"] = pd.concat(mats).groupby(level=0).mean().to_dict()

    # write outputs
    (outdir / "report.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    flat = pd.DataFrame(metrics_rows)
    flat.to_csv(outdir / "report.csv", index=False)

    # store ic timeseries too
    ic_df.to_csv(outdir / "ic_timeseries.csv")

    print(json.dumps({"run_id": run_id, "outdir": str(outdir), "eligible": int(df["code"].nunique())}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
