#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Fill missing mv_day coverage by querying Tushare forwarder day-by-day.

Rationale: some forwarders truncate/omit results for wide date ranges.
We therefore:
1) derive the trading-day list from stock_day (local truth)
2) query pro.daily_basic for each trade_date (start_date=end_date)
3) upsert into mv_day
4) print per-day coverage stats

Env:
- TUSHARE_HTTP_URL (forwarder url)
- TUSHARE_PROXY_TOKEN (forwarder token)
- Mongo envs (MONGO_ROOT_USER/PASSWORD etc)

Example:
  python3 scripts/fetch_tushare_mv_day_fill_missing.py --start 2026-01-01 --end 2026-02-03 --min-coverage 0.95
"""

from __future__ import annotations

import argparse
import os
import time
from typing import Dict, List, Optional

import pandas as pd
import pymongo


def mongo_client() -> pymongo.MongoClient:
    host = os.getenv("MONGODB_HOST", "mongodb")
    port = int(os.getenv("MONGODB_PORT", "27017"))
    db = os.getenv("MONGODB_DATABASE", "quantaxis")
    user = os.getenv("MONGODB_USER", "quantaxis")
    password = os.getenv("MONGODB_PASSWORD", "quantaxis")
    ru = os.getenv("MONGO_ROOT_USER", "root")
    rp = os.getenv("MONGO_ROOT_PASSWORD", "root")

    uris = [
        f"mongodb://{user}:{password}@{host}:{port}/{db}?authSource=admin",
        f"mongodb://{ru}:{rp}@{host}:{port}/{db}?authSource=admin",
        f"mongodb://{host}:{port}/{db}",
    ]
    last = None
    for uri in uris:
        try:
            c = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)
            c.admin.command("ping")
            return c
        except Exception as e:
            last = e
    raise RuntimeError(f"mongo connect failed: {last!r}")


def ensure_indexes(coll: pymongo.collection.Collection) -> None:
    coll.create_index([("code", 1), ("date", 1)], unique=True, name="code_1_date_1")
    coll.create_index([("date", 1)], name="date_1")


def tushare_pro():
    import tushare as ts

    http_url = os.getenv("TUSHARE_HTTP_URL", "").strip()
    token = os.getenv("TUSHARE_PROXY_TOKEN", "").strip() or os.getenv("TUSHARE_TOKEN", "").strip()
    if not token:
        raise RuntimeError("missing TUSHARE_PROXY_TOKEN/TUSHARE_TOKEN")

    if http_url:
        pro = ts.pro_api("DUMMY")
        pro._DataApi__token = token  # type: ignore[attr-defined]
        pro._DataApi__http_url = http_url  # type: ignore[attr-defined]
        return pro

    return ts.pro_api(token)


def yyyymmdd_to_iso(s: str) -> str:
    s = str(s)
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}" if (len(s) == 8 and s.isdigit()) else str(pd.to_datetime(s).date())


def upsert_mv(coll: pymongo.collection.Collection, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    now = int(time.time())
    ops = []
    for r in df.itertuples(index=False):
        ts_code = getattr(r, "ts_code", None)
        trade_date = getattr(r, "trade_date", None)
        if not ts_code or not trade_date:
            continue
        code = str(ts_code).split(".")[0].zfill(6)
        date = yyyymmdd_to_iso(trade_date)
        circ_mv = getattr(r, "circ_mv", None)
        total_mv = getattr(r, "total_mv", None)
        doc = {
            "code": code,
            "date": date,
            "float_mv": float(circ_mv) if circ_mv is not None and pd.notna(circ_mv) else None,
            "total_mv": float(total_mv) if total_mv is not None and pd.notna(total_mv) else None,
            "source": "tushare",
            "updated_at": now,
        }
        ops.append(pymongo.UpdateOne({"code": code, "date": date}, {"$set": doc}, upsert=True))
    if not ops:
        return 0
    res = coll.bulk_write(ops, ordered=False)
    return int(res.upserted_count + res.modified_count)


def trading_days_from_stock_day(sd: pymongo.collection.Collection, start: str, end: str) -> List[str]:
    q = {"date": {"$gte": start, "$lte": end}}
    dates = sd.distinct("date", q)
    dates = [str(d) for d in dates if d]
    dates = sorted(set(dates))
    return dates


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--min-coverage", type=float, default=0.95)
    ap.add_argument("--sleep", type=float, default=0.0)
    ap.add_argument("--max-days", type=int, default=0, help="0=all")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    start = str(pd.to_datetime(args.start).date())
    end = str(pd.to_datetime(args.end).date())

    client = mongo_client()
    dbn = os.getenv("MONGODB_DATABASE", "quantaxis")
    db = client[dbn]
    sd = db["stock_day"]
    mv = db["mv_day"]
    ensure_indexes(mv)

    days = trading_days_from_stock_day(sd, start, end)
    if int(args.max_days) > 0:
        days = days[: int(args.max_days)]

    pro = tushare_pro()
    fields = "ts_code,trade_date,circ_mv,total_mv"

    filled = 0
    bad_days: List[Dict[str, object]] = []

    for ds in days:
        td = ds.replace("-", "")
        n_sd = len(sd.distinct("code", {"date": ds}))
        n_mv0 = len(mv.distinct("code", {"date": ds}))
        cov0 = (n_mv0 / n_sd) if n_sd else 0.0

        need = (n_sd > 0) and (cov0 < float(args.min_coverage))
        if not need:
            print({"date": ds, "n_stock_day": n_sd, "n_mv_day": n_mv0, "coverage": cov0, "action": "skip"})
            continue

        df = pro.daily_basic(start_date=td, end_date=td, fields=fields)
        rows = int(df.shape[0]) if df is not None else 0
        if args.dry_run:
            print({"date": ds, "n_stock_day": n_sd, "n_mv_day": n_mv0, "coverage": cov0, "rows": rows, "action": "dry_run"})
            continue

        written = upsert_mv(mv, df) if df is not None else 0
        n_mv1 = len(mv.distinct("code", {"date": ds}))
        cov1 = (n_mv1 / n_sd) if n_sd else 0.0
        filled += 1
        out = {
            "date": ds,
            "n_stock_day": n_sd,
            "n_mv_day_before": n_mv0,
            "n_mv_day_after": n_mv1,
            "coverage_before": cov0,
            "coverage_after": cov1,
            "rows": rows,
            "written": written,
            "action": "fetch",
        }
        print(out)
        if cov1 < float(args.min_coverage):
            bad_days.append(out)

        if float(args.sleep) > 0:
            time.sleep(float(args.sleep))

    if bad_days:
        print({"bad_days": len(bad_days), "examples": bad_days[:10]})

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
