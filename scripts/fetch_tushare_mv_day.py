#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Fetch and store daily market value (mv) into Mongo.

Writes collection: mv_day
Schema (per doc):
- code: 6-digit
- date: YYYY-MM-DD
- float_mv: circulating market value (Tushare circ_mv)
- total_mv: total market value
- source: 'tushare'
- updated_at: unix ts

Requirements:
- env TUSHARE_TOKEN (set in docker-compose)
- Mongo envs MONGODB_* or MONGO_ROOT_*

Example (in container):
  python3 scripts/fetch_tushare_mv_day.py --start 2024-01-01 --end 2026-02-03
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import datetime
from typing import Optional

import pandas as pd
import pymongo


def _dt_to_yyyymmdd(s: str) -> str:
    return pd.to_datetime(s).strftime("%Y%m%d")


def _yyyymmdd_to_iso(s: str) -> str:
    s = str(s)
    if len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return str(pd.to_datetime(s).date())


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

    token = os.getenv("TUSHARE_TOKEN", "").strip()
    if not token:
        raise RuntimeError("missing TUSHARE_TOKEN")

    pro = ts.pro_api(token)

    # Optional proxy/forwarder support (same pattern as stock_day fetch)
    http_url = os.getenv("TUSHARE_HTTP_URL", "").strip()
    if http_url:
        try:
            pro._DataApi__http_url = http_url  # type: ignore[attr-defined]
        except Exception:
            pass
    return pro


def fetch_daily_basic(pro, start_date: str, end_date: str) -> pd.DataFrame:
    # daily_basic returns per-stock per-trade_date metrics including circ_mv/total_mv
    fields = "ts_code,trade_date,circ_mv,total_mv"
    df = pro.daily_basic(start_date=start_date, end_date=end_date, fields=fields)
    if df is None:
        return pd.DataFrame()
    return df


def upsert_mv(coll: pymongo.collection.Collection, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    now = int(time.time())
    ops = []
    for r in df.itertuples(index=False):
        ts_code = getattr(r, "ts_code", None)
        trade_date = getattr(r, "trade_date", None)
        circ_mv = getattr(r, "circ_mv", None)
        total_mv = getattr(r, "total_mv", None)
        if not ts_code or not trade_date:
            continue
        code = str(ts_code).split(".")[0].zfill(6)
        date = _yyyymmdd_to_iso(trade_date)
        doc = {
            "code": code,
            "date": date,
            "float_mv": float(circ_mv) if circ_mv is not None and pd.notna(circ_mv) else None,
            "total_mv": float(total_mv) if total_mv is not None and pd.notna(total_mv) else None,
            "source": "tushare",
            "updated_at": now,
        }
        ops.append(
            pymongo.UpdateOne({"code": code, "date": date}, {"$set": doc}, upsert=True)
        )

    if not ops:
        return 0
    res = coll.bulk_write(ops, ordered=False)
    return int(res.upserted_count + res.modified_count)


def month_range(start: str, end: str):
    s = pd.to_datetime(start).normalize().replace(day=1)
    e = pd.to_datetime(end).normalize().replace(day=1)
    cur = s
    while cur <= e:
        nxt = (cur + pd.offsets.MonthBegin(1)).to_pydatetime()
        yield cur, nxt
        cur = pd.Timestamp(nxt)


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--batch-month", type=int, default=1, help="months per API call")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    start = str(args.start)
    end = str(args.end)

    client = mongo_client()
    dbn = os.getenv("MONGODB_DATABASE", "quantaxis")
    coll = client[dbn]["mv_day"]
    ensure_indexes(coll)

    pro = tushare_pro()

    total_written = 0
    t0 = time.time()

    # month batching to keep payloads reasonable
    months = list(month_range(start, end))
    step = max(1, int(args.batch_month))

    for i in range(0, len(months), step):
        b0 = months[i][0]
        b1 = months[min(i + step - 1, len(months) - 1)][1]
        s0 = b0.strftime("%Y%m%d")
        e0 = (pd.Timestamp(b1) - pd.Timedelta(days=1)).strftime("%Y%m%d")
        # clamp to requested range
        s0 = max(s0, _dt_to_yyyymmdd(start))
        e0 = min(e0, _dt_to_yyyymmdd(end))
        if s0 > e0:
            continue

        df = fetch_daily_basic(pro, start_date=s0, end_date=e0)
        if df.empty:
            continue

        if args.dry_run:
            print({"range": [s0, e0], "rows": int(df.shape[0])})
            continue

        n = upsert_mv(coll, df)
        total_written += n
        print({"range": [s0, e0], "rows": int(df.shape[0]), "written": n, "total_written": total_written})

    dt = time.time() - t0
    print({"ok": True, "total_written": total_written, "seconds": dt})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
