#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Fetch ETF/fund daily bars from Tushare (fund_daily) and write into Mongo.

Needed to ingest 510300 (HS300 ETF) so fallback leg has a real return series.

Env:
- TUSHARE_TOKEN (required)
- TUSHARE_HTTP_URL (optional proxy)
- MONGODB_HOST/PORT/DATABASE/USER/PASSWORD

Writes into quantaxis.stock_day using code=ts_code base (e.g. 510300).
Schema aligns with stock_day (open/high/low/close/vol/amount/date ISO).

Usage:
  python scripts/fetch_tushare_fund_daily.py --codes 510300.SH --start 20160101 --end 20181231
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional

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


def _mongo_client(cfg: MongoCfg) -> pymongo.MongoClient:
    uris = [
        f"mongodb://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.db}?authSource=admin",
        f"mongodb://{cfg.root_user}:{cfg.root_password}@{cfg.host}:{cfg.port}/{cfg.db}?authSource=admin",
    ]
    last_err = None
    for uri in uris:
        try:
            client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)
            client.admin.command("ping")
            return client
        except Exception as e:
            last_err = e
    raise last_err  # type: ignore[misc]


def _get_mongo_cfg() -> MongoCfg:
    return MongoCfg(
        host=os.getenv("MONGODB_HOST", "mongodb"),
        port=int(os.getenv("MONGODB_PORT", "27017")),
        db=os.getenv("MONGODB_DATABASE", "quantaxis"),
        user=os.getenv("MONGODB_USER", "quantaxis"),
        password=os.getenv("MONGODB_PASSWORD", "quantaxis"),
        root_user=os.getenv("MONGO_ROOT_USER", "root"),
        root_password=os.getenv("MONGO_ROOT_PASSWORD", "root"),
    )


def _ensure_indexes(coll: pymongo.collection.Collection) -> None:
    coll.create_index([("code", 1), ("date", 1)], unique=True)
    coll.create_index([("date", 1)])


def _chunks(xs: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(xs), n):
        yield xs[i : i + n]


def _to_iso(td: str) -> str:
    td = str(td or "").strip()
    if len(td) == 8 and td.isdigit():
        return f"{td[0:4]}-{td[4:6]}-{td[6:8]}"
    return td


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYYMMDD")
    ap.add_argument("--end", required=True, help="YYYYMMDD")
    ap.add_argument("--codes", required=True, help="comma-separated ts_code list, e.g. 510300.SH")
    ap.add_argument("--sleep", type=float, default=0.15)
    ap.add_argument("--batch", type=int, default=1)
    args = ap.parse_args(argv)

    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        print("ERROR: missing env TUSHARE_TOKEN", file=sys.stderr)
        return 2

    try:
        import tushare as ts
    except Exception as e:
        print(f"ERROR: tushare not installed: {e}", file=sys.stderr)
        return 3

    http_url = os.getenv("TUSHARE_HTTP_URL", "").strip()
    if http_url:
        pro = ts.pro_api("DUMMY")
        pro._DataApi__token = token  # type: ignore[attr-defined]
        pro._DataApi__http_url = http_url  # type: ignore[attr-defined]
    else:
        pro = ts.pro_api(token)

    codes = [c.strip() for c in str(args.codes).split(",") if c.strip()]
    print(f"[tushare/fund_daily] codes={len(codes)} start={args.start} end={args.end}")

    cfg = _get_mongo_cfg()
    client = _mongo_client(cfg)
    db = client[cfg.db]
    coll = db["stock_day"]
    _ensure_indexes(coll)

    total_rows = 0
    total_upsert = 0

    for code_batch in _chunks(codes, max(1, int(args.batch))):
        for tscode in code_batch:
            df = pro.fund_daily(ts_code=tscode, start_date=args.start, end_date=args.end)
            if df is None or df.empty:
                print(f"[tushare/fund_daily] {tscode}: no data")
                continue

            recs = []
            for r in df.to_dict("records"):
                ts_code = r.get("ts_code")
                base = str(ts_code).split(".")[0] if ts_code else None
                recs.append(
                    {
                        "code": (base or ts_code),
                        "ts_code": ts_code,
                        "date": _to_iso(r.get("trade_date")),
                        "open": r.get("open"),
                        "high": r.get("high"),
                        "low": r.get("low"),
                        "close": r.get("close"),
                        "pre_close": r.get("pre_close"),
                        "change": r.get("change"),
                        "pct_chg": r.get("pct_chg"),
                        "vol": r.get("vol"),
                        "amount": r.get("amount"),
                        "source": "tushare_fund_daily",
                        "updated_at": int(time.time()),
                    }
                )

            ops = [
                pymongo.UpdateOne({"code": rec["code"], "date": rec["date"]}, {"$set": rec}, upsert=True)
                for rec in recs
            ]
            res = coll.bulk_write(ops, ordered=False)
            total_rows += len(recs)
            total_upsert += (res.upserted_count or 0)
            print(
                f"[mongo] {tscode}: bars={len(recs)} upserted={res.upserted_count} modified={res.modified_count} matched={res.matched_count}"
            )
            time.sleep(max(0.0, float(args.sleep)))

    print(f"DONE: total_rows={total_rows} total_upserted={total_upsert}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
