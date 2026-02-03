#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Fetch A-share daily bars from Tushare and write into MongoDB.

Design goals:
- Reproducible, CLI-friendly.
- Minimal assumptions about upstream QUANTAXIS import graph.
- Writes into quantaxis.stock_day with (code,date) unique index.

Env:
- TUSHARE_TOKEN (required)
- MONGODB_HOST, MONGODB_PORT, MONGODB_DATABASE
- MONGODB_USER, MONGODB_PASSWORD
- MONGO_ROOT_USER, MONGO_ROOT_PASSWORD (fallback)

Usage examples:
  python scripts/fetch_tushare_stock_day.py --start 20240101 --end 20241231 --limit 50
  python scripts/fetch_tushare_stock_day.py --codes 000001.SZ,600000.SH --start 20240101 --end 20240131
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
    # Try app user first; if the persistent mongo volume was initialized with different creds,
    # fall back to root user.
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


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYYMMDD")
    ap.add_argument("--end", required=True, help="YYYYMMDD")
    ap.add_argument("--codes", default=None, help="comma-separated ts_code list, e.g. 000001.SZ,600000.SH")
    ap.add_argument(
        "--from-stock-list",
        action="store_true",
        help="load codes from Mongo stock_list (expects fields: code, ts_code).",
    )
    ap.add_argument(
        "--theme",
        default=None,
        help="optional filter when using --from-stock-list: hs10|cyb20|a_ex_kcb_bse",
    )
    ap.add_argument("--limit", type=int, default=200, help="limit number of symbols (when --codes not given)")
    ap.add_argument("--sleep", type=float, default=0.15, help="sleep between API calls")
    ap.add_argument("--batch", type=int, default=1, help="codes per request; keep 1 for stability")
    args = ap.parse_args(argv)

    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        print("ERROR: missing env TUSHARE_TOKEN", file=sys.stderr)
        return 2

    try:
        import tushare as ts
    except Exception as e:
        print(f"ERROR: tushare not installed in this environment: {e}", file=sys.stderr)
        return 3

    # Optional: support self-hosted/proxied Tushare DataApi endpoint.
    # Some users have a forwarding endpoint + token that works even when official permissions are limited.
    http_url = os.getenv("TUSHARE_HTTP_URL", "").strip()
    if http_url:
        # Follow the proxy pattern: pro_api arg can be a dummy; then override private fields.
        pro = ts.pro_api("DUMMY")
        # NOTE: Tushare uses name-mangled private attributes.
        pro._DataApi__token = token  # type: ignore[attr-defined]
        pro._DataApi__http_url = http_url  # type: ignore[attr-defined]
    else:
        pro = ts.pro_api(token)

    def _theme_ok(code6: str, theme: Optional[str]) -> bool:
        if not theme:
            return True
        theme = theme.strip()
        if theme in {"hs10", "cn_hs10", "a_hs10"}:
            if code6.startswith(("300", "301", "688")):
                return False
            if code6.startswith(("8", "4")):
                return False
            return code6.startswith(("600", "601", "603", "605", "000", "001", "002", "003"))
        if theme in {"cyb20", "cn_cyb20", "a_cyb20"}:
            return code6.startswith(("300", "301"))
        if theme in {"a_ex_kcb_bse", "cn_a_ex_kcb_bse", "a_no_kcb_bse"}:
            if code6.startswith("688"):
                return False
            if code6.startswith(("8", "4")):
                return False
            return code6.startswith(("600", "601", "603", "605", "000", "001", "002", "003", "300", "301"))
        return True

    # Choose codes
    if args.codes:
        codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    elif args.from_stock_list:
        cfg = _get_mongo_cfg()
        client = _mongo_client(cfg)
        db = client[cfg.db]
        coll = db["stock_list"]
        # Prefer ts_code if present; otherwise construct from code with best-effort suffix guess.
        tmp = []
        for doc in coll.find({}, {"_id": 0, "code": 1, "ts_code": 1}).sort("code", 1):
            code6 = str(doc.get("code") or "").zfill(6)
            if not code6 or not code6.isdigit():
                continue
            if not _theme_ok(code6, args.theme):
                continue
            tsc = doc.get("ts_code")
            if isinstance(tsc, str) and "." in tsc:
                tmp.append(tsc)
            else:
                # naive exchange inference
                suffix = ".SH" if code6.startswith(("600", "601", "603", "605", "688")) else ".SZ"
                tmp.append(f"{code6}{suffix}")
        codes = tmp[: max(1, int(args.limit))] if args.limit else tmp
        if not codes:
            print("ERROR: empty codes from stock_list (check collection + theme filter)", file=sys.stderr)
            return 5
    else:
        # stock_basic returns ts_code like 000001.SZ
        try:
            df = pro.stock_basic(exchange="", list_status="L", fields="ts_code,symbol,name,area,industry,list_date")
            df = df.sort_values("ts_code")
            codes = df["ts_code"].head(args.limit).tolist()
        except Exception as e:
            print(
                "ERROR: cannot call Tushare stock_basic with this token. "
                "This endpoint requires specific permissions/points.\n"
                f"  underlying error: {e}\n"
                "Fix: call with explicit --codes, e.g. --codes 000001.SZ,600000.SH",
                file=sys.stderr,
            )
            return 4

    print(f"[tushare] codes={len(codes)} start={args.start} end={args.end}")

    cfg = _get_mongo_cfg()
    client = _mongo_client(cfg)
    db = client[cfg.db]
    coll = db["stock_day"]
    _ensure_indexes(coll)

    total_rows = 0
    total_upsert = 0

    # Fetch daily bars and upsert
    for code_batch in _chunks(codes, max(1, args.batch)):
        # Tushare daily endpoint supports ts_code; keep 1 per request to avoid surprises.
        for code in code_batch:
            df = pro.daily(ts_code=code, start_date=args.start, end_date=args.end)
            if df is None or df.empty:
                print(f"[tushare] {code}: no data")
                continue

            # Normalize fields to QUANTAXIS-ish schema
            # Tushare uses trade_date YYYYMMDD, vol in (hands?), amount in (thousand?) depending; we keep raw.
            records = []
            for r in df.to_dict("records"):
                ts_code = r.get("ts_code")
                base_code = None
                if isinstance(ts_code, str) and ts_code:
                    base_code = ts_code.split(".")[0]
                records.append(
                    {
                        # Keep QUANTAXIS-style 6-digit code as primary key for compatibility
                        "code": base_code or ts_code,
                        # Preserve original tushare identifier for traceability
                        "ts_code": ts_code,
                        "date": r.get("trade_date"),
                        "open": r.get("open"),
                        "high": r.get("high"),
                        "low": r.get("low"),
                        "close": r.get("close"),
                        "pre_close": r.get("pre_close"),
                        "change": r.get("change"),
                        "pct_chg": r.get("pct_chg"),
                        "vol": r.get("vol"),
                        "amount": r.get("amount"),
                        "source": "tushare",
                        "updated_at": int(time.time()),
                    }
                )

            ops = [
                pymongo.UpdateOne(
                    {"code": rec["code"], "date": rec["date"]},
                    {"$set": rec},
                    upsert=True,
                )
                for rec in records
            ]

            res = coll.bulk_write(ops, ordered=False)
            total_rows += len(records)
            total_upsert += (res.upserted_count or 0)
            print(
                f"[mongo] {code}: bars={len(records)} upserted={res.upserted_count} modified={res.modified_count} matched={res.matched_count}"
            )

            time.sleep(max(0.0, args.sleep))

    print(f"DONE: total_rows={total_rows} total_upserted={total_upsert}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
