#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Fetch CN A-share stock list from Tushare and write into MongoDB stock_list.

Goal:
- Build a *full* universe for Main boards + ChiNext, excluding STAR (科创板) and BSE/NQ.

Env:
- TUSHARE_TOKEN (required)
- Optional: TUSHARE_HTTP_URL (proxy/forward endpoint)
- Mongo: MONGODB_HOST, MONGODB_PORT, MONGODB_DATABASE, MONGODB_USER, MONGODB_PASSWORD
  plus MONGO_ROOT_USER/MONGO_ROOT_PASSWORD fallback.

Usage:
  python scripts/fetch_tushare_stock_list.py --list-status L

Notes:
- We keep both 6-digit `code` and `ts_code`.
- We intentionally do NOT include STAR (688) nor BSE/NQ.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

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


def _is_main_or_cyb_ex_kcb_bse(code: str, ts_code: Optional[str]) -> bool:
    code = (code or "").strip()
    ts_code = (ts_code or "").strip().upper()
    if not code.isdigit() or len(code) != 6:
        return False

    # Exclude STAR / 科创板
    if code.startswith("688"):
        return False

    # Exclude BSE / NQ style codes (often 8xxxx/4xxxx)
    if code.startswith(("8", "4")):
        return False

    # Exclude explicit BJ/NQ exchanges if present
    if ts_code.endswith(".BJ") or ts_code.endswith(".NQ"):
        return False

    # Include: SH main + SZ main + ChiNext
    return code.startswith((
        "600",
        "601",
        "603",
        "605",
        "000",
        "001",
        "002",
        "003",
        "300",
        "301",
    ))


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--list-status", default="L", help="L=listed, D=delisted, P=suspended")
    ap.add_argument("--sleep", type=float, default=0.0, help="sleep seconds after tushare call")
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

    http_url = os.getenv("TUSHARE_HTTP_URL", "").strip()
    if http_url:
        pro = ts.pro_api("DUMMY")
        pro._DataApi__token = token  # type: ignore[attr-defined]
        pro._DataApi__http_url = http_url  # type: ignore[attr-defined]
    else:
        pro = ts.pro_api(token)

    # Fetch stock_basic
    fields = "ts_code,symbol,name,area,industry,market,exchange,list_status,list_date,delist_date"
    try:
        df = pro.stock_basic(exchange="", list_status=str(args.list_status), fields=fields)
    except Exception as e:
        print(f"ERROR: pro.stock_basic failed: {e}", file=sys.stderr)
        return 4

    if args.sleep > 0:
        time.sleep(max(0.0, float(args.sleep)))

    # Upsert into Mongo
    cfg = _get_mongo_cfg()
    client = _mongo_client(cfg)
    db = client[cfg.db]
    coll = db["stock_list"]
    coll.create_index([("code", 1)], unique=True)
    coll.create_index([("ts_code", 1)])

    now = int(time.time())
    total = int(df.shape[0])
    kept = 0
    ops: List[pymongo.UpdateOne] = []

    for r in df.to_dict("records"):
        ts_code = r.get("ts_code")
        code = r.get("symbol") or (str(ts_code).split(".")[0] if ts_code else None)
        code = str(code).zfill(6) if code else ""
        if not _is_main_or_cyb_ex_kcb_bse(code, ts_code):
            continue

        kept += 1
        doc: Dict[str, Any] = {
            "code": code,
            "ts_code": ts_code,
            "name": r.get("name"),
            "area": r.get("area"),
            "industry": r.get("industry"),
            "market": r.get("market"),
            "exchange": r.get("exchange"),
            "list_status": r.get("list_status"),
            "list_date": r.get("list_date"),
            "delist_date": r.get("delist_date"),
            "source": "tushare",
            "updated_at": now,
        }
        ops.append(pymongo.UpdateOne({"code": code}, {"$set": doc}, upsert=True))

    if not ops:
        print("No rows kept after filters; nothing to write.")
        return 0

    # Bulk write
    res = coll.bulk_write(ops, ordered=False)
    print(
        f"[stock_list] tushare_total={total} kept={kept} "
        f"upserted={res.upserted_count} modified={res.modified_count} matched={res.matched_count}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
