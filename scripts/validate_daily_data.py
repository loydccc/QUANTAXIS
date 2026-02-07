#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Validate daily data completeness for a given trading date.

Production v1 checks (fixed spec):
- n_codes_today >= 3000 (stock_day)
- uniqueness: no duplicate (code,date)
- close/amount non-null ratio > 98% (stock_day)

Best-effort / non-blocking checks (recorded in counts but do NOT block sealing):
- 510300 close exists for date (legacy expectation; some datasets may not include ETF)

Outputs JSON summary to stdout.
Exit code:
- 0: validate_ok
- 2: validate_failed
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

import pymongo

ROOT = Path(__file__).resolve().parents[1]


def mongo():
    host = os.getenv("MONGODB_HOST", "127.0.0.1")
    port = int(os.getenv("MONGODB_PORT", "27017"))
    db = os.getenv("MONGODB_DATABASE", "quantaxis")
    user = os.getenv("MONGODB_USER", "quantaxis")
    pwd = os.getenv("MONGODB_PASSWORD", "quantaxis")
    uri = f"mongodb://{user}:{pwd}@{host}:{port}/{db}?authSource=admin"
    c = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)
    c.admin.command("ping")
    return c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--min-codes", type=int, default=3000)
    ap.add_argument("--nonnull-ratio", type=float, default=0.98)
    args = ap.parse_args()

    c = mongo()
    db = c[os.getenv("MONGODB_DATABASE", "quantaxis")]
    coll = db["stock_day"]

    d = str(args.date)

    # count distinct codes for the date
    codes = coll.distinct("code", {"date": d})
    n_codes = len(codes)

    # 510300 close exists
    etf = coll.find_one({"code": "510300", "date": d}, {"_id": 0, "close": 1, "amount": 1})
    etf_ok = bool(etf and etf.get("close") is not None)

    # uniqueness check (should be enforced by index)
    dup_pipe = [
        {"$match": {"date": d}},
        {"$group": {"_id": {"code": "$code", "date": "$date"}, "n": {"$sum": 1}}},
        {"$match": {"n": {"$gt": 1}}},
        {"$limit": 5},
    ]
    dups = list(coll.aggregate(dup_pipe, allowDiskUse=True))
    uniq_ok = len(dups) == 0

    # non-null ratios
    total = coll.count_documents({"date": d})
    missing_close = coll.count_documents({"date": d, "$or": [{"close": None}, {"close": {"$exists": False}}]})
    missing_amount = coll.count_documents({"date": d, "$or": [{"amount": None}, {"amount": {"$exists": False}}]})

    close_ratio = 1.0 if total == 0 else (1.0 - missing_close / total)
    amount_ratio = 1.0 if total == 0 else (1.0 - missing_amount / total)

    validate_ok = (
        n_codes >= int(args.min_codes)
        and uniq_ok
        and close_ratio >= float(args.nonnull_ratio)
        and amount_ratio >= float(args.nonnull_ratio)
    )

    counts = {
        "n_codes": int(n_codes),
        "total_docs": int(total),
        "missing_close": int(missing_close),
        "missing_amount": int(missing_amount),
        "close_ratio": float(close_ratio),
        "amount_ratio": float(amount_ratio),
        "etf_ok": bool(etf_ok),
        "uniq_ok": bool(uniq_ok),
        "dup_samples": dups,
    }

    etag = hashlib.sha256(json.dumps(counts, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()

    out = {
        "date": d,
        "ingest_ok": True,
        "validate_ok": bool(validate_ok),
        "counts": counts,
        "etag": etag,
    }

    print(json.dumps(out, ensure_ascii=False))
    raise SystemExit(0 if validate_ok else 2)


if __name__ == "__main__":
    main()
