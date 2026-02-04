#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Health-check: stock_day.date is normalized to YYYY-MM-DD strings.

Run after imports/migrations; intended to prevent mixed date formats from
silently reappearing.

Examples:
  python3 scripts/data_healthcheck_stock_day_date.py --db quantaxis --sample 20000
  python3 scripts/data_healthcheck_stock_day_date.py --uri mongodb://localhost:27017 --db quantaxis --user root --password ...
"""

from __future__ import annotations

import argparse
import re

import pymongo

ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def connect(uri: str, user: str, password: str, authdb: str) -> pymongo.MongoClient:
    if user and password:
        if "authSource=" not in uri:
            sep = "&" if "?" in uri else "?"
            uri = f"{uri}{sep}authSource={authdb}"
        client = pymongo.MongoClient(uri, username=user, password=password, serverSelectionTimeoutMS=8000)
    else:
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    return client


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", default="mongodb://localhost:27017")
    ap.add_argument("--db", default="quantaxis")
    ap.add_argument("--user", default="")
    ap.add_argument("--password", default="")
    ap.add_argument("--authdb", default="admin")
    ap.add_argument("--sample", type=int, default=20000)
    args = ap.parse_args()

    client = connect(args.uri, args.user, args.password, args.authdb)
    coll = client[args.db]["stock_day"]

    bad = 0
    scanned = 0
    cursor = coll.find({"date": {"$exists": True}}, {"_id": 0, "date": 1, "code": 1}).limit(args.sample)
    for doc in cursor:
        scanned += 1
        d = doc.get("date")
        if not isinstance(d, str) or not ISO_RE.match(d):
            bad += 1
            if bad <= 20:
                print("BAD", {"code": doc.get("code"), "date": d, "type": type(d).__name__})

    if bad:
        raise SystemExit(f"FAILED: scanned={scanned} bad={bad}")
    print(f"OK: scanned={scanned} bad=0")


if __name__ == "__main__":
    main()
