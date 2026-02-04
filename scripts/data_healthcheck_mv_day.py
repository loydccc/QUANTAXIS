#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Health-check for mv_day collection."""

from __future__ import annotations

import argparse
import re

import pymongo

ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", default="mongodb://localhost:27017")
    ap.add_argument("--db", default="quantaxis")
    ap.add_argument("--user", default="")
    ap.add_argument("--password", default="")
    ap.add_argument("--authdb", default="admin")
    ap.add_argument("--sample", type=int, default=50000)
    args = ap.parse_args()

    uri = args.uri
    if args.user and args.password and "authSource=" not in uri:
        sep = "&" if "?" in uri else "?"
        uri = f"{uri}{sep}authSource={args.authdb}"

    if args.user and args.password:
        client = pymongo.MongoClient(uri, username=args.user, password=args.password, serverSelectionTimeoutMS=8000)
    else:
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")

    coll = client[args.db]["mv_day"]
    bad = 0
    scanned = 0
    cur = coll.find({}, {"_id": 0, "code": 1, "date": 1, "float_mv": 1, "total_mv": 1}).limit(int(args.sample))
    for doc in cur:
        scanned += 1
        code = str(doc.get("code") or "")
        date = doc.get("date")
        if len(code) != 6 or not code.isdigit():
            bad += 1
            if bad <= 20:
                print("BAD_CODE", doc)
        if not isinstance(date, str) or not ISO_RE.match(date):
            bad += 1
            if bad <= 20:
                print("BAD_DATE", doc)

    if bad:
        raise SystemExit(f"FAILED: scanned={scanned} bad={bad}")
    print(f"OK: scanned={scanned} bad=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
