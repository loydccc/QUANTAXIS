#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Check coverage of mv_day vs stock_day for a date range.

Computes, per date:
- n_stock_day_codes
- n_mv_day_codes
- coverage_ratio

Example (container):
  python3 scripts/data_healthcheck_mv_coverage.py --start 2026-01-01 --end 2026-02-03
"""

from __future__ import annotations

import argparse
import os

import pandas as pd
import pymongo


def mongo() -> pymongo.MongoClient:
    host = os.getenv("MONGODB_HOST", "mongodb")
    port = int(os.getenv("MONGODB_PORT", "27017"))
    db = os.getenv("MONGODB_DATABASE", "quantaxis")
    ru = os.getenv("MONGO_ROOT_USER", "root")
    rp = os.getenv("MONGO_ROOT_PASSWORD", "root")
    uri = f"mongodb://{ru}:{rp}@{host}:{port}/{db}?authSource=admin"
    c = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)
    c.admin.command("ping")
    return c


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--db", default=os.getenv("MONGODB_DATABASE", "quantaxis"))
    args = ap.parse_args()

    start = str(pd.to_datetime(args.start).date())
    end = str(pd.to_datetime(args.end).date())

    client = mongo()
    d = client[args.db]
    sd = d["stock_day"]
    mv = d["mv_day"]

    dates = pd.date_range(start=start, end=end, freq="D")
    rows = []
    for dt in dates:
        ds = str(dt.date())
        n_sd = len(sd.distinct("code", {"date": ds}))
        n_mv = len(mv.distinct("code", {"date": ds}))
        cov = (n_mv / n_sd) if n_sd else 0.0
        rows.append({"date": ds, "n_stock_day": n_sd, "n_mv_day": n_mv, "coverage": cov})

    out = pd.DataFrame(rows)
    print(out.describe(include="all"))
    worst = out.sort_values("coverage").head(10)
    print("worst_coverage")
    print(worst.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
