#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Seal a trading day after successful validation.

Writes:
- Mongo collection: ops_data_status (one doc per date)
- Local JSON: output/reports/ops_data_status/YYYY-MM-DD.json

Only sealed_ok=true days are allowed for HI + signal generation (enforced by daily_pipeline).
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import pymongo

ROOT = Path(__file__).resolve().parents[1]
OUTDIR = ROOT / "output" / "reports" / "ops_data_status"
OUTDIR.mkdir(parents=True, exist_ok=True)


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
    ap.add_argument("--validate-json", required=True, help="JSON string from validate_daily_data.py")
    args = ap.parse_args()

    d = str(args.date)
    obj = json.loads(str(args.validate_json))
    sealed_ok = bool(obj.get("validate_ok"))

    doc = {
        "date": d,
        "ingest_ok": bool(obj.get("ingest_ok")),
        "validate_ok": bool(obj.get("validate_ok")),
        "sealed_ok": sealed_ok,
        "counts": obj.get("counts"),
        "etag": obj.get("etag"),
        "ts": int(__import__("time").time()),
    }

    c = mongo()
    db = c[os.getenv("MONGODB_DATABASE", "quantaxis")]
    coll = db["ops_data_status"]
    coll.create_index([("date", 1)], unique=True)
    coll.update_one({"date": d}, {"$set": doc}, upsert=True)

    (OUTDIR / f"{d}.json").write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(doc, ensure_ascii=False))

    raise SystemExit(0 if sealed_ok else 2)


if __name__ == "__main__":
    main()
