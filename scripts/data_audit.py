#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Data audit for Mongo stock_day used by baseline backtests.

Produces simple, product-friendly diagnostics:
- coverage per code (first/last date, bars)
- missing ratio on close panel

This is NOT a full data QA (no corporate actions/adjustments), but it makes runs auditable.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
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


def get_mongo_cfg() -> MongoCfg:
    return MongoCfg(
        host=os.getenv("MONGODB_HOST", "mongodb"),
        port=int(os.getenv("MONGODB_PORT", "27017")),
        db=os.getenv("MONGODB_DATABASE", "quantaxis"),
        user=os.getenv("MONGODB_USER", "quantaxis"),
        password=os.getenv("MONGODB_PASSWORD", "quantaxis"),
        root_user=os.getenv("MONGO_ROOT_USER", "root"),
        root_password=os.getenv("MONGO_ROOT_PASSWORD", "root"),
    )


def mongo_client(cfg: MongoCfg) -> pymongo.MongoClient:
    uris = [
        f"mongodb://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.db}?authSource=admin",
        f"mongodb://{cfg.root_user}:{cfg.root_password}@{cfg.host}:{cfg.port}/{cfg.db}?authSource=admin",
    ]
    last = None
    for uri in uris:
        try:
            c = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)
            c.admin.command("ping")
            return c
        except Exception as e:
            last = e
    raise last  # type: ignore[misc]


def norm_date(s: str) -> str:
    s = s.strip()
    if "-" in s:
        return s
    if len(s) == 8:
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    raise ValueError(f"bad date: {s}")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--codes", required=True, help="comma-separated 6-digit codes")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--outdir", default="/tmp/output")
    args = ap.parse_args(argv)

    codes = [c.strip().zfill(6) for c in args.codes.split(",") if c.strip()]
    start = norm_date(args.start)
    end = norm_date(args.end)

    cfg = get_mongo_cfg()
    client = mongo_client(cfg)
    coll = client[cfg.db]["stock_day"]

    rows = []
    for code in codes:
        cur = coll.find(
            {"code": code, "date": {"$gte": start, "$lte": end}},
            {"_id": 0, "date": 1, "close": 1},
        ).sort("date", 1)
        data = list(cur)
        if not data:
            rows.append({"code": code, "bars": 0, "first": None, "last": None, "missing_close": None})
            continue
        df = pd.DataFrame(data)
        df = df.dropna(subset=["close"]).drop_duplicates(subset=["date"]).sort_values("date")
        miss_close = float(pd.isna(df.get("close")).mean()) if "close" in df.columns else None
        rows.append(
            {
                "code": code,
                "bars": int(len(df)),
                "first": str(df["date"].iloc[0]),
                "last": str(df["date"].iloc[-1]),
                "missing_close": miss_close,
            }
        )

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(outdir / "data_audit.csv", index=False)

    summary = {
        "generated_at": int(time.time()),
        "start": start,
        "end": end,
        "codes": len(codes),
        "codes_with_data": int(sum(1 for r in rows if r["bars"] > 0)),
        "min_bars": int(min((r["bars"] for r in rows), default=0)),
        "max_bars": int(max((r["bars"] for r in rows), default=0)),
    }
    (outdir / "data_audit_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
