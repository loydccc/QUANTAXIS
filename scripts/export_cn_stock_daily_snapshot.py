#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Export CN A-share daily bars snapshot from QUANTAXIS MongoDB to a versioned parquet + manifest.

Why this exists
- QUANTAXIS stores the authoritative daily bars in MongoDB (stock_day).
- For reproducible backtests and artifact provenance, we need an immutable-ish
  on-disk snapshot with a content hash (manifest).

Output layout (by default)
  data/qa_cn_stock_daily/versions/<ASOF>/
    bars.parquet
    manifest.json

Manifest hash policy
- sha256 for each exported file
- manifest_sha256 is sha256 of the canonical manifest JSON (sorted keys, compact)

Env (Mongo)
- MONGODB_HOST, MONGODB_PORT, MONGODB_DATABASE
- MONGODB_USER, MONGODB_PASSWORD
- MONGO_ROOT_USER, MONGO_ROOT_PASSWORD (fallback)

Usage
  python3 scripts/export_cn_stock_daily_snapshot.py --asof 2026-02-01 \
    --start 2019-01-01 --end 2026-02-01

Notes
- This script exports **raw (unadjusted) OHLCV** and includes adj_factor when available.
- If you want forward/back adjusted prices, do it explicitly in the strategy.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import pymongo


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class MongoCfg:
    host: str
    port: int
    db: str
    user: str
    password: str
    root_user: str
    root_password: str


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


def _mongo_client(cfg: MongoCfg) -> pymongo.MongoClient:
    uris = [
        f"mongodb://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.db}?authSource=admin",
        f"mongodb://{cfg.root_user}:{cfg.root_password}@{cfg.host}:{cfg.port}/{cfg.db}?authSource=admin",
    ]
    last_err: Optional[Exception] = None
    for uri in uris:
        try:
            client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)
            client.admin.command("ping")
            return client
        except Exception as e:  # noqa: BLE001
            last_err = e
    raise last_err or RuntimeError("failed to connect mongo")


def _sha256_file(p: Path, chunk: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _canonical_json_bytes(obj: Any) -> bytes:
    s = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return s.encode("utf-8")


def _sha256_json(obj: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(obj)).hexdigest()


def _parse_ymd(s: str) -> str:
    """Return YYYY-MM-DD."""
    s = s.strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    # accept YYYY-MM-DD
    return s[0:10]


def _iter_stock_day(coll, start_date: str, end_date: str, batch_size: int) -> Iterable[dict]:
    # QUANTAXIS stock_day usually has both `date` (YYYY-MM-DD) and `date_stamp`.
    # We query on `date` string for simplicity.
    q = {"date": {"$gte": start_date, "$lte": end_date}}
    cur = coll.find(q, {"_id": 0}, batch_size=batch_size)
    for doc in cur:
        yield doc


def _load_stock_day_df(db, start_date: str, end_date: str, batch_size: int, limit: Optional[int]) -> pd.DataFrame:
    coll = db["stock_day"]
    rows: List[dict] = []
    for i, doc in enumerate(_iter_stock_day(coll, start_date, end_date, batch_size=batch_size)):
        rows.append(doc)
        if limit is not None and i + 1 >= limit:
            break
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return df


def _load_stock_adj_df(db, start_date: str, end_date: str, batch_size: int) -> pd.DataFrame:
    coll = db["stock_adj"]
    q = {"date": {"$gte": start_date, "$lte": end_date}}
    cur = coll.find(q, {"_id": 0}, batch_size=batch_size)
    rows = [doc for doc in cur]
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _normalize_bars(df_day: pd.DataFrame) -> pd.DataFrame:
    if df_day.empty:
        return df_day

    # tolerate different field names
    if "vol" in df_day.columns and "volume" not in df_day.columns:
        df_day["volume"] = df_day["vol"]

    want = ["date", "code", "open", "high", "low", "close", "volume", "amount"]
    missing = [c for c in ("date", "code") if c not in df_day.columns]
    if missing:
        raise SystemExit(f"missing required fields in stock_day: {missing}")

    # keep only known numeric cols when present
    cols = [c for c in want if c in df_day.columns]
    out = df_day.loc[:, cols].copy()

    # types
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date", "code"])
    out["code"] = out["code"].astype(str)

    # enforce numeric where possible
    for c in ["open", "high", "low", "close", "volume", "amount"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.dropna(subset=["open", "high", "low", "close"], how="any")

    # sort & de-dup
    out = out.drop_duplicates(subset=["date", "code"], keep="last")
    out = out.sort_values(["date", "code"], kind="mergesort").reset_index(drop=True)
    return out


def _merge_adj_factor(bars: pd.DataFrame, adj: pd.DataFrame) -> pd.DataFrame:
    if bars.empty:
        bars["adj_factor"] = pd.Series(dtype="float64")
        return bars
    if adj.empty:
        bars["adj_factor"] = pd.NA
        return bars

    # adj schema in QUANTAXIS: date, code, adj
    if "adj" in adj.columns and "adj_factor" not in adj.columns:
        adj = adj.rename(columns={"adj": "adj_factor"})

    if not {"date", "code", "adj_factor"}.issubset(set(adj.columns)):
        # don't hard fail; just omit
        bars["adj_factor"] = pd.NA
        return bars

    a = adj.loc[:, ["date", "code", "adj_factor"]].copy()
    a["date"] = pd.to_datetime(a["date"], errors="coerce")
    a = a.dropna(subset=["date", "code"])
    a["code"] = a["code"].astype(str)
    a["adj_factor"] = pd.to_numeric(a["adj_factor"], errors="coerce")
    a = a.drop_duplicates(subset=["date", "code"], keep="last")

    merged = bars.merge(a, on=["date", "code"], how="left")
    return merged


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--asof", required=True, help="snapshot label (YYYY-MM-DD or YYYYMMDD)")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD or YYYYMMDD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD or YYYYMMDD")
    ap.add_argument("--out", default=str(ROOT / "data" / "qa_cn_stock_daily" / "versions"), help="base output dir")
    ap.add_argument("--batch", type=int, default=20000, help="mongo batch size")
    ap.add_argument("--limit", type=int, default=None, help="debug: limit rows")
    ap.add_argument("--no-adj", action="store_true", help="do not attempt to join adj_factor")
    args = ap.parse_args(argv)

    asof = _parse_ymd(args.asof)
    start_date = _parse_ymd(args.start)
    end_date = _parse_ymd(args.end)

    base = Path(args.out)
    out_dir = base / asof
    out_dir.mkdir(parents=True, exist_ok=True)

    cfg = _get_mongo_cfg()
    client = _mongo_client(cfg)
    db = client[cfg.db]

    df_day = _load_stock_day_df(db, start_date, end_date, batch_size=args.batch, limit=args.limit)
    bars = _normalize_bars(df_day)

    adj_included = False
    if not args.no_adj:
        df_adj = _load_stock_adj_df(db, start_date, end_date, batch_size=args.batch)
        if not df_adj.empty:
            bars = _merge_adj_factor(bars, df_adj)
            adj_included = True
        else:
            bars["adj_factor"] = pd.NA

    # parquet write
    bars_path = out_dir / "bars.parquet"
    try:
        bars.to_parquet(bars_path, index=False)
    except Exception as e:  # noqa: BLE001
        raise SystemExit(
            f"failed to write parquet: {e}\n"
            "Tip: install pyarrow (recommended) or fastparquet."
        )

    files = [{"path": "bars.parquet", "sha256": _sha256_file(bars_path)}]

    # date range from data
    if not bars.empty:
        r_start = str(pd.to_datetime(bars["date"]).min().date())
        r_end = str(pd.to_datetime(bars["date"]).max().date())
        n_rows = int(len(bars))
        n_symbols = int(bars["code"].nunique())
    else:
        r_start, r_end, n_rows, n_symbols = start_date, end_date, 0, 0

    manifest: Dict[str, Any] = {
        "dataset_id": "qa_cn_stock_daily",
        "data_version_id": f"qa_cn_stock_daily@{asof}",
        "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "timezone": "Asia/Shanghai",
        "frequency": "1d",
        "market": "CN_A",
        "price": "raw",
        "adj_factor": "included" if adj_included else "missing_or_not_exported",
        "range": {"start": r_start, "end": r_end},
        "counts": {"rows": n_rows, "symbols": n_symbols},
        "schema": {
            "bars": [
                "date",
                "code",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "adj_factor",
            ]
        },
        "files": files,
    }
    manifest["manifest_sha256"] = _sha256_json(manifest)

    (out_dir / "manifest.json").write_bytes(_canonical_json_bytes(manifest) + b"\n")

    print(json.dumps({"ok": True, "out_dir": str(out_dir), "data_version_id": manifest["data_version_id"], "manifest_sha256": manifest["manifest_sha256"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
