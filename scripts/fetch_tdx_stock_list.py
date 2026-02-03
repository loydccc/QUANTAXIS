#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Build CN A-share stock_list from TDX (pytdx) without any API key.

Goal:
- Populate Mongo `stock_list` with **沪深主板 + 创业板** codes
- Exclude: 科创板(688) and BSE/NQ style codes (8xxxx/4xxxx)

This is used as the universe source for large-scale imports.

Env:
- Optional: TDX_IP, TDX_PORT
- Mongo: MONGODB_HOST, MONGODB_PORT, MONGODB_DATABASE, MONGODB_USER, MONGODB_PASSWORD

Usage:
  python scripts/fetch_tdx_stock_list.py
"""

from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

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


def _candidate_tdx_servers() -> List[Tuple[str, int]]:
    env_ip = os.getenv("TDX_IP")
    env_port = os.getenv("TDX_PORT")
    if env_ip and env_port:
        return [(env_ip, int(env_port))]
    return [
        ("119.147.212.81", 7709),
        ("119.147.164.60", 7709),
        ("113.105.73.88", 7709),
        ("114.80.80.222", 7709),
    ]


def _is_a_ex_kcb_bse(code: str) -> bool:
    if not code or len(code) != 6 or not code.isdigit():
        return False
    # Exclude STAR
    if code.startswith("688"):
        return False
    # Exclude BSE/NQ style
    if code.startswith(("8", "4")):
        return False
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


def _connect_tdx():
    try:
        from pytdx.hq import TdxHq_API
    except Exception as e:
        raise RuntimeError(f"pytdx not installed: {e}")

    api = TdxHq_API(heartbeat=True)
    for ip, port in _candidate_tdx_servers():
        try:
            ok = api.connect(ip, port)
            if ok:
                return api, (ip, port)
        except Exception:
            continue
    raise RuntimeError("cannot connect to any TDX server")


def _list_codes(api, market: int) -> List[str]:
    # market: 0=SZ, 1=SH
    codes: List[str] = []
    start = 0
    while True:
        rows = api.get_security_list(market, start)
        if not rows:
            break
        for r in rows:
            c = str(r.get("code") or "").strip()
            if c and len(c) == 6 and c.isdigit():
                codes.append(c)
        start += len(rows)
        # safety break
        if len(rows) < 1000:
            break
    return codes


def main(argv: Optional[List[str]] = None) -> int:
    cfg = _get_mongo_cfg()
    client = _mongo_client(cfg)
    db = client[cfg.db]
    coll = db["stock_list"]
    coll.create_index([("code", 1)], unique=True)
    coll.create_index([("ts_code", 1)])

    api, addr = _connect_tdx()
    ip, port = addr
    print(f"[tdx] connected {ip}:{port}")

    try:
        sz = _list_codes(api, 0)
        sh = _list_codes(api, 1)
    finally:
        try:
            api.disconnect()
        except Exception:
            pass

    all_codes = sorted(set(sz + sh))
    kept = [c for c in all_codes if _is_a_ex_kcb_bse(c)]

    now = int(time.time())
    ops = []
    for c in kept:
        # best-effort suffix
        suffix = ".SH" if c.startswith(("600", "601", "603", "605", "688")) else ".SZ"
        ops.append(
            pymongo.UpdateOne(
                {"code": c},
                {
                    "$set": {
                        "code": c,
                        "ts_code": f"{c}{suffix}",
                        "source": "tdx",
                        "updated_at": now,
                    }
                },
                upsert=True,
            )
        )

    if not ops:
        print("[stock_list] nothing to write")
        return 0

    res = coll.bulk_write(ops, ordered=False)
    print(
        f"[stock_list] total_raw={len(all_codes)} kept={len(kept)} "
        f"upserted={res.upserted_count} modified={res.modified_count} matched={res.matched_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
