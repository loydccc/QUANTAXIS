#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Fetch A-share daily bars via pytdx (TDX) and write into MongoDB.

No API key required; suitable for "first real data" closed-loop.

Writes into quantaxis.stock_day with (code,date) unique index.

Env:
- MONGODB_HOST, MONGODB_PORT, MONGODB_DATABASE
- MONGODB_USER, MONGODB_PASSWORD
- MONGO_ROOT_USER, MONGO_ROOT_PASSWORD (fallback)

Usage:
  python scripts/fetch_tdx_stock_day.py --start 20240101 --end 20240131 --codes 000001,600000
  python scripts/fetch_tdx_stock_day.py --start 20240101 --end 20240131 --limit 200
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

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


def _ensure_indexes(coll: pymongo.collection.Collection) -> None:
    coll.create_index([("code", 1), ("date", 1)], unique=True)
    coll.create_index([("date", 1)])


def _chunks(xs: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(xs), n):
        yield xs[i : i + n]


def _market_from_code(code: str) -> int:
    # 1=SH, 0=SZ
    return 1 if code.startswith("6") else 0


def _candidate_tdx_servers() -> List[Tuple[str, int]]:
    # Allow override
    env_ip = os.getenv("TDX_IP")
    env_port = os.getenv("TDX_PORT")
    if env_ip and env_port:
        return [(env_ip, int(env_port))]

    # Static shortlist (avoid long probe loops / noisy BAD RESPONSE logs).
    # These are common public TDX HQ servers; reachability depends on network.
    return [
        ("119.147.212.81", 7709),
        ("119.147.212.81", 7721),
        ("119.147.212.81", 7727),
        ("119.147.164.60", 7709),
        ("113.105.73.88", 7709),
        ("114.80.80.222", 7709),
    ]


def _list_codes(api, limit: int) -> List[str]:
    # Pull codes from TDX itself: market 0(SZ) + 1(SH).
    # get_security_list returns up to 1000 per page.
    codes: List[str] = []
    for market in (0, 1):
        start = 0
        while len(codes) < limit:
            rows = api.get_security_list(market, start)
            if not rows:
                break
            for r in rows:
                c = str(r.get("code") or "").strip()
                if c and len(c) == 6 and c.isdigit():
                    codes.append(c)
                    if len(codes) >= limit:
                        break
            start += len(rows)
            if len(rows) < 1000:
                break
        if len(codes) >= limit:
            break
    # de-dup while preserving order
    seen = set()
    out = []
    for c in codes:
        if c not in seen:
            out.append(c)
            seen.add(c)
    return out[:limit]


def _fetch_daily_bars(api, code: str, max_bars: int = 6000) -> List[dict]:
    # category 9 = daily
    category = 9
    market = _market_from_code(code)

    all_bars: List[dict] = []
    start = 0
    page_size = 800
    while len(all_bars) < max_bars:
        bars = api.get_security_bars(category, market, code, start, page_size)
        if not bars:
            break
        all_bars.extend(bars)
        if len(bars) < page_size:
            break
        start += page_size
        # tiny delay to be polite
        time.sleep(0.02)

    return all_bars


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYYMMDD")
    ap.add_argument("--end", required=True, help="YYYYMMDD")
    ap.add_argument("--codes", default=None, help="comma-separated 6-digit codes, e.g. 000001,600000")
    ap.add_argument("--limit", type=int, default=200, help="limit number of symbols (when --codes not given)")
    ap.add_argument("--sleep", type=float, default=0.05, help="sleep between symbols")
    args = ap.parse_args(argv)

    try:
        from pytdx.hq import TdxHq_API
    except Exception as e:
        print(f"ERROR: pytdx not installed: {e}", file=sys.stderr)
        return 3

    cfg = _get_mongo_cfg()
    client = _mongo_client(cfg)
    db = client[cfg.db]
    coll = db["stock_day"]
    _ensure_indexes(coll)

    servers = _candidate_tdx_servers()
    print(f"[tdx] candidates={','.join([f'{ip}:{port}' for ip,port in servers])}")

    codes: List[str]
    if args.codes:
        codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    else:
        # list from TDX itself (requires a successful connect)
        pass

    # Disable auto_retry here; we want deterministic failures rather than long hangs.
    with TdxHq_API(heartbeat=False, auto_retry=False) as api:
        import signal

        class _Timeout(Exception):
            pass

        def _alarm_handler(signum, frame):
            raise _Timeout()

        connected = False
        old_handler = signal.signal(signal.SIGALRM, _alarm_handler)
        try:
            for ip, port in servers:
                try:
                    signal.alarm(3)
                    try:
                        ok = api.connect(ip, port, time_out=2)
                    except TypeError:
                        ok = api.connect(ip, port)
                    finally:
                        signal.alarm(0)

                    if ok:
                        connected = True
                        print(f"[tdx] connected={ip}:{port}")
                        break
                except _Timeout:
                    print(f"[tdx] connect timeout: {ip}:{port}")
                    connected = False
                except Exception:
                    connected = False
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)

        if not connected:
            raise SystemExit(
                "ERROR: failed to connect any TDX server. "
                "Your network may block public TDX servers. "
                "Try setting env TDX_IP/TDX_PORT to a reachable server in your network."
            )

        if not args.codes:
            codes = _list_codes(api, args.limit)

        print(f"[tdx] codes={len(codes)} start={args.start} end={args.end}")

        total_rows = 0
        total_upsert = 0
        for code in codes:
            bars = _fetch_daily_bars(api, code)
            if not bars:
                print(f"[tdx] {code}: no data")
                continue

            records = []
            for b in bars:
                # datetime like '2024-01-02 00:00:00'
                dt = str(b.get("datetime") or "")
                if not dt:
                    continue
                date = dt.split(" ", 1)[0]
                ymd = date.replace("-", "")
                if ymd < args.start or ymd > args.end:
                    continue

                records.append(
                    {
                        "code": code,
                        "date": date,
                        "open": b.get("open"),
                        "high": b.get("high"),
                        "low": b.get("low"),
                        "close": b.get("close"),
                        "vol": b.get("vol"),
                        "amount": b.get("amount"),
                        "source": "tdx",
                        "updated_at": int(time.time()),
                    }
                )

            if not records:
                print(f"[tdx] {code}: no bars in range")
                continue

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
