#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Normalize stock_day.date to a single canonical format.

Why: mixed formats (YYYYMMDD ints/strings + YYYY-MM-DD strings) make every
factor/backtest query more complex and error-prone. This script migrates
"stock_day" docs to ISO date strings: YYYY-MM-DD.

- Safe-by-default: dry-run unless --apply is passed.
- Idempotent: re-running should result in 0 changes after convergence.

Example:
  python scripts/mongo_migrate_stock_day_dates.py --db quantaxis --apply

If you run Mongo in docker, pass --uri accordingly.
"""

from __future__ import annotations

import argparse
import datetime as dt
from typing import Any, Optional, Tuple

import pymongo


def _parse_to_iso_date(v: Any) -> Optional[str]:
    """Return YYYY-MM-DD or None if unparseable."""
    if v is None:
        return None

    # Mongo Date
    if isinstance(v, dt.datetime):
        return v.date().isoformat()
    if isinstance(v, dt.date):
        return v.isoformat()

    # int like 20260203
    if isinstance(v, int):
        s = str(v)
        if len(s) == 8 and s.isdigit():
            try:
                d = dt.date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
                return d.isoformat()
            except Exception:
                return None
        return None

    # string: 'YYYYMMDD' or 'YYYY-MM-DD'
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        if len(s) == 8 and s.isdigit():
            try:
                d = dt.date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
                return d.isoformat()
            except Exception:
                return None
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            # basic sanity
            try:
                d = dt.date(int(s[0:4]), int(s[5:7]), int(s[8:10]))
                return d.isoformat()
            except Exception:
                return None
        return None

    return None


def _connect(uri: str, db: str, user: str = "", password: str = "", authdb: str = "admin") -> pymongo.collection.Collection:
    if user and password:
        # Inject authSource if not present
        if "authSource=" not in uri:
            sep = "&" if "?" in uri else "?"
            uri = f"{uri}{sep}authSource={authdb}"
        client = pymongo.MongoClient(uri, username=user, password=password, serverSelectionTimeoutMS=8000)
    else:
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    return client[db]["stock_day"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", default="mongodb://localhost:27017", help="Mongo URI (unauthenticated)")
    ap.add_argument("--db", default="quantaxis", help="Database name")
    ap.add_argument("--user", default="", help="Mongo username (optional)")
    ap.add_argument("--password", default="", help="Mongo password (optional)")
    ap.add_argument("--authdb", default="admin", help="Mongo authSource DB (default: admin)")
    ap.add_argument("--apply", action="store_true", help="Actually write changes")
    ap.add_argument("--on-dup", default="skip", choices=["skip", "delete_source"], help="What to do if normalizing date causes duplicate key on (code,date). skip=leave as-is; delete_source=delete the doc being updated")
    ap.add_argument("--batch", type=int, default=2000)
    ap.add_argument("--limit", type=int, default=0, help="Limit docs scanned (0=all)")
    ap.add_argument("--progress", type=int, default=200000, help="Print progress every N scanned docs (0=disable)")
    args = ap.parse_args()

    coll = _connect(args.uri, args.db, user=args.user, password=args.password, authdb=args.authdb)

    q = {"date": {"$exists": True}}
    proj = {"_id": 1, "date": 1}

    cursor = coll.find(q, proj, no_cursor_timeout=True)
    if args.limit and args.limit > 0:
        cursor = cursor.limit(args.limit)

    scanned = 0
    changed = 0
    skipped = 0
    dup_conflicts = 0
    dup_deleted = 0

    ops = []

    try:
        for doc in cursor:
            scanned += 1
            if args.progress and scanned % int(args.progress) == 0:
                mode = "APPLY" if args.apply else "DRY-RUN"
                print(f"[{mode}] progress scanned={scanned} changed={changed} skipped={skipped} dup_conflicts={dup_conflicts} dup_deleted={dup_deleted}")
            old = doc.get("date")
            new = _parse_to_iso_date(old)
            if new is None:
                skipped += 1
                continue
            if isinstance(old, str) and old.strip() == new:
                continue
            # normalize everything to string
            ops.append(
                pymongo.UpdateOne({"_id": doc["_id"]}, {"$set": {"date": new}})
            )
            changed += 1

            if len(ops) >= args.batch:
                if args.apply:
                    try:
                        coll.bulk_write(ops, ordered=False)
                    except pymongo.errors.BulkWriteError as e:
                        # Most common case: normalizing YYYYMMDD -> YYYY-MM-DD may collide with an existing doc
                        # under unique index (code, date).
                        errs = e.details.get("writeErrors", []) if hasattr(e, "details") and e.details else []
                        dup_conflicts += len(errs)
                        if args.on_dup == "delete_source":
                            for we in errs:
                                op = we.get("op") or {}
                                q = op.get("q") or {}
                                _id = q.get("_id")
                                if _id is not None:
                                    coll.delete_one({"_id": _id})
                                    dup_deleted += 1
                        # else: skip the conflicting updates (leave old date as-is)
                ops = []

        if ops:
            if args.apply:
                try:
                    coll.bulk_write(ops, ordered=False)
                except pymongo.errors.BulkWriteError as e:
                    errs = e.details.get("writeErrors", []) if hasattr(e, "details") and e.details else []
                    dup_conflicts += len(errs)
                    if args.on_dup == "delete_source":
                        for we in errs:
                            op = we.get("op") or {}
                            q = op.get("q") or {}
                            _id = q.get("_id")
                            if _id is not None:
                                coll.delete_one({"_id": _id})
                                dup_deleted += 1
                    # skip otherwise

    finally:
        try:
            cursor.close()
        except Exception:
            pass

    mode = "APPLY" if args.apply else "DRY-RUN"
    extra = f" dup_conflicts={dup_conflicts} dup_deleted={dup_deleted}" if args.apply else ""
    print(f"[{mode}] scanned={scanned} changed={changed} skipped_unparseable={skipped}{extra}")
    if not args.apply:
        print("Pass --apply to write changes.")
    else:
        if dup_conflicts and args.on_dup == "skip":
            print("NOTE: duplicate-key conflicts were skipped; re-run with --on-dup delete_source to delete the conflicting docs being updated.")


if __name__ == "__main__":
    main()
