#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build daily paper PnL ledger from production signals and close prices.

Definition (paper, close-to-close):
- Day T return uses holdings from signal sealed on previous trading day T-1.
- Cash leg return is 0.
- Equity rolls from an initial AUM (default from QUANTAXIS_AUM_CNY or 200,000,000).

Outputs:
- output/reports/pnl/paper_pnl_ledger.csv
- output/reports/pnl/paper_pnl_ledger.json
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Any

import pymongo

ROOT = Path(__file__).resolve().parents[1]
SIGNALS_DIR = ROOT / "output" / "signals"
OUT_DIR = ROOT / "output" / "reports" / "pnl"


def _to_code6(code: Any) -> str:
    s = str(code or "").strip()
    if not s:
        return s
    if s.isdigit():
        return s.zfill(6)
    return s


def _load_latest_signal_per_sealed_date(signal_prefix: str) -> list[dict]:
    out_by_date: dict[str, dict] = {}
    for p in SIGNALS_DIR.glob(f"{signal_prefix}*.json"):
        if str(p).endswith(".status.json"):
            continue
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if str(obj.get("status")) != "succeeded":
            continue
        meta = obj.get("meta", {}) or {}
        ops = meta.get("ops", {}) or {}
        sealed_date = ops.get("sealed_date")
        if not isinstance(sealed_date, str) or len(sealed_date) != 10:
            continue
        gen = int(obj.get("generated_at") or 0)
        prev = out_by_date.get(sealed_date)
        if prev is None or int(prev.get("generated_at") or 0) <= gen:
            out_by_date[sealed_date] = {
                "sealed_date": sealed_date,
                "generated_at": gen,
                "signal_id": str(obj.get("signal_id") or ""),
                "path": str(p),
                "obj": obj,
            }
    return [out_by_date[d] for d in sorted(out_by_date.keys())]


def _positions_map(signal_obj: dict) -> tuple[dict[str, float], float]:
    stock: dict[str, float] = {}
    cash_w = 0.0
    for p in signal_obj.get("positions", []) or []:
        code = str(p.get("code", "")).upper()
        try:
            w = float(p.get("weight", 0.0) or 0.0)
        except Exception:
            w = 0.0
        if w <= 0:
            continue
        if code == "CASH":
            cash_w += w
            continue
        stock[_to_code6(code)] = stock.get(_to_code6(code), 0.0) + w
    return stock, float(cash_w)


def _mongo_coll(args) -> pymongo.collection.Collection:
    host = str(args.mongo_host)
    port = int(args.mongo_port)
    dbn = str(args.mongo_db)
    user = str(args.mongo_user)
    pwd = str(args.mongo_password)
    uri = f"mongodb://{user}:{pwd}@{host}:{port}/{dbn}?authSource=admin"
    cli = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)
    cli.admin.command("ping")
    return cli[dbn]["stock_day"]


def _close_map(coll: pymongo.collection.Collection, date: str, codes: set[str]) -> dict[str, float]:
    if not codes:
        return {}
    out: dict[str, float] = {}
    q = {"date": date, "code": {"$in": sorted(codes)}}
    for d in coll.find(q, {"_id": 0, "code": 1, "close": 1}):
        code = _to_code6(d.get("code"))
        try:
            c = float(d.get("close"))
        except Exception:
            continue
        if c > 0:
            out[code] = c
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", default=None, help="YYYY-MM-DD")
    ap.add_argument("--end-date", default=None, help="YYYY-MM-DD")
    ap.add_argument("--initial-aum", type=float, default=float(os.getenv("QUANTAXIS_AUM_CNY", "200000000")))
    ap.add_argument("--signal-prefix", default="prod_signal_")
    ap.add_argument("--mongo-host", default=os.getenv("MONGODB_HOST", "127.0.0.1"))
    ap.add_argument("--mongo-port", default=os.getenv("MONGODB_PORT", "27017"))
    ap.add_argument("--mongo-db", default=os.getenv("MONGODB_DATABASE", "quantaxis"))
    ap.add_argument("--mongo-user", default=os.getenv("MONGODB_USER", "quantaxis"))
    ap.add_argument("--mongo-password", default=os.getenv("MONGODB_PASSWORD", "quantaxis"))
    args = ap.parse_args()

    signals = _load_latest_signal_per_sealed_date(str(args.signal_prefix))
    if len(signals) < 2:
        print(json.dumps({"ok": False, "reason": "need_at_least_two_signals"}))
        return 2

    start_date = str(args.start_date) if args.start_date else None
    end_date = str(args.end_date) if args.end_date else None

    coll = _mongo_coll(args)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, Any]] = []
    equity = float(args.initial_aum)
    missing_total = 0
    used_pairs = 0

    for i in range(1, len(signals)):
        prev = signals[i - 1]
        curr = signals[i]
        d0 = str(prev["sealed_date"])
        d1 = str(curr["sealed_date"])

        if start_date and d1 < start_date:
            continue
        if end_date and d1 > end_date:
            continue

        prev_sig = prev["obj"]
        stock_w, cash_w = _positions_map(prev_sig)
        gross_stock_w = float(sum(stock_w.values()))
        if not stock_w:
            day_ret = 0.0
            day_pnl = 0.0
            equity_next = equity
            rows.append(
                {
                    "date": d1,
                    "prev_date": d0,
                    "prev_signal_id": prev.get("signal_id"),
                    "signal_id": curr.get("signal_id"),
                    "gross_stock_weight": gross_stock_w,
                    "cash_weight": cash_w,
                    "stock_count": 0,
                    "missing_close_count": 0,
                    "missing_weight": 0.0,
                    "day_return": day_ret,
                    "day_pnl_cny": day_pnl,
                    "equity_cny": equity_next,
                    "cum_return": (equity_next / float(args.initial_aum) - 1.0),
                }
            )
            equity = equity_next
            used_pairs += 1
            continue

        codes = set(stock_w.keys())
        c0 = _close_map(coll, d0, codes)
        c1 = _close_map(coll, d1, codes)

        day_ret = 0.0
        missing_n = 0
        missing_w = 0.0
        for c, w in stock_w.items():
            p0 = c0.get(c)
            p1 = c1.get(c)
            if p0 is None or p1 is None or p0 <= 0 or p1 <= 0:
                missing_n += 1
                missing_w += float(w)
                continue
            day_ret += float(w) * (float(p1) / float(p0) - 1.0)

        missing_total += missing_n
        day_pnl = equity * day_ret
        equity_next = equity + day_pnl
        rows.append(
            {
                "date": d1,
                "prev_date": d0,
                "prev_signal_id": prev.get("signal_id"),
                "signal_id": curr.get("signal_id"),
                "gross_stock_weight": gross_stock_w,
                "cash_weight": cash_w,
                "stock_count": int(len(stock_w)),
                "missing_close_count": int(missing_n),
                "missing_weight": float(missing_w),
                "day_return": float(day_ret),
                "day_pnl_cny": float(day_pnl),
                "equity_cny": float(equity_next),
                "cum_return": float(equity_next / float(args.initial_aum) - 1.0),
            }
        )
        equity = equity_next
        used_pairs += 1

    csv_path = OUT_DIR / "paper_pnl_ledger.csv"
    json_path = OUT_DIR / "paper_pnl_ledger.json"

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "date",
                "prev_date",
                "prev_signal_id",
                "signal_id",
                "gross_stock_weight",
                "cash_weight",
                "stock_count",
                "missing_close_count",
                "missing_weight",
                "day_return",
                "day_pnl_cny",
                "equity_cny",
                "cum_return",
            ]
        )
        for r in rows:
            w.writerow(
                [
                    r["date"],
                    r["prev_date"],
                    r["prev_signal_id"],
                    r["signal_id"],
                    f"{float(r['gross_stock_weight']):.10f}",
                    f"{float(r['cash_weight']):.10f}",
                    int(r["stock_count"]),
                    int(r["missing_close_count"]),
                    f"{float(r['missing_weight']):.10f}",
                    f"{float(r['day_return']):.10f}",
                    f"{float(r['day_pnl_cny']):.2f}",
                    f"{float(r['equity_cny']):.2f}",
                    f"{float(r['cum_return']):.10f}",
                ]
            )

    latest = rows[-1] if rows else None
    summary = {
        "ok": True,
        "start_date": (rows[0]["date"] if rows else None),
        "end_date": (rows[-1]["date"] if rows else None),
        "n_days": int(len(rows)),
        "n_pairs_used": int(used_pairs),
        "missing_close_total": int(missing_total),
        "initial_aum": float(args.initial_aum),
        "final_equity": float(equity),
        "total_return": float(equity / float(args.initial_aum) - 1.0),
        "latest_day": latest,
        "csv": str(csv_path),
    }
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "ok": True,
                "csv": str(csv_path),
                "json": str(json_path),
                "n_days": int(len(rows)),
                "final_equity": float(equity),
                "total_return": float(equity / float(args.initial_aum) - 1.0),
                "latest_day": latest,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
