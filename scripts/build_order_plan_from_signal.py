#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build base/OMS order plan from a signal JSON.

Outputs:
- output/orders/order_plan_{trade_date}_from_signal_{signal_date}.json/csv
- output/orders/oms_order_plan_{trade_date}_from_signal_{signal_date}.json/csv
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import pymongo

ROOT = Path(__file__).resolve().parents[1]
SIGNALS_DIR = ROOT / "output" / "signals"
ORDERS_DIR = ROOT / "output" / "orders"


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
        f"mongodb://{cfg.host}:{cfg.port}/{cfg.db}",
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


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _find_latest_signal() -> Path:
    paths = sorted([p for p in glob.glob(str(SIGNALS_DIR / "prod_signal_*.json")) if not p.endswith(".status.json")])
    if not paths:
        raise FileNotFoundError("no signal json found under output/signals")
    return Path(paths[-1])


def _weights(sig: dict) -> Dict[str, float]:
    w: Dict[str, float] = {}
    for p in sig.get("positions", []) or []:
        c = str(p.get("code") or "")
        if not c:
            continue
        w[c] = float(p.get("weight", 0.0) or 0.0)
    if "CASH" not in w:
        s = sum(v for k, v in w.items() if k.upper() != "CASH")
        w["CASH"] = max(0.0, 1.0 - s)
    return w


def _fmtf(x: Any, nd: int) -> str:
    try:
        return f"{float(x):.{nd}f}"
    except Exception:
        return ""


def _load_prev_signal(meta: dict) -> Optional[dict]:
    ta = (meta.get("turnover_attrib") or {})
    prev_id = ta.get("prev_signal_id")
    if not prev_id:
        return None
    p = SIGNALS_DIR / f"{prev_id}.json"
    if not p.exists():
        return None
    try:
        return _read_json(p)
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--signal-path", default="", help="default: latest prod_signal_*.json")
    ap.add_argument("--lot-size", type=int, default=100)
    ap.add_argument("--mongo-host", default=os.getenv("MONGODB_HOST", "127.0.0.1"))
    ap.add_argument("--mongo-port", type=int, default=int(os.getenv("MONGODB_PORT", "27017")))
    ap.add_argument("--mongo-db", default=os.getenv("MONGODB_DATABASE", "quantaxis"))
    ap.add_argument("--mongo-user", default=os.getenv("MONGODB_USER", "quantaxis"))
    ap.add_argument("--mongo-password", default=os.getenv("MONGODB_PASSWORD", "quantaxis"))
    args = ap.parse_args()

    if args.signal_path:
        sp = Path(args.signal_path)
        signal_path = sp if sp.is_absolute() else (ROOT / sp)
    else:
        signal_path = _find_latest_signal()
    sig = _read_json(signal_path)
    meta = sig.get("meta", {}) or {}

    signal_date = str(((meta.get("ops", {}) or {}).get("sealed_date") or sig.get("as_of_date") or ""))
    if not signal_date:
        raise RuntimeError("cannot infer signal date")
    ref_price_date = signal_date

    prev_sig = _load_prev_signal(meta)
    prev_w = _weights(prev_sig or {})
    tgt_w = _weights(sig)

    aum = float(meta.get("aum_cny") or 200_000_000.0)
    codes = sorted({c for c in (set(prev_w) | set(tgt_w)) if c.upper() != "CASH"})
    orders: List[dict] = []
    for c in codes:
        p = float(prev_w.get(c, 0.0))
        t = float(tgt_w.get(c, 0.0))
        d = t - p
        if abs(d) <= 1e-12:
            continue
        orders.append(
            {
                "code": c,
                "action": "BUY" if d > 0 else "SELL",
                "delta_weight": float(d),
                "prev_weight": float(p),
                "target_weight": float(t),
                "est_notional_cny": float(abs(d) * aum),
            }
        )
    orders.sort(key=lambda x: (-float(x["est_notional_cny"]), x["code"]))

    cash_delta = float(tgt_w.get("CASH", 0.0) - prev_w.get("CASH", 0.0))
    base_json = {
        "trade_date": str(args.trade_date),
        "signal_path": str(signal_path.relative_to(ROOT)),
        "signal_id": sig.get("signal_id"),
        "signal_as_of_date": sig.get("as_of_date"),
        "prev_signal_id": ((meta.get("turnover_attrib") or {}).get("prev_signal_id")),
        "prev_as_of_date": ((meta.get("turnover_attrib") or {}).get("prev_as_of_date")),
        "aum_cny": aum,
        "orders": [
            {
                "code": "CASH",
                "action": "BUY" if cash_delta > 0 else "SELL",
                "delta_weight": cash_delta,
                "prev_weight": float(prev_w.get("CASH", 0.0)),
                "target_weight": float(tgt_w.get("CASH", 0.0)),
                "est_notional_cny": float(abs(cash_delta) * aum),
            },
            *orders,
        ],
    }

    ORDERS_DIR.mkdir(parents=True, exist_ok=True)
    base_stem = f"order_plan_{args.trade_date}_from_signal_{signal_date}"
    base_json_path = ORDERS_DIR / f"{base_stem}.json"
    base_csv_path = ORDERS_DIR / f"{base_stem}.csv"
    base_json_path.write_text(json.dumps(base_json, ensure_ascii=False, indent=2), encoding="utf-8")

    with base_csv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["code", "action", "prev_weight", "target_weight", "delta_weight", "est_notional_cny"])
        for o in orders:
            w.writerow(
                [
                    o["code"],
                    o["action"],
                    _fmtf(o["prev_weight"], 10),
                    _fmtf(o["target_weight"], 10),
                    _fmtf(o["delta_weight"], 10),
                    _fmtf(o["est_notional_cny"], 2),
                ]
            )

    cfg = MongoCfg(
        host=str(args.mongo_host),
        port=int(args.mongo_port),
        db=str(args.mongo_db),
        user=str(args.mongo_user),
        password=str(args.mongo_password),
        root_user=os.getenv("MONGO_ROOT_USER", "root"),
        root_password=os.getenv("MONGO_ROOT_PASSWORD", "root"),
    )
    client = _mongo_client(cfg)
    coll = client[cfg.db]["stock_day"]

    oms_orders: List[dict] = []
    missing_price_codes: List[str] = []
    for o in orders:
        code = str(o["code"])
        doc = coll.find_one({"code": code, "date": ref_price_date}, {"_id": 0, "close": 1})
        px = float(doc.get("close")) if (doc and doc.get("close") is not None) else None
        target_notional = float(o["est_notional_cny"])
        if (px is None) or (px <= 0):
            missing_price_codes.append(code)
            oms_orders.append(
                {
                    "trade_date": str(args.trade_date),
                    "code": code,
                    "action": str(o["action"]),
                    "ref_price_date": ref_price_date,
                    "ref_price_close": None,
                    "target_notional_cny": target_notional,
                    "raw_shares": 0.0,
                    "order_shares": 0,
                    "order_lots": 0,
                    "order_notional_cny": 0.0,
                    "lot_rounding_shortfall_cny": target_notional,
                    "delta_weight": float(o["delta_weight"]),
                    "prev_weight": float(o["prev_weight"]),
                    "target_weight": float(o["target_weight"]),
                    "skip": True,
                    "skip_reason": "missing_ref_price",
                }
            )
            continue

        raw_shares = target_notional / px
        order_shares = int(raw_shares // int(args.lot_size)) * int(args.lot_size)
        skip = order_shares <= 0
        skip_reason = "lot_too_small" if skip else ""
        order_notional = float(order_shares * px) if not skip else 0.0
        shortfall = float(target_notional - order_notional)
        oms_orders.append(
            {
                "trade_date": str(args.trade_date),
                "code": code,
                "action": str(o["action"]),
                "ref_price_date": ref_price_date,
                "ref_price_close": float(px),
                "target_notional_cny": target_notional,
                "raw_shares": float(raw_shares),
                "order_shares": int(order_shares),
                "order_lots": int(order_shares // int(args.lot_size)),
                "order_notional_cny": order_notional,
                "lot_rounding_shortfall_cny": shortfall,
                "delta_weight": float(o["delta_weight"]),
                "prev_weight": float(o["prev_weight"]),
                "target_weight": float(o["target_weight"]),
                "skip": bool(skip),
                "skip_reason": skip_reason,
            }
        )

    buy_target = sum(o["target_notional_cny"] for o in oms_orders if o["action"] == "BUY")
    sell_target = sum(o["target_notional_cny"] for o in oms_orders if o["action"] == "SELL")
    buy_exec = sum(o["order_notional_cny"] for o in oms_orders if o["action"] == "BUY" and not o["skip"])
    sell_exec = sum(o["order_notional_cny"] for o in oms_orders if o["action"] == "SELL" and not o["skip"])
    skipped_n = sum(1 for o in oms_orders if o["skip"])
    rounding_shortfall = sum(float(o["lot_rounding_shortfall_cny"]) for o in oms_orders)

    oms_json = {
        "summary": {
            "trade_date": str(args.trade_date),
            "source_plan": str(base_json_path.relative_to(ROOT)),
            "aum_cny": aum,
            "price_ref_date": ref_price_date,
            "stock_orders_n": len(oms_orders),
            "skipped_n": skipped_n,
            "missing_price_codes": sorted(set(missing_price_codes)),
            "buy_target_cny": buy_target,
            "sell_target_cny": sell_target,
            "buy_exec_cny": buy_exec,
            "sell_exec_cny": sell_exec,
            "net_sell_target_cny": sell_target - buy_target,
            "net_sell_exec_cny": sell_exec - buy_exec,
            "rounding_total_shortfall_cny": rounding_shortfall,
        },
        "orders": oms_orders,
    }

    oms_stem = f"oms_order_plan_{args.trade_date}_from_signal_{signal_date}"
    oms_json_path = ORDERS_DIR / f"{oms_stem}.json"
    oms_csv_path = ORDERS_DIR / f"{oms_stem}.csv"
    oms_json_path.write_text(json.dumps(oms_json, ensure_ascii=False, indent=2), encoding="utf-8")

    with oms_csv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "trade_date",
                "code",
                "action",
                "ref_price_date",
                "ref_price_close",
                "target_notional_cny",
                "raw_shares",
                "order_shares",
                "order_lots",
                "order_notional_cny",
                "lot_rounding_shortfall_cny",
                "delta_weight",
                "skip",
                "skip_reason",
            ]
        )
        for o in oms_orders:
            w.writerow(
                [
                    o["trade_date"],
                    o["code"],
                    o["action"],
                    o["ref_price_date"],
                    _fmtf(o["ref_price_close"], 4),
                    _fmtf(o["target_notional_cny"], 2),
                    _fmtf(o["raw_shares"], 2),
                    int(o["order_shares"]),
                    int(o["order_lots"]),
                    _fmtf(o["order_notional_cny"], 2),
                    _fmtf(o["lot_rounding_shortfall_cny"], 2),
                    _fmtf(o["delta_weight"], 10),
                    "true" if o["skip"] else "false",
                    o["skip_reason"],
                ]
            )

    print(
        json.dumps(
            {
                "ok": True,
                "trade_date": str(args.trade_date),
                "signal_path": str(signal_path),
                "order_plan_json": str(base_json_path),
                "order_plan_csv": str(base_csv_path),
                "oms_order_plan_json": str(oms_json_path),
                "oms_order_plan_csv": str(oms_csv_path),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
