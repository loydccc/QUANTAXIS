#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Weekly turnover/stale/HI operational report.

Input:
- Scan output/signals/prod_signal_*.json

Output:
- output/reports/turnover_attrib/weekly_YYYY.csv

One row per rebalance as_of_date (latest sealed_date for that as_of_date).

This is observational only; it does not alter strategy behavior.
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIG_DIR = ROOT / "output" / "signals"
OUT_DIR = ROOT / "output" / "reports" / "turnover_attrib"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def load_json(p: str) -> dict | None:
    try:
        return json.loads(Path(p).read_text(encoding="utf-8"))
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=None, help="default: current year")
    args = ap.parse_args()

    year = args.year or datetime.now().year

    paths = sorted(glob.glob(str(SIG_DIR / "prod_signal_*.json")))
    paths = [p for p in paths if not p.endswith(".status.json")]

    rows_by_asof = {}
    for p in paths:
        obj = load_json(p)
        if not obj or obj.get("status") != "succeeded":
            continue
        m = obj.get("meta", {}) or {}
        ops = m.get("ops", {}) or {}
        sealed_date = ops.get("sealed_date")
        if not isinstance(sealed_date, str) or not sealed_date.startswith(f"{year}-"):
            continue
        as_of = obj.get("as_of_date")
        if not isinstance(as_of, str):
            continue

        # Keep latest sealed_date for this as_of bucket
        key = as_of
        prev = rows_by_asof.get(key)
        if prev is None or str(prev.get("sealed_date")) < sealed_date:
            rows_by_asof[key] = {
                "sealed_date": sealed_date,
                "as_of_date": as_of,
                "turnover_attrib": m.get("turnover_attrib") or {},
                "hold_smoothing": m.get("hold_smoothing") or {},
                "health": m.get("health") or {},
                "ladder": m.get("ladder") or {},
            }

    out_path = OUT_DIR / f"weekly_{year}.csv"
    fieldnames = [
        "sealed_date",
        "as_of_date",
        "turnover_2way_total",
        "turnover_2way_by_reason",
        "stale_weight_ratio",
        "n_stale_codes",
        "health_score",
        "exposure",
        "cash_weight",
        "level_used",
    ]

    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for as_of in sorted(rows_by_asof.keys()):
            r = rows_by_asof[as_of]
            ta = r["turnover_attrib"]
            hs = r["hold_smoothing"]
            h = r["health"]
            lad = r["ladder"]

            reasons = Counter()
            for x in (ta.get("entered") or []):
                reasons[str(x.get("reason"))] += float(x.get("new_weight") or 0.0)
            for x in (ta.get("exited") or []):
                reasons[str(x.get("reason"))] += float(x.get("old_weight") or 0.0)
            for x in (ta.get("kept") or []):
                # attribute absolute change to reason
                reasons[str(x.get("reason"))] += abs(float(x.get("new_weight") or 0.0) - float(x.get("old_weight") or 0.0))

            w.writerow(
                {
                    "sealed_date": r["sealed_date"],
                    "as_of_date": r["as_of_date"],
                    "turnover_2way_total": float(ta.get("turnover_2way") or 0.0),
                    "turnover_2way_by_reason": json.dumps(dict(reasons), ensure_ascii=False, sort_keys=True),
                    "stale_weight_ratio": float(hs.get("stale_weight_ratio") or 0.0),
                    "n_stale_codes": int(hs.get("n_stale_codes") or 0),
                    "health_score": h.get("health_score"),
                    "exposure": h.get("exposure"),
                    "cash_weight": h.get("cash_weight"),
                    "level_used": lad.get("level_used"),
                }
            )

    print(str(out_path))


if __name__ == "__main__":
    main()
