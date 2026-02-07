#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Evaluate HI alerts from cached health_score files.

This does NOT send messages (provider-specific). It outputs a JSON report that
an external scheduler can use to page.

Inputs:
- output/reports/health_index/daily/health_score_YYYY-MM-DD.json

Outputs:
- output/reports/health_index/alerts_status.json

Alerts implemented:
1) health_score<0.2 streak>=3
2) pct_exposure_at_floor rolling20 > 0.60

Other operational alerts are evaluated in the signal/shadow runner.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DAILY = ROOT / "output" / "reports" / "health_index" / "daily"
OUT = ROOT / "output" / "reports" / "health_index" / "alerts_status.json"


def main():
    files = sorted(DAILY.glob("health_score_*.json"))
    rows = []
    for p in files:
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        d = obj.get("date")
        hs = obj.get("health_score")
        if d is None:
            continue
        rows.append({"date": d, "health_score": hs})

    df = pd.DataFrame(rows)
    if df.empty:
        OUT.write_text(json.dumps({"status": "no_data"}, ensure_ascii=False, indent=2), encoding="utf-8")
        print(str(OUT))
        return

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    df["health_score"] = pd.to_numeric(df["health_score"], errors="coerce")
    df["exposure"] = df["health_score"].clip(lower=0.4, upper=1.0)
    df["at_floor"] = (df["exposure"].round(10) == 0.4)

    # alert1
    bad = df["health_score"] < 0.2
    streak = 0
    max_streak = 0
    for v in bad.fillna(False).tolist():
        streak = streak + 1 if v else 0
        max_streak = max(max_streak, streak)

    # alert2
    roll = df["at_floor"].rolling(20, min_periods=20).mean()
    alert2 = bool((roll > 0.60).iloc[-1]) if len(roll.dropna()) else False

    out = {
        "as_of": str(df["date"].iloc[-1].date()),
        "alert1_health_lt_0_2_streak_ge_3": bool(max_streak >= 3),
        "alert1_max_streak": int(max_streak),
        "alert2_floor_roll20_gt_60pct": alert2,
        "alert2_roll20": float(roll.iloc[-1]) if len(roll.dropna()) else None,
    }

    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(OUT))


if __name__ == "__main__":
    main()
