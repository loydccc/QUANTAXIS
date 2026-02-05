#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Compute L3 primary-cause attribution from ladder_audit outputs.

Primary cause = first step where count drops below MIN_POS (6):
- dist (N1_after_dist)
- downvol (N2_after_downvol_hard) [L0 only]
- score_invalid (N3_after_score_valid)
- min_weight (N4_after_min_weight)
- cap (N5_after_cap)

This uses weekly audits and reports counts by week and overall shares.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
AUDIT = ROOT / "output" / "reports" / "ladder_audit" / "2022-01-01_2022-10-31_weekly_audits_fb.json"
OUT = ROOT / "output" / "reports" / "ladder_audit" / "2022-01-01_2022-10-31_attrib.csv"

MIN_POS = 6


def load_df() -> pd.DataFrame:
    obj = json.loads(AUDIT.read_text(encoding="utf-8"))
    # pandas to_json default orient=columns
    df = pd.DataFrame({k: pd.Series(v) for k, v in obj.items()})
    return df


def primary_cause(row) -> str:
    # empty_elig etc
    if isinstance(row.get("reason"), str) and row.get("reason"):
        return row.get("reason")

    runs = row.get("ladder_runs") or []
    # find L0 run for N1/N2/N3; L2 for N5
    r0 = None
    r2 = None
    for r in runs:
        if r.get("level") == "L0":
            r0 = r
        if r.get("level") == "L2":
            r2 = r
    if not r0:
        return "unknown"

    # stepwise
    n1 = r0.get("N1_after_dist")
    n2 = r0.get("N2_after_downvol_hard")
    n3 = r0.get("N3_after_score_valid")
    n4 = r0.get("N4_after_min_weight")
    n5 = r2.get("N5_after_cap") if r2 else None

    # first drop below MIN_POS
    for name, val in [
        ("dist", n1),
        ("downvol", n2),
        ("score_invalid", n3),
        ("min_weight", n4),
        ("cap", n5),
    ]:
        if val is None:
            continue
        try:
            if int(val) < MIN_POS:
                return name
        except Exception:
            continue

    return "other"


def main():
    df = load_df().sort_values("date")
    df["primary_cause"] = df.apply(primary_cause, axis=1)
    # only weeks that ended in L3
    l3 = df[df["level_used"] == "L3"].copy()
    # summary
    summ = l3["primary_cause"].value_counts().reset_index()
    summ.columns = ["cause", "weeks"]
    summ["share"] = summ["weeks"] / max(1, int(l3.shape[0]))

    # per-week table
    out = l3[["date", "level_used", "fallback_weight", "primary_cause", "reason"]].copy()
    out.to_csv(OUT, index=False)
    print("wrote", str(OUT))
    print(summ.to_string(index=False))


if __name__ == "__main__":
    main()
