#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Diagnose empty_elig / score_valid failures in 2022-01-01~2022-04-15.

Does NOT change any strategy parameters.

Outputs:
- output/reports/ladder_audit/2022_empty_elig_daily_counts.csv
  date-level counts through eligibility gates
- output/reports/ladder_audit/2022_empty_elig_score_invalid_summary.csv
  per-date top invalid reasons (aggregated)

We focus on dates inside weeks that ended as L3 with reason=empty_elig.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import pymongo

ROOT = Path(__file__).resolve().parents[1]
OUTDIR = ROOT / "output" / "reports" / "ladder_audit"
OUTDIR.mkdir(parents=True, exist_ok=True)

START = "2022-01-01"
END = "2022-04-15"
HIST_START = "2019-01-01"
THEME = "a_ex_kcb_bse"

# Use exactly the same knobs as ladder audit script (do not change)
MIN_BARS = 800
LIQ_WINDOW = 20
LIQ_MIN_RATIO = 1.0

# factor windows
RET5 = 5
RET10 = 10
RET20 = 20
MAW = 60
VOLW = 20


def mongo() -> pymongo.MongoClient:
    uri = "mongodb://quantaxis:quantaxis@127.0.0.1:27017/quantaxis?authSource=admin"
    c = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)
    c.admin.command("ping")
    return c


def load_universe(db, theme: str) -> List[str]:
    theme = (theme or "all").strip()

    def _is_a_ex_kcb_bse(code: str) -> bool:
        if not code or len(code) != 6 or not code.isdigit():
            return False
        if code.startswith("688"):
            return False
        if code.startswith(("8", "4")):
            return False
        return code.startswith(("600", "601", "603", "605", "000", "001", "002", "003", "300", "301"))

    codes: set[str] = set()
    coll = db.get_collection("stock_list")
    try:
        n = coll.estimated_document_count()
    except Exception:
        n = 0

    if n and n > 0:
        for doc in coll.find({}, {"_id": 0, "code": 1, "ts_code": 1}):
            c = doc.get("code")
            if not c and doc.get("ts_code"):
                c = str(doc.get("ts_code")).split(".")[0]
            if c:
                codes.add(str(c).zfill(6))
    else:
        for c in db["stock_day"].distinct("code"):
            if c:
                codes.add(str(c).zfill(6))

    out = sorted([c for c in codes if _is_a_ex_kcb_bse(c)]) if theme == "a_ex_kcb_bse" else sorted(codes)
    if not out:
        raise RuntimeError("empty universe")
    return out


def fetch_long(coll, codes: List[str], start: str, end: str) -> pd.DataFrame:
    start2 = start.replace("-", "")
    end2 = end.replace("-", "")
    start_i = int(start2)
    end_i = int(end2)

    proj = {"_id": 0, "code": 1, "date": 1, "close": 1, "high": 1, "open": 1, "low": 1, "amount": 1}
    q = {
        "code": {"$in": codes},
        "$or": [
            {"date": {"$gte": start, "$lte": end}},
            {"date": {"$gte": start2, "$lte": end2}},
            {"date": {"$gte": start_i, "$lte": end_i}},
        ],
    }
    rows = list(coll.find(q, proj, no_cursor_timeout=True))
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("no data")
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["date"] = pd.to_datetime(df["date"].astype(str), format="mixed", errors="coerce")
    df = df.dropna(subset=["date", "code"])
    for c in ["open", "high", "low", "close", "amount"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.sort_values(["code", "date"]).drop_duplicates(subset=["code", "date"], keep="last")
    return df


def panel(df: pd.DataFrame, col: str) -> pd.DataFrame:
    return df.pivot(index="date", columns="code", values=col).sort_index()


def invalid_reasons_for_date(d: pd.Timestamp, close: pd.DataFrame, high: pd.DataFrame, amt: pd.DataFrame, codes: List[str]) -> Dict[str, int]:
    """Compute why codes are invalid for scoring on date d (candidate pool = all eligible codes for debug).

    Reasons:
    - price_missing
    - amount_missing_or_zero
    - rolling_window_not_ready
    - div0_or_inf
    - nan_factor
    """

    out: Dict[str, int] = {}

    def bump(k: str, n: int = 1):
        out[k] = out.get(k, 0) + n

    # minimal prerequisites
    if d not in close.index:
        return {"date_missing": len(codes)}

    # history slices
    hist = close.loc[:d]
    if hist.shape[0] < 260:
        bump("rolling_window_not_ready", len(codes))
        return out

    c = close.loc[d, codes]
    a = amt.loc[d, codes] if d in amt.index else pd.Series(index=codes, dtype=float)

    # price missing
    pm = c.isna().sum()
    if pm:
        bump("price_missing", int(pm))

    # amount missing/zero
    if not a.empty:
        am = (a.isna() | (a <= 0)).sum()
        if am:
            bump("amount_missing_or_zero", int(am))

    # rolling readiness for factors
    need = max(RET20 + 1, MAW + 1, VOLW + 1, 252 + 1, 60 + 1)
    if hist.shape[0] < need:
        bump("rolling_window_not_ready", len(codes))
        return out

    # compute a few factors and see invalids
    # returns
    r20 = c / hist.iloc[-1 - RET20] - 1.0
    ma60 = c / hist.tail(MAW).mean() - 1.0

    # dist_252h
    hh = high.loc[:d, codes].tail(252)
    hi = hh.max(axis=0)
    dist = c / hi - 1.0

    # downvol
    ret = hist.pct_change(fill_method=None).tail(VOLW)
    dv = ret.where(ret < 0).std(axis=0, ddof=1)

    # classify
    for s in [r20, ma60, dist, dv]:
        bad = (~np.isfinite(s)).sum()
        if bad:
            bump("div0_or_inf", int(bad))

    nan_bad = (r20.isna() | ma60.isna() | dist.isna() | dv.isna()).sum()
    if nan_bad:
        bump("nan_factor", int(nan_bad))

    return out


def main():
    c = mongo()
    db = c["quantaxis"]
    coll = db["stock_day"]

    codes = load_universe(db, THEME)
    df = fetch_long(coll, codes, HIST_START, END)

    close = panel(df, "close")
    high = panel(df, "high")
    amt = panel(df, "amount")

    # Align panels
    idx = close.index.sort_values()
    high = high.reindex(index=idx)
    amt = amt.reindex(index=idx)

    idx_eval = idx[(idx >= pd.to_datetime(START)) & (idx <= pd.to_datetime(END))]

    # eligibility gates
    bars = close.notna().cumsum(axis=0)
    bars_ok = bars >= int(MIN_BARS)

    win = int(LIQ_WINDOW)
    need = int(np.floor(win * float(LIQ_MIN_RATIO) + 1e-9))
    close_ok = close.notna().rolling(win, min_periods=win).sum() >= need
    amt_ok = (amt.fillna(0.0) > 0).rolling(win, min_periods=win).sum() >= need

    rows = []
    invalid_rows = []

    for d in idx_eval:
        b0 = bars_ok.loc[d]
        n_bars = int(b0.sum())
        b1 = b0 & close_ok.loc[d]
        n_closew = int(b1.sum())
        b2 = b1 & amt_ok.loc[d]
        n_amtw = int(b2.sum())

        empty_step = None
        if n_bars == 0:
            empty_step = "min_bars"
        elif n_closew == 0:
            empty_step = "close_window"
        elif n_amtw == 0:
            empty_step = "amount_window"

        rows.append(
            {
                "date": str(d.date()),
                "n_min_bars": n_bars,
                "n_after_close_window": n_closew,
                "n_after_amount_window": n_amtw,
                "empty": bool(n_amtw == 0),
                "empty_step": empty_step,
            }
        )

        if n_amtw == 0:
            # also compute score invalid reasons on a broader set: after close window but before amount window
            codes_dbg = list(b1[b1].index)
            inv = invalid_reasons_for_date(d, close, high, amt, codes_dbg)
            invalid_rows.append({"date": str(d.date()), **inv})

    out_counts = pd.DataFrame(rows)
    out_counts.to_csv(OUTDIR / "2022_empty_elig_daily_counts.csv", index=False)

    if invalid_rows:
        inv_df = pd.DataFrame(invalid_rows).fillna(0)
        # compute top reason share
        reason_cols = [c for c in inv_df.columns if c != "date"]
        inv_df["top_reason"] = inv_df[reason_cols].idxmax(axis=1)
        inv_df["top_reason_count"] = inv_df[reason_cols].max(axis=1)
        inv_df["total"] = inv_df[reason_cols].sum(axis=1)
        inv_df["top_reason_share"] = inv_df["top_reason_count"] / inv_df["total"].replace(0, np.nan)
        inv_df.to_csv(OUTDIR / "2022_empty_elig_score_invalid_summary.csv", index=False)

    print(json.dumps({
        "outdir": str(OUTDIR),
        "counts_csv": "2022_empty_elig_daily_counts.csv",
        "invalid_csv": "2022_empty_elig_score_invalid_summary.csv",
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
