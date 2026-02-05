#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Extract root-cause table for the 8 tail tranches (<6 effective positions).

Per user instruction:
- No parameter change.
- Re-run ONLY the needed daily endpoints with a new audit version, so meta contains
  N1/N2/N3 + invalid reason fields.

Outputs a CSV to output/reports/tranche_audit/tail8_tranche_causes.csv
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
import sys

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

OUTDIR = ROOT / "output" / "reports" / "tranche_audit"
OUTDIR.mkdir(parents=True, exist_ok=True)

FALLBACK_ASSET = "510300"


def _load(p: Path):
    return json.loads(p.read_text(encoding="utf-8"))


def _status_path(sid: str) -> Path:
    return ROOT / "output" / "signals" / f"{sid}.status.json"


def _signal_path(sid: str) -> Path:
    return ROOT / "output" / "signals" / f"{sid}.json"


def main():
    theme = os.getenv("THEME", "a_ex_kcb_bse")

    # Identify the 8 tail tranche instances from v2 outputs (already computed truth window)
    tails = []
    for stp in (ROOT / "output" / "signals").glob("daily_2022????_fb_v2.status.json"):
        st = _load(stp)
        if st.get("status") != "succeeded":
            continue
        sid = stp.name.replace(".status.json", "")
        p_json = _signal_path(sid)
        if not p_json.exists():
            sp = _status_path(sid)
            if sp.exists():
                st = _load(sp)
                raise RuntimeError(f"missing json for tail source {sid} status={st}")
            raise FileNotFoundError(str(p_json))
        obj = _load(p_json)
        asof = obj.get("as_of_date")
        meta = obj.get("meta", {}) or {}
        ladder = meta.get("ladder", {}) or {}
        used = ladder.get("level_used")
        runs = ladder.get("runs") or []
        run_used = None
        for r in runs:
            if r.get("level") == used:
                run_used = r
                break
        if run_used is None and runs:
            run_used = runs[-1]
        for t in (run_used or {}).get("tranche_audit") or []:
            eff = int(t.get("effective_positions_after_min_weight", 0))
            if eff < 6:
                # end date can be parsed from sid
                end_date = sid.split("_")[1]
                end_date = f"{end_date[0:4]}-{end_date[4:6]}-{end_date[6:8]}"
                tails.append({"end_date": end_date, "tranche": int(t.get("tranche")), "as_of": asof})

    # unique by (end_date,tranche)
    uniq = {}
    for r in tails:
        uniq[(r["end_date"], r["tranche"])] = r
    tails = [uniq[k] for k in sorted(uniq.keys())]

    # Ensure Mongo is reachable on host (compose hostname 'mongodb' is not resolvable).
    # Force host settings (do not use setdefault) because some environments export MONGODB_HOST=mongodb.
    os.environ["MONGODB_HOST"] = "127.0.0.1"
    os.environ["MONGODB_PORT"] = "27017"
    os.environ["MONGODB_DATABASE"] = "quantaxis"
    os.environ["MONGODB_USER"] = "quantaxis"
    os.environ["MONGODB_PASSWORD"] = "quantaxis"
    # legacy env aliases used by some scripts
    os.environ["MONGO_HOST"] = "127.0.0.1"
    os.environ["MONGO_PORT"] = "27017"
    os.environ["MONGO_DATABASE"] = "quantaxis"
    os.environ["MONGO_USER"] = "quantaxis"
    os.environ["MONGO_PASSWORD"] = "quantaxis"

    # Re-run ONLY these days with audit version v3
    from api.signals_impl import run_signal

    cfg_base = {
        "strategy": "hybrid_baseline_weekly_topk",
        "theme": theme,
        "rebalance": "weekly",
        "top_k": 20,
        "candidate_k": 100,
        "min_bars": 800,
        "liq_window": 20,
        "liq_min_ratio": 1.0,
        "hold_weeks": 2,
        "tranche_overlap": True,
        "ma_mode": "filter",
        "score_mode": "factor",
        # alpha weights (current)
        "score_w_ret_20d": -1.0,
        "score_w_ret_10d": -0.5,
        "score_w_ret_5d": -0.2,
        "score_w_ma_60d": 0.3,
        "score_w_vol_20d": -0.5,
        "score_w_liq_20d": 0.0,
        "execution_mode": "naive",
        "backup_k": 50,
        "min_weight": 0.04,
        # hard bottom-line
        "hard_dist_252h_min": -0.4,
        "hard_downvol_q": 0.70,
        "fallback_asset": FALLBACK_ASSET,
        "start": "2019-01-01",
        # end filled per day
    }

    out_rows = []

    for r in tails:
        end_date = r["end_date"]
        tranche_id = int(r["tranche"])
        run_tag = os.getenv("RUN_TAG", "")
        # Use a new versioned id; if prior attempts failed we want a clean rerun without deleting files.
        sid = f"tail_{end_date.replace('-', '')}_t{tranche_id}_v4" + (f"_{run_tag}" if run_tag else "")

        sp = _status_path(sid)
        p_json = _signal_path(sid)

        need_run = True
        if sp.exists():
            st = _load(sp)
            if st.get("status") == "succeeded" and p_json.exists():
                need_run = False

        if need_run:
            cfg = dict(cfg_base)
            cfg["end"] = end_date
            run_signal(sid, cfg)

        if not p_json.exists():
            if sp.exists():
                st = _load(sp)
                raise RuntimeError(f"signal missing json: {sid} status={st}")
            raise FileNotFoundError(str(p_json))
        obj = _load(p_json)
        meta = obj.get("meta", {}) or {}
        ladder = meta.get("ladder", {}) or {}
        used = ladder.get("level_used")
        runs = ladder.get("runs") or []
        run_used = None
        for rr in runs:
            if rr.get("level") == used:
                run_used = rr
                break
        if run_used is None and runs:
            run_used = runs[-1]

        # hard filter stats per tranche
        hfs = (run_used or {}).get("hard_filter_stats") or []
        hrow = None
        for x in hfs:
            if int(x.get("tranche", -1)) == tranche_id:
                hrow = x
                break

        # tranche audit per tranche
        ta = (run_used or {}).get("tranche_audit") or []
        trow = None
        for x in ta:
            if int(x.get("tranche", -1)) == tranche_id:
                trow = x
                break

        out_rows.append(
            {
                "date": end_date,
                "tranche_id": tranche_id,
                "ladder_level_used": used,
                "N1_after_dist": (hrow or {}).get("N1_after_dist"),
                "N2_after_downvol_hard": (hrow or {}).get("N2_after_downvol_hard"),
                "N3_after_score_valid": (hrow or {}).get("N3_after_score_valid"),
                "effective_positions_after_min_weight": (trow or {}).get("effective_positions_after_min_weight"),
                "sum_weight_after_min_weight": (trow or {}).get("sum_weight_after_min_weight"),
                "top1_invalid_reason": (hrow or {}).get("top1_invalid_reason"),
            }
        )

    df = pd.DataFrame(out_rows).sort_values(["date", "tranche_id"]).reset_index(drop=True)
    out_csv = OUTDIR / "tail8_tranche_causes.csv"
    df.to_csv(out_csv, index=False)
    print(str(out_csv))
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
