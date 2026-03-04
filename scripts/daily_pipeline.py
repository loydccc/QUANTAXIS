#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Daily pipeline v1: ingest -> validate -> seal -> (optional) HI -> (optional) signal.

Fixed ordering and degradation rules:
- Only sealed_ok=true allows HI cache + signal.
- If not sealed: do not compute HI, do not run new signal.

This is the minimal production-grade pipeline.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# ensure repo root on import path so `import api.*` works when running from scripts/
sys.path.insert(0, str(ROOT))


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return float(default)
    return float(v)


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    if v is None:
        return str(default)
    s = str(v).strip()
    return s if s else str(default)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--theme", default="a_ex_kcb_bse")
    ap.add_argument("--run-hi", action="store_true")
    ap.add_argument("--run-signal", action="store_true")
    ap.add_argument("--skip-ingest", action="store_true", help="for dry-run acceptance: do not call ingest step")
    ap.add_argument("--shadow", action="store_true", help="shadow-run mode: never execute orders (pipeline remains dry-run)")
    ap.add_argument("--signal-theme", default="a_ex_kcb_bse")
    ap.add_argument("--signal-top-k", type=int, default=20)
    ap.add_argument("--mongo-host", default="127.0.0.1")
    ap.add_argument("--mongo-port", default="27017")
    ap.add_argument("--mongo-db", default="quantaxis")
    ap.add_argument("--mongo-user", default="quantaxis")
    ap.add_argument("--mongo-password", default="quantaxis")
    args = ap.parse_args()

    env = os.environ.copy()
    # Explicitly set Mongo target for this pipeline run (do not inherit accidental defaults like host=mongodb).
    env["MONGODB_HOST"] = str(args.mongo_host)
    env["MONGODB_PORT"] = str(args.mongo_port)
    env["MONGODB_DATABASE"] = str(args.mongo_db)
    env["MONGODB_USER"] = str(args.mongo_user)
    env["MONGODB_PASSWORD"] = str(args.mongo_password)
    # ensure in-process modules (run_signal) see the same env
    os.environ.update(env)

    # A) ingest
    if not args.skip_ingest:
        subprocess.check_call(["python3", "scripts/ingest_daily_market_data.py", "--date", args.date, "--theme", args.theme], cwd=str(ROOT), env=env)

    # B) validate
    v = subprocess.run(["python3", "scripts/validate_daily_data.py", "--date", args.date], cwd=str(ROOT), env=env, capture_output=True, text=True)
    if v.returncode not in (0, 2):
        raise RuntimeError((v.stderr or v.stdout or "validate error")[-2000:])
    validate_json = (v.stdout or "").strip().splitlines()[-1]
    obj = json.loads(validate_json)

    # C) seal
    s = subprocess.run(["python3", "scripts/seal_trading_day.py", "--date", args.date, "--validate-json", validate_json], cwd=str(ROOT), env=env, capture_output=True, text=True)
    sealed_ok = s.returncode == 0
    sealed_doc = json.loads((s.stdout or "{}").strip().splitlines()[-1]) if (s.stdout or "").strip() else None

    if not sealed_ok:
        # degradation: do not run HI/signal
        print(json.dumps({"date": args.date, "sealed_ok": False, "action": "HOLD_PREV"}, ensure_ascii=False))
        raise SystemExit(2)

    # Optional: compute daily HI cache (single-day)
    if args.run_hi:
        subprocess.check_call(["python3", "scripts/health_index_daily_cache.py", "--date", args.date], cwd=str(ROOT), env=env)

    # Optional: run signal (minimal v1): fixed cfg + embed sealed_date into meta
    if args.run_signal:
        import time
        from api.signals_impl import run_signal

        signal_id = f"prod_signal_{args.date.replace('-', '')}_{int(time.time())}"
        data_etag = str((sealed_doc or {}).get("etag") or obj.get("etag") or "")
        cfg = {
            "strategy": "hybrid_baseline_weekly_topk",
            "shadow": bool(args.shadow),
            "execution_mode": "shadow" if args.shadow else "naive",
            "theme": args.signal_theme,
            "rebalance": "weekly",
            "top_k": int(args.signal_top_k),
            "candidate_k": 100,
            "min_bars": 800,
            "liq_window": 20,
            "liq_min_ratio": 1.0,
            "hold_weeks": 2,
            "tranche_overlap": True,
            "ma_mode": "filter",
            "score_mode": "factor",
            "min_weight": 0.04,
            "score_temp": _env_float("QUANTAXIS_SCORE_TEMP", 0.35),
            "max_name_weight": _env_float("QUANTAXIS_MAX_NAME_WEIGHT", 1.0 / 6.0),
            "min_trade_weight": _env_float("QUANTAXIS_MIN_TRADE_WEIGHT", 0.005),
            "rebalance_trigger_mode": _env_str("QUANTAXIS_REBALANCE_TRIGGER_MODE", "event_only"),
            "rebalance_drift_min_turnover_2way": _env_float("QUANTAXIS_REBALANCE_DRIFT_MIN_TURNOVER_2WAY", 0.03),
            "rebalance_drift_max_cost_bps": _env_float("QUANTAXIS_REBALANCE_DRIFT_MAX_COST_BPS", 25.0),
            "hard_dist_252h_min": -0.4,
            "hard_downvol_q": 0.70,
            "aum_cny": _env_float("QUANTAXIS_AUM_CNY", 200_000_000.0),
            "adv_participation_max": _env_float("QUANTAXIS_ADV_PARTICIPATION_MAX", 0.02),
            "impact_k": _env_float("QUANTAXIS_IMPACT_K", 0.01),
            "impact_alpha": _env_float("QUANTAXIS_IMPACT_ALPHA", 0.70),
            "impact_liq_floor": _env_float("QUANTAXIS_IMPACT_LIQ_FLOOR", 1_000_000.0),
            "impact_cost_budget_bps": _env_float("QUANTAXIS_IMPACT_COST_BUDGET_BPS", 25.0),
            "fee_bps": _env_float("QUANTAXIS_FEE_BPS", 8.0),
            "score_w_ret_20d": _env_float("QUANTAXIS_SCORE_W_RET_20D", 1.0),
            "score_w_ret_10d": _env_float("QUANTAXIS_SCORE_W_RET_10D", 0.5),
            "score_w_ret_5d": _env_float("QUANTAXIS_SCORE_W_RET_5D", 0.2),
            "score_w_ma_60d": _env_float("QUANTAXIS_SCORE_W_MA_60D", 0.2),
            "score_w_vol_20d": _env_float("QUANTAXIS_SCORE_W_VOL_20D", -0.5),
            "score_w_liq_20d": _env_float("QUANTAXIS_SCORE_W_LIQ_20D", 0.2),
            "fallback_asset": "510300",
            "start": "2019-01-01",
            "end": args.date,
            "health_date": args.date,
            "sealed_date": args.date,
            # Unified reproducibility contract with /run.
            "data_version_id": f"ops_data_status@{args.date}",
            "manifest_sha256": data_etag,
        }
        run_signal(signal_id, cfg)

        # Patch meta.ops fields without changing positions.
        sig_path = ROOT / "output" / "signals" / f"{signal_id}.json"
        sig = json.loads(sig_path.read_text(encoding="utf-8"))
        sig.setdefault("meta", {})
        sig["meta"].setdefault("ops", {})
        sig["meta"]["ops"].update(
            {
                "sealed_date": args.date,
                "sealed_ok": True,
                "data_etag": (sealed_doc or {}).get("etag"),
            }
        )
        sig_path.write_text(json.dumps(sig, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({"date": args.date, "sealed_ok": True, "validate": obj.get("counts")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
