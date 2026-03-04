#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shadow-run single-day entrypoint (production hardening).

Spec (fixed):
- ONLY shadow-run (no execution / no orders).
- Fixed sequence: daily_pipeline --run-hi --run-signal --shadow -> assertions.
- Stdout: one-line JSON with {date, shadow, sealed_ok, signal_ok, assertions_ok, alerts_sent}.
- Exit code: 0 if all PASS else 2.
- Writes daily report to output/reports/shadow_run/YYYY-MM-DD.json (overwrites on rerun).
"""

from __future__ import annotations

import argparse
import glob
import json
import math
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "output" / "reports" / "shadow_run"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

MIN_POS = 6


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _sum_weights(positions: list[dict]) -> float:
    return float(sum(float(p.get("weight", 0.0)) for p in positions))


def _cash_weight(sig: dict) -> float:
    # canonical: meta.health.cash_weight
    h = (sig.get("meta", {}) or {}).get("health", {}) or {}
    if "cash_weight" in h and h["cash_weight"] is not None:
        return float(h["cash_weight"])
    # fallback: positions include CASH
    for p in sig.get("positions", []) or []:
        if str(p.get("code")).upper() == "CASH":
            return float(p.get("weight", 0.0))
    return 0.0


def _safe_float(v) -> float | None:
    try:
        fv = float(v)
    except Exception:
        return None
    if math.isfinite(fv):
        return fv
    return None


def _eval_alerts_for_date(date: str) -> tuple[dict, bool]:
    """Run alerts_eval.py for date and return (meta, gate_ok).

    gate_ok is True only when:
    - alerts evaluator executed successfully (exit 0 or 2), and
    - there is no ERROR-level alert.
    """
    cmd = ["python3", "scripts/alerts_eval.py", "--date", date]
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    stdout_tail = (proc.stdout or "").strip().splitlines()[-1] if (proc.stdout or "").strip() else ""
    stderr_tail = (proc.stderr or "").strip().splitlines()[-1] if (proc.stderr or "").strip() else ""

    parsed = None
    parse_error = None
    if stdout_tail:
        try:
            parsed = json.loads(stdout_tail)
        except Exception as e:
            parse_error = repr(e)

    alerts = (parsed or {}).get("alerts") if isinstance(parsed, dict) else []
    if not isinstance(alerts, list):
        alerts = []
    has_error_alert = any((isinstance(a, dict) and a.get("severity") == "error") for a in alerts)

    eval_ok = proc.returncode in (0, 2)
    gate_ok = bool(eval_ok and (parse_error is None) and (not has_error_alert))

    meta = {
        "date": date,
        "returncode": int(proc.returncode),
        "stdout_tail": stdout_tail,
        "stderr_tail": stderr_tail,
        "result": parsed,
        "parse_error": parse_error,
        "has_error_alert": bool(has_error_alert),
        "n_alerts": int((parsed or {}).get("n_alerts", 0)) if isinstance(parsed, dict) else 0,
    }
    return meta, gate_ok


def _build_paper_pnl_ledger(date: str, args) -> dict:
    """Build/update paper PnL ledger up to `date` (non-gating observability)."""
    cmd = [
        "python3",
        "scripts/build_paper_pnl_ledger.py",
        "--end-date",
        str(date),
        "--mongo-host",
        str(args.mongo_host),
        "--mongo-port",
        str(args.mongo_port),
        "--mongo-db",
        str(args.mongo_db),
        "--mongo-user",
        str(args.mongo_user),
        "--mongo-password",
        str(args.mongo_password),
    ]
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    stdout_tail = (proc.stdout or "").strip().splitlines()[-1] if (proc.stdout or "").strip() else ""
    stderr_tail = (proc.stderr or "").strip().splitlines()[-1] if (proc.stderr or "").strip() else ""
    parsed = None
    parse_error = None
    if stdout_tail:
        try:
            parsed = json.loads(stdout_tail)
        except Exception as e:
            parse_error = repr(e)
    return {
        "date": str(date),
        "returncode": int(proc.returncode),
        "stdout_tail": stdout_tail,
        "stderr_tail": stderr_tail,
        "result": parsed,
        "parse_error": parse_error,
        "ok": bool(proc.returncode == 0 and isinstance(parsed, dict) and parsed.get("ok") is True),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--skip-ingest", action="store_true", help="backfill/regression only; T-3 should NOT use this")
    ap.add_argument("--mongo-host", default=os.getenv("MONGODB_HOST", "127.0.0.1"))
    ap.add_argument("--mongo-port", default=os.getenv("MONGODB_PORT", "27017"))
    ap.add_argument("--mongo-db", default=os.getenv("MONGODB_DATABASE", "quantaxis"))
    ap.add_argument("--mongo-user", default=os.getenv("MONGODB_USER", "quantaxis"))
    ap.add_argument("--mongo-password", default=os.getenv("MONGODB_PASSWORD", "quantaxis"))
    args = ap.parse_args()

    date = str(args.date)

    # 1) Fixed-order pipeline (shadow only)
    cmd = [
        "python3",
        "scripts/daily_pipeline.py",
        "--date",
        date,
        "--mongo-host",
        str(args.mongo_host),
        "--mongo-port",
        str(args.mongo_port),
        "--mongo-db",
        str(args.mongo_db),
        "--mongo-user",
        str(args.mongo_user),
        "--mongo-password",
        str(args.mongo_password),
        "--run-hi",
        "--run-signal",
        "--shadow",
    ]
    if args.skip_ingest:
        cmd.insert(cmd.index("--run-hi"), "--skip-ingest")

    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)

    pipeline_ok = proc.returncode == 0
    pipeline_tail = (proc.stdout or "").strip().splitlines()[-1] if (proc.stdout or "").strip() else ""
    pipeline_err_tail = (proc.stderr or "").strip().splitlines()[-1] if (proc.stderr or "").strip() else ""

    # 2) Required artifacts
    ops_path = ROOT / "output" / "reports" / "ops_data_status" / f"{date}.json"
    hi_path = ROOT / "output" / "reports" / "health_index" / "daily" / f"health_score_{date}.json"

    sig_glob = str(ROOT / "output" / "signals" / f"prod_signal_{date.replace('-', '')}_*.json")
    sig_paths = sorted([p for p in glob.glob(sig_glob) if not p.endswith(".status.json")])
    sig_path = Path(sig_paths[-1]) if sig_paths else None

    health_log_path = ROOT / "output" / "reports" / "health_index" / "health_signal_log.csv"

    missing = []
    if not ops_path.exists():
        missing.append(str(ops_path))
    if not hi_path.exists():
        missing.append(str(hi_path))
    if sig_path is None or (not sig_path.exists()):
        missing.append(sig_glob)
    if not health_log_path.exists():
        missing.append(str(health_log_path))

    report: dict = {
        "date": date,
        "shadow": True,
        "pipeline": {
            "ok": pipeline_ok,
            "returncode": proc.returncode,
            "tail": pipeline_tail,
            "stderr_tail": pipeline_err_tail,
        },
        "artifacts": {
            "ops_data_status": str(ops_path),
            "health_cache": str(hi_path),
            "signal": str(sig_path) if sig_path else None,
            "health_signal_log": str(health_log_path),
            "missing": missing,
        },
        "execution_skipped": True,
        "assertions": {},
    }

    if missing:
        report["ok"] = False
        (REPORT_DIR / f"{date}.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"date": date, "shadow": True, "sealed_ok": False, "signal_ok": False, "assertions_ok": False, "alerts_sent": False}, ensure_ascii=False))
        raise SystemExit(2)

    ops = _read_json(ops_path)
    hi = _read_json(hi_path)
    sig = _read_json(sig_path)

    # A1 sealed_ok
    a1 = bool(ops.get("sealed_ok") is True)

    # A2 n_components_used >= 5
    a2 = int(hi.get("n_components_used") or 0) >= 5

    # A3 signal succeeded
    a3 = str(sig.get("status")) == "succeeded"

    EPS = 1e-9

    # A4 funds conservation: abs(sum(non_cash_weights)+cash-1) <= 1e-9
    positions = sig.get("positions", []) or []
    sw = _sum_weights([p for p in positions if str(p.get("code")).upper() != "CASH"])
    cw_raw = _cash_weight(sig)
    # For checks only (do NOT affect trading): tolerate tiny IEEE754 drift.
    cw_clip = float(min(max(cw_raw, -EPS), 0.6 + EPS))
    a4 = abs((sw + cw_raw) - 1.0) <= EPS

    # A5 cash range (epsilon-tolerant)
    a5 = (-EPS <= cw_raw <= 0.6 + EPS)

    # A6 sealed date
    sealed_date = (((sig.get("meta", {}) or {}).get("ops", {}) or {}).get("sealed_date"))
    a6 = (sealed_date == date)

    # A7 HI consistency (meta vs cache must match exactly)
    mh = (sig.get("meta", {}) or {}).get("health", {}) or {}
    a7 = (
        (mh.get("health_missing") is False)
        and (mh.get("health_score") is not None)
        and (float(mh.get("health_score")) == float(hi.get("health_score")))
    )

    # A8 position sanity
    any_neg = any(float(p.get("weight", 0.0)) < 0 for p in positions)
    a8 = (len(positions) >= MIN_POS) and (not any_neg)
    a8_pos_n = len(positions) >= MIN_POS

    # A9 score_not_all_zero (non-cash only)
    non_cash = [p for p in positions if str(p.get("code", "")).upper() != "CASH"]
    score_vals = []
    for p in non_cash:
        fv = _safe_float(p.get("score"))
        if fv is not None:
            score_vals.append(fv)
    score_abs_sum = float(sum(abs(x) for x in score_vals))
    a9 = bool(score_vals) and (score_abs_sum > 1e-12)

    report["assertions"] = {
        "funds_conservation": a4,
        "cash_weight_in_range": a5,
        "meta_ops_sealed_date_match": a6,
        "meta_health_matches_cache": a7,
        "positions_n_ge_min": a8_pos_n,
        "positions_sane": a8,
        "scores_not_all_zero": a9,
        "debug": {
            "sum_non_cash_weights": sw,
            "cash_weight_raw": cw_raw,
            "cash_weight_clipped_for_check": cw_clip,
            "sealed_date": sealed_date,
            "health_score_meta": mh.get("health_score"),
            "health_score_cache": hi.get("health_score"),
            "positions_n": len(positions),
            "score_items_n": len(score_vals),
            "score_abs_sum": score_abs_sum,
            "signal_status": sig.get("status"),
            "n_components_used": hi.get("n_components_used"),
        },
    }

    # Paper PnL ledger (non-gating; observability only)
    try:
        pnl_meta = _build_paper_pnl_ledger(date, args)
        report["paper_pnl"] = pnl_meta
        pnl_res = pnl_meta.get("result") if isinstance(pnl_meta, dict) else None
        if isinstance(pnl_res, dict):
            if pnl_res.get("csv"):
                report["artifacts"]["paper_pnl_csv"] = str(pnl_res.get("csv"))
            if pnl_res.get("json"):
                report["artifacts"]["paper_pnl_json"] = str(pnl_res.get("json"))
    except Exception as e:
        report["paper_pnl"] = {"ok": False, "error": repr(e)}

    # Observability (meta-only)
    try:
        m = sig.get("meta", {}) or {}
        report["turnover_attrib"] = m.get("turnover_attrib")
        report["hold_smoothing"] = m.get("hold_smoothing")

        # Task B: turnover sanity invariants
        ta = m.get("turnover_attrib") or {}
        is_new_reb = bool(ta.get("is_new_rebalance"))
        entered = ta.get("entered") or []
        exited = ta.get("exited") or []
        entered_nc = [x for x in entered if str(x.get("code", "")).upper() != "CASH"]
        exited_nc = [x for x in exited if str(x.get("code", "")).upper() != "CASH"]

        buy = float(ta.get("turnover_buy") or 0.0)
        sell = float(ta.get("turnover_sell") or 0.0)
        t2 = float(ta.get("turnover_2way") or 0.0)

        # cash delta
        prev_cash = None
        curr_cash = None
        kept = ta.get("kept") or []
        for k in kept:
            if str(k.get("code", "")).upper() == "CASH":
                prev_cash = float(k.get("old_weight") or 0.0)
                curr_cash = float(k.get("new_weight") or 0.0)
        cash_delta = None if (prev_cash is None or curr_cash is None) else float(curr_cash - prev_cash)

        inv = {
            "is_new_rebalance": is_new_reb,
            "non_rebalance_entered_exited_zero": (len(entered_nc) == 0 and len(exited_nc) == 0) if not is_new_reb else None,
            "buy_sell_balance": abs(buy - sell) <= 1e-9,
            "cash_mirror_exposure_scale": (abs(t2 - abs(cash_delta)) <= 1e-6) if (not is_new_reb and cash_delta is not None) else None,
            "rebalance_turnover_ge_cash": (t2 >= abs(cash_delta)) if (is_new_reb and cash_delta is not None) else None,
            "entered_n": int(len(entered_nc)),
            "exited_n": int(len(exited_nc)),
            "cash_delta": cash_delta,
        }
        report["turnover_sanity"] = inv
    except Exception:
        report["turnover_attrib"] = None
        report["hold_smoothing"] = None
        report["turnover_sanity"] = None

    sealed_ok = a1
    signal_ok = a3
    assertions_ok = all([a4, a5, a6, a7, a8, a9])
    core_ok = bool(sealed_ok and a2 and signal_ok and assertions_ok)
    report["ok"] = core_ok
    report["core_ok"] = core_ok
    report["sealed_ok"] = sealed_ok
    report["signal_ok"] = signal_ok
    report["assertions_ok"] = assertions_ok
    report["alerts_sent"] = False

    # Persist base report first so alerts_eval can consume same-day report.
    out_path = REPORT_DIR / f"{date}.json"
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    alerts_meta, alerts_gate_ok = _eval_alerts_for_date(date)
    report["alerts_eval"] = alerts_meta
    report["alerts_gate_ok"] = bool(alerts_gate_ok)

    ok = bool(core_ok and alerts_gate_ok)
    report["ok"] = ok
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    # Spec: stdout one-line JSON
    print(
        json.dumps(
            {
                "date": date,
                "shadow": True,
                "sealed_ok": bool(sealed_ok),
                "signal_ok": bool(signal_ok),
                "assertions_ok": bool(assertions_ok),
                "alerts_sent": False,
            },
            ensure_ascii=False,
        )
    )
    raise SystemExit(0 if ok else 2)


if __name__ == "__main__":
    main()
