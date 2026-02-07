#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Operational alerts evaluator (shadow-run + HI).

This script is designed to be scheduler-friendly:
- Outputs a single JSON object to stdout (fixed structure).
- Exit code:
  - 2 if any ERROR-level alert exists
  - 0 otherwise (warn/info/no alerts)

Data sources (v1):
- output/reports/shadow_run/YYYY-MM-DD.json  (preferred, for turnover/stale/sanity)
- output/reports/health_index/daily/health_score_YYYY-MM-DD.json (optional, HI context)

Alerts (hard spec):
1) STALE_TOO_HIGH
   - trigger: stale_weight_ratio > 0.25
   - severity: warn
   - context: stale_weight_ratio, n_stale_codes, stale_top5

2) TURNOVER_SPIKE
   - only on rebalance day (is_new_rebalance==true)
   - trigger: turnover_2way_total > max(0.30, 2.5 * rolling_median(last_8_rebalance_turnover))
   - severity: warn
   - context: turnover_2way_total, median_last8, turnover_by_reason

3) EXPOSURE_JUMP
   - only on non-rebalance day
   - trigger: abs(cash_curr - cash_prev) > 0.25
   - severity: info
   - context: cash_prev, cash_curr, exposure_prev, exposure_curr

4) TURNOVER_SANITY_FAIL
   - trigger: any turnover_sanity invariant is false
   - severity: error
   - context: full turnover_sanity object
"""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SHADOW_DIR = ROOT / "output" / "reports" / "shadow_run"
HI_DAILY_DIR = ROOT / "output" / "reports" / "health_index" / "daily"


def _read_json(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8"))


def _latest_shadow_date() -> str | None:
    if not SHADOW_DIR.exists():
        return None
    files = sorted(SHADOW_DIR.glob("????-??-??.json"))
    return files[-1].stem if files else None


def _median(xs: list[float]) -> float | None:
    xs = [float(x) for x in xs if x is not None]
    if not xs:
        return None
    return float(statistics.median(xs))


def _turnover_by_reason(turnover_attrib: dict) -> dict:
    # Minimal stable aggregation: absolute weight changes attributed by reason.
    out: dict[str, float] = {}
    for k in (turnover_attrib.get("kept") or []):
        r = str(k.get("reason"))
        dw = abs(float(k.get("new_weight") or 0.0) - float(k.get("old_weight") or 0.0))
        out[r] = out.get(r, 0.0) + float(dw)
    for e in (turnover_attrib.get("entered") or []):
        r = str(e.get("reason"))
        out[r] = out.get(r, 0.0) + float(e.get("new_weight") or 0.0)
    for x in (turnover_attrib.get("exited") or []):
        r = str(x.get("reason"))
        out[r] = out.get(r, 0.0) + float(x.get("old_weight") or 0.0)
    # round to keep JSON compact
    return {k: float(round(v, 12)) for k, v in sorted(out.items())}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (default: latest shadow_run report)")
    args = ap.parse_args()

    date = args.date or _latest_shadow_date()
    if not date:
        out = {"date": None, "alerts": [], "n_alerts": 0, "status": "no_shadow_reports"}
        print(json.dumps(out, ensure_ascii=False))
        raise SystemExit(0)

    report_path = SHADOW_DIR / f"{date}.json"
    if not report_path.exists():
        out = {"date": date, "alerts": [], "n_alerts": 0, "status": "shadow_report_missing", "missing": str(report_path)}
        print(json.dumps(out, ensure_ascii=False))
        raise SystemExit(2)

    rep = _read_json(report_path)
    meta_turnover = rep.get("turnover_attrib") or {}
    meta_hold = rep.get("hold_smoothing") or {}
    sanity = rep.get("turnover_sanity") or {}

    is_new_reb = bool(meta_turnover.get("is_new_rebalance"))
    t2 = float(meta_turnover.get("turnover_2way") or 0.0)
    reasons = _turnover_by_reason(meta_turnover)

    alerts: list[dict] = []

    # 1) STALE_TOO_HIGH
    stale_ratio = float(meta_hold.get("stale_weight_ratio") or 0.0)
    if stale_ratio > 0.25:
        alerts.append(
            {
                "code": "STALE_TOO_HIGH",
                "severity": "warn",
                "context": {
                    "stale_weight_ratio": stale_ratio,
                    "n_stale_codes": int(meta_hold.get("n_stale_codes") or 0),
                    "stale_top5": meta_hold.get("stale_top5") or [],
                },
            }
        )

    # 2) TURNOVER_SPIKE (rebalance days only)
    if is_new_reb:
        # collect previous rebalance turnovers from shadow_run history
        prev_turnovers: list[float] = []
        if SHADOW_DIR.exists():
            for p in sorted(SHADOW_DIR.glob("????-??-??.json")):
                if p.stem >= date:
                    break
                try:
                    o = _read_json(p)
                    ta = o.get("turnover_attrib") or {}
                    if bool(ta.get("is_new_rebalance")):
                        prev_turnovers.append(float(ta.get("turnover_2way") or 0.0))
                except Exception:
                    continue
        last8 = prev_turnovers[-8:]
        med8 = _median(last8)
        thresh = max(0.30, 2.5 * (med8 or 0.0))
        if med8 is not None and t2 > thresh:
            alerts.append(
                {
                    "code": "TURNOVER_SPIKE",
                    "severity": "warn",
                    "context": {
                        "turnover_2way_total": t2,
                        "median_last8": med8,
                        "turnover_by_reason": reasons,
                    },
                }
            )

    # 3) EXPOSURE_JUMP (non-rebalance only)
    if not is_new_reb:
        cash_prev = None
        cash_curr = None
        exp_prev = None
        exp_curr = None
        for k in (meta_turnover.get("kept") or []):
            if str(k.get("code", "")).upper() == "CASH":
                cash_prev = float(k.get("old_weight") or 0.0)
                cash_curr = float(k.get("new_weight") or 0.0)
        # exposure is 1-cash (by design); still expose both fields
        if cash_prev is not None and cash_curr is not None:
            exp_prev = float(1.0 - cash_prev)
            exp_curr = float(1.0 - cash_curr)
            if abs(cash_curr - cash_prev) > 0.25:
                alerts.append(
                    {
                        "code": "EXPOSURE_JUMP",
                        "severity": "info",
                        "context": {
                            "cash_prev": cash_prev,
                            "cash_curr": cash_curr,
                            "exposure_prev": exp_prev,
                            "exposure_curr": exp_curr,
                        },
                    }
                )

    # 4) TURNOVER_SANITY_FAIL (engineering)
    any_false = False
    if isinstance(sanity, dict):
        invariant_keys = {
            "non_rebalance_entered_exited_zero",
            "buy_sell_balance",
            "cash_mirror_exposure_scale",
            "rebalance_turnover_ge_cash",
        }
        for k in invariant_keys:
            v = sanity.get(k)
            if v is False:
                any_false = True
                break
    if any_false:
        alerts.append(
            {
                "code": "TURNOVER_SANITY_FAIL",
                "severity": "error",
                "context": sanity,
            }
        )

    out = {"date": date, "alerts": alerts, "n_alerts": len(alerts)}
    print(json.dumps(out, ensure_ascii=False))

    if any(a.get("severity") == "error" for a in alerts):
        raise SystemExit(2)
    raise SystemExit(0)


if __name__ == "__main__":
    main()
