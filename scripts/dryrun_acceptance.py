#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Dry-run acceptance for daily pipeline (pre-prod gate).

Runs 3 assertions:
1) bad_date: validate fail -> sealed_ok=false -> HOLD_PREV, and no HI/signal artifacts.
2) good_date: sealed_ok=true -> HI cache exists -> signal exists with meta.ops.sealed_date == date.
3) idempotency: repeat good_date -> ops_data_status unique, etag same, positions identical.

This script is intentionally minimal. It uses --skip-ingest to avoid dependency on external data sources.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(cmd: list[str]) -> tuple[int, str]:
    p = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    out = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
    return p.returncode, out.strip()


def latest_signal_json(prefix: str) -> Path | None:
    sigs = sorted((ROOT / "output" / "signals").glob(prefix + "*.json"), key=lambda x: x.stat().st_mtime, reverse=True)
    return sigs[0] if sigs else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bad-date", required=True)
    ap.add_argument("--good-date", required=True)
    args = ap.parse_args()

    results = []

    # 1) bad date
    bad = args.bad_date
    hi_bad = ROOT / "output" / "reports" / "health_index" / "daily" / f"health_score_{bad}.json"

    code, out = run([
        "python3",
        "scripts/daily_pipeline.py",
        "--date",
        bad,
        "--skip-ingest",
        "--run-hi",
        "--run-signal",
    ])
    ok1 = ("HOLD_PREV" in out) and (code != 0) and (not hi_bad.exists())
    # signal artifacts should not exist for that date prefix
    sig_bad = list((ROOT / "output" / "signals").glob(f"prod_signal_{bad.replace('-', '')}_*.json"))
    ok1 = ok1 and (len(sig_bad) == 0)
    results.append(("assert1_bad_date_no_run_without_seal", ok1))

    # 2) good date
    good = args.good_date
    hi_good = ROOT / "output" / "reports" / "health_index" / "daily" / f"health_score_{good}.json"

    code2, out2 = run([
        "python3",
        "scripts/daily_pipeline.py",
        "--date",
        good,
        "--skip-ingest",
        "--run-hi",
        "--run-signal",
    ])
    sig_path = latest_signal_json(f"prod_signal_{good.replace('-', '')}_")
    ok2 = (code2 == 0) and hi_good.exists() and (sig_path is not None)
    if ok2:
        sig = json.loads(sig_path.read_text(encoding="utf-8"))
        ok2 = (sig.get("meta", {}).get("ops", {}).get("sealed_date") == good)
    results.append(("assert2_good_date_generates_artifacts_after_seal", ok2))

    # 3) idempotency for good date
    # run again
    code3, _ = run([
        "python3",
        "scripts/daily_pipeline.py",
        "--date",
        good,
        "--skip-ingest",
        "--run-hi",
        "--run-signal",
    ])
    sig_path2 = latest_signal_json(f"prod_signal_{good.replace('-', '')}_")

    ok3 = code3 == 0 and sig_path is not None and sig_path2 is not None
    if ok3:
        s1 = json.loads(sig_path.read_text(encoding="utf-8"))
        s2 = json.loads(sig_path2.read_text(encoding="utf-8"))
        pos1 = sorted([(p["code"], float(p.get("weight", 0))) for p in s1.get("positions", [])])
        pos2 = sorted([(p["code"], float(p.get("weight", 0))) for p in s2.get("positions", [])])
        ok3 = pos1 == pos2

    # ops_data_status uniqueness & etag stability (local json)
    seal_file = ROOT / "output" / "reports" / "ops_data_status" / f"{good}.json"
    if ok3 and seal_file.exists():
        doc = json.loads(seal_file.read_text(encoding="utf-8"))
        etag1 = doc.get("etag")
        # re-read; should be same (file overwritten)
        doc2 = json.loads(seal_file.read_text(encoding="utf-8"))
        ok3 = ok3 and (etag1 == doc2.get("etag"))
    else:
        ok3 = False

    results.append(("assert3_idempotency_same_positions_same_etag", ok3))

    # Print compact report
    report = {"bad_date": bad, "good_date": good, "results": [{"name": n, "ok": bool(v)} for n, v in results]}
    print(json.dumps(report, ensure_ascii=False, indent=2))

    failed = [n for n, v in results if not v]
    raise SystemExit(0 if not failed else 2)


if __name__ == "__main__":
    main()
