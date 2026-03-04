#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Watchdog: mark stale running statuses as failed.

Targets:
- output/api_runs/<job_id>.json
- output/signals/<signal_id>.status.json

Rules:
- only status == "running" is considered
- stale if started_at missing/invalid OR age_sec > configured threshold
- stale file is updated in place to status="failed" with watchdog metadata
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RUNS_DIR = ROOT / "output" / "api_runs"
DEFAULT_SIGNALS_DIR = ROOT / "output" / "signals"


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _parse_started_at(obj: dict[str, Any]) -> int | None:
    raw = obj.get("started_at")
    try:
        if raw is None:
            return None
        return int(float(raw))
    except Exception:
        return None


def _is_stale(started_at: int | None, now_ts: int, max_age_sec: int) -> tuple[bool, str]:
    if started_at is None:
        return True, "missing_started_at"
    age = int(now_ts - int(started_at))
    if age < 0:
        return False, f"future_started_at(age={age})"
    if age > max_age_sec:
        return True, f"age_sec={age}>max_age_sec={max_age_sec}"
    return False, f"age_sec={age}"


def _mark_failed(path: Path, obj: dict[str, Any], now_ts: int, reason: str, dry_run: bool) -> None:
    out = dict(obj)
    out["status"] = "failed"
    out["finished_at"] = int(now_ts)
    out["error"] = f"watchdog stale running status: {reason}"
    out["watchdog_marked"] = True
    out["watchdog_marked_at"] = int(now_ts)
    out["watchdog_reason"] = reason
    if not dry_run:
        path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")


def _iter_api_run_status_files(runs_dir: Path):
    for p in sorted(runs_dir.glob("*.json")):
        name = p.name
        if name.endswith(".cfg.json") or name.endswith(".result.json"):
            continue
        yield p


def _iter_signal_status_files(signals_dir: Path):
    for p in sorted(signals_dir.glob("*.status.json")):
        yield p


def _process_paths(paths, now_ts: int, max_age_sec: int, dry_run: bool, kind: str) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "kind": kind,
        "total_files": 0,
        "running_files": 0,
        "stale_files": 0,
        "marked_failed": 0,
        "skipped": 0,
        "errors": 0,
        "actions": [],
    }

    for p in paths:
        summary["total_files"] += 1
        obj = _read_json(p)
        if not isinstance(obj, dict):
            summary["skipped"] += 1
            continue
        if str(obj.get("status")) != "running":
            summary["skipped"] += 1
            continue

        summary["running_files"] += 1
        started_at = _parse_started_at(obj)
        stale, reason = _is_stale(started_at, now_ts=now_ts, max_age_sec=max_age_sec)
        if not stale:
            summary["skipped"] += 1
            continue

        summary["stale_files"] += 1
        action = {
            "path": str(p),
            "started_at": started_at,
            "reason": reason,
            "dry_run": bool(dry_run),
        }
        try:
            _mark_failed(p, obj, now_ts=now_ts, reason=reason, dry_run=dry_run)
            summary["marked_failed"] += 1
            summary["actions"].append(action)
        except Exception as e:
            summary["errors"] += 1
            action["error"] = repr(e)
            summary["actions"].append(action)

    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs-dir", default=str(DEFAULT_RUNS_DIR))
    ap.add_argument("--signals-dir", default=str(DEFAULT_SIGNALS_DIR))
    ap.add_argument(
        "--api-max-age-sec",
        type=int,
        default=int(os.getenv("QUANTAXIS_WATCHDOG_API_MAX_AGE_SEC", "7200")),
        help="max allowed age for api run status=running",
    )
    ap.add_argument(
        "--signal-max-age-sec",
        type=int,
        default=int(os.getenv("QUANTAXIS_WATCHDOG_SIGNAL_MAX_AGE_SEC", "7200")),
        help="max allowed age for signal status=running",
    )
    ap.add_argument("--only", choices=["all", "api", "signals"], default="all")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--now-ts", type=int, default=int(time.time()))
    args = ap.parse_args()

    runs_dir = Path(args.runs_dir)
    signals_dir = Path(args.signals_dir)
    now_ts = int(args.now_ts)

    out: dict[str, Any] = {
        "now_ts": now_ts,
        "dry_run": bool(args.dry_run),
        "api_max_age_sec": int(args.api_max_age_sec),
        "signal_max_age_sec": int(args.signal_max_age_sec),
        "results": [],
    }

    if args.only in ("all", "api"):
        out["results"].append(
            _process_paths(
                _iter_api_run_status_files(runs_dir),
                now_ts=now_ts,
                max_age_sec=int(args.api_max_age_sec),
                dry_run=bool(args.dry_run),
                kind="api_runs",
            )
        )

    if args.only in ("all", "signals"):
        out["results"].append(
            _process_paths(
                _iter_signal_status_files(signals_dir),
                now_ts=now_ts,
                max_age_sec=int(args.signal_max_age_sec),
                dry_run=bool(args.dry_run),
                kind="signals",
            )
        )

    out["total_errors"] = int(sum(int(r.get("errors", 0)) for r in out["results"]))
    out["total_marked_failed"] = int(sum(int(r.get("marked_failed", 0)) for r in out["results"]))
    print(json.dumps(out, ensure_ascii=False))
    raise SystemExit(2 if out["total_errors"] > 0 else 0)


if __name__ == "__main__":
    main()
