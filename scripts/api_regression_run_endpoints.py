#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Regression checks for /run and /runs/{job_id} without external dependencies."""

from __future__ import annotations

import json
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

from fastapi import BackgroundTasks, HTTPException

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import api.app as app_mod


def _ok(name: str, detail: dict | None = None) -> dict:
    return {"name": name, "ok": True, "detail": detail or {}}


def _fail(name: str, detail: dict | None = None) -> dict:
    return {"name": name, "ok": False, "detail": detail or {}}


def _run_background_tasks(background: BackgroundTasks) -> None:
    for t in background.tasks:
        t.func(*t.args, **t.kwargs)


def main():
    checks: list[dict] = []
    with tempfile.TemporaryDirectory(prefix="qa_api_reg_") as td:
        runs_dir = Path(td)
        runs_dir.mkdir(parents=True, exist_ok=True)

        def fake_run_job(job_id: str, cfg: dict):
            try:
                p = runs_dir / f"{job_id}.json"
                p.write_text(
                    json.dumps(
                        {
                            "job_id": job_id,
                            "status": "succeeded",
                            "finished_at": int(time.time()),
                            "return_code": 0,
                            "result": {"run_id": "regression_dummy"},
                            "cfg_strategy": cfg.get("strategy"),
                        },
                        ensure_ascii=False,
                        indent=2,
                    ),
                    encoding="utf-8",
                )
            finally:
                # run() acquires semaphore before scheduling background task.
                try:
                    app_mod._job_sem.release()
                except Exception:
                    pass

        with (
            patch.object(app_mod, "RUNS_DIR", runs_dir),
            patch.object(app_mod, "run_job", fake_run_job),
            patch.object(app_mod, "require_token", lambda request: None),
            patch.object(app_mod, "rate_limit_run", lambda request: None),
        ):
            dummy_request = object()

            # 1) /run missing reproducibility fields -> 400
            bg1 = BackgroundTasks()
            try:
                app_mod.run({"strategy": "hybrid_baseline_weekly_topk"}, bg1, dummy_request)
                checks.append(_fail("run_missing_repro_fields", {"error": "expected HTTPException(400), got success"}))
            except HTTPException as e:
                if int(e.status_code) == 400:
                    checks.append(_ok("run_missing_repro_fields", {"status_code": int(e.status_code), "detail": e.detail}))
                else:
                    checks.append(_fail("run_missing_repro_fields", {"status_code": int(e.status_code), "detail": e.detail}))

            # 2) /run valid payload -> queued + job_id
            bg2 = BackgroundTasks()
            cfg = {
                "strategy": "hybrid_baseline_weekly_topk",
                "data_version_id": "regression-v1",
                "manifest_sha256": "a" * 64,
            }
            try:
                out = app_mod.run(cfg, bg2, dummy_request)
                if isinstance(out, dict) and out.get("status") == "queued" and out.get("job_id"):
                    job_id = str(out["job_id"])
                    checks.append(_ok("run_queues_valid_request", {"job_id": job_id}))
                else:
                    job_id = ""
                    checks.append(_fail("run_queues_valid_request", {"out": out}))
            except HTTPException as e:
                job_id = ""
                checks.append(_fail("run_queues_valid_request", {"status_code": int(e.status_code), "detail": e.detail}))

            # execute background task so /runs can observe persisted status
            _run_background_tasks(bg2)

            # 3) /runs/{job_id} should return job status
            if job_id:
                try:
                    rs = app_mod.run_status(job_id, dummy_request)
                    obj = json.loads(rs.body.decode("utf-8"))
                    if obj.get("status") == "succeeded":
                        checks.append(_ok("runs_returns_status", {"status": obj.get("status")}))
                    else:
                        checks.append(_fail("runs_returns_status", {"obj": obj}))
                except HTTPException as e:
                    checks.append(_fail("runs_returns_status", {"status_code": int(e.status_code), "detail": e.detail}))

            # 4) /runs/{job_id} unknown id -> 404
            try:
                app_mod.run_status("does_not_exist_regression", dummy_request)
                checks.append(_fail("runs_unknown_job_404", {"error": "expected HTTPException(404), got success"}))
            except HTTPException as e:
                if int(e.status_code) == 404:
                    checks.append(_ok("runs_unknown_job_404", {"status_code": int(e.status_code), "detail": e.detail}))
                else:
                    checks.append(_fail("runs_unknown_job_404", {"status_code": int(e.status_code), "detail": e.detail}))

    ok = all(bool(c.get("ok")) for c in checks)
    out = {"ok": ok, "n_checks": len(checks), "checks": checks}
    print(json.dumps(out, ensure_ascii=False))
    raise SystemExit(0 if ok else 2)


if __name__ == "__main__":
    main()
