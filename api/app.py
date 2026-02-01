#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""QUANTAXIS product API (MVP).

Mode B (execute + read):
- Read endpoints serve artifacts under output/reports
- Run endpoint triggers existing CLI runner (scripts/run_from_cfg.py)

Security note: this is a local MVP. Do NOT expose to the public internet without auth.
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
import time
import uuid
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Deque, Dict, Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "output" / "reports"
RUNS_DIR = ROOT / "output" / "api_runs"
RUNS_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="QUANTAXIS API", version="0.1.0")

# --- Security / hardening knobs (env) ---
API_TOKEN = os.getenv("QUANTAXIS_API_TOKEN", "").strip()
API_MAX_CONCURRENT = int(os.getenv("QUANTAXIS_API_MAX_CONCURRENT", "2"))
API_RUNS_PER_MIN = int(os.getenv("QUANTAXIS_API_RUNS_PER_MIN", "6"))
API_JOB_TIMEOUT_SEC = int(os.getenv("QUANTAXIS_API_JOB_TIMEOUT_SEC", "3600"))
API_LOG_TAIL = int(os.getenv("QUANTAXIS_API_LOG_TAIL", "2000"))
API_INCLUDE_LOGS = os.getenv("QUANTAXIS_API_INCLUDE_LOGS", "").strip().lower() in {"1", "true", "yes"}

# In-memory concurrency + rate limit (good enough for local/one-process MVP)
_job_sem = threading.BoundedSemaphore(max(1, API_MAX_CONCURRENT))
_rl_lock = threading.Lock()
_rl_hits: Dict[str, Deque[float]] = defaultdict(deque)


def require_token(req: Request) -> None:
    """Require X-API-Key when QUANTAXIS_API_TOKEN is set."""
    if not API_TOKEN:
        return
    key = req.headers.get("x-api-key") or req.headers.get("X-API-Key")
    if key != API_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")


def _rate_limit_run(req: Request) -> None:
    """Simple per-IP rate limit for /run (in-memory)."""
    if API_RUNS_PER_MIN <= 0:
        return
    ip = (req.client.host if req.client else "unknown")
    now = time.monotonic()
    window = 60.0
    with _rl_lock:
        dq = _rl_hits[ip]
        # drop old entries
        while dq and now - dq[0] > window:
            dq.popleft()
        if len(dq) >= API_RUNS_PER_MIN:
            raise HTTPException(status_code=429, detail="rate limit exceeded")
        dq.append(now)


def _validate_cfg(cfg: Dict[str, Any]) -> None:
    """Very small config sanity checks (MVP).

    This is not a full schema yet, but blocks obvious abuse:
    - huge objects
    - pathological strings
    - weird keys
    """
    if not isinstance(cfg, dict):
        raise HTTPException(status_code=400, detail="config must be a JSON object")
    if "strategy" not in cfg:
        raise HTTPException(status_code=400, detail="missing strategy")
    if len(cfg) > 200:
        raise HTTPException(status_code=400, detail="config too large")
    for k, v in cfg.items():
        if not isinstance(k, str):
            raise HTTPException(status_code=400, detail="config keys must be strings")
        if len(k) > 200 or ".." in k or "/" in k or "\\" in k:
            raise HTTPException(status_code=400, detail="bad config key")
        if isinstance(v, str) and len(v) > 5000:
            raise HTTPException(status_code=400, detail=f"config value too long: {k}")


def read_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))


def run_job(job_id: str, cfg: Dict[str, Any]) -> None:
    """Run a backtest job and persist status/result under output/api_runs/<job_id>.json"""
    job_path = RUNS_DIR / f"{job_id}.json"
    job_path.write_text(
        json.dumps({"job_id": job_id, "status": "running", "started_at": int(time.time())}, ensure_ascii=False),
        encoding="utf-8",
    )

    cfg_path = RUNS_DIR / f"{job_id}.cfg.json"
    cfg_path.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")

    result_path = RUNS_DIR / f"{job_id}.result.json"

    try:
        # run_from_cfg prints result JSON on stdout; also writes --result file
        cmd = ["python3", "scripts/run_from_cfg.py", "--config", str(cfg_path), "--result", str(result_path)]
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=max(1, API_JOB_TIMEOUT_SEC),
        )
        rc = proc.returncode

        result_obj: Optional[Dict[str, Any]] = None
        if result_path.exists():
            result_obj = read_json(result_path)

        payload: Dict[str, Any] = {
            "job_id": job_id,
            "status": "succeeded" if rc == 0 else "failed",
            "finished_at": int(time.time()),
            "return_code": rc,
            "result": result_obj,
            # store tails for troubleshooting (optionally served by API)
            "stdout_tail": (proc.stdout or "")[-API_LOG_TAIL:],
            "stderr_tail": (proc.stderr or "")[-API_LOG_TAIL:],
        }
        job_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    except subprocess.TimeoutExpired:
        payload = {
            "job_id": job_id,
            "status": "failed",
            "finished_at": int(time.time()),
            "error": f"timeout after {API_JOB_TIMEOUT_SEC}s",
        }
        job_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    except Exception as e:
        payload = {
            "job_id": job_id,
            "status": "failed",
            "finished_at": int(time.time()),
            "error": repr(e),
        }
        job_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    finally:
        # release the concurrency slot acquired in /run
        try:
            _job_sem.release()
        except Exception:
            pass


@app.get("/health")
def health():
    return {
        "ok": True,
        "ts": int(time.time()),
        "auth_required": bool(API_TOKEN),
        "limits": {
            "max_concurrent": API_MAX_CONCURRENT,
            "runs_per_min": API_RUNS_PER_MIN,
            "job_timeout_sec": API_JOB_TIMEOUT_SEC,
            "include_logs": API_INCLUDE_LOGS,
        },
    }


@app.get("/latest/manifest")
def latest_manifest():
    p = REPORTS_DIR / "latest_manifest.json"
    if not p.exists():
        raise HTTPException(status_code=404, detail="latest_manifest.json not found")
    return JSONResponse(read_json(p))


@app.get("/reports/{run_id}/manifest")
def report_manifest(run_id: str):
    p = REPORTS_DIR / run_id / "manifest.json"
    if not p.exists():
        raise HTTPException(status_code=404, detail="manifest.json not found")
    return JSONResponse(read_json(p))


@app.get("/reports/{run_id}/file/{name}")
def report_file(run_id: str, name: str):
    # allowlist simple filenames to reduce path traversal risk
    if "/" in name or ".." in name:
        raise HTTPException(status_code=400, detail="bad filename")
    p = REPORTS_DIR / run_id / name
    if not p.exists() or not p.is_file():
        raise HTTPException(status_code=404, detail="file not found")
    return FileResponse(str(p))


@app.post("/run")
def run(cfg: Dict[str, Any], background: BackgroundTasks, request: Request):
    require_token(request)
    _rate_limit_run(request)
    _validate_cfg(cfg)

    # concurrency guard
    acquired = _job_sem.acquire(blocking=False)
    if not acquired:
        raise HTTPException(status_code=429, detail="too many concurrent runs")

    job_id = uuid.uuid4().hex
    try:
        background.add_task(run_job, job_id, cfg)
    except Exception:
        # if BackgroundTasks fails for any reason, release the slot
        try:
            _job_sem.release()
        except Exception:
            pass
        raise

    return {"job_id": job_id, "status": "queued"}


@app.get("/runs/{job_id}")
def run_status(job_id: str, request: Request):
    require_token(request)
    p = RUNS_DIR / f"{job_id}.json"
    if not p.exists():
        raise HTTPException(status_code=404, detail="job not found")
    obj = read_json(p)
    if not API_INCLUDE_LOGS:
        obj.pop("stdout_tail", None)
        obj.pop("stderr_tail", None)
    return JSONResponse(obj)
