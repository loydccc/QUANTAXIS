#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Signals API (Mode C) router.

This module is extracted from api/app.py to keep routing clean.

NOTE: For now, implementation functions are imported from api.app to avoid
large-scale behavioral changes. Next iterations can migrate logic here.
"""

from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, Request

from api.security import require_token, rate_limit_run
from api.core import SIGNALS_DIR

import json
from fastapi import HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pathlib import Path


router = APIRouter(prefix="", tags=["signals"])


@router.post("/signals/run")
def signals_run(cfg: dict, background: BackgroundTasks, request: Request):
    require_token(request)
    rate_limit_run(request)
    # Lazy import to avoid circular imports at module import time.
    from api.app import _validate_signal_cfg as _validate_signal_cfg_impl  # type: ignore
    from api.app import run_signal as _run_signal_impl  # type: ignore
    from api.app import _job_sem  # type: ignore

    _validate_signal_cfg_impl(cfg)

    import uuid

    acquired = _job_sem.acquire(blocking=False)
    if not acquired:
        raise HTTPException(status_code=429, detail="too many concurrent runs")

    signal_id = uuid.uuid4().hex
    try:
        background.add_task(_run_signal_impl, signal_id, cfg)
    except Exception:
        try:
            _job_sem.release()
        except Exception:
            pass
        raise

    return {"signal_id": signal_id, "status": "queued"}


@router.get("/signals/{signal_id}.csv")
def signals_csv(signal_id: str, request: Request):
    require_token(request)
    p = SIGNALS_DIR / f"{signal_id}.csv"
    if not p.exists():
        raise HTTPException(status_code=404, detail="csv not found")
    return FileResponse(str(p), media_type="text/csv")


@router.get("/signals/{signal_id}_factors.csv")
def signals_factors_csv(signal_id: str, request: Request):
    require_token(request)
    p = SIGNALS_DIR / f"{signal_id}_factors.csv"
    if not p.exists():
        raise HTTPException(status_code=404, detail="factors csv not found")
    return FileResponse(str(p), media_type="text/csv")


@router.get("/signals/{signal_id}")
def signals_get(signal_id: str, request: Request):
    require_token(request)
    p = SIGNALS_DIR / f"{signal_id}.json"
    if not p.exists():
        st = SIGNALS_DIR / f"{signal_id}.status.json"
        if st.exists():
            return JSONResponse(json.loads(st.read_text(encoding="utf-8")))
        raise HTTPException(status_code=404, detail="signal not found")
    return JSONResponse(json.loads(p.read_text(encoding="utf-8")))
