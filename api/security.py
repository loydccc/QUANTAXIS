#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Security / hardening helpers for the QUANTAXIS API."""

from __future__ import annotations

import json
import os
import re
import time
from collections import defaultdict, deque
from typing import Any, Deque, Dict

from fastapi import HTTPException, Request


API_TOKEN = os.getenv("QUANTAXIS_API_TOKEN", "").strip()
API_RUNS_PER_MIN = int(os.getenv("QUANTAXIS_API_RUNS_PER_MIN", "6"))
API_CFG_MAX_BYTES = int(os.getenv("QUANTAXIS_API_CFG_MAX_BYTES", "200000"))
API_CFG_MAX_DEPTH = int(os.getenv("QUANTAXIS_API_CFG_MAX_DEPTH", "12"))

_rl_lock = None  # lazily initialized
_rl_hits: Dict[str, Deque[float]] = defaultdict(deque)


def require_token(req: Request) -> None:
    """Require X-API-Key when QUANTAXIS_API_TOKEN is set."""
    if not API_TOKEN:
        return
    key = req.headers.get("x-api-key") or req.headers.get("X-API-Key")
    if key != API_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")


def rate_limit_run(req: Request) -> None:
    """Simple per-IP rate limit for /run and /signals/run (in-memory)."""
    if API_RUNS_PER_MIN <= 0:
        return
    ip = (req.client.host if req.client else "unknown")
    now = time.monotonic()
    window = 60.0
    dq = _rl_hits[ip]
    while dq and now - dq[0] > window:
        dq.popleft()
    if len(dq) >= API_RUNS_PER_MIN:
        raise HTTPException(status_code=429, detail="rate limit exceeded")
    dq.append(now)


def _walk_depth(x: Any, depth: int = 0) -> int:
    if isinstance(x, dict) and x:
        return max(_walk_depth(v, depth + 1) for v in x.values())
    if isinstance(x, list) and x:
        return max(_walk_depth(v, depth + 1) for v in x)
    return depth


def validate_cfg_envelope(cfg: Any) -> None:
    """Generic config envelope guards (size/depth/key sanity).

    NOTE: strategy-specific validation remains in api/app.py for now.
    """
    if not isinstance(cfg, dict):
        raise HTTPException(status_code=400, detail="config must be a JSON object")

    try:
        raw = json.dumps(cfg, ensure_ascii=False)
    except Exception:
        raise HTTPException(status_code=400, detail="config must be JSON-serializable")

    if API_CFG_MAX_BYTES > 0 and len(raw.encode("utf-8")) > API_CFG_MAX_BYTES:
        raise HTTPException(status_code=400, detail="config too large")

    if API_CFG_MAX_DEPTH > 0 and _walk_depth(cfg) > API_CFG_MAX_DEPTH:
        raise HTTPException(status_code=400, detail="config too deeply nested")

    if len(cfg) > 300:
        raise HTTPException(status_code=400, detail="config has too many keys")

    for forbidden in ("cmd", "command", "shell", "cwd", "workdir", "path"):
        if forbidden in cfg:
            raise HTTPException(status_code=400, detail=f"forbidden field: {forbidden}")

    for k, v in cfg.items():
        if not isinstance(k, str):
            raise HTTPException(status_code=400, detail="config keys must be strings")
        if len(k) > 200:
            raise HTTPException(status_code=400, detail="bad config key")
        if k.startswith("$") or ".." in k or "/" in k or "\\" in k or "\x00" in k:
            raise HTTPException(status_code=400, detail="bad config key")
        if isinstance(v, str) and len(v) > 5000:
            raise HTTPException(status_code=400, detail=f"config value too long: {k}")
        if isinstance(v, list) and len(v) > 5000:
            raise HTTPException(status_code=400, detail=f"config list too long: {k}")


_strategy_re = re.compile(r"^[A-Za-z0-9_.-]{1,100}$")


def validate_strategy_name(s: Any) -> str:
    if not isinstance(s, str) or not _strategy_re.match(s):
        raise HTTPException(status_code=400, detail="bad strategy")
    return s
