#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Shared utilities/constants for the QUANTAXIS API.

This module exists to keep api/app.py small and focused on routing.
"""

from __future__ import annotations

import json
import re
import os
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "output" / "reports"
RUNS_DIR = ROOT / "output" / "api_runs"
SIGNALS_DIR = ROOT / "output" / "signals"

RUNS_DIR.mkdir(parents=True, exist_ok=True)
SIGNALS_DIR.mkdir(parents=True, exist_ok=True)


def read_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))


def _redact_text(s: str, api_token: str = "") -> str:
    """Best-effort redaction for logs."""
    if not s:
        return s
    out = s
    if api_token:
        out = out.replace(api_token, "<REDACTED>")
    out = re.sub(r"(?i)(x-api-key\s*[:=]\s*)([^\s]+)", r"\1<REDACTED>", out)
    out = re.sub(r"(?i)(token\s*[:=]\s*)([^\s]+)", r"\1<REDACTED>", out)
    out = re.sub(r"(?i)(password\s*[:=]\s*)([^\s]+)", r"\1<REDACTED>", out)
    return out


def redact_text(s: str) -> str:
    api_token = os.getenv("QUANTAXIS_API_TOKEN", "").strip()
    return _redact_text(s, api_token=api_token)
