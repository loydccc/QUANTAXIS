#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Misc internal utilities for api/app.py."""

from __future__ import annotations

from typing import Any


def walk_depth(x: Any, depth: int = 0) -> int:
    if isinstance(x, dict) and x:
        return max(walk_depth(v, depth + 1) for v in x.values())
    if isinstance(x, list) and x:
        return max(walk_depth(v, depth + 1) for v in x)
    return depth
