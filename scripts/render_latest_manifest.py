#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Render a small manifest pointing to the latest aggregate artifacts.

This is a stable entrypoint for front-end/API.
Adds file bytes + sha256 for cache/integrity.
"""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Dict, Any


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def file_meta(relpath: str) -> Dict[str, Any]:
    p = Path(relpath)
    if not p.exists() or not p.is_file():
        return {"path": relpath, "exists": False}
    return {
        "path": relpath,
        "exists": True,
        "bytes": p.stat().st_size,
        "sha256": sha256_file(p),
        "mtime": int(p.stat().st_mtime),
    }


def main() -> int:
    out = Path('output/reports/latest_manifest.json')
    out.parent.mkdir(parents=True, exist_ok=True)

    artifacts = {
        'baseline_compare_csv': 'output/reports/latest_compare.csv',
        'baseline_compare_md': 'output/reports/latest_compare.md',
        'factor_eval_csv': 'output/reports/latest_factor_eval.csv',
        'factor_eval_md': 'output/reports/latest_factor_eval.md',
        'strategy_compare_csv': 'output/reports/latest_strategy_compare.csv',
        'strategy_compare_md': 'output/reports/latest_strategy_compare.md',
        'strategy_compare_configs': 'output/reports/latest_strategy_compare_configs.json',
    }

    manifest = {
        'generated_at': int(time.time()),
        'artifacts': {k: file_meta(v) for k, v in artifacts.items()},
    }

    out.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding='utf-8')
    print(f"WROTE {out}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
