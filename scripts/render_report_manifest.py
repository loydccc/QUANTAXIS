#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Render manifest.json for a report directory.

The manifest is a machine-friendly index for later API/front-end:
- run_id (dir name)
- created_at (best-effort)
- included artifacts + sizes + sha256
- key fields copied from metrics.json (strategy/theme/dates)

Usage:
  python3 scripts/render_report_manifest.py --report output/reports/<run_id>
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True)
    args = ap.parse_args()

    rpt = Path(args.report)
    if not rpt.is_dir():
        raise SystemExit(f"not a dir: {rpt}")

    metrics_path = rpt / "metrics.json"
    if not metrics_path.exists():
        raise SystemExit(f"missing {metrics_path}")

    m: Dict[str, Any] = json.loads(metrics_path.read_text(encoding='utf-8'))    # include all files in the report dir (excluding directories)
    artifacts = []
    for pth in sorted(rpt.iterdir()):
        if not pth.is_file():
            continue
        artifacts.append({"name": pth.name, "bytes": pth.stat().st_size, "sha256": sha256_file(pth)})

    created_at = int(rpt.stat().st_mtime)

    manifest = {
        "run_id": rpt.name,
        "created_at": created_at,
        "strategy": m.get("strategy"),
        "theme": m.get("theme"),
        "start_effective": m.get("start_effective") or m.get("start"),
        "end_effective": m.get("end_effective") or m.get("end"),
        "universe_fingerprint": m.get("universe_fingerprint"),
        "artifacts": artifacts,
        "metrics_path": "metrics.json",
    }

    (rpt / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding='utf-8')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
