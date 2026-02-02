#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Build a simple artifact manifest for a report/run directory.

Goal
- Standardize artifacts without introducing a big framework.
- Produce a machine-readable manifest with file hashes + minimal schema hints.

This is used for:
- verifying artifacts weren't mutated
- enabling API/UI to list artifacts consistently

Usage
  python3 scripts/build_artifact_manifest.py --dir output/reports/<run_id>

It will write:
  <dir>/artifact_manifest.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List


def sha256_file(p: Path, chunk: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def canonical_json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sha256_json(obj: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(obj)).hexdigest()


DEFAULT_ALLOW = {
    # core
    "run.json",
    "metrics.json",
    "artifact_manifest.json",
    # standardized parquet
    "equity_curve.parquet",
    "positions.parquet",
    "trades.parquet",
    # legacy compat
    "equity.csv",
    "positions.csv",
    "trades.csv",
    "console.txt",
    "summary.md",
    "manifest.json",  # existing report manifest (human-oriented)
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True, help="report/run directory")
    ap.add_argument("--allow", default=None, help="comma-separated allowlist override")
    args = ap.parse_args()

    d = Path(args.dir)
    if not d.exists() or not d.is_dir():
        raise SystemExit(f"not a directory: {d}")

    allow = DEFAULT_ALLOW
    if args.allow:
        allow = {x.strip() for x in args.allow.split(",") if x.strip()}

    files: List[Dict[str, Any]] = []
    for p in sorted(d.iterdir()):
        if not p.is_file():
            continue
        name = p.name
        if name not in allow:
            continue
        files.append(
            {
                "name": name,
                "bytes": int(p.stat().st_size),
                "sha256": sha256_file(p),
            }
        )

    spec_version = "qa-artifacts/0.1"
    out: Dict[str, Any] = {
        "spec": spec_version,
        "dir": str(d.as_posix()),
        "files": files,
    }
    out["manifest_sha256"] = sha256_json({k: v for k, v in out.items() if k != "manifest_sha256"})

    (d / "artifact_manifest.json").write_bytes(canonical_json_bytes(out) + b"\n")
    print(json.dumps({"ok": True, "dir": str(d), "files": len(files), "manifest_sha256": out["manifest_sha256"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
