#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Render a small manifest pointing to the latest aggregate artifacts.

This is a stable entrypoint for front-end/API.
"""

from __future__ import annotations

import json
import time
from pathlib import Path


def main() -> int:
    out = Path('output/reports/latest_manifest.json')
    out.parent.mkdir(parents=True, exist_ok=True)

    manifest = {
        'generated_at': int(time.time()),
        'artifacts': {
            'baseline_compare_csv': 'output/reports/latest_compare.csv',
            'baseline_compare_md': 'output/reports/latest_compare.md',
            'factor_eval_csv': 'output/reports/latest_factor_eval.csv',
            'factor_eval_md': 'output/reports/latest_factor_eval.md',
        },
    }
    out.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding='utf-8')
    print(f"WROTE {out}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
