#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Summarize latest factor evaluation reports into CSV + Markdown.

It reads output/reports/*_factor_eval_*/metrics.json and produces:
- output/reports/latest_factor_eval.csv
- output/reports/latest_factor_eval.md

This is intended as a stable artifact for API/front-end.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--reports-dir', default='output/reports')
    ap.add_argument('--latest', type=int, default=5)
    ap.add_argument('--out', default='output/reports/latest_factor_eval.csv')
    ap.add_argument('--md', default='output/reports/latest_factor_eval.md')
    args = ap.parse_args()

    rdir = Path(args.reports_dir)
    reports = [p for p in rdir.iterdir() if p.is_dir() and '_factor_eval_' in p.name and (p / 'metrics.json').exists()]
    reports.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    reports = reports[: args.latest]

    rows: List[Dict[str, Any]] = []
    for rpt in reports:
        m = json.loads((rpt / 'metrics.json').read_text(encoding='utf-8'))
        base = {
            'run_id': rpt.name,
            'theme': m.get('theme'),
            'start': m.get('start'),
            'end': m.get('end'),
            'horizon': m.get('horizon'),
            'quantiles': m.get('quantiles'),
        }
        for fc, v in (m.get('factors') or {}).items():
            ic = (v.get('ic') or {})
            rows.append(
                {
                    **base,
                    'factor': fc,
                    'ic_mean': ic.get('mean'),
                    'ic_ir': ic.get('ir'),
                    'avg_qspread': v.get('avg_qspread'),
                    'n': ic.get('n'),
                }
            )

    # CSV
    import csv

    out_csv = Path(args.out)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        fieldnames = list(rows[0].keys())
    else:
        fieldnames = ['run_id','theme','start','end','horizon','quantiles','factor','ic_mean','ic_ir','avg_qspread','n']

    with out_csv.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    # Markdown grouped by run_id
    out_md = Path(args.md)
    lines = ['# Latest factor evaluations', '']

    def fmt(x):
        try:
            return f"{float(x):.4f}"
        except Exception:
            return 'nan'

    by_run: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        by_run.setdefault(r['run_id'], []).append(r)

    for run_id, rr in by_run.items():
        meta = rr[0]
        lines += [
            f"## {run_id}",
            f"- theme: **{meta['theme']}**  horizon: **{meta['horizon']}**  quantiles: **{meta['quantiles']}**",
            f"- start/end: **{meta['start']}** → **{meta['end']}**",
            '',
            '|factor|mean IC|IR|avg q-spread|n|',
            '|---|---:|---:|---:|---:|',
        ]
        for r in rr:
            lines.append(f"|{r['factor']}|{fmt(r['ic_mean'])}|{fmt(r['ic_ir'])}|{fmt(r['avg_qspread'])}|{fmt(r['n'])}|")
        lines.append('')

    out_md.write_text('\n'.join(lines) + '\n', encoding='utf-8')

    print(f"WROTE {out_csv}")
    print(f"WROTE {out_md}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
