#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Summarize latest strategy backtest reports (baseline + factor_bt) into one table."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--reports-dir', default='output/reports')
    ap.add_argument('--latest', type=int, default=12)
    ap.add_argument('--out', default='output/reports/latest_strategy_compare.csv')
    ap.add_argument('--md', default='output/reports/latest_strategy_compare.md')
    args = ap.parse_args()

    rdir = Path(args.reports_dir)
    reports = [p for p in rdir.iterdir() if p.is_dir() and (p / 'metrics.json').exists()]
    reports.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    rows: List[Dict[str, Any]] = []
    for rpt in reports:
        name = rpt.name
        if '_factor_bt_' not in name and ('_xsec_momentum_' not in name and '_ts_ma_' not in name):
            continue
        m = json.loads((rpt / 'metrics.json').read_text(encoding='utf-8'))
        rows.append(
            {
                'run_id': name,
                'strategy': m.get('strategy'),
                'theme': m.get('theme'),
                'start': m.get('start_effective') or m.get('start'),
                'end': m.get('end_effective') or m.get('end'),
                'cagr': m.get('cagr'),
                'sharpe': m.get('sharpe'),
                'max_dd': m.get('max_drawdown'),
                'final_equity': m.get('final_equity'),
                'annual_turnover': m.get('annual_turnover'),
                'factor': m.get('factor'),
                'direction': m.get('direction'),
                'rebalance': m.get('rebalance'),
                'topk': m.get('topk'),
                'cost_bps': m.get('cost_bps'),
            }
        )
        if len(rows) >= args.latest:
            break

    # CSV
    import csv

    out_csv = Path(args.out)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys()) if rows else []
    with out_csv.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    # Markdown
    out_md = Path(args.md)
    def fmt(x):
        try:
            return f"{float(x):.4f}"
        except Exception:
            return 'nan'

    lines = [
        '# Latest strategy compare',
        '',
        '|strategy|factor|direction|rebalance|cagr|sharpe|max_dd|final_equity|annual_turnover|theme|run_id|',
        '|---|---|---|---|---:|---:|---:|---:|---:|---|---|',
    ]
    for r in rows:
        lines.append(
            f"|{r['strategy']}|{r.get('factor') or ''}|{r.get('direction') or ''}|{r.get('rebalance') or ''}|{fmt(r['cagr'])}|{fmt(r['sharpe'])}|{fmt(r['max_dd'])}|{fmt(r['final_equity'])}|{fmt(r['annual_turnover'])}|{r.get('theme')}|{r['run_id']}|"
        )
    out_md.write_text('\n'.join(lines) + '\n', encoding='utf-8')

    print(f"WROTE {out_csv}")
    print(f"WROTE {out_md}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
