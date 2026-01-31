#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Summarize latest strategy backtest reports (baseline + factor_bt) into one table."""

from __future__ import annotations

import argparse
import json
import hashlib
from pathlib import Path
from typing import Any, Dict, List

def cfg_sig(m: dict) -> str:
    # Stable signature across runs for de-duplication
    parts = [
        str(m.get('strategy')),
        str(m.get('theme')),
        str(m.get('start_effective') or m.get('start')),
        str(m.get('end_effective') or m.get('end')),
        str(m.get('rebalance')),
        str(m.get('cost_bps')),
        str(m.get('factor')),
        str((m.get('direction') or ('long_high' if m.get('strategy') == 'factor_portfolio' else None))),
        str(m.get('topk')),
        str(m.get('quantile')),
        str(m.get('universe_fingerprint')),
        # baseline params if present
        str((m.get('params') or {}).get('lookback')) if isinstance(m.get('params'), dict) else '',
        str((m.get('params') or {}).get('top')) if isinstance(m.get('params'), dict) else '',
        str((m.get('params') or {}).get('ma')) if isinstance(m.get('params'), dict) else '',
        str((m.get('params') or {}).get('vol_window')) if isinstance(m.get('params'), dict) else '',
        str((m.get('params') or {}).get('max_weight')) if isinstance(m.get('params'), dict) else '',
    ]
    s = '|'.join(parts).encode('utf-8')
    return hashlib.sha256(s).hexdigest()[:16]



def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--reports-dir', default='output/reports')
    ap.add_argument('--latest', type=int, default=12)
    ap.add_argument('--theme', default=None)
    ap.add_argument('--kind', choices=['all','baseline','factor'], default='all')
    ap.add_argument('--sort', choices=['mtime','sharpe','cagr','max_dd'], default='mtime')
    ap.add_argument('--top', type=int, default=None, help='if set, keep only top N after sorting')
    ap.add_argument('--dedup', action='store_true', help='deduplicate by config signature (keep best after sorting)')
    ap.add_argument('--out', default='output/reports/latest_strategy_compare.csv')
    ap.add_argument('--md', default='output/reports/latest_strategy_compare.md')
    args = ap.parse_args()

    rdir = Path(args.reports_dir)
    reports = [p for p in rdir.iterdir() if p.is_dir() and (p / 'metrics.json').exists()]
    CAP = max(args.latest, 50)
    reports.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    rows: List[Dict[str, Any]] = []
    for rpt in reports:
        name = rpt.name
        if '_factor_bt_' not in name and ('_xsec_momentum_' not in name and '_ts_ma_' not in name):
            continue
        m = json.loads((rpt / 'metrics.json').read_text(encoding='utf-8'))
        if args.theme is not None and m.get('theme') != args.theme:
            continue
        if args.kind == 'baseline' and m.get('strategy') == 'factor_portfolio':
            continue
        if args.kind == 'factor' and m.get('strategy') != 'factor_portfolio':
            continue
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
                'cfg_sig': cfg_sig(m),
            }
        )
        if len(rows) >= CAP:
            break


    # Sort
    if args.sort == 'mtime':
        # already in mtime order via report scan
        pass
    else:
        key = args.sort
        reverse = True
        if key == 'max_dd':
            # less negative drawdown is better
            reverse = True
        rows.sort(key=lambda r: (r.get(key) is None, r.get(key)), reverse=reverse)


    if args.dedup:
        seen = set()
        deduped = []
        for r in rows:
            sig = r.get('cfg_sig')
            if sig in seen:
                continue
            seen.add(sig)
            deduped.append(r)
        rows = deduped
    if args.top is not None:
        rows = rows[: int(args.top)]

    # add rank
    for i, r in enumerate(rows, 1):
        r['rank'] = i

    # CSV
    import csv

    out_csv = Path(args.out)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ['rank','strategy','factor','direction','rebalance','cagr','sharpe','max_dd','final_equity','annual_turnover','theme','start','end','cost_bps','topk','cfg_sig','run_id'] if rows else []
    with out_csv.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
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
        '|rank|strategy|factor|direction|rebalance|cagr|sharpe|max_dd|final_equity|annual_turnover|theme|cfg_sig|run_id|',
        '|---:|---|---|---|---|---:|---:|---:|---:|---:|---|---|---|',
    ]
    for r in rows:
        lines.append(
            f"|{r.get('rank','')}|{r['strategy']}|{r.get('factor') or ''}|{r.get('direction') or ''}|{r.get('rebalance') or ''}|{fmt(r['cagr'])}|{fmt(r['sharpe'])}|{fmt(r['max_dd'])}|{fmt(r['final_equity'])}|{fmt(r['annual_turnover'])}|{r.get('theme')}|{r.get('cfg_sig','')}|{r['run_id']}|"
        )
    # Write configs map (for front-end drilldown)
    cfg_out = Path(str(out_csv).replace('.csv', '_configs.json'))
    cfg_map = {}
    for r in rows:
        sig = r.get('cfg_sig')
        if not sig:
            continue
        # keep minimal config fields
        cfg_map[sig] = {
            'strategy': r.get('strategy'),
            'theme': r.get('theme'),
            'start': r.get('start'),
            'end': r.get('end'),
            'factor': r.get('factor'),
            'direction': r.get('direction') or ('long_high' if r.get('strategy') == 'factor_portfolio' else None),
            'rebalance': r.get('rebalance'),
            'topk': r.get('topk'),
            'cost_bps': r.get('cost_bps'),
        }
    cfg_out.write_text(__import__('json').dumps(cfg_map, indent=2, ensure_ascii=False), encoding='utf-8')

    out_md.write_text('\n'.join(lines) + '\n', encoding='utf-8')

    print(f"WROTE {out_csv}")
    print(f"WROTE {out_md}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
