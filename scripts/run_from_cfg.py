#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Run a strategy backtest from a config JSON or a cfg_sig lookup.

This is the "API skeleton": given a config, dispatch to the correct runner.

Current supported strategies:
- xsec_momentum_weekly_topk
- xsec_momentum_weekly_invvol
- ts_ma_weekly
- factor_portfolio

Config format (minimum):
{
  "strategy": "factor_portfolio",
  "start": "20190101",
  "end": "20241231",
  "theme": "all",
  "cost_bps": 10,
  ...
}

If --sig is provided, we load output/reports/latest_strategy_compare_configs.json and use that config.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any, Dict, Optional


def sh(cmd: list[str]) -> None:
    print('[run_from_cfg] ' + ' '.join(cmd))
    subprocess.check_call(cmd)




def newest_report_dir() -> str:
    rdir = Path('output/reports')
    dirs = [p for p in rdir.iterdir() if p.is_dir()]
    dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return str(dirs[0]) if dirs else ''


def load_cfg_from_sig(sig: str) -> Dict[str, Any]:
    p = Path('output/reports/latest_strategy_compare_configs.json')
    if not p.exists():
        raise SystemExit('missing latest_strategy_compare_configs.json; run summarize_strategy_reports first')
    m = json.loads(p.read_text(encoding='utf-8'))
    if sig not in m:
        raise SystemExit(f'sig not found: {sig}')
    return m[sig]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', help='path to JSON config')
    ap.add_argument('--sig', help='cfg_sig to run (looks up latest_strategy_compare_configs.json)')
    ap.add_argument('--start', help='override start (YYYYMMDD)')
    ap.add_argument('--end', help='override end (YYYYMMDD)')
    ap.add_argument('--result', default=None, help='optional path to write result JSON (run_id/report_dir)')
    args = ap.parse_args()

    cfg: Optional[Dict[str, Any]] = None
    if args.sig:
        cfg = load_cfg_from_sig(args.sig)
    elif args.config:
        cfg = json.loads(Path(args.config).read_text(encoding='utf-8'))
    else:
        raise SystemExit('provide --config or --sig')

    if args.start:
        cfg['start'] = args.start
    if args.end:
        cfg['end'] = args.end

    strategy = cfg.get('strategy')
    start = str(cfg.get('start') or '20190101')
    end = str(cfg.get('end') or '20241231')
    theme = str(cfg.get('theme') or 'all')
    cost_bps = str(cfg.get('cost_bps') or 10)

    if strategy in ('xsec_momentum_weekly_topk','xsec_momentum_weekly_invvol','ts_ma_weekly'):
        lookback = str(cfg.get('lookback') or 60)
        topk = str(cfg.get('top') or 10)
        ma = str(cfg.get('ma') or 60)
        minbars = str(cfg.get('min_bars') or 800)
        vol_window = str(cfg.get('vol_window') or 20)
        max_weight = str(cfg.get('max_weight') or 0.10)
        sh(['./scripts/run_baseline_backtest.sh', start, end, theme, strategy, lookback, topk, ma, cost_bps, minbars, vol_window, max_weight])
        report_dir = newest_report_dir()
        run_id = Path(report_dir).name if report_dir else None
        result_obj = {'run_id': run_id, 'report_dir': report_dir}
        if args.result:
            rp = Path(args.result)
            rp.parent.mkdir(parents=True, exist_ok=True)
            rp.write_text(json.dumps(result_obj, indent=2, ensure_ascii=False), encoding='utf-8')
        print(json.dumps(result_obj, ensure_ascii=False))
        return 0

    if strategy == 'factor_portfolio':
        factor = str(cfg.get('factor') or 'mom_60')
        reb = str(cfg.get('rebalance') or 'weekly')
        direction = str(cfg.get('direction') or 'long_high')
        topk = str(cfg.get('topk') or 10)
        quantile = cfg.get('quantile')
        if quantile is None or quantile == '' or str(quantile) == 'None':
            sh(['./scripts/run_factor_portfolio_backtest.sh', start, end, theme, factor, reb, direction, topk, '', cost_bps])
        else:
            sh(['./scripts/run_factor_portfolio_backtest.sh', start, end, theme, factor, reb, direction, topk, str(quantile), cost_bps])
        report_dir = newest_report_dir()
        run_id = Path(report_dir).name if report_dir else None
        result_obj = {'run_id': run_id, 'report_dir': report_dir}
        if args.result:
            rp = Path(args.result)
            rp.parent.mkdir(parents=True, exist_ok=True)
            rp.write_text(json.dumps(result_obj, indent=2, ensure_ascii=False), encoding='utf-8')
        print(json.dumps(result_obj, ensure_ascii=False))
        return 0

    raise SystemExit(f'unknown strategy: {strategy}')


if __name__ == '__main__':
    raise SystemExit(main())
