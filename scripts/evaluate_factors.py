#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Evaluate cross-sectional factors with simple IC + quantile portfolio returns.

Inputs:
- factor_values.parquet: long table with columns [date, code, <factor...>]
- close panel (date x code) fetched from Mongo (same as other scripts)

Outputs:
- ic.csv: per-date IC for each factor
- ic_summary.json: mean IC/IR
- quantile_returns.csv: per-date q1..qN portfolio returns (equal-weight)
- metrics.json: overall stats per factor (meanIC, IR, qspread)

This is MVP-level evaluation; future versions can add:
- rank IC (Spearman), neutralization, transaction costs, turnover, constraints.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pymongo


@dataclass
class MongoCfg:
    host: str
    port: int
    db: str
    user: str
    password: str
    root_user: str
    root_password: str


def get_mongo_cfg() -> MongoCfg:
    return MongoCfg(
        host=os.getenv("MONGODB_HOST", "mongodb"),
        port=int(os.getenv("MONGODB_PORT", "27017")),
        db=os.getenv("MONGODB_DATABASE", "quantaxis"),
        user=os.getenv("MONGODB_USER", "quantaxis"),
        password=os.getenv("MONGODB_PASSWORD", "quantaxis"),
        root_user=os.getenv("MONGO_ROOT_USER", "root"),
        root_password=os.getenv("MONGO_ROOT_PASSWORD", "root"),
    )


def mongo_client(cfg: MongoCfg) -> pymongo.MongoClient:
    uris = [
        f"mongodb://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.db}?authSource=admin",
        f"mongodb://{cfg.root_user}:{cfg.root_password}@{cfg.host}:{cfg.port}/{cfg.db}?authSource=admin",
    ]
    last = None
    for uri in uris:
        try:
            c = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)
            c.admin.command("ping")
            return c
        except Exception as e:
            last = e
    raise last  # type: ignore[misc]


def norm_date(s: str) -> str:
    s = s.strip()
    if '-' in s:
        return s
    if len(s) == 8:
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    raise ValueError(s)


def load_universe(theme: str) -> List[str]:
    """Return the base universe by theme.

    Supports special themes derived from Mongo:
    - hs10: 沪深主板 10%（排除创业板/科创板/北交所/新三板）
    - cyb20: 创业板 20%（300/301）
    - a_ex_kcb_bse: 沪深主板 + 创业板（仅排除科创板 688 与北交所/新三板）
    """

    theme = (theme or 'all').strip()

    def _is_hs10(code: str) -> bool:
        if not code or len(code) != 6 or not code.isdigit():
            return False
        if code.startswith(('300', '301', '688')):
            return False
        if code.startswith(('8', '4')):
            return False
        return code.startswith(('600', '601', '603', '605', '000', '001', '002', '003'))

    def _is_cyb20(code: str) -> bool:
        return bool(code) and len(code) == 6 and code.isdigit() and code.startswith(('300', '301'))

    def _is_a_ex_kcb_bse(code: str) -> bool:
        if not code or len(code) != 6 or not code.isdigit():
            return False
        if code.startswith('688'):
            return False
        if code.startswith(('8', '4')):
            return False
        return code.startswith(('600', '601', '603', '605', '000', '001', '002', '003', '300', '301'))

    if theme in {'hs10', 'cn_hs10', 'a_hs10'} or theme in {'cyb20', 'cn_cyb20', 'a_cyb20'} or theme in {'a_ex_kcb_bse', 'cn_a_ex_kcb_bse', 'a_no_kcb_bse'}:
        cfg = get_mongo_cfg()
        client = mongo_client(cfg)
        db = client[cfg.db]
        codes: set[str] = set()
        coll = db.get_collection('stock_list')
        try:
            n = coll.estimated_document_count()
        except Exception:
            n = 0
        if n and n > 0:
            for doc in coll.find({}, {'_id': 0, 'code': 1, 'ts_code': 1}):
                c = doc.get('code')
                if not c and doc.get('ts_code'):
                    c = str(doc.get('ts_code')).split('.')[0]
                if c:
                    codes.add(str(c).zfill(6))
        else:
            for c in db['stock_day'].distinct('code'):
                if c:
                    codes.add(str(c).zfill(6))
        if theme.startswith('hs'):
            return sorted([c for c in codes if _is_hs10(c)])
        if theme.startswith('cy'):
            return sorted([c for c in codes if _is_cyb20(c)])
        return sorted([c for c in codes if _is_a_ex_kcb_bse(c)])

    obj = json.loads(Path('watchlists/themes_seed_cn.json').read_text(encoding='utf-8'))
    codes = set()
    for t in obj['themes']:
        if theme == 'all' or t['theme'] == theme:
            for c in t['seed_codes']:
                codes.add(str(c).zfill(6))
    return sorted(codes)


def fetch_close_panel(coll, codes: List[str], start: str, end: str) -> pd.DataFrame:
    series = {}
    for code in codes:
        cur = coll.find({'code': code, 'date': {'$gte': start, '$lte': end}}, {'_id': 0, 'date': 1, 'close': 1}).sort('date', 1)
        rows = list(cur)
        if not rows:
            continue
        df = pd.DataFrame(rows)
        df['date'] = pd.to_datetime(df['date'])
        df = df.dropna(subset=['close']).drop_duplicates(subset=['date']).set_index('date')
        series[code] = df['close'].astype(float)
    if not series:
        raise RuntimeError('no data')
    return pd.concat(series, axis=1).sort_index()


def forward_return(close: pd.DataFrame, horizon: int) -> pd.DataFrame:
    return close.shift(-horizon) / close - 1.0




def spearman_ic(factor: pd.Series, fwd_ret: pd.Series) -> float:
    df = pd.concat([factor, fwd_ret], axis=1).dropna()
    if len(df) < 5:
        return float('nan')
    x = df.iloc[:, 0].rank(method='average')
    y = df.iloc[:, 1].rank(method='average')
    if x.std(ddof=0) == 0 or y.std(ddof=0) == 0:
        return float('nan')
    return float(x.corr(y, method='pearson'))


def pearson_ic(factor: pd.Series, fwd_ret: pd.Series) -> float:
    df = pd.concat([factor, fwd_ret], axis=1).dropna()
    if len(df) < 5:
        return float('nan')
    x = df.iloc[:, 0]
    y = df.iloc[:, 1]
    if x.std(ddof=0) == 0 or y.std(ddof=0) == 0:
        return float('nan')
    return float(x.corr(y, method='pearson'))


def quantile_portfolio_returns(
    factor: pd.Series,
    fwd_ret: pd.Series,
    n_quantiles: int,
) -> Tuple[pd.Series, float]:
    df = pd.concat([factor, fwd_ret], axis=1).dropna()
    if len(df) < n_quantiles * 3:
        return pd.Series(dtype=float), float('nan')

    # qcut with duplicates handling
    try:
        qs = pd.qcut(df.iloc[:, 0], q=n_quantiles, labels=False, duplicates='drop')
    except Exception:
        return pd.Series(dtype=float), float('nan')

    out = {}
    for q in range(int(qs.min()), int(qs.max()) + 1):
        m = qs == q
        if m.sum() == 0:
            continue
        out[f"q{q+1}"] = float(df.loc[m, df.columns[1]].mean())

    # spread: top - bottom
    spread = float('nan')
    if 'q1' in out and f"q{n_quantiles}" in out:
        spread = float(out[f"q{n_quantiles}"] - out['q1'])
    return pd.Series(out, dtype=float), spread


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', required=True)
    ap.add_argument('--end', required=True)
    ap.add_argument('--theme', default='all')
    ap.add_argument('--factor-parquet', required=True)
    ap.add_argument('--horizon', type=int, default=5, help='forward return horizon in trading days')
    ap.add_argument('--rebalance', choices=['daily','weekly','monthly'], default='daily', help='evaluate only on rebalance dates')
    ap.add_argument('--ic-method', choices=['pearson','spearman','both'], default='both')
    ap.add_argument('--quantiles', type=int, default=5)
    ap.add_argument('--outdir', default='/tmp/output')
    args = ap.parse_args(argv)

    start = norm_date(args.start)
    end = norm_date(args.end)
    theme = args.theme

    fac = pd.read_parquet(args.factor_parquet)
    fac['date'] = pd.to_datetime(fac['date'])
    fac = fac.set_index(['date', 'code']).sort_index()

    factor_cols = [c for c in fac.columns if c not in []]

    codes = load_universe(theme)
    cfg = get_mongo_cfg()
    client = mongo_client(cfg)
    coll = client[cfg.db]['stock_day']

    close = fetch_close_panel(coll, codes, start, end)
    fwd = forward_return(close, horizon=int(args.horizon))

    dates = sorted(set(fac.index.get_level_values(0)) & set(close.index))

    if args.rebalance == 'weekly':
        # use last available trading day of each week (Fri label) within available dates
        di = pd.DatetimeIndex(dates)
        weekly = di.to_series(index=di).groupby(di.to_period('W-FRI')).max().sort_values().tolist()
        dates = [d for d in weekly if d in set(dates)]

    if args.rebalance == 'monthly':
        di = pd.DatetimeIndex(dates)
        monthly = di.to_series(index=di).groupby(di.to_period('M')).max().sort_values().tolist()
        dates = [d for d in monthly if d in set(dates)]

    ic_rows = []
    qret_rows = []

    for d in dates:
        fwd_d = fwd.loc[d]
        row_ic = {'date': d}
        row_q = {'date': d}
        for fc in factor_cols:
            f = fac.xs(d, level=0)[fc]
            ic_p = pearson_ic(f, fwd_d)
            ic_s = spearman_ic(f, fwd_d)
            if args.ic_method in ('pearson','both'):
                row_ic[f"{fc}_ic_pearson"] = ic_p
            if args.ic_method in ('spearman','both'):
                row_ic[f"{fc}_ic_spearman"] = ic_s

            qrets, spread = quantile_portfolio_returns(f, fwd_d, n_quantiles=int(args.quantiles))
            # store per-factor spread as <factor>_qspread
            row_q[f"{fc}_qspread"] = spread
            for k, v in qrets.items():
                row_q[f"{fc}_{k}"] = float(v)

        ic_rows.append(row_ic)
        qret_rows.append(row_q)

    ic_df = pd.DataFrame(ic_rows).set_index('date').sort_index()
    q_df = pd.DataFrame(qret_rows).set_index('date').sort_index()

    def ic_summary(s: pd.Series) -> Dict[str, float]:
        s = s.dropna()
        if len(s) < 10:
            return {'mean': float('nan'), 'std': float('nan'), 'ir': float('nan'), 'n': float(len(s))}
        mu = float(s.mean())
        sd = float(s.std(ddof=0))
        ir = float(mu / (sd + 1e-12) * np.sqrt(252))
        return {'mean': mu, 'std': sd, 'ir': ir, 'n': float(len(s))}

    metrics = {
        'generated_at': int(time.time()),
        'theme': theme,
        'start': str(close.index.min().date()),
        'end': str(close.index.max().date()),
        'horizon': int(args.horizon),
        'quantiles': int(args.quantiles),
        'rebalance': args.rebalance,
        'ic_method': args.ic_method,
        'factors': {},
    }

    for fc in factor_cols:
        out: Dict[str, object] = {}
        if args.ic_method in ('pearson', 'both'):
            col = f"{fc}_ic_pearson"
            if col in ic_df.columns:
                out['ic_pearson'] = ic_summary(ic_df[col])
        if args.ic_method in ('spearman', 'both'):
            col = f"{fc}_ic_spearman"
            if col in ic_df.columns:
                out['ic_spearman'] = ic_summary(ic_df[col])

        out['avg_qspread'] = float(q_df[f"{fc}_qspread"].dropna().mean()) if f"{fc}_qspread" in q_df.columns else float('nan')
        metrics['factors'][fc] = out

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    ic_df.to_csv(outdir / 'ic.csv')
    q_df.to_csv(outdir / 'quantile_returns.csv')
    (outdir / 'metrics.json').write_text(json.dumps(metrics, indent=2, ensure_ascii=False), encoding='utf-8')

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
