#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Run factor MVP on Mongo stock_day for a theme universe and export artifacts.

Outputs under /tmp/output:
- factor_values.parquet (long: date, code, factor columns)
- factor_zscore.parquet

This is a stepping stone to factor evaluation (IC/quantiles).
"""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pandas as pd
import pymongo

from factors.factor_mvp import compute_factors, zscore_by_date


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


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', required=True)
    ap.add_argument('--end', required=True)
    ap.add_argument('--theme', default='all')
    ap.add_argument('--outdir', default='/tmp/output')
    args = ap.parse_args(argv)

    start = norm_date(args.start)
    end = norm_date(args.end)
    codes = load_universe(args.theme)

    cfg = get_mongo_cfg()
    client = mongo_client(cfg)
    coll = client[cfg.db]['stock_day']

    close = fetch_close_panel(coll, codes, start, end)
    fac = compute_factors(close)
    fac_z = zscore_by_date(fac)

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # parquet
    fac.reset_index().to_parquet(outdir / 'factor_values.parquet', index=False)
    fac_z.reset_index().to_parquet(outdir / 'factor_zscore.parquet', index=False)

    meta = {
        'generated_at': int(time.time()),
        'theme': args.theme,
        'start': str(close.index.min().date()),
        'end': str(close.index.max().date()),
        'codes': int(close.shape[1]),
        'rows': int(len(fac)),
        'factors': list(fac.columns),
    }
    (outdir / 'factor_meta.json').write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding='utf-8')
    print(json.dumps(meta, indent=2, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
