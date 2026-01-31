#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Factor MVP (price/volume only) for mid/low-frequency research.

Provides:
- factor computation on a close panel (date x code)
- standardized outputs suitable for later IC / quantile portfolio evaluation

Factors (v0):
- mom_60: 60-day momentum (close / close.shift(60) - 1)
- mom_20: 20-day momentum
- vol_20: 20-day realized volatility (std of daily returns)
- rev_5: 5-day reversal (-5-day return)
- liq_20: 20-day average traded value proxy (placeholder: uses |ret|*close as proxy if amount unavailable)

Note: we currently only store close in the minimal backtest panel extraction.
If amount/volume fields are later added to panel, we can upgrade liq factor.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def compute_factors(close: pd.DataFrame) -> pd.DataFrame:
    """Return a MultiIndex DataFrame indexed by (date, code) with factor columns."""
    close = close.sort_index()
    ret = close.pct_change(fill_method=None)

    factors = {}
    factors['mom_60'] = close / close.shift(60) - 1.0
    factors['mom_20'] = close / close.shift(20) - 1.0
    factors['vol_20'] = ret.rolling(20).std()
    factors['rev_5'] = -(close / close.shift(5) - 1.0)

    # crude liquidity proxy: abs return * price (better once we have amount)
    factors['liq_20'] = (ret.abs() * close).rolling(20).mean()

    # stack to long format
    df = pd.concat({k: v.stack(dropna=False, future_stack=True) for k, v in factors.items()}, axis=1)
    df.index.set_names(['date', 'code'], inplace=True)
    return df


def zscore_by_date(factor_long: pd.DataFrame) -> pd.DataFrame:
    """Cross-sectional zscore per date for each factor column."""
    def _z(x: pd.Series) -> pd.Series:
        mu = x.mean()
        sd = x.std(ddof=0)
        if sd == 0 or np.isnan(sd):
            return x * 0.0
        return (x - mu) / sd

    out = factor_long.groupby(level=0).transform(_z)
    return out
