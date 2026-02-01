# Factor System Plan (MVP)

## Philosophy
- Avoid a "factor zoo" early; start with ~8–12 factors.
- Group by sources of edge and failure modes:
  - Trend/momentum (core)
  - Reversal/mean reversion (hedge momentum crash)
  - Volume/flow (A-share relevant)
  - Risk + tradability (must-have constraints)

## Initial factor set (8–12)

### 1) Momentum / Trend (core)
- `ret_10d`
- `ret_20d` (or risk-adjusted momentum `ret_20d / vol_20d`)
- `breakout_20d` = close / rolling_20d_high - 1

### 2) Reversal / Mean reversion (hedge)
- `ret_1d` (reversal) or `ret_3d`
- `rsi_14` (use cautiously; prefer as a filter rather than a strong alpha)

### 3) Volume / Flow / Money (A-share weighted)
- `turnover_change` (e.g. avg_turnover_5d / avg_turnover_20d - 1)
- `dollar_volume_change` (avg_dvol_5d / avg_dvol_20d - 1)
- `volatility_change` (vol_5d / vol_20d - 1)

### 4) Risk + Tradability (must)
- `vol_20d` (risk filter / penalty; exclude extreme high vol)
- `liq_20d` (liquidity filter: amount/volume/turnover thresholds)
- (A-share optional) `limit_up_count_20d` (filter extreme mania / consecutive limit-up regimes)

### (Optional) US equities add-on
- `eps_revision` (if data available)
- `earnings_reaction_10d` (price reaction around earnings)

## “Importance” of factor system in this setup
1) Stability of candidate supply: keep having "something to buy" reliably; avoid brittle single-signal behavior.
2) Versionability: factor-level attribution enables deliberate iteration instead of rule-tweaking.
3) Controllability: turnover, liquidity, and tail risk become first-class constraints.

## Portfolio / rebalance design (2-week hold)
- Use weekly rebalance with two overlapping tranches:
  - Every week, select a new topK basket and hold for 2 weeks.
  - Portfolio holds tranche(t) + tranche(t-1); turnover becomes smoother and closer to live operations.

## Minimum deliverables (tables / outputs)
1) Unified factor computation contract:
   - winsorize (optional), z-score, missing handling, (optional) industry/size neutralization.
2) Fixed factor evaluation outputs:
   - RankIC, stratified returns, decay curve, turnover, cost sensitivity.
3) Fixed portfolio constraints:
   - liquidity thresholds, single-name cap, sector cap, transaction cost model.
