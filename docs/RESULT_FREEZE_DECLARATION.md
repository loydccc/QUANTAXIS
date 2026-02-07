# Result Freeze Declaration (pre-2025 OOS gate)

**Effective immediately** and until 2025 OOS gate is passed:

## Frozen Components / Rules
1) **HI v1**
   - 6 components: tail_risk, crowding, liquidity_stress, signal_stability, breakout_failure, portfolio_feasibility
   - `health_score = mean(available components)`
   - `exposure = clip(health_score, 0.4, 1.0)`

2) **Signal + Portfolio construction**
   - Weight pipeline: **Top-K → normalize → min_weight**
   - dist hard filter / ladder / L3 remain as implemented.

3) **No parameter changes allowed**
- No changes to: MIN_POS, min_weight, dist/downvol thresholds, HI weights/clip.
- No “small refactors” that change numeric results.

## Allowed changes
- Documentation, runbook, alerting, logging, caching, and tooling that do not alter strategy outputs.

> One-line rule: **2025 OOS passes first; then we can debate improvements.**
