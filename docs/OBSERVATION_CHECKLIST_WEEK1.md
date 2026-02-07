# Launch Week 1 Observation Checklist (behavior, not performance)

Goal: confirm system behaves correctly under live ops.

Daily (each trading day):
- [ ] health_score, exposure, cash_weight logged
- [ ] exposure changes are smooth (no unexplained jumps)
- [ ] effective_positions_after_min_weight (p05) stays in healthy band
- [ ] ladder level_used (L0/L1/L2/L3) matches expectation
- [ ] health_missing is always false (or explained)
- [ ] Any discrepancy between “market felt bad” vs low health_score noted

Weekly:
- [ ] Number of rebalances matches cadence
- [ ] Turnover within expected bounds
- [ ] No repeated stuck/running statuses
