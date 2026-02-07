# QUANTAXIS Production Runbook (HI v1 + Ladder)

> Goal: a non-author can run the system end-to-end.
> Scope: **daily ops**, **degradation rules**, **who handles alerts**.

## 0) System Overview (frozen)
- Signal engine: `api/signals_impl.py` (hybrid_baseline_weekly_topk)
- Weight pipeline: **Top-K → normalize → min_weight**
- HI v1: 6 components → `health_score ∈ [0,1]` → `exposure = clip(health_score, 0.4, 1.0)`
- Exposure affects **only** overall scale + cash top-up (no impact to selection/ranking/factors).

## 1) Daily Schedule (Asia/Shanghai)

### T-1 after close (or T before open)
1) **Compute HI cache** for next trading day
   - Script: `scripts/health_index_range_v1.py` (batch) OR `scripts/health_index_daily_cache.py --date YYYY-MM-DD` (single-day)
   - Cache dir: `output/reports/health_index/daily/health_score_YYYY-MM-DD.json`
   - Output must exist and be non-empty.

2) **Sanity check HI cache**
   - If cache missing for upcoming day: proceed (see degradation), but open an alert ticket.

### T signal generation (weekly cadence)
1) Generate signal
   - Entry point: `/signals/run` API or direct `run_signal(signal_id, cfg)`
   - Output artifacts:
     - `output/signals/<signal_id>.json`
     - `output/signals/<signal_id>.csv`
     - `output/signals/<signal_id>_factors.csv`

2) Verify output
   - `status == succeeded`
   - `positions` includes `CASH` line when exposure < 1
   - `sum(weights) == 1` within tolerance

### T execution window (broker)
1) Place orders according to positions
2) Execution realism checks (limits, etc.) are handled inside signal generator when enabled.

## 1.5) How “latest pick” is produced (fixed)
For a given trading date `D`:
1) `daily_pipeline.py --date D --run-hi --run-signal`
2) Use the produced signal JSON in `output/signals/prod_signal_<D>_*.json`.
3) If `sealed_ok=false`: **HOLD_PREV** (no new signal; no HI cache is produced).

## 2) Degradation / Failure Handling (write-dead simple)

### HI cache missing
- Rule (fixed): `exposure = 1`
- Signal meta must record: `meta.health.health_missing = true`
- Action: raise alert #3 (health_missing)

### Signal generation fails
- Default rule: **hold yesterday’s portfolio** (no trades)
- Action: page operator + write incident note with signal_id + stacktrace

### Data feed/Mongo issues
- If Mongo unreachable: do not trade; hold yesterday.

## 3) Roles / Ownership
- Primary operator: (fill)
- Backup operator: (fill)
- Escalation: (fill)

## 4) Required Files / Paths
- HI cache (daily): `output/reports/health_index/daily/health_score_<date>.json`
- Health artifacts: `output/reports/health_index/`
- Signal artifacts: `output/signals/`

## 5) Runbook “Done” checklist
- [ ] HI cache produced for the day
- [ ] Signal generated and succeeded
- [ ] Orders placed or hold decision documented
- [ ] Alerts checked and acknowledged
