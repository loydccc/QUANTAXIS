# QUANTAXIS Roadmap v2 (as of 2026-02-28)

> Goal: run a **reproducible, auditable, high-win-rate-oriented** signal system that is safe in daily operations before scaling live capital.

## 0) Why v2
- Old roadmap was last updated on 2026-02-04 and under-represented later ops work.
- This v2 reflects actual progress after introducing daily pipeline, shadow run, health exposure, and alerting.

## 1) Completed Foundation (Mode A/B/C baseline)
- [x] Repo hygiene and artifact policy (`output/` ignore + latest aggregates)
- [x] Read-only API (`/health`, `/latest/manifest`, `/reports/...`)
- [x] Execute API (`POST /run`, `GET /runs/{job_id}`) and run metadata persistence
- [x] Optional token auth (`QUANTAXIS_API_TOKEN`, `X-API-Key`)
- [x] Signals API (`POST /signals/run`, `GET /signals/{id}`, CSV endpoints)
- [x] Hybrid weekly signal baseline (momentum + MA, weekly rebalance, topK)
- [x] Factor score mode and attribution exports
- [x] 2-tranche overlap (2-week hold smoothing)

## 2) Completed Operational Shift (post-2026-02-04)
- [x] Production daily pipeline: `ingest -> validate -> seal -> HI -> signal`
- [x] Degradation rule: if `sealed_ok=false`, force `HOLD_PREV`
- [x] Shadow-run single-day entrypoint with assertions and fixed output contract
- [x] Signal ladder L0-L3 with fallback asset flow
- [x] Health exposure integration via cash scaling (`exposure = clip(health_score, 0.4, 1.0)`)
- [x] Observability fields in signal meta (`turnover_attrib`, `hold_smoothing`, `ops`)
- [x] Operational alerts evaluator and weekly turnover report
- [x] Runbook/freeze/alerts/checklist docs for pre-prod operation

## 3) P0 — Production Correctness (must finish before scaling)
- [x] Fix `/run` hardening refactor regression (`rate_limit_run`/`redact_text` symbols)
- [x] Integrate alert severity into shadow pass/fail gate (error alert must fail the day)
- [x] Fix daily HI scoring method to use robust history-based ranking (not single-point rank)
- [x] Add watchdog cleanup for stale `running` statuses in signals/jobs
- [x] Unify reproducibility contract between `/run` and `run_signal` (data version fingerprints)
- [x] Add API regression checks that cover `/run` and `/runs/{job_id}` (not only import + `/health`)

## 4) P1 — Ops Hardening and Release Gate (next 2-4 weeks)
- [ ] Define explicit go/no-go checklist for live switch (shadow streak, alert streak, data seal streak)
- [ ] Add structured logs for pipeline/shadow/alerts with stable fields
- [ ] Add small SLO dashboard inputs: seal success rate, shadow pass rate, alert counts
- [ ] Decide `/health` auth policy and document external exposure policy
- [ ] Add CI job for core scripts (`daily_pipeline`, `shadow_run_day`, `alerts_eval`)

## 5) P2 — Research Iteration on Top of Stable Ops
- [ ] Freeze and version a factor computation contract (winsorize/zscore/missing/neutralization switches)
- [ ] Expand factor pack toward 8-12 with grouped purpose tags (trend/reversal/flow/risk)
- [ ] Standardize factor evaluation outputs (RankIC, spread, decay, corr, turnover sensitivity)
- [ ] Establish walk-forward promotion rule: only promote configs passing OOS and ops constraints
- [ ] Add portfolio constraints pack (liquidity cap, single-name cap, sector cap, cost model)

## 6) Scope Notes
- This roadmap prioritizes **production correctness first**, then research improvements.
- No parameter tweaking for performance should bypass freeze/runbook/alert gates.
