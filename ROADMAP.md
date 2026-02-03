# QUANTAXIS API – Roadmap / Milestones

> Goal: productize a small local HTTP API around QUANTAXIS so we can **read artifacts**, **execute backtests**, and (Mode C) generate **high-win-rate oriented** stock selection signals safely.

## Milestone 0 — Repo hygiene / baseline
- [x] `.gitignore` excludes `output/` and other generated artifacts (keep only latest aggregates)
- [x] Basic run docs / quickstart (API README updated)

## Milestone 1 — Mode A (read-only API)
- [x] `GET /health`
- [x] `GET /latest/manifest`
- [x] `GET /reports/{run_id}/manifest`
- [x] `GET /reports/{run_id}/file/{name}` (basic filename allowlist)

## Milestone 2 — Mode B (execute API) + job tracking
- [x] `POST /run` triggers `scripts/run_from_cfg.py` in background
- [x] `GET /runs/{job_id}` returns status/result
- [x] Persist run metadata under `output/api_runs/`

## Milestone 3 — Minimal security baseline (optional token auth)
- [x] Env var `QUANTAXIS_API_TOKEN`
- [x] If set, require header `X-API-Key` for:
  - `POST /run`
  - `GET /runs/{job_id}`
- [x] `/health` includes `auth_required: true/false`

## Milestone 4 — Hardening (next)
- [ ] Decide policy for `/health` (keep open vs require auth when token set)
- [ ] Rate limit (simple in-memory) on `/run` and `/runs/*`
- [ ] IP allowlist (optional)
- [ ] Safer run execution (timeout, max concurrent jobs, sanitized config)
- [ ] Log redaction / avoid returning sensitive stdout/stderr

## Milestone 5 — Packaging / Ops
- [ ] Dockerfile / compose
- [ ] Proper config management (Pydantic settings)
- [ ] Structured logging
- [ ] Tests for auth + path traversal

## Milestone 6 — Mode C (Signals) — baseline weekly topK (manual trading)
> Goal: produce a **weekly “what to buy” list** (JSON + CSV) for manual execution, optimized for **high win rate** and stability (not just backtest CAGR).

Phase 6.1 (baseline → workflow)
- [x] Add signal job store under `output/signals/`
- [x] Define signal artifact schema (JSON) + CSV export
  - required: `as_of_date`, `strategy`, `theme`, `top_k`, `rebalance`, `positions[] (code, weight, rank, score)`
  - include: version fingerprints (`meta.config_signature`, `meta.universe_fingerprint`, `meta.universe_size`)
  - optional: factor attribution (`positions[].factors`, `positions[].zfactors`) and tranche debug info (`meta.tranches`)
- [x] Add API endpoints:
  - `POST /signals/run` → returns `signal_id` (async)
  - `GET /signals/{signal_id}` → returns JSON (or status while running)
  - `GET /signals/{signal_id}.csv` → returns CSV
  - `GET /signals/{signal_id}_factors.csv` → returns factor attribution CSV
- [x] Implement generator for **baseline strategies** (momentum / MA) with **weekly rebalance** and **equal-weight topK**

Phase 6.2 (lightweight score → hybrid “c”)
- [x] Add a lightweight score/rank aggregator (normalize 1–2 baseline signals into a score)
- [x] Export `score` and stable tie-break rules
- [x] (Optional) Add 2-tranche overlap for 2-week hold (weekly rebalance, hold 2 weeks)

Phase 6.3 (factor system path → toward “b”)
- [ ] Add factor plan + computation contract (winsorize/zscore/missing; optional neutralization)
- [ ] Implement initial 8–12 factors (grouped: momentum, reversal, flow, risk+tradability)
- [ ] Extend score to support factor-based ranking (multi-factor)
- [ ] Factor evaluation fixed outputs (RankIC, stratified returns, decay, turnover, cost sensitivity)
- [ ] Portfolio constraints fixed (liquidity, single-name cap, sector cap, cost model)
- [ ] Plug factor eval outputs / factor-bt signals as optional inputs
