# QUANTAXIS API – Roadmap / Milestones

> Goal: productize a small local HTTP API around QUANTAXIS so we can **read artifacts** and **execute backtests** safely.

## Milestone 0 — Repo hygiene / baseline
- [ ] `.gitignore` excludes `output/` and other generated artifacts
- [ ] Basic run docs / quickstart

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
