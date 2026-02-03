# QUANTAXIS API (MVP)

This is a local MVP HTTP API that can:
- Read latest artifacts under `output/reports/`
- Trigger backtests by posting a config (executes existing CLI runners)

## Start

Recommended (lightweight install for API only):

```bash
pip3 install -r requirements-api.txt
./scripts/run_api.sh
# default: http://127.0.0.1:8000
```

Full research stack (heavier; optional):

```bash
pip3 install -r requirements.txt
./scripts/run_api.sh
```

## Endpoints

- `GET /health`
- `GET /latest/manifest`
- `GET /reports/{run_id}/manifest`
- `GET /reports/{run_id}/artifacts` (artifact_manifest.json)
- `GET /reports/{run_id}/file/{name}` (only files listed in artifact_manifest.json when present)
- `POST /run` (JSON body config; requires `data_version_id` + `manifest_sha256`)
- `GET /runs/{job_id}`

## Security

This is not hardened (beyond basic token auth and local-process safety knobs). Keep it local.


## Auth (recommended)

Set an environment variable to require a token for execution endpoints (`POST /run`, `GET /runs/*`):

```bash
export QUANTAXIS_API_TOKEN="your-secret"
```

Then call with header:

- `X-API-Key: your-secret`

If the token is not set, the API is open (local MVP).

## Hardening knobs (recommended)

These are MVP-level protections (in-memory; single-process). Configure via env vars:

```bash
# Max concurrent background jobs triggered by /run
export QUANTAXIS_API_MAX_CONCURRENT=2

# Per-IP rate limit for POST /run (requests per minute)
export QUANTAXIS_API_RUNS_PER_MIN=6

# Kill a job if it runs too long
export QUANTAXIS_API_JOB_TIMEOUT_SEC=3600

# Tail length stored for troubleshooting
export QUANTAXIS_API_LOG_TAIL=2000

# Max config payload size (bytes, UTF-8)
export QUANTAXIS_API_CFG_MAX_BYTES=200000

# Max nesting depth for config objects
export QUANTAXIS_API_CFG_MAX_DEPTH=12

# Whether GET /runs/{job_id} returns stdout/stderr tails
# (default: false, to avoid leaking sensitive output)
export QUANTAXIS_API_INCLUDE_LOGS=false
```

`GET /health` reports whether auth is required and the current limits.

## Signals (Mode C MVP)

Generate a weekly **equal-weight topK** signal (manual trading workflow).

- `POST /signals/run` (JSON body)
- `GET /signals/{signal_id}` (JSON)
- `GET /signals/{signal_id}.csv` (CSV)
- `GET /signals/{signal_id}_factors.csv` (CSV with factor attribution)

Example (baseline momentum):

```bash
curl -s \
  -H "X-API-Key: your-secret" \
  -H "Content-Type: application/json" \
  -d '{"strategy":"xsec_momentum_weekly_topk","theme":"all","rebalance":"weekly","top_k":10,"min_bars":800,"liq_window":20,"liq_min_ratio":1.0}' \
  http://127.0.0.1:8000/signals/run
```

Example (hybrid c: momentum + MA **hard filter**; default). Add `score_mode:"factor"` to rank by the first factor pack (ret_10d/ret_20d/vol_20d/liq_20d). The signal JSON will include `meta.config_signature`, `meta.score_weights`, `meta.factor_windows`, and `meta.liq_field_detected` for reproducibility:

```bash
curl -s \
  -H "X-API-Key: your-secret" \
  -H "Content-Type: application/json" \
  -d '{"strategy":"hybrid_baseline_weekly_topk","ma_mode":"filter","score_mode":"factor","theme":"all","rebalance":"weekly","top_k":10,"min_bars":800,"liq_window":20,"liq_min_ratio":1.0,"hold_weeks":2,"tranche_overlap":true}' \
  http://127.0.0.1:8000/signals/run
```

Example (hybrid c: momentum + MA **soft boost**):

```bash
curl -s \
  -H "X-API-Key: your-secret" \
  -H "Content-Type: application/json" \
  -d '{"strategy":"hybrid_baseline_weekly_topk","ma_mode":"boost","theme":"all","rebalance":"weekly","top_k":10,"min_bars":800,"liq_window":20,"liq_min_ratio":1.0,"hold_weeks":2,"tranche_overlap":true}' \
  http://127.0.0.1:8000/signals/run
```

Then fetch:

```bash
curl -s -H "X-API-Key: your-secret" http://127.0.0.1:8000/signals/<signal_id> | python3 -m json.tool
curl -s -H "X-API-Key: your-secret" http://127.0.0.1:8000/signals/<signal_id>.csv
curl -s -H "X-API-Key: your-secret" http://127.0.0.1:8000/signals/<signal_id>_factors.csv
```

Notes:
- This uses `scripts/backtest_baseline.py` to produce an internal `positions.csv`, then extracts tranche snapshots.
- Default signals output uses **2-week hold via tranche overlap** (2 tranches, each 50%) and reports `as_of_date` as the **rebalance date** (week end), matching the backtest convention.
  - Control with: `hold_weeks` (default 2) and `tranche_overlap` (default true).
- Liquidity/suspension filter is applied in baseline: `liq_window=20` requires both close present and volume>0 in the recent window.
- Optional hard threshold filters (high-win-rate / tradability oriented):
  - Request cfg: `hard_vol_20d_max`, `hard_liq_20d_min` (set >0 to enable)
  - Env defaults: `QUANTAXIS_HARD_VOL_20D_MAX`, `QUANTAXIS_HARD_LIQ_20D_MIN`
  - Applied when `score_mode:"factor"` (because it relies on factor pack values).
  - Debug info: `meta.hard_filters` + `meta.hard_filter_stats` (hybrid).
