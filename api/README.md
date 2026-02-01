# QUANTAXIS API (MVP)

This is a local MVP HTTP API that can:
- Read latest artifacts under `output/reports/`
- Trigger backtests by posting a config (executes existing CLI runners)

## Start

```bash
./scripts/run_api.sh
# default: http://127.0.0.1:8000
```

## Endpoints

- `GET /health`
- `GET /latest/manifest`
- `GET /reports/{run_id}/manifest`
- `GET /reports/{run_id}/file/{name}`
- `POST /run` (JSON body config)
- `GET /runs/{job_id}`

## Security

This is not hardened (no auth). Keep it local.


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

Example (baseline momentum):

```bash
curl -s \
  -H "X-API-Key: your-secret" \
  -H "Content-Type: application/json" \
  -d '{"strategy":"xsec_momentum_weekly_topk","theme":"all","rebalance":"weekly","top_k":10,"min_bars":800,"liq_window":20,"liq_min_ratio":1.0}' \
  http://127.0.0.1:8000/signals/run
```

Example (hybrid c: momentum + MA filter):

```bash
curl -s \
  -H "X-API-Key: your-secret" \
  -H "Content-Type: application/json" \
  -d '{"strategy":"hybrid_baseline_weekly_topk","theme":"all","rebalance":"weekly","top_k":10,"min_bars":800,"liq_window":20,"liq_min_ratio":1.0}' \
  http://127.0.0.1:8000/signals/run
```

Then fetch:

```bash
curl -s -H "X-API-Key: your-secret" http://127.0.0.1:8000/signals/<signal_id> | python3 -m json.tool
curl -s -H "X-API-Key: your-secret" http://127.0.0.1:8000/signals/<signal_id>.csv
```

Notes:
- This uses `scripts/backtest_baseline.py` to produce an internal `positions.csv`, then extracts the **latest non-zero portfolio** as the signal.
- Basic filtering currently relies on `min_bars` and non-NaN data. More liquidity/suspension filters can be added next.
