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
