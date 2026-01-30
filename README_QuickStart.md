# QUANTAXIS Quick Start (macOS + Docker)

> This repo contains a reproducible Docker Compose setup and a minimal research loop demo (data -> backtest -> output).

## TL;DR

```bash
cp .env.example .env
./scripts/up.sh
./scripts/doctor.sh
./scripts/run_demo.sh

# Real data (TDX, no token)
./scripts/fetch_tdx_stock_day.sh 20240101 20240131 200 000001,600000
./scripts/verify_data.sh

./scripts/down.sh keep
```

