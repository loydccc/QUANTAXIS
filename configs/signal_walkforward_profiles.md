# signal_walkforward profiles (notes)

## execution_realism_v1 (2026-02-04)

Goal: introduce basic A-share execution feasibility (limit-up/down) while keeping portfolio feasible via backup list.

Command skeleton:

```bash
MONGODB_HOST=127.0.0.1 python3 scripts/backtest_signal_walkforward.py \
  --start 2019-01-01 --end 2026-02-03 --theme a_ex_kcb_bse \
  --topk 50 --candidate-k 250 --backup-k 150 \
  --ma-mode boost --lookback 60 --ma 60 \
  --min-bars 800 --liq-window 20 --liq-min-ratio 1.0 \
  --hold-weeks 2 --cost-bps 10 \
  --w-ret-20d 1.0 --w-ret-10d 0.5 --w-vol-20d -0.5 --w-liq-20d 0.2 \
  --limit-move-mode freeze --limit-tiering \
  --limit-pct 0.10 --limit-touch-mode hl --limit-price-eps-bps 5 --limit-touch-eps 1e-6 \
  --impact-k 0
```

Reference run:
- outdir: output/reports/signal_walkforward/da6f68795e22
- sharpe: 0.7404, cagr: 0.1079, maxdd: -0.2502

Notes:
- Uses tiered limits: 20% for 300/301, 10% otherwise. ST 5% not implemented.
- HL-touch mode: treat `high` touching up-limit as buy-blocking, `low` touching down-limit as sell-blocking.
- Backup list fills blocked buys to maintain gross exposure.
