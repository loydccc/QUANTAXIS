#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p output

# run demo inside quantaxis container to avoid local python deps
CONTAINER=quantaxis-core

if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "[demo] container ${CONTAINER} not running; start first with ./scripts/up.sh" >&2
  exit 1
fi

# Copy demo script into container and run
cat > /tmp/qa_demo.py <<'PY'
import os
import pandas as pd
import numpy as np
from datetime import datetime

# This demo is designed to be stable/reproducible without external data sources.
# It generates OHLCV sample data for 1 A-share symbol and 1 US symbol, writes to Mongo,
# then runs a minimal moving-average crossover backtest and outputs metrics + results.

# Mongo connection from env (compose sets these)
MONGO_HOST = os.getenv('MONGODB_HOST', 'mongodb')
MONGO_PORT = int(os.getenv('MONGODB_PORT', '27017'))
MONGO_USER = os.getenv('MONGODB_USER', 'quantaxis')
MONGO_PWD  = os.getenv('MONGODB_PASSWORD', 'quantaxis')
MONGO_DB   = os.getenv('MONGODB_DATABASE', 'quantaxis')

import pymongo

# Try app user first; if the persistent mongo volume was initialized with different creds,
# fall back to the root user (default root/root in docker-compose).
uris = [
    f"mongodb://{MONGO_USER}:{MONGO_PWD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource=admin",
    f"mongodb://{os.getenv('MONGO_ROOT_USER','root')}:{os.getenv('MONGO_ROOT_PASSWORD','root')}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource=admin",
]
last_err = None
client = None
for uri in uris:
    try:
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        break
    except Exception as e:
        last_err = e
        client = None

if client is None:
    raise last_err

db = client[MONGO_DB]

np.random.seed(42)

def gen_ohlcv(symbol, start='2020-01-01', end='2020-12-31'):
    dates = pd.date_range(start, end, freq='B')
    n = len(dates)
    price = 100 + np.cumsum(np.random.normal(0, 1, n))
    close = price
    open_ = close + np.random.normal(0, 0.5, n)
    high = np.maximum(open_, close) + np.abs(np.random.normal(0, 0.8, n))
    low = np.minimum(open_, close) - np.abs(np.random.normal(0, 0.8, n))
    vol = np.random.randint(1000, 5000, n)
    df = pd.DataFrame({
        'date': dates.strftime('%Y-%m-%d'),
        'open': open_, 'high': high, 'low': low, 'close': close,
        'volume': vol,
        'code': symbol,
    })
    return df

# collections
COLL = db['demo_ohlcv']
COLL.create_index([('code', 1), ('date', 1)], unique=True)

symbols = [('000001', 'CN'), ('AAPL', 'US')]

for sym, mkt in symbols:
    df = gen_ohlcv(sym)
    # upsert
    ops = []
    for row in df.to_dict('records'):
        ops.append(pymongo.UpdateOne({'code': row['code'], 'date': row['date']}, {'$set': {**row, 'market': mkt}}, upsert=True))
    if ops:
        res = COLL.bulk_write(ops, ordered=False)
        print(f"Inserted/updated {sym}: upserted={res.upserted_count} modified={res.modified_count}")

# load back for one symbol and run MA crossover

def ma_backtest(sym, fast=5, slow=20):
    cursor = COLL.find({'code': sym}).sort('date', 1)
    df = pd.DataFrame(list(cursor))
    if df.empty:
        raise RuntimeError(f"no data for {sym}")
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    df['fast'] = df['close'].rolling(fast).mean()
    df['slow'] = df['close'].rolling(slow).mean()
    df['signal'] = (df['fast'] > df['slow']).astype(int)
    df['pos'] = df['signal'].shift(1).fillna(0)
    df['ret'] = df['close'].pct_change().fillna(0)
    df['strategy_ret'] = df['pos'] * df['ret']
    equity = (1 + df['strategy_ret']).cumprod()

    total_return = equity.iloc[-1] - 1
    # max drawdown
    peak = equity.cummax()
    dd = equity/peak - 1
    max_dd = dd.min()
    win_rate = (df['strategy_ret'] > 0).mean()

    out = {
        'symbol': sym,
        'total_return': float(total_return),
        'max_drawdown': float(max_dd),
        'win_rate': float(win_rate),
        'start': df['date'].min().strftime('%Y-%m-%d'),
        'end': df['date'].max().strftime('%Y-%m-%d'),
        'bars': int(len(df)),
    }
    df_out = df[['date','open','high','low','close','volume','fast','slow','pos','ret','strategy_ret']].copy()
    df_out['equity'] = equity.values
    return out, df_out

results = []
all_rows = []
for sym, _ in symbols:
    metrics, df_out = ma_backtest(sym)
    results.append(metrics)
    df_out['symbol'] = sym
    all_rows.append(df_out)

metrics_text = "\n".join([
    f"symbol={m['symbol']} bars={m['bars']} range={m['start']}..{m['end']} total_return={m['total_return']:.4f} max_drawdown={m['max_drawdown']:.4f} win_rate={m['win_rate']:.4f}"
    for m in results
])
print(metrics_text)

# write outputs to mounted volume (repo is not mounted; we will docker cp back)
out_dir = '/tmp/output'
os.makedirs(out_dir, exist_ok=True)
with open(os.path.join(out_dir, 'metrics.txt'), 'w', encoding='utf-8') as f:
    f.write(metrics_text + "\n")

pd.concat(all_rows, ignore_index=True).to_csv(os.path.join(out_dir, 'results.csv'), index=False)

print("WROTE:", os.path.join(out_dir, 'metrics.txt'))
print("WROTE:", os.path.join(out_dir, 'results.csv'))
PY

docker cp /tmp/qa_demo.py ${CONTAINER}:/tmp/qa_demo.py

echo "[demo] running demo in container..."
docker exec -e MONGODB_DATABASE=quantaxis ${CONTAINER} python /tmp/qa_demo.py | tee output/metrics_console.txt

# copy generated artifacts back
docker cp ${CONTAINER}:/tmp/output/metrics.txt output/metrics.txt
docker cp ${CONTAINER}:/tmp/output/results.csv output/results.csv

echo "[demo] output written to ./output"
ls -lah output | sed -n '1,200p'
