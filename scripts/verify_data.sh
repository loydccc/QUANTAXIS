#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

CONTAINER=quantaxis-core

if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "[verify] container ${CONTAINER} not running; start first with ./scripts/up.sh" >&2
  exit 1
fi

docker exec \
  -e MONGODB_HOST -e MONGODB_PORT -e MONGODB_DATABASE -e MONGODB_USER -e MONGODB_PASSWORD \
  -e MONGO_ROOT_USER -e MONGO_ROOT_PASSWORD \
  ${CONTAINER} \
  python - <<'PY'
import os
import random
import pymongo

host=os.getenv('MONGODB_HOST','mongodb')
port=int(os.getenv('MONGODB_PORT','27017'))
dbname=os.getenv('MONGODB_DATABASE','quantaxis')
user=os.getenv('MONGODB_USER','quantaxis')
pwd=os.getenv('MONGODB_PASSWORD','quantaxis')
ru=os.getenv('MONGO_ROOT_USER','root')
rp=os.getenv('MONGO_ROOT_PASSWORD','root')

uris=[
    f"mongodb://{user}:{pwd}@{host}:{port}/{dbname}?authSource=admin",
    f"mongodb://{ru}:{rp}@{host}:{port}/{dbname}?authSource=admin",
]

client=None
for uri in uris:
    try:
        client=pymongo.MongoClient(uri,serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        break
    except Exception:
        client=None

if client is None:
    raise SystemExit('mongo auth failed')

db=client[dbname]
coll=db['stock_day']

n=coll.estimated_document_count()
print('stock_day estimated_count=', n)

sample_codes=coll.aggregate([
    {'$group': {'_id': '$code'}},
    {'$sample': {'size': 3}},
])
for row in sample_codes:
    code=row['_id']
    last=coll.find({'code': code}).sort('date',-1).limit(1)
    last=list(last)
    if last:
        print('sample', code, 'last_date=', last[0].get('date'), 'close=', last[0].get('close'))
PY
