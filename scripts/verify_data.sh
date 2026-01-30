#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

CONTAINER=quantaxis-core

if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "[verify] container ${CONTAINER} not running; start first with ./scripts/up.sh" >&2
  exit 1
fi

cat > /tmp/qa_verify.py <<'PY'
import os
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

print('stock_day estimated_count=', coll.estimated_document_count())

codes=list(coll.aggregate([
    {'$group': {'_id': '$code'}},
    {'$sample': {'size': 3}},
]))

for row in codes:
    code=row['_id']
    doc=coll.find_one({'code': code}, sort=[('date', -1)])
    if doc:
        print('sample', code, 'last_date=', doc.get('date'), 'close=', doc.get('close'), 'source=', doc.get('source'))

# Show a deterministic check too
last=coll.find_one({'code': '000001'}, sort=[('date', -1)])
if last:
    print('last 000001', last.get('date'), last.get('close'), last.get('source'))
PY

docker cp /tmp/qa_verify.py ${CONTAINER}:/tmp/qa_verify.py

docker exec \
  -e MONGODB_HOST -e MONGODB_PORT -e MONGODB_DATABASE -e MONGODB_USER -e MONGODB_PASSWORD \
  -e MONGO_ROOT_USER -e MONGO_ROOT_PASSWORD \
  ${CONTAINER} \
  python /tmp/qa_verify.py
