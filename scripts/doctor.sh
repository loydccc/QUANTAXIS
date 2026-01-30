#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

RED='\033[0;31m'
GRN='\033[0;32m'
YEL='\033[0;33m'
NC='\033[0m'

ok(){ echo -e "${GRN}OK${NC} $*"; }
warn(){ echo -e "${YEL}WARN${NC} $*"; }
fail(){ echo -e "${RED}FAIL${NC} $*"; exit 1; }

command -v docker >/dev/null || fail "docker not found"
docker info >/dev/null 2>&1 || fail "docker daemon not reachable"
ok "docker daemon reachable"

docker compose version >/dev/null 2>&1 || fail "docker compose not available"
ok "docker compose available"

echo "--- containers ---"
docker compose ps

# ports
check_port(){
  local port=$1
  if lsof -nP -iTCP:${port} -sTCP:LISTEN >/dev/null 2>&1; then
    ok "port ${port} listening"
  else
    warn "port ${port} not listening"
  fi
}
check_port 27017
check_port 6379
# check_port 5672
# check_port 15672
# check_port 8888
# check_port 8010

# basic service checks using docker exec (no local deps)
if docker ps --format '{{.Names}}' | grep -q '^quantaxis-mongodb$'; then
  docker exec quantaxis-mongodb mongosh --quiet --eval "db.adminCommand('ping')" >/dev/null && ok "mongodb ping" || fail "mongodb ping failed"
else
  warn "mongodb container not found"
fi

if docker ps --format '{{.Names}}' | grep -q '^quantaxis-redis$'; then
  docker exec quantaxis-redis redis-cli -a "${REDIS_PASSWORD:-quantaxis}" ping >/dev/null 2>&1 && ok "redis ping" || warn "redis ping failed (check REDIS_PASSWORD)"
else
  warn "redis container not found"
fi

echo "--- logs (tail) ---"
docker compose logs --tail=80 mongodb redis quantaxis 2>/dev/null || true

ok "doctor finished"
