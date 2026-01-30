#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
  echo "[up] .env not found; creating from .env.example"
  cp .env.example .env
fi

echo "[up] starting docker compose..."
docker compose --env-file .env up -d

echo "[up] waiting for healthchecks (up to 180s)..."
end=$((SECONDS+180))
while [ $SECONDS -lt $end ]; do
  bad=$(docker ps --filter "name=quantaxis-" --format '{{.Names}} {{.Status}}' | grep -E 'health: (starting|unhealthy)' || true)
  if [ -z "$bad" ]; then
    break
  fi
  sleep 5
done

echo "[up] done. Run ./scripts/doctor.sh for full diagnostics."
