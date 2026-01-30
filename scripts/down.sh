#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mode=${1:-keep}

case "$mode" in
  keep)
    echo "[down] stopping services (keeping volumes)"
    docker compose down
    ;;
  purge)
    echo "[down] stopping services and removing volumes (DANGEROUS)"
    docker compose down -v
    ;;
  *)
    echo "Usage: $0 {keep|purge}" >&2
    exit 2
    ;;
esac
