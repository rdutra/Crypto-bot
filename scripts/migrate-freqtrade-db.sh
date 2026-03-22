#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SQLITE_DB_URL="${SQLITE_DB_URL:-sqlite:////freqtrade/user_data/tradesv3.sqlite}"
FREQTRADE_DB_URL="${FREQTRADE_DB_URL:-postgresql+psycopg2://stack:stack@stack-postgres:5432/freqtrade}"
BACKUP_DIR="${BACKUP_DIR:-$ROOT_DIR/freqtrade/user_data/db-backups}"
SQLITE_DB_FILE="$ROOT_DIR/freqtrade/user_data/tradesv3.sqlite"

mkdir -p "$BACKUP_DIR"

if [ -f "$SQLITE_DB_FILE" ]; then
  stamp="$(date +%Y%m%d_%H%M%S)"
  cp "$SQLITE_DB_FILE" "$BACKUP_DIR/tradesv3.sqlite.before_postgres_migration.$stamp"
fi

docker compose build freqtrade
docker compose up -d stack-postgres

docker compose run --rm --no-deps \
  -e FREQTRADE__API_SERVER__ENABLED=true \
  -e FREQTRADE__API_SERVER__LISTEN_IP_ADDRESS=0.0.0.0 \
  -e FREQTRADE__API_SERVER__LISTEN_PORT=8080 \
  freqtrade \
  convert-db \
  --db-url-from "$SQLITE_DB_URL" \
  --db-url "$FREQTRADE_DB_URL"

echo "Migration complete."
echo "Source:      $SQLITE_DB_URL"
echo "Destination: $FREQTRADE_DB_URL"
