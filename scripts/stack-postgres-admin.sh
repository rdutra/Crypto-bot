#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"
SHELL_HELPER="${ROOT_DIR}/scripts/shell_helpers.py"
CONTAINER="${STACK_POSTGRES_CONTAINER:-stack-postgres}"
BACKUP_DIR="${STACK_POSTGRES_BACKUP_DIR:-${ROOT_DIR}/postgres-backups}"

get_env_file_value() {
  python3 "${SHELL_HELPER}" env-value "${ENV_FILE}" "$1"
}

STACK_POSTGRES_USER="${STACK_POSTGRES_USER:-$(get_env_file_value STACK_POSTGRES_USER || true)}"
FREQTRADE_POSTGRES_DB="${FREQTRADE_POSTGRES_DB:-$(get_env_file_value FREQTRADE_POSTGRES_DB || true)}"
SPIKE_POSTGRES_DB="${SPIKE_POSTGRES_DB:-$(get_env_file_value SPIKE_POSTGRES_DB || true)}"
STACK_ANALYTICS_DB="${STACK_ANALYTICS_DB:-$(get_env_file_value STACK_ANALYTICS_DB || true)}"
STACK_POSTGRES_USER="${STACK_POSTGRES_USER:-stack}"
FREQTRADE_POSTGRES_DB="${FREQTRADE_POSTGRES_DB:-freqtrade}"
SPIKE_POSTGRES_DB="${SPIKE_POSTGRES_DB:-spike_scanner}"
STACK_ANALYTICS_DB="${STACK_ANALYTICS_DB:-stack_analytics}"

usage() {
  cat <<EOF
Usage:
  ./scripts/stack-postgres-admin.sh check
  ./scripts/stack-postgres-admin.sh backup
EOF
}

check() {
  docker exec "$CONTAINER" pg_isready -U "$STACK_POSTGRES_USER" >/dev/null
  for db in "$FREQTRADE_POSTGRES_DB" "$SPIKE_POSTGRES_DB" "$STACK_ANALYTICS_DB"; do
    echo "== $db =="
    docker exec "$CONTAINER" psql -U "$STACK_POSTGRES_USER" -d "$db" -c "SELECT current_database() AS db, now() AS checked_at;"
  done
}

backup() {
  mkdir -p "$BACKUP_DIR"
  stamp="$(date +%Y%m%d_%H%M%S)"
  for db in "$FREQTRADE_POSTGRES_DB" "$SPIKE_POSTGRES_DB" "$STACK_ANALYTICS_DB"; do
    out="$BACKUP_DIR/${db}.${stamp}.sql"
    docker exec "$CONTAINER" pg_dump -U "$STACK_POSTGRES_USER" -d "$db" > "$out"
    gzip -f "$out"
    echo "Wrote ${out}.gz"
  done
}

cmd="${1:-}"
case "$cmd" in
  check) check ;;
  backup) backup ;;
  *) usage; exit 1 ;;
esac
