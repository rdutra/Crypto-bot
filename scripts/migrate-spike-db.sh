#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"
SHELL_HELPER="${ROOT_DIR}/scripts/shell_helpers.py"

get_env_file_value() {
  python3 "${SHELL_HELPER}" env-value "${ENV_FILE}" "$1"
}

STACK_POSTGRES_USER="${STACK_POSTGRES_USER:-$(get_env_file_value STACK_POSTGRES_USER || true)}"
STACK_POSTGRES_PASSWORD="${STACK_POSTGRES_PASSWORD:-$(get_env_file_value STACK_POSTGRES_PASSWORD || true)}"
SPIKE_POSTGRES_DB="${SPIKE_POSTGRES_DB:-$(get_env_file_value SPIKE_POSTGRES_DB || true)}"
STACK_POSTGRES_USER="${STACK_POSTGRES_USER:-stack}"
STACK_POSTGRES_PASSWORD="${STACK_POSTGRES_PASSWORD:-stack}"
SPIKE_POSTGRES_DB="${SPIKE_POSTGRES_DB:-spike_scanner}"
SPIKE_DB_URL="${SPIKE_DB_URL:-$(get_env_file_value SPIKE_DB_URL || true)}"
SPIKE_DB_URL="${SPIKE_DB_URL:-postgresql+psycopg2://${STACK_POSTGRES_USER}:${STACK_POSTGRES_PASSWORD}@stack-postgres:5432/${SPIKE_POSTGRES_DB}}"
SPIKE_DB_PATH="${SPIKE_DB_PATH:-$(get_env_file_value SPIKE_DB_PATH || true)}"
SPIKE_DB_PATH="${SPIKE_DB_PATH:-/data/spike-scanner.sqlite}"
SPIKE_ADMIN_DB_URL="${SPIKE_ADMIN_DB_URL:-$(get_env_file_value SPIKE_ADMIN_DB_URL || true)}"
SPIKE_ADMIN_DB_URL="${SPIKE_ADMIN_DB_URL:-postgresql+psycopg2://${STACK_POSTGRES_USER}:${STACK_POSTGRES_PASSWORD}@stack-postgres:5432/postgres}"

cd "${ROOT_DIR}"
docker compose up -d stack-postgres >/dev/null
sleep 5
docker compose build spike-scanner >/dev/null

docker compose run --rm --no-deps \
  -e SPIKE_DB_URL="${SPIKE_DB_URL}" \
  -v "${ROOT_DIR}/freqtrade/user_data/logs:/data" \
  spike-scanner \
  python -m app.migrate_db \
  --from "${SPIKE_DB_PATH}" \
  --to "${SPIKE_DB_URL}" \
  --admin-url "${SPIKE_ADMIN_DB_URL}"
