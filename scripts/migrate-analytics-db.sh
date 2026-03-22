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
STACK_ANALYTICS_DB="${STACK_ANALYTICS_DB:-$(get_env_file_value STACK_ANALYTICS_DB || true)}"
LLM_DEBUG_DB_URL="${LLM_DEBUG_DB_URL:-$(get_env_file_value LLM_DEBUG_DB_URL || true)}"
LLM_ROTATE_OUTCOME_DB_URL="${LLM_ROTATE_OUTCOME_DB_URL:-$(get_env_file_value LLM_ROTATE_OUTCOME_DB_URL || true)}"
LLM_DEBUG_SQLITE="${LLM_DEBUG_SQLITE:-${ROOT_DIR}/bot-api/data/llm-debug.sqlite}"
ROTATION_OUTCOMES_SQLITE="${ROTATION_OUTCOMES_SQLITE:-${ROOT_DIR}/freqtrade/user_data/logs/rotation-outcomes.sqlite}"
ANALYTICS_ADMIN_DB_URL="${ANALYTICS_ADMIN_DB_URL:-postgresql+psycopg2://${STACK_POSTGRES_USER:-stack}:${STACK_POSTGRES_PASSWORD:-stack}@stack-postgres:5432/postgres}"

STACK_POSTGRES_USER="${STACK_POSTGRES_USER:-stack}"
STACK_POSTGRES_PASSWORD="${STACK_POSTGRES_PASSWORD:-stack}"
STACK_ANALYTICS_DB="${STACK_ANALYTICS_DB:-stack_analytics}"
LLM_DEBUG_DB_URL="${LLM_DEBUG_DB_URL:-postgresql+psycopg2://${STACK_POSTGRES_USER}:${STACK_POSTGRES_PASSWORD}@stack-postgres:5432/${STACK_ANALYTICS_DB}}"
LLM_ROTATE_OUTCOME_DB_URL="${LLM_ROTATE_OUTCOME_DB_URL:-postgresql+psycopg2://${STACK_POSTGRES_USER}:${STACK_POSTGRES_PASSWORD}@stack-postgres:5432/${STACK_ANALYTICS_DB}}"

cd "${ROOT_DIR}"
docker compose up -d stack-postgres >/dev/null
sleep 5
docker compose build bot-api >/dev/null

docker compose run --rm --no-deps \
  -v "${ROOT_DIR}:/workspace" \
  bot-api \
  python /workspace/scripts/migrate-analytics-db.py \
  --target-url "${LLM_DEBUG_DB_URL}" \
  --admin-url "${ANALYTICS_ADMIN_DB_URL}" \
  --llm-debug-sqlite "/workspace/${LLM_DEBUG_SQLITE#${ROOT_DIR}/}" \
  --rotation-outcomes-sqlite "/workspace/${ROTATION_OUTCOMES_SQLITE#${ROOT_DIR}/}"
