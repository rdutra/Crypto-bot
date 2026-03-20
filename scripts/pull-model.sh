#!/usr/bin/env bash
set -euo pipefail

MODEL="${1:-${OLLAMA_MODEL:-qwen2.5:3b}}"
LLM_PROVIDER_VALUE="$(./scripts/llm-runtime.sh provider)"
OLLAMA_MODE_VALUE="$(./scripts/llm-runtime.sh ollama-mode)"

if [[ "${LLM_PROVIDER_VALUE}" != "ollama" ]]; then
  echo "LLM_PROVIDER=${LLM_PROVIDER_VALUE}; local model pull only applies to ollama." >&2
  exit 1
fi

if [[ "${OLLAMA_MODE_VALUE}" != "docker" ]]; then
  echo "OLLAMA_BASE_URL=${OLLAMA_BASE_URL:-unset}; local model pull only applies to the dockerized ollama service." >&2
  exit 1
fi

docker compose up -d ollama

echo "[model] Pulling ${MODEL}"
docker compose exec -T ollama ollama pull "${MODEL}"
