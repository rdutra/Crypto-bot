#!/usr/bin/env bash
set -euo pipefail

MODEL="${1:-${OLLAMA_MODEL:-llama3.1:8b}}"
LLM_PROVIDER_VALUE="$(./scripts/llm-runtime.sh provider)"

if [[ "${LLM_PROVIDER_VALUE}" != "ollama" ]]; then
  echo "LLM_PROVIDER=${LLM_PROVIDER_VALUE}; local model pull only applies to ollama." >&2
  exit 1
fi

docker compose up -d ollama

echo "[model] Pulling ${MODEL}"
docker compose exec -T ollama ollama pull "${MODEL}"
