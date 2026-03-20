#!/usr/bin/env bash
set -euo pipefail

MODEL="${1:-${OLLAMA_MODEL:-qwen2.5:3b}}"
LLM_PROVIDER_VALUE="$(./scripts/llm-runtime.sh provider)"
OLLAMA_MODE_VALUE="$(./scripts/llm-runtime.sh ollama-mode)"
read -r -a llm_services <<<"$(./scripts/llm-runtime.sh services)"

echo "[bootstrap] Starting ${LLM_PROVIDER_VALUE} runtime, bot-api, and spike-scanner..."
docker compose up -d --build "${llm_services[@]}"

if [[ "${LLM_PROVIDER_VALUE}" == "ollama" && "${OLLAMA_MODE_VALUE}" == "docker" ]]; then
  echo "[bootstrap] Waiting for Ollama API..."
  until docker compose exec -T ollama ollama list >/dev/null 2>&1; do
    sleep 2
  done

  echo "[bootstrap] Pulling model: ${MODEL}"
  docker compose exec -T ollama ollama pull "${MODEL}"
elif [[ "${LLM_PROVIDER_VALUE}" == "ollama" ]]; then
  echo "[bootstrap] External Ollama selected at ${OLLAMA_BASE_URL:-host-configured URL}; skipping local model pull."
else
  echo "[bootstrap] External LLM provider selected; skipping local model pull."
fi

echo "[bootstrap] Ready. Start trading bot with: docker compose up -d freqtrade scheduler pair-rotator policy-pivot"
