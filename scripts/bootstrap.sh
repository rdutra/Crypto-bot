#!/usr/bin/env bash
set -euo pipefail

MODEL="${1:-${OLLAMA_MODEL:-llama3.1:8b}}"

echo "[bootstrap] Starting Ollama, bot-api, and spike-scanner..."
docker compose up -d --build ollama bot-api spike-scanner

echo "[bootstrap] Waiting for Ollama API..."
until docker compose exec -T ollama ollama list >/dev/null 2>&1; do
  sleep 2
done

echo "[bootstrap] Pulling model: ${MODEL}"
docker compose exec -T ollama ollama pull "${MODEL}"

echo "[bootstrap] Ready. Start trading bot with: docker compose up -d freqtrade scheduler pair-rotator policy-pivot"
