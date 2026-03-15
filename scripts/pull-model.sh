#!/usr/bin/env bash
set -euo pipefail

MODEL="${1:-${OLLAMA_MODEL:-llama3.1:8b}}"

docker compose up -d ollama

echo "[model] Pulling ${MODEL}"
docker compose exec -T ollama ollama pull "${MODEL}"
