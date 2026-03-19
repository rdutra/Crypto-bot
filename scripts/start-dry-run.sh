#!/usr/bin/env bash
set -euo pipefail

MODE="${STRATEGY_MODE:-conservative}"
ROTATE_RISK_PAIRS=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --rotate-risk-pairs)
      ROTATE_RISK_PAIRS=true
      shift
      ;;
    *)
      echo "Usage: ./scripts/start-dry-run.sh [--mode conservative|aggressive] [--rotate-risk-pairs]" >&2
      exit 1
      ;;
  esac
done

MODE="$(printf '%s' "${MODE}" | tr '[:upper:]' '[:lower:]')"
if [[ "${MODE}" != "conservative" && "${MODE}" != "aggressive" ]]; then
  echo "--mode must be either conservative or aggressive." >&2
  exit 1
fi

echo "Starting dry-run in ${MODE} mode..."
if [[ "${ROTATE_RISK_PAIRS}" == "true" ]]; then
  echo "Rotating risk pairs (LLM advisor)..."
  docker compose up -d ollama bot-api spike-scanner >/dev/null
  if ! ./scripts/rotate-risk-pairs.sh --apply --mode "${MODE}"; then
    echo "Risk-pair rotation failed; continuing with current RISK_PAIRS."
  fi
fi
STRATEGY_MODE="${MODE}" docker compose up -d bot-api spike-scanner freqtrade scheduler pair-rotator policy-pivot

docker compose logs -f --tail=100 freqtrade scheduler pair-rotator policy-pivot spike-scanner
