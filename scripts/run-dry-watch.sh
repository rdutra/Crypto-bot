#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="${ROOT_DIR}/freqtrade/user_data/config.json"
SHELL_HELPER="${ROOT_DIR}/scripts/shell_helpers.py"
MODE="${STRATEGY_MODE:-conservative}"
ROTATE_RISK_PAIRS=false

usage() {
  cat <<'EOF'
Usage:
  ./scripts/run-dry-watch.sh
  ./scripts/run-dry-watch.sh --mode aggressive
  ./scripts/run-dry-watch.sh --mode aggressive --rotate-risk-pairs

Options:
  --mode VALUE         Strategy profile: conservative|aggressive.
  --rotate-risk-pairs  Refresh RISK_PAIRS with LLM ranking before start.
  --help               Show help.
EOF
}

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
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

MODE="$(printf '%s' "${MODE}" | tr '[:upper:]' '[:lower:]')"
if [[ "${MODE}" != "conservative" && "${MODE}" != "aggressive" ]]; then
  echo "--mode must be either conservative or aggressive." >&2
  exit 1
fi

python3 "${SHELL_HELPER}" validate-config-mode "${CONFIG_PATH}" dry-run

cd "${ROOT_DIR}"

read -r -a llm_services <<<"$(./scripts/llm-runtime.sh services)"

echo "Starting dry-run in ${MODE} mode..."
if [[ "${ROTATE_RISK_PAIRS}" == "true" ]]; then
  echo "Rotating risk pairs (LLM advisor)..."
  docker compose up -d "${llm_services[@]}" >/dev/null
  if ./scripts/rotate-risk-pairs.sh --apply --mode "${MODE}"; then
    rotate_log="${LLM_ROTATE_LOG_PATH:-./freqtrade/user_data/logs/llm-pair-rotation.log}"
    if [[ "${rotate_log}" != /* ]]; then
      rotate_log="${ROOT_DIR}/${rotate_log#./}"
    fi
    if [[ -f "${rotate_log}" ]]; then
      python3 "${SHELL_HELPER}" summarize-rotation-log "${rotate_log}"
    fi
  else
    echo "Risk-pair rotation failed; continuing with current RISK_PAIRS."
  fi
fi
STRATEGY_MODE="${MODE}" docker compose up -d "${llm_services[@]}" scheduler pair-rotator policy-pivot freqtrade
docker compose ps
echo "Dry-run stack started."
echo "Follow logs with: docker compose logs -f --tail=100 bot-api spike-scanner scheduler pair-rotator policy-pivot freqtrade"
