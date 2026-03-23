#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SHELL_HELPER="${ROOT_DIR}/scripts/shell_helpers.py"
TIMERANGE="${1:-20230101-}"
CONFIG_PATH="${ROOT_DIR}/freqtrade/user_data/config.json"
ROTATION_LOG_PATH="${ROOT_DIR}/freqtrade/user_data/logs/llm-pair-rotation.log"
DEFAULT_SCHED_PAIRS="${SCHED_DOWNLOAD_PAIRS:-BTC/USDT ETH/USDT BNB/USDT SOL/USDT XRP/USDT AVAX/USDT LINK/USDT INJ/USDT DOGE/USDT ADA/USDT SUI/USDT TRX/USDT TAO/USDT ZEC/USDT WLD/USDT PEPE/USDT}"
WHITELIST_PAIRS="$(python3 "${SHELL_HELPER}" current-whitelist-pairs "${CONFIG_PATH}")"
RECENT_ROTATION_PAIRS="$(python3 "${SHELL_HELPER}" recent-rotation-pairs "${ROTATION_LOG_PATH}" 8 20)"
DEFAULT_PAIRS="${CORE_PAIRS:-BTC/USDT ETH/USDT BNB/USDT} ${RISK_PAIRS:-} ${SPIKE_PAIRS:-} ${DEFAULT_SCHED_PAIRS} ${WHITELIST_PAIRS} ${RECENT_ROTATION_PAIRS}"
PAIRS_INPUT="${PAIRS:-$DEFAULT_PAIRS}"
UNIQUE_PAIRS="$(python3 "${SHELL_HELPER}" unique-pairs "${PAIRS_INPUT}")"
read -r -a PAIR_ARR <<< "${UNIQUE_PAIRS}"

docker compose run --rm freqtrade download-data \
  --config /freqtrade/user_data/config.json \
  --timeframes 15m 1h 4h \
  --pairs "${PAIR_ARR[@]}" \
  --timerange "${TIMERANGE}"
