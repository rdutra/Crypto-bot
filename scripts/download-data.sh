#!/usr/bin/env bash
set -euo pipefail

TIMERANGE="${1:-20230101-}"
DEFAULT_PAIRS="${CORE_PAIRS:-BTC/USDT ETH/USDT BNB/USDT} ${RISK_PAIRS:-SOL/USDT XRP/USDT AVAX/USDT}"
PAIRS_INPUT="${PAIRS:-$DEFAULT_PAIRS}"
read -r -a PAIR_ARR <<< "${PAIRS_INPUT}"

docker compose run --rm freqtrade download-data \
  --config /freqtrade/user_data/config.json \
  --timeframes 15m 1h 4h \
  --pairs "${PAIR_ARR[@]}" \
  --timerange "${TIMERANGE}"
