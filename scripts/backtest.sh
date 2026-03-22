#!/usr/bin/env bash
set -euo pipefail

TIMERANGE="${1:-20230101-}"
DEFAULT_PAIRS="${CORE_PAIRS:-BTC/USDT ETH/USDT BNB/USDT} ${RISK_PAIRS:-SOL/USDT XRP/USDT AVAX/USDT}"
PAIRS_INPUT="${2:-${PAIRS:-$DEFAULT_PAIRS}}"
STRATEGY_NAME="${FREQTRADE_STRATEGY:-LlmTrendPullbackStrategy}"
read -r -a PAIR_ARR <<< "${PAIRS_INPUT}"

docker compose run --rm \
  -e ENABLE_LLM_FILTER=false \
  freqtrade backtesting \
  --config /freqtrade/user_data/config.json \
  --strategy-path /freqtrade/user_data/strategies \
  --strategy "${STRATEGY_NAME}" \
  --pairs "${PAIR_ARR[@]}" \
  --timerange "${TIMERANGE}" \
  --export trades
