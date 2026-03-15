#!/usr/bin/env bash
set -euo pipefail

TIMERANGE="${1:-20230101-}"
PAIRS_INPUT="${2:-${PAIRS:-BTC/USDT ETH/USDT}}"
read -r -a PAIR_ARR <<< "${PAIRS_INPUT}"

docker compose run --rm \
  -e ENABLE_LLM_FILTER=false \
  freqtrade backtesting \
  --config /freqtrade/user_data/config.json \
  --strategy-path /freqtrade/user_data/strategies \
  --strategy LlmTrendPullbackStrategy \
  --timeframe 1h \
  --pairs "${PAIR_ARR[@]}" \
  --timerange "${TIMERANGE}" \
  --export trades
