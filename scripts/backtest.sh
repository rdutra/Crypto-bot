#!/usr/bin/env bash
set -euo pipefail

TIMERANGE="${1:-20230101-}"

docker compose run --rm \
  -e ENABLE_LLM_FILTER=false \
  freqtrade backtesting \
  --config /freqtrade/user_data/config.json \
  --strategy-path /freqtrade/user_data/strategies \
  --strategy LlmTrendPullbackStrategy \
  --timeframe 1h \
  --timerange "${TIMERANGE}" \
  --export trades
