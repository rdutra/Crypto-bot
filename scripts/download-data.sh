#!/usr/bin/env bash
set -euo pipefail

TIMERANGE="${1:-20230101-}"
read -r -a PAIR_ARR <<< "${PAIRS:-BTC/USDT ETH/USDT}"

docker compose run --rm freqtrade download-data \
  --config /freqtrade/user_data/config.json \
  --timeframes 1h 4h \
  --pairs "${PAIR_ARR[@]}" \
  --timerange "${TIMERANGE}"
