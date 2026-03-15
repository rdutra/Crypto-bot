#!/usr/bin/env bash
set -euo pipefail

docker compose up -d freqtrade

docker compose logs -f --tail=100 freqtrade
