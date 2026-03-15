#!/usr/bin/env bash
set -euo pipefail

docker compose up -d freqtrade scheduler

docker compose logs -f --tail=100 freqtrade scheduler
