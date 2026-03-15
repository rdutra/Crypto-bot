# Crypto Bot (Freqtrade + Ollama + Sidecar API)

Backtest-first crypto trading bot stack with deterministic strategy logic and an optional LLM filter.

## Stack

- `freqtrade`: execution, dry-run, backtesting
- `bot-api`: bounded classifier API (never places orders)
- `ollama`: local model inference

## Strategy

`LlmTrendPullbackStrategy` implements:

- 1h trend + pullback entries
- 4h trend confirmation
- volatility and momentum filters
- built-in protections (`CooldownPeriod`, `StoplossGuard`, `MaxDrawdown`)
- ATR-based dynamic stoploss
- optional live/dry-run LLM gate on the latest signal only

By default, LLM filtering is disabled (`ENABLE_LLM_FILTER=false`) so backtests remain deterministic.

## Prerequisites

- Docker + Docker Compose
- Enough disk for historical candles and Ollama model weights

## Full Runbook (Raw Docker Commands)

1. Create local config from example:

```bash
cp freqtrade/user_data/config.json.example freqtrade/user_data/config.json
```

2. Edit your local config:

- `freqtrade/user_data/config.json`
- Keep `dry_run: true` for paper trading
- Set Binance keys if you want live exchange connectivity later

3. Build and start infrastructure (`ollama` + `bot-api`):

```bash
docker compose up -d --build ollama bot-api
```

4. Confirm services are healthy:

```bash
docker compose ps
curl -s http://localhost:8000/healthz
```

5. Pull the local LLM model in the Ollama container:

```bash
docker compose exec -T ollama ollama pull llama3.1:8b
```

6. Download market data using a one-off Freqtrade container command:

```bash
docker compose run --rm freqtrade download-data \
  --config /freqtrade/user_data/config.json \
  --strategy-path /freqtrade/user_data/strategies \
  --timeframes 1h 4h \
  --pairs BTC/USDT ETH/USDT \
  --timerange 20230101-
```

7. Run deterministic backtest (LLM disabled explicitly):

```bash
docker compose run --rm \
  -e ENABLE_LLM_FILTER=false \
  freqtrade backtesting \
  --config /freqtrade/user_data/config.json \
  --strategy-path /freqtrade/user_data/strategies \
  --strategy LlmTrendPullbackStrategy \
  --timeframe 1h \
  --timerange 20230101- \
  --export trades
```

8. Start the bot in dry-run mode:

```bash
docker compose up -d freqtrade
docker compose logs -f --tail=100 freqtrade
```

9. Stop stack:

```bash
docker compose down
```

## Script Shortcuts

- Bootstrap and pull model: `./scripts/bootstrap.sh llama3.1:8b`
- Pull/update model only: `./scripts/pull-model.sh llama3.1:8b`
- Download data: `./scripts/download-data.sh 20230101-`
- Backtest: `./scripts/backtest.sh 20230101-`
- Start dry-run and tail logs: `./scripts/start-dry-run.sh`

## LLM Gate Controls

Set on `freqtrade` service environment in `docker-compose.yml`:

- `ENABLE_LLM_FILTER`: `true` or `false`
- `LLM_MIN_CONFIDENCE`: default `0.65`
- `BOT_API_URL`: default `http://bot-api:8000`

To enable LLM gating in dry-run/live:

1. Set `ENABLE_LLM_FILTER: "true"` in `docker-compose.yml`
2. Restart freqtrade:

```bash
docker compose up -d freqtrade
```

## Safety

- Keep `dry_run: true` until you validate backtests and paper trade behavior.
- Use Binance API key IP restrictions and do not enable withdrawals.
- Start with small stake and conservative pair whitelist.
