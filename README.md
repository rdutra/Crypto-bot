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

2. Create `.env` from the template:

```bash
cp .env.example .env
```

3. Fill secrets in `.env`:

- `BINANCE_API_KEY`
- `BINANCE_API_SECRET`
- `FREQTRADE_API_JWT_SECRET`
- `FREQTRADE_API_PASSWORD`

Generate API secrets:

```bash
openssl rand -hex 32
openssl rand -base64 24
```

4. Edit your local config (non-secret settings):

- `freqtrade/user_data/config.json`
- Keep `dry_run: true` for paper trading
- Set pairs/stake/timeouts as needed

5. Build and start infrastructure (`ollama` + `bot-api`):

```bash
docker compose up -d --build ollama bot-api
```

6. Confirm services are healthy:

```bash
docker compose ps
curl -s http://localhost:8000/healthz
```

7. Pull the local LLM model in the Ollama container:

```bash
docker compose exec -T ollama ollama pull llama3.1:8b
```

8. Download market data using a one-off Freqtrade container command:

```bash
docker compose run --rm freqtrade download-data \
  --config /freqtrade/user_data/config.json \
  --strategy-path /freqtrade/user_data/strategies \
  --timeframes 1h 4h \
  --pairs BTC/USDT ETH/USDT \
  --timerange 20230101-
```

9. Run deterministic backtest (LLM disabled explicitly):

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

10. Start the bot in dry-run mode:

```bash
docker compose up -d freqtrade
docker compose logs -f --tail=100 freqtrade
```

11. Stop stack:

```bash
docker compose down
```

## Script Shortcuts

- Bootstrap and pull model: `./scripts/bootstrap.sh llama3.1:8b`
- Pull/update model only: `./scripts/pull-model.sh llama3.1:8b`
- Download data: `./scripts/download-data.sh 20230101-`
- Backtest: `./scripts/backtest.sh 20230101-`
- Start dry-run and tail logs: `./scripts/start-dry-run.sh`

## Production (Live Trading)

Use this only after you validate backtests and dry-run behavior.

1. Set live credentials in `.env`:

- `BINANCE_API_KEY`
- `BINANCE_API_SECRET`
- `FREQTRADE_API_JWT_SECRET`
- `FREQTRADE_API_PASSWORD`

2. Switch config to live mode:

- Edit `freqtrade/user_data/config.json`
- Set `"dry_run": false`
- Review `stake_amount`, `max_open_trades`, and `pair_whitelist`

3. Build and start services:

```bash
docker compose up -d --build ollama bot-api
docker compose up -d freqtrade
```

4. Verify bot startup:

```bash
docker compose ps
docker compose logs -f --tail=200 freqtrade
```

5. Restart after config/env changes:

```bash
docker compose up -d freqtrade
```

6. Stop live trading:

```bash
docker compose stop freqtrade
```

## LLM Gate Controls

Set on `freqtrade` service environment in `docker-compose.yml`:

- `ENABLE_LLM_FILTER`: `true` or `false`
- `LLM_MIN_CONFIDENCE`: default `0.65`
- `BOT_API_URL`: default `http://bot-api:8000`

## Credentials Handling

- `freqtrade/user_data/config.json` intentionally keeps secret fields empty.
- `docker compose` passes values from `.env` into the `freqtrade` container.
- Freqtrade reads these as runtime overrides via:
  - `FREQTRADE__EXCHANGE__KEY`
  - `FREQTRADE__EXCHANGE__SECRET`
  - `FREQTRADE__API_SERVER__JWT_SECRET_KEY`
  - `FREQTRADE__API_SERVER__USERNAME`
  - `FREQTRADE__API_SERVER__PASSWORD`
- `.env` is gitignored and should never be committed.

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
