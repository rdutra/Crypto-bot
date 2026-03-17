# Quickstart

This is a command-first guide to bring up the full stack and run it with common options.

## 1) One-time setup

```bash
cp freqtrade/user_data/config.json.example freqtrade/user_data/config.json
cp .env.example .env
```

Fill required values in `.env`:

- `BINANCE_API_KEY`
- `BINANCE_API_SECRET`
- `FREQTRADE_API_JWT_SECRET`
- `FREQTRADE_API_PASSWORD`

Keep `dry_run: true` in `freqtrade/user_data/config.json` for paper trading.

Pull the model:

```bash
docker compose up -d --build ollama
docker compose exec -T ollama ollama pull qwen3:8b
```

## 2) Start the full stack

Core services (bot + API + maintenance + periodic pair rotation):

```bash
docker compose up -d --build ollama bot-api freqtrade scheduler pair-rotator
```

Optional spike scanner + dashboard:

```bash
docker compose --profile scanner up -d --build spike-scanner
```

Check status:

```bash
docker compose ps
curl -s http://localhost:8000/healthz
```

Notes:
- `spike-scanner` only starts with `--profile scanner`.
- `pair-rotator` may take extra time on first start while it installs runtime tools.

## 3) Start using helper scripts (recommended)

Dry-run (conservative):

```bash
./scripts/run-dry-watch.sh --watch 30 --mode conservative
```

Dry-run (aggressive):

```bash
./scripts/run-dry-watch.sh --watch 30 --mode aggressive
```

Dry-run + LLM risk-pair rotation before startup:

```bash
./scripts/run-dry-watch.sh --watch 30 --mode aggressive --rotate-risk-pairs
```

Live trading (only when `dry_run=false`):

```bash
./scripts/run-live-watch.sh --watch 30 --mode aggressive --confirm LIVE
```

## 4) Main command options

`./scripts/run-dry-watch.sh`
- `--watch <seconds>`
- `--stop-below <value>`
- `--mode conservative|aggressive`
- `--rotate-risk-pairs`

`./scripts/run-live-watch.sh`
- `--watch <seconds>`
- `--stop-below <value>`
- `--mode conservative|aggressive`
- `--rotate-risk-pairs`
- `--confirm LIVE`

`./scripts/rotate-risk-pairs.sh`
- `--top <n>`
- `--min-confidence <0..1>`
- `--data-source local|exchange|auto`
- `--max-candidates <n>`
- `--allowed-risk "low medium"`
- `--allowed-regimes "trend_pullback breakout mean_reversion"`
- `--apply`
- `--restart`
- `--mode conservative|aggressive`
- `--sync-whitelist` / `--no-sync-whitelist`

`./scripts/rotate-risk-pairs-loop.sh`
- `--interval-minutes <n>`
- `--mode conservative|aggressive`
- `--restart` / `--no-restart`
- `--jitter-seconds <n>`
- `--once`

`./scripts/wallet-control.sh`
- `--watch <seconds>`
- `--stop`
- `--stop-below <value>`

## 5) Operations

Live logs:

```bash
docker compose logs -f --tail=200 freqtrade bot-api scheduler pair-rotator
docker compose logs -f --tail=200 spike-scanner
```

Restart trading bot after config/env changes:

```bash
docker compose up -d --force-recreate freqtrade
```

Stop everything:

```bash
docker compose down
docker compose --profile scanner down
```

## 6) Useful endpoints/files

- Freqtrade API: `http://localhost:8080`
- Bot API health: `http://localhost:8000/healthz`
- Spike scanner dashboard: `http://localhost:8091`
- Freqtrade log: `freqtrade/user_data/logs/freqtrade.log`
- Rotation log: `freqtrade/user_data/logs/llm-pair-rotation.log`
- Scanner DB: `freqtrade/user_data/logs/spike-scanner.sqlite`
