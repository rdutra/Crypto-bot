# Quickstart

This is a command-first guide to bring up the full stack and run it with common options.

## 1) One-time setup

```bash
cp freqtrade/user_data/config.json.example freqtrade/user_data/config.json
cp .env.minimal.example .env
```

Need all advanced knobs? Use `cp .env.example .env` instead.

Fill required values in `.env`:

- `BINANCE_API_KEY`
- `BINANCE_API_SECRET`
- `FREQTRADE_API_JWT_SECRET`
- `FREQTRADE_API_PASSWORD`

Keep `dry_run: true` in `freqtrade/user_data/config.json` for paper trading.

Install or start Ollama on the host if using `LLM_PROVIDER=ollama`, then bootstrap the stack:

```bash
./scripts/bootstrap.sh
```

Optional external LLM:

```bash
LLM_PROVIDER=openai_compatible
LLM_BASE_URL=https://api.openai.com/v1
LLM_API_KEY=your_api_key
LLM_MODEL=gpt-4.1-mini
docker compose up -d --force-recreate bot-api
```

Optional: tune the dockerized Ollama container if you explicitly set `OLLAMA_BASE_URL=http://ollama:11434`, then recreate:

```bash
OLLAMA_CPU_LIMIT=6
OLLAMA_MEM_LIMIT=10g
OLLAMA_MEM_RESERVATION=6g
OLLAMA_SHM_SIZE=1g
OLLAMA_NUM_PARALLEL=2
OLLAMA_MAX_LOADED_MODELS=1
OLLAMA_KEEP_ALIVE=10m
docker compose up -d --force-recreate ollama bot-api
```

## 2) Start the full stack

Core services (bot + API + maintenance + periodic pair rotation + spike scanner):

```bash
./scripts/bootstrap.sh
docker compose up -d freqtrade scheduler pair-rotator policy-pivot
```

Check status:

```bash
docker compose ps
curl -s http://localhost:8000/healthz
```

Notes:
- `spike-scanner` starts with the default stack.
- With `OLLAMA_BASE_URL=http://host.docker.internal:11434`, the startup scripts do not start the dockerized `ollama` service.
- Scanner health: `http://localhost:8091/healthz`
- `pair-rotator` may take extra time on first start while it installs runtime tools.
- `policy-pivot` writes adaptive runtime policy to `freqtrade/user_data/logs/llm-runtime-policy.json`.

## 3) Start using helper scripts (recommended)

Dry-run (conservative):

```bash
./scripts/run-dry-watch.sh --mode conservative
```

Dry-run (aggressive):

```bash
./scripts/run-dry-watch.sh --mode aggressive
```

Dry-run (aggressive, rotation-aligned strategy):

```bash
FREQTRADE_STRATEGY=LlmHybridStrategy ./scripts/run-dry-watch.sh --mode aggressive
```

Dry-run + LLM risk-pair rotation before startup:

```bash
./scripts/run-dry-watch.sh --mode aggressive --rotate-risk-pairs
```

Current defaults already assume `LlmHybridStrategy` and `LLM_ROTATE_PROFILE=balanced`.

Live trading (only when `dry_run=false`):

```bash
./scripts/run-live-watch.sh --mode aggressive --confirm LIVE
```

## 4) Main command options

`./scripts/run-dry-watch.sh`
- `--mode conservative|aggressive`
- `--rotate-risk-pairs`

`./scripts/run-live-watch.sh`
- `--mode conservative|aggressive`
- `--rotate-risk-pairs`
- `--confirm LIVE`

`./scripts/rotate-risk-pairs.sh`
- `--profile focused|balanced|expansive`
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
- Spike-scanner integration is usually profile-driven now. Use `LLM_ROTATE_PROFILE=balanced` or `expansive`, then only override `LLM_ROTATE_USE_SPIKE_BIAS` / `LLM_ROTATE_RESERVE_SPIKE_SLOT` if needed.

`./scripts/rotate-risk-pairs-loop.sh`
- `--interval-minutes <n>`
- `--mode conservative|aggressive`
- `--restart` / `--no-restart`
- `--jitter-seconds <n>`
- `--once`

## 5) Operations

Live logs:

```bash
docker compose logs -f --tail=200 freqtrade bot-api scheduler pair-rotator
docker compose logs -f --tail=200 spike-scanner
docker compose logs -f --tail=200 policy-pivot
```

Restart trading bot after config/env changes:

```bash
docker compose up -d --force-recreate freqtrade scheduler pair-rotator policy-pivot
```

Stop everything:

```bash
docker compose down
```

## 6) Useful endpoints/files

- Freqtrade API: `http://localhost:18080`
- Bot API health: `http://localhost:8000/healthz`
- Spike scanner dashboard: `http://localhost:8091`
- Freqtrade log: `freqtrade/user_data/logs/freqtrade.log`
- Rotation log: `freqtrade/user_data/logs/llm-pair-rotation.log`
- Runtime policy: `freqtrade/user_data/logs/llm-runtime-policy.json`
- Scanner DB: `postgresql+psycopg2://stack:stack@stack-postgres:5432/spike_scanner` (`freqtrade/user_data/logs/spike-scanner.sqlite` remains the migration source/fallback)
- Analytics DB: `postgresql+psycopg2://stack:stack@stack-postgres:5432/stack_analytics`

Postgres admin:

```bash
./scripts/stack-postgres-admin.sh check
./scripts/stack-postgres-admin.sh backup
```
