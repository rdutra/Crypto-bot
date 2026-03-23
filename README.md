# Crypto Bot (Freqtrade + Bot API + Ollama/External LLM)

Freqtrade executes trades, `bot-api` provides bounded LLM decisions, and the LLM backend can be either local Ollama or an external OpenAI-compatible API.

Need the shortest path to run everything? See `QUICKSTART.md`.

## 1) First-Time Setup (Dry-Run)

1. Create local files:

```bash
cp freqtrade/user_data/config.json.example freqtrade/user_data/config.json
cp .env.minimal.example .env
```

If you want every tunable option, use `cp .env.example .env` instead.

2. Fill required secrets in `.env`:

- `BINANCE_API_KEY`
- `BINANCE_API_SECRET`
- `FREQTRADE_API_JWT_SECRET`
- `FREQTRADE_API_PASSWORD`

3. Generate API secrets (if needed):

```bash
openssl rand -hex 32
openssl rand -base64 24
```

4. Keep `dry_run: true` in `freqtrade/user_data/config.json`.

5. Build and start infra:

```bash
./scripts/bootstrap.sh
docker compose ps
curl -s http://localhost:8000/healthz
```

Important:
- `spike-scanner` is part of the default stack.
- `bootstrap.sh` starts the dockerized `ollama` service only when `LLM_PROVIDER=ollama` and `OLLAMA_BASE_URL=http://ollama:11434`.
- Scanner health is available at `http://localhost:8091/healthz`.

6. If you are using host-installed Ollama, make sure it is running and the model is installed:

```bash
ollama list
```

If you are using the dockerized Ollama service instead, pull the model with:

```bash
docker compose exec -T ollama ollama pull qwen2.5:3b
```

Optional performance tuning for the dockerized Ollama service (set in `.env`, then recreate `ollama`):

```bash
OLLAMA_CPU_LIMIT=6
OLLAMA_MEM_LIMIT=10g
OLLAMA_MEM_RESERVATION=6g
OLLAMA_SHM_SIZE=1g
OLLAMA_NUM_PARALLEL=2
OLLAMA_MAX_LOADED_MODELS=1
OLLAMA_KEEP_ALIVE=10m
docker compose up -d --force-recreate ollama bot-api spike-scanner
```

Optional external LLM instead of Ollama:

```bash
LLM_PROVIDER=openai_compatible
LLM_BASE_URL=https://api.openai.com/v1
LLM_API_KEY=your_api_key
LLM_MODEL=gpt-4.1-mini
docker compose up -d --force-recreate bot-api
```

## 2) Start Trading (Dry-Run)

Standard dry-run:

```bash
./scripts/run-dry-watch.sh --mode conservative
```

Aggressive dry-run:

```bash
./scripts/run-dry-watch.sh --mode aggressive
```

After changing `.env` strategy values, recreate `freqtrade` to apply them:

```bash
docker compose up -d --force-recreate freqtrade scheduler pair-rotator policy-pivot
```

Strategy selection:

- `FREQTRADE_STRATEGY=LlmHybridStrategy` is the current default and keeps one bot while giving spike-tagged pairs a dedicated execution branch
- `FREQTRADE_STRATEGY=LlmTrendPullbackStrategy` keeps the stricter trend-following execution logic if you want the conservative baseline
- `FREQTRADE_STRATEGY=LlmRotationAlignedStrategy` remains available if you want the older single-path aggressive profile

Dry-run with risk-pair rotation before startup:

```bash
./scripts/run-dry-watch.sh --mode aggressive --rotate-risk-pairs
```

## 3) Backtesting

Download candles:

```bash
./scripts/download-data.sh 20230101-
```

Backtest deterministic strategy:

```bash
./scripts/backtest.sh 20230101-
```

## 4) LLM Pair Rotation (With Logging)

Preview LLM-selected risk pairs:

```bash
./scripts/rotate-risk-pairs.sh
```

Apply selected pairs to `.env` (`RISK_PAIRS`):

```bash
./scripts/rotate-risk-pairs.sh --apply
```

Apply + restart freqtrade:

```bash
./scripts/rotate-risk-pairs.sh --apply --restart --mode aggressive
```

Run periodic automatic rotation (recommended if you want continuous refresh):

```bash
./scripts/rotate-risk-pairs-loop.sh --mode aggressive --interval-minutes 60
```

One-shot loop test:

```bash
./scripts/rotate-risk-pairs-loop.sh --once --mode aggressive
```

Optional background run:

```bash
nohup ./scripts/rotate-risk-pairs-loop.sh --mode aggressive --interval-minutes 60 > freqtrade/user_data/logs/rotate-loop.log 2>&1 &
```

Rotation logs are appended to:

- `freqtrade/user_data/logs/llm-pair-rotation.log`

Runtime policy pivot (adaptive risk posture, no bot restart required):

```bash
docker compose up -d --build policy-pivot
docker compose logs -f --tail=200 policy-pivot
```

Policy output file used by strategy:

- `freqtrade/user_data/logs/llm-runtime-policy.json`

Inspect latest decisions:

```bash
tail -n 20 freqtrade/user_data/logs/llm-pair-rotation.log
```

The log includes:

- selected pairs
- per-pair regime/risk/confidence/note
- deterministic score + final score
- data source (`local` or `exchange`)
- discovery notes and skipped pairs

## 5) Live Trading (Production)

1. Set `dry_run: false` in `freqtrade/user_data/config.json`.
2. Start live mode (confirmation required):

```bash
./scripts/run-live-watch.sh --mode conservative
```

Aggressive live mode:

```bash
./scripts/run-live-watch.sh --mode aggressive
```

Live + risk-pair rotation before startup:

```bash
./scripts/run-live-watch.sh --mode aggressive --rotate-risk-pairs
```

## 6) Useful Logs

Freqtrade + scheduler logs:

```bash
docker compose logs -f --tail=200 freqtrade scheduler policy-pivot
```

Stack logs:

```bash
docker compose logs -f --tail=200 bot-api spike-scanner scheduler pair-rotator policy-pivot freqtrade
```

## 7) Spike Scanner Alerts

```bash
docker compose up -d --build spike-scanner
```

Recreate scanner after config/env changes:

```bash
docker compose up -d --force-recreate spike-scanner
```

Dashboard:

```bash
open http://localhost:8091
```

The dashboard now includes a **Freqtrade Entry Diagnostics** section sourced from `freqtrade.log`, with the same pagination/filter behavior as the alert tables.
Dashboard tables are now split into tabs (`Overview`, `Predictions`, `LLM Evals`, `Entry Diags`, `Outcomes`, `LLM Debug`) so large datasets stay manageable.

Watch scanner logs:

```bash
docker compose logs -f --tail=200 spike-scanner
```

Alert output file (JSONL):

```bash
tail -f freqtrade/user_data/logs/spike-alerts.jsonl
```

Prediction database target:

```bash
docker exec stack-postgres psql -U stack -d spike_scanner -c "SELECT COUNT(*) FROM alerts;"
```

Optional notifications (Telegram/Discord) are sent on startup and each alert when configured.

Stop scanner:

```bash
docker compose stop spike-scanner
```

Notes:

- Scanner is alert-only (no trading), but it now runs as a first-class stack service.
- It reuses existing env variables where possible: `BINANCE_REST_BASE`, `BINANCE_WS_BASE`, `LLM_ROTATE_QUOTE`, `LLM_ROTATE_MIN_QUOTE_VOLUME`, `LLM_ROTATE_EXCLUDE_REGEX`.
- The scanner now stores predictions and evaluates outcomes at configured horizons (default `60,240,1440,2880` minutes).
- Optional LLM shadow mode can score the same alerts via `bot-api` and store a parallel verdict (`allowed/blocked/unknown`) for later comparison.
- API endpoint `GET /api/alerts?limit=200`
- API endpoint `GET /api/outcomes?limit=300&status=resolved|pending`
- API endpoint `GET /api/summary`
- API endpoint `GET /api/llm-evals?limit=200` (recent LLM shadow evaluations by symbol)
- API endpoint `GET /api/llm-debug?limit=200` (recent bot-api prompt/response debug feed shown in LLM Debug tab)
- Bot API endpoint `GET /debug/llm-calls?limit=200` (prompt/response history persisted in sqlite)
- Bot API endpoint `GET /skills/crypto-market-rank?limit=50` (Binance market-rank skill cache/preview)
- Bot API endpoint `GET /skills/trading-signal?limit=50` (Binance trading-signal skill cache/preview)
- Bot API endpoint `POST /rank-pairs` now returns `market_rank_source/errors` and `trading_signal_source/errors` for source/fallback visibility

## 8) Key `.env` Controls

Core strategy:

- `STRATEGY_MODE=conservative|aggressive`
- `FREQTRADE_STRATEGY=LlmTrendPullbackStrategy|LlmRotationAlignedStrategy|LlmHybridStrategy`
- `ENABLE_LLM_FILTER=true|false`
- `LLM_MIN_CONFIDENCE=0.65`
- `LLM_CONNECT_TIMEOUT_SECONDS=2`
- `LLM_READ_TIMEOUT_SECONDS=45` is mainly useful if local inference is still timing out
- `LLM_PROVIDER=ollama|openai_compatible`
- `LLM_BASE_URL=https://api.openai.com/v1` (used when `LLM_PROVIDER=openai_compatible`)
- `LLM_API_KEY=...` (used when `LLM_PROVIDER=openai_compatible`)
- `LLM_MODEL=...` (generic model override for external providers; Ollama still uses `OLLAMA_MODEL`)
- `LLM_TIMEOUT=...` (generic provider timeout override; falls back to `OLLAMA_TIMEOUT`)
- `LLM_OPENAI_CHAT_PATH=/chat/completions`
- `LLM_OPENAI_RESPONSE_FORMAT_JSON=true`
- `LLM_OPENAI_MAX_COMPLETION_TOKENS=...` (optional)
- `LLM_OPENAI_TEMPERATURE=0.1`
- `OLLAMA_TIMEOUT=30` is the preferred fast-fail baseline for `bot-api` model calls
- `OLLAMA_BASE_URL=http://host.docker.internal:11434` uses host-installed Ollama; set `http://ollama:11434` only if you want the dockerized Ollama service
- `LLM_FAIL_OPEN=true` recommended in live/dry-run to avoid blocking entries when LLM is temporarily unavailable
- `LLM_DEBUG_ENABLED=true`, `LLM_DEBUG_MAX_ENTRIES=250` (in-memory hot cache)
- `LLM_DEBUG_DB_URL=postgresql+psycopg2://stack:stack@stack-postgres:5432/stack_analytics`, `LLM_DEBUG_DB_MAX_ROWS=50000` (preferred persistent debug history backend)
- `LLM_DEBUG_DB_PATH=/app/data/llm-debug.sqlite` (SQLite fallback / migration source)
- `LLM_MARKET_RANK_SKILL_ENABLED=true|false` (inject Binance market-rank context into `/rank-pairs` prompts)
- `LLM_MARKET_RANK_BASE_URL=https://web3.binance.com`
- `LLM_MARKET_RANK_TIMEOUT_SECONDS=6`, `LLM_MARKET_RANK_CACHE_SECONDS=300`
- `LLM_MARKET_RANK_MAX_TOKENS=120`, `LLM_MARKET_RANK_PERIOD=50`, `LLM_MARKET_RANK_QUOTE=USDT`
- `LLM_MARKET_RANK_UNIFIED_ENDPOINT=/bapi/defi/v1/public/wallet-direct/buw/wallet/market/token/pulse/unified/rank/list`
- `LLM_MARKET_RANK_CHAIN_ID=` (optional chain filter; blank for all)
- `LLM_MARKET_RANK_EXCLUDE_REGEX=...` (exclude leveraged symbols from rank context)
- `LLM_TRADING_SIGNAL_SKILL_ENABLED=true|false` (inject trading-signal context into `/rank-pairs` prompts)
- `LLM_TRADING_SIGNAL_BASE_URL=https://web3.binance.com`
- `LLM_TRADING_SIGNAL_TIMEOUT_SECONDS=6`, `LLM_TRADING_SIGNAL_CACHE_SECONDS=180`
- `LLM_TRADING_SIGNAL_MAX_ITEMS=120`, `LLM_TRADING_SIGNAL_QUOTE=USDT`
- `LLM_TRADING_SIGNAL_MIN_SCORE=0.25`
- `LLM_TRADING_SIGNAL_BUY_BONUS_MULT=0.35` (bonus weight for `buy` signals in ranking; only applied to Binance-skill sourced candidates)
- `LLM_TRADING_SIGNAL_SELL_PENALTY_MULT=0.0` (default off for long-only; set `>0` to penalize `sell`)
- `LLM_TRADING_SIGNAL_CHAIN_ID=56` and `LLM_TRADING_SIGNAL_USER_AGENT=binance-web3/1.0 (Skill)`
- `LLM_TRADING_SIGNAL_ENDPOINTS=...` (comma-separated endpoint paths to probe)
- `LLM_TRADING_SIGNAL_EXCLUDE_REGEX=...`
- `LLM_RANK_SINGLE_MAX_FAILURES=2` (if single-call recovery keeps hitting LLM provider errors, fall back to deterministic ranking early)
- `CORE_PAIRS=...`
- `RISK_PAIRS=...`
- Keep `exchange.pair_whitelist` lean (usually `CORE_PAIRS + RISK_PAIRS`) to avoid slow analysis cycles and missed signals.
- `STRATEGY_MODE=conservative|aggressive`
- `AGGR_ENTRY_STRICTNESS=strict|normal` (only matters in aggressive mode)
- `BENCHMARK_PAIR=BTC/USDT` and `BENCHMARK_FILTER_FOR_RISK=true|false`
- `ENTRY_RANKING_ENABLED=true|false`, `ENTRY_TOP_N=1`, `ENTRY_MIN_SCORE=0.58`
- `RISK_STAKE_MULTIPLIER=...` and `RISK_MAX_OPEN_TRADES=...`
- `IGNORE_BUYING_EXPIRED_CANDLE_AFTER=1200` for 15m aggressive mode
- `RUNTIME_POLICY_ENABLED=true|false` (read runtime policy JSON and adapt risk knobs on the fly)
- `RUNTIME_POLICY_PATH=/freqtrade/user_data/logs/llm-runtime-policy.json`
- `RUNTIME_POLICY_REFRESH_SECONDS=30`
- Strategy micro-thresholds now live in Python profiles instead of dozens of env vars. If you need to change the detailed gates, edit the strategy classes directly instead of pushing more top-level env.

LLM rotation:

- `LLM_ROTATE_PROFILE=focused|balanced|expansive`
- `LLM_ROTATE_AUTO_DISCOVER=true`
- `LLM_ROTATE_DATA_SOURCE=auto` (`local|exchange|auto`)
- `LLM_ROTATE_EXCHANGE=binance`
- `LLM_ROTATE_QUOTE=USDT`
- `LLM_ROTATE_TOP_N=6`
- `LLM_ROTATE_MIN_CONFIDENCE=0.60`
- `LLM_ROTATE_ALLOWED_RISK=low medium high`
- `LLM_ROTATE_ALLOWED_REGIMES=trend_pullback breakout mean_reversion`
- `LLM_ROTATE_USE_SPIKE_BIAS=true|false` (prepend recent high-score scanner symbols into rotation candidates)
- `LLM_ROTATE_USE_SMART_MONEY_BIAS=true|false`
- `LLM_ROTATE_RESERVE_SPIKE_SLOT=true|false`
- `LLM_ROTATE_SPIKE_DB_URL=postgresql+psycopg2://stack:stack@stack-postgres:5432/spike_scanner` (preferred)
- `LLM_ROTATE_EXCLUDED_BASES=USDC USDT FDUSD TUSD USDP BUSD DAI EUR USD1` (drop obvious low-opportunity bases from the risk basket)
- `LLM_ROTATE_OUTCOME_DB_URL=postgresql+psycopg2://stack:stack@stack-postgres:5432/stack_analytics` (preferred)
- `LLM_ROTATE_LOOP_INTERVAL_MINUTES=60` (used by `rotate-risk-pairs-loop.sh`)
- `LLM_ROTATE_LOOP_JITTER_SECONDS=0` (optional random delay before each cycle)
- Advanced rotator thresholds still exist as script-level overrides, but the intended path is to use a named `LLM_ROTATE_PROFILE` and only override high-level behavior when there is evidence you need it.

Postgres admin:

- `./scripts/stack-postgres-admin.sh check`
- `./scripts/stack-postgres-admin.sh backup`

LLM runtime policy loop:

- `LLM_POLICY_LOOP_ENABLED=true|false`
- `LLM_POLICY_INTERVAL_MINUTES=15`
- `LLM_POLICY_LOOKBACK_HOURS=24`
- `LLM_POLICY_HTTP_TIMEOUT_SECONDS=45`
- `LLM_POLICY_TRADES_DB=postgresql+psycopg2://stack:stack@stack-postgres:5432/freqtrade`
- `LLM_POLICY_OUTPUT_PATH=./freqtrade/user_data/logs/llm-runtime-policy.json`
- `LLM_POLICY_USE_SPIKE=true|false`
- `LLM_POLICY_SPIKE_DB_URL=postgresql+psycopg2://stack:stack@stack-postgres:5432/spike_scanner` (preferred)
- `LLM_POLICY_SPIKE_DB_PATH=./freqtrade/user_data/logs/spike-scanner.sqlite` (SQLite fallback / migration source)

Scheduler:

- `SCHED_DOWNLOAD_ENABLED=true`
- `SCHED_DOWNLOAD_TIME=02:15`
- `SCHED_DOWNLOAD_TIMERANGE=20230101-`
- `SCHED_DOWNLOAD_PAIRS=...` (seed list; scheduler also pulls current whitelist and recent rotation names automatically)
- `SCHED_PRUNE_ENABLED=true`
- `SCHED_PRUNE_TIME=03:00`
- `SCHED_PRUNE_WEEKDAY=0`
- `SCHED_PRUNE_DAYS=180`
- `SCHED_STATE_DIR=/freqtrade/user_data/logs/scheduler_state`

Spike scanner:

- `BINANCE_REST_BASE=https://api.binance.com`
- `BINANCE_WS_BASE=wss://stream.binance.com:9443/stream`
- `SPIKE_QUOTE_ASSET=USDT` (optional override; defaults to `LLM_ROTATE_QUOTE`)
- `SPIKE_MIN_QUOTE_VOLUME=5000000` (optional override; default is tuned lower so scanner sees more fast movers)
- `SPIKE_EXCLUDE_REGEX=...` (optional override; defaults to `LLM_ROTATE_EXCLUDE_REGEX`)
- `SPIKE_PROFILE=conservative|balanced|aggressive`
- `SPIKE_INCLUDE_SYMBOLS=...` (optional explicit include list)
- `SPIKE_EXCLUDE_SYMBOLS=BTCUSDT ETHUSDT`
- `SPIKE_UNIVERSE_MAX_SYMBOLS=120`
- `SPIKE_TOP_N_ALERTS=5`
- `SPIKE_MIN_SCORE=0.76`
- `SPIKE_ALERT_COOLDOWN_MINUTES=30`
- `SPIKE_LOOP_SECONDS=5`
- `SPIKE_LOG_PATH=/data/spike-alerts.jsonl`
- `SPIKE_DB_URL=postgresql+psycopg2://stack:stack@stack-postgres:5432/spike_scanner` (preferred)
- `SPIKE_DB_PATH=/data/spike-scanner.sqlite` (SQLite fallback / migration source)
- `SPIKE_OUTCOME_HORIZONS_MINUTES=60,240,1440,2880`
- `SPIKE_OUTCOME_LOOP_SECONDS=30`
- `SPIKE_OUTCOME_BATCH_SIZE=200`
- `SPIKE_LLM_SHADOW_ENABLED=false`
- `SPIKE_LLM_SHADOW_BOT_API_URL=http://bot-api:8000`
- `SPIKE_LLM_SHADOW_TIMEOUT_SECONDS=45` (only raise this if local inference is still timing out)
- `SPIKE_WEB_ENABLED=true`
- `SPIKE_WEB_HOST=0.0.0.0`
- `SPIKE_WEB_PORT=8090` and `SPIKE_WEB_PUBLIC_PORT=8091`
- `SPIKE_LLM_DEBUG_TAB_ENABLED=true` (show/hide LLM Debug tab)
- `SPIKE_LLM_DEBUG_BOT_API_URL=http://bot-api:8000` (source of prompt/response feed)
- `SPIKE_LLM_DEBUG_TIMEOUT_SECONDS=3`
- `SPIKE_LLM_DEBUG_FETCH_LIMIT=500`
- `FREQTRADE_LOG_PATH=/data/freqtrade.log` (source file for entry diagnostics in the scanner web UI)
- `FREQTRADE_DIAG_MAX_LINES=200000` (how many log lines to scan when building diagnostics table)
- `SPIKE_NOTIFY_ENABLED=true`
- `SPIKE_NOTIFY_TIMEOUT_SECONDS=10`
- `SPIKE_TELEGRAM_BOT_TOKEN=...` and `SPIKE_TELEGRAM_CHAT_ID=...` (or generic `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`)
- `SPIKE_DISCORD_WEBHOOK_URL=...` (or generic `DISCORD_WEBHOOK_URL`)
- Scanner score-shaping and shadow-eval thresholds are now owned by `SPIKE_PROFILE`. Override the individual `SPIKE_*` threshold vars only if you have evidence the profile is wrong for your tape.

## 9) Safety

- Keep `dry_run=true` until behavior is validated.
- Start with small stake and low `max_open_trades`.
- Use API key IP restrictions and disable withdrawals.
