# Crypto Bot (Freqtrade + Ollama + Bot API)

Freqtrade executes trades, `bot-api` provides bounded LLM decisions, and Ollama runs the model locally.

Need the shortest path to run everything? See `QUICKSTART.md`.

## 1) First-Time Setup (Dry-Run)

1. Create local files:

```bash
cp freqtrade/user_data/config.json.example freqtrade/user_data/config.json
cp .env.example .env
```

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
docker compose up -d --build ollama bot-api
docker compose ps
curl -s http://localhost:8000/healthz
```

Important:
- `spike-scanner` uses the `scanner` profile and does not start with the default command above.
- Start it explicitly with:

```bash
docker compose --profile scanner up -d --build spike-scanner
```

6. Pull model:

```bash
docker compose exec -T ollama ollama pull qwen3:8b
```

## 2) Start Trading (Dry-Run)

Standard dry-run:

```bash
./scripts/run-dry-watch.sh --watch 30 --mode conservative
```

Aggressive dry-run:

```bash
./scripts/run-dry-watch.sh --watch 30 --mode aggressive
```

After changing `.env` strategy values, recreate `freqtrade` to apply them:

```bash
docker compose up -d --force-recreate freqtrade
```

Dry-run with risk-pair rotation before startup:

```bash
./scripts/run-dry-watch.sh --watch 30 --mode aggressive --rotate-risk-pairs
```

Auto-stop when wallet drops below threshold:

```bash
./scripts/run-dry-watch.sh --watch 30 --stop-below 290
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
./scripts/run-live-watch.sh --watch 30 --mode conservative
```

Aggressive live mode:

```bash
./scripts/run-live-watch.sh --watch 30 --mode aggressive
```

Live + risk-pair rotation before startup:

```bash
./scripts/run-live-watch.sh --watch 30 --mode aggressive --rotate-risk-pairs
```

## 6) Useful Logs

Freqtrade + scheduler logs:

```bash
docker compose logs -f --tail=200 freqtrade scheduler
```

Wallet status / manual stop:

```bash
./scripts/wallet-control.sh
./scripts/wallet-control.sh --stop
```

## 7) Spike Scanner Alerts (Separate Service)

Run scanner service (optional profile):

```bash
docker compose --profile scanner up -d --build spike-scanner
```

Recreate scanner after config/env changes:

```bash
docker compose --profile scanner up -d --force-recreate spike-scanner
```

Dashboard:

```bash
open http://localhost:8091
```

Watch scanner logs:

```bash
docker compose logs -f --tail=200 spike-scanner
```

Alert output file (JSONL):

```bash
tail -f freqtrade/user_data/logs/spike-alerts.jsonl
```

Prediction database (SQLite):

```bash
ls -lh freqtrade/user_data/logs/spike-scanner.sqlite
```

Optional notifications (Telegram/Discord) are sent on startup and each alert when configured.

Stop scanner:

```bash
docker compose --profile scanner stop spike-scanner
```

Notes:

- Scanner is alert-only (no trading), runs as a separate service.
- It reuses existing env variables where possible: `BINANCE_REST_BASE`, `BINANCE_WS_BASE`, `LLM_ROTATE_QUOTE`, `LLM_ROTATE_MIN_QUOTE_VOLUME`, `LLM_ROTATE_EXCLUDE_REGEX`.
- The scanner now stores predictions and evaluates outcomes at configured horizons (default `60,240,1440,2880` minutes).
- Optional LLM shadow mode can score the same alerts via `bot-api` and store a parallel verdict (`allowed/blocked/unknown`) for later comparison.
- API endpoint `GET /api/alerts?limit=200`
- API endpoint `GET /api/outcomes?limit=300&status=resolved|pending`
- API endpoint `GET /api/summary`
- API endpoint `GET /api/llm-evals?limit=200` (recent LLM shadow evaluations by symbol)

## 8) Key `.env` Controls

Core strategy:

- `STRATEGY_MODE=conservative|aggressive`
- `ENABLE_LLM_FILTER=true|false`
- `LLM_MIN_CONFIDENCE=0.65`
- `LLM_CONNECT_TIMEOUT_SECONDS=2`
- `LLM_READ_TIMEOUT_SECONDS=45` recommended for slower local models like `qwen3:8b` (`15` is often too low)
- `LLM_FAIL_OPEN=true` recommended in live/dry-run to avoid blocking entries when LLM is temporarily unavailable
- `CORE_PAIRS=...`
- `RISK_PAIRS=...`
- Keep `exchange.pair_whitelist` lean (usually `CORE_PAIRS + RISK_PAIRS`) to avoid slow analysis cycles and missed signals.
- `STRATEGY_MINIMAL_ROI_JSON={"0":0.05,"180":0.025,"720":0.0}` (optional override)
- `STRATEGY_STOPLOSS=-0.08` (optional override)
- `STRATEGY_TRAILING_STOP=true|false` (optional override; keep `false` when using `custom_stoploss`)
- `STRATEGY_TRAILING_POSITIVE=0.015` (optional override)
- `STRATEGY_TRAILING_OFFSET=0.04` (optional override)
- `EXIT_USE_RSI_TAKE=false` (default off; enables RSI-based take-profit exits)
- `STALE_TRADE_HOURS=24` and `STALE_MIN_PROFIT=0.01` (close stale low-progress trades)
- `STALE_LOSS_HOURS=12` and `STALE_LOSS_PCT=-0.02` (faster stale loser cut)
- `STALE_MAX_HOURS=72` (hard max holding age unless trade is strong)
- `CUSTOM_SL_ATR_MULT=1.6`, `CUSTOM_SL_MIN=-0.08`, `CUSTOM_SL_MAX=-0.015` (dynamic custom stoploss)
- `AGGR_ENTRY_STRICTNESS=strict|normal` (aggressive entry gate profile; default `strict`)
- `BENCHMARK_PAIR=BTC/USDT` (benchmark market used for regime filter)
- `BENCHMARK_FILTER_FOR_RISK=true` and `BENCHMARK_ALLOW_NEUTRAL_FOR_RISK=false` (gate alt/risk entries by BTC regime)
- `BENCHMARK_CHAOS_ADX=16` and `BENCHMARK_MIN_SPREAD_PCT=-0.05` (BTC chaos definition)
- `BENCHMARK_REDUCE_STAKE_WHEN_WEAK=true` with `BENCHMARK_RISK_STAKE_MULT_WHEN_WEAK=0.6` and `BENCHMARK_CORE_STAKE_MULT_WHEN_WEAK=0.85`
- `ENTRY_RANKING_ENABLED=true`, `ENTRY_TOP_N=1`, `ENTRY_MIN_SCORE=0.58` (take only best candidates)
- `ENTRY_RANKING_LOG=true|false` (defaults to on in live/dry-run, off in backtests)

LLM rotation:

- `LLM_ROTATE_AUTO_DISCOVER=true`
- `LLM_ROTATE_DATA_SOURCE=auto` (`local|exchange|auto`)
- `LLM_ROTATE_EXCHANGE=binance`
- `LLM_ROTATE_QUOTE=USDT`
- `LLM_ROTATE_MAX_CANDIDATES=20`
- `LLM_ROTATE_MIN_QUOTE_VOLUME=20000000`
- `LLM_ROTATE_EXCLUDE_REGEX=(UP|DOWN|BULL|BEAR|1000|[0-9][0-9][0-9]+L|[0-9][0-9][0-9]+S)`
- `LLM_ROTATE_TOP_N=3`
- `LLM_ROTATE_MIN_CONFIDENCE=0.60`
- `LLM_ROTATE_ALLOWED_RISK=low medium`
- `LLM_ROTATE_ALLOWED_REGIMES=trend_pullback` (aggressive default in script: `trend_pullback breakout mean_reversion`)
- `LLM_ROTATE_SYNC_WHITELIST=true`
- `LLM_ROTATE_LOG_PATH=./freqtrade/user_data/logs/llm-pair-rotation.log`
- `LLM_ROTATE_LOOP_INTERVAL_MINUTES=60` (used by `rotate-risk-pairs-loop.sh`)
- `LLM_ROTATE_LOOP_JITTER_SECONDS=0` (optional random delay before each cycle)

Scheduler:

- `SCHED_DOWNLOAD_ENABLED=true`
- `SCHED_DOWNLOAD_TIME=02:15`
- `SCHED_DOWNLOAD_TIMERANGE=20230101-`
- `SCHED_DOWNLOAD_PAIRS=...`
- `SCHED_PRUNE_ENABLED=true`
- `SCHED_PRUNE_TIME=03:00`
- `SCHED_PRUNE_WEEKDAY=0`
- `SCHED_PRUNE_DAYS=180`

Spike scanner:

- `BINANCE_REST_BASE=https://api.binance.com`
- `BINANCE_WS_BASE=wss://stream.binance.com:9443/stream`
- `SPIKE_QUOTE_ASSET=USDT` (optional override; defaults to `LLM_ROTATE_QUOTE`)
- `SPIKE_MIN_QUOTE_VOLUME=20000000` (optional override; defaults to `LLM_ROTATE_MIN_QUOTE_VOLUME`)
- `SPIKE_EXCLUDE_REGEX=...` (optional override; defaults to `LLM_ROTATE_EXCLUDE_REGEX`)
- `SPIKE_INCLUDE_SYMBOLS=...` (optional explicit include list)
- `SPIKE_EXCLUDE_SYMBOLS=BTCUSDT ETHUSDT`
- `SPIKE_UNIVERSE_MAX_SYMBOLS=60`
- `SPIKE_TOP_N_ALERTS=5`
- `SPIKE_MIN_SCORE=0.80`
- `SPIKE_MAX_SPREAD_PCT=0.30`
- `SPIKE_ALERT_COOLDOWN_MINUTES=30`
- `SPIKE_LOOP_SECONDS=5`
- `SPIKE_LOG_PATH=/data/spike-alerts.jsonl`
- `SPIKE_DB_PATH=/data/spike-scanner.sqlite`
- `SPIKE_OUTCOME_HORIZONS_MINUTES=60,240,1440,2880`
- `SPIKE_OUTCOME_LOOP_SECONDS=30`
- `SPIKE_OUTCOME_BATCH_SIZE=200`
- `SPIKE_LLM_SHADOW_ENABLED=false`
- `SPIKE_LLM_SHADOW_BOT_API_URL=http://bot-api:8000`
- `SPIKE_LLM_SHADOW_TIMEOUT_SECONDS=45` (raise this for slower local models, e.g. `qwen3:8b`)
- `SPIKE_LLM_SHADOW_MIN_CONFIDENCE=0.65`
- `SPIKE_LLM_SHADOW_ALLOWED_REGIMES=trend_pullback,breakout,mean_reversion`
- `SPIKE_LLM_SHADOW_ALLOWED_RISK_LEVELS=low,medium`
- `SPIKE_LLM_SHADOW_EVAL_TOP_N=5` (evaluate top N scored symbols per cycle)
- `SPIKE_LLM_SHADOW_EVAL_MIN_SCORE=0.70` (minimum deterministic score to evaluate)
- `SPIKE_LLM_SHADOW_EVAL_CACHE_SECONDS=300` (reuse per-symbol LLM result to limit API load)
- `SPIKE_WEB_ENABLED=true`
- `SPIKE_WEB_HOST=0.0.0.0`
- `SPIKE_WEB_PORT=8090` and `SPIKE_WEB_PUBLIC_PORT=8091`
- `SPIKE_NOTIFY_ENABLED=true`
- `SPIKE_NOTIFY_TIMEOUT_SECONDS=10`
- `SPIKE_TELEGRAM_BOT_TOKEN=...` and `SPIKE_TELEGRAM_CHAT_ID=...` (or generic `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`)
- `SPIKE_DISCORD_WEBHOOK_URL=...` (or generic `DISCORD_WEBHOOK_URL`)

## 9) Safety

- Keep `dry_run=true` until behavior is validated.
- Start with small stake and low `max_open_trades`.
- Use API key IP restrictions and disable withdrawals.
