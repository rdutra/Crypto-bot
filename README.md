# Crypto Bot (Freqtrade + Ollama + Bot API)

Freqtrade executes trades, `bot-api` provides bounded LLM decisions, and Ollama runs the model locally.

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

6. Pull model:

```bash
docker compose exec -T ollama ollama pull llama3.1:8b
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

## 7) Key `.env` Controls

Core strategy:

- `STRATEGY_MODE=conservative|aggressive`
- `ENABLE_LLM_FILTER=true|false`
- `LLM_MIN_CONFIDENCE=0.65`
- `CORE_PAIRS=...`
- `RISK_PAIRS=...`

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

Scheduler:

- `SCHED_DOWNLOAD_ENABLED=true`
- `SCHED_DOWNLOAD_TIME=02:15`
- `SCHED_DOWNLOAD_TIMERANGE=20230101-`
- `SCHED_DOWNLOAD_PAIRS=...`
- `SCHED_PRUNE_ENABLED=true`
- `SCHED_PRUNE_TIME=03:00`
- `SCHED_PRUNE_WEEKDAY=0`
- `SCHED_PRUNE_DAYS=180`

## Safety

- Keep `dry_run=true` until behavior is validated.
- Start with small stake and low `max_open_trades`.
- Use API key IP restrictions and disable withdrawals.
