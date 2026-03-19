#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"
CONFIG_FILE="${ROOT_DIR}/freqtrade/user_data/config.json"

BOT_API_URL="${LLM_BOT_API_URL:-http://localhost:8000}"
BINANCE_REST_BASE_URL="${BINANCE_REST_BASE:-https://api.binance.com}"
TIMEFRAME="${LLM_ROTATE_TIMEFRAME:-1h}"
LOOKBACK_CANDLES="${LLM_ROTATE_LOOKBACK_CANDLES:-240}"
TOP_N="${LLM_ROTATE_TOP_N:-3}"
MIN_CONFIDENCE="${LLM_ROTATE_MIN_CONFIDENCE:-0.60}"
ALLOWED_RISK="${LLM_ROTATE_ALLOWED_RISK:-low medium}"
ALLOWED_REGIMES="${LLM_ROTATE_ALLOWED_REGIMES:-}"
CANDIDATES="${LLM_ROTATE_CANDIDATES:-}"
AUTO_DISCOVER="${LLM_ROTATE_AUTO_DISCOVER:-true}"
DATA_SOURCE="${LLM_ROTATE_DATA_SOURCE:-auto}"
EXCHANGE_ID="${LLM_ROTATE_EXCHANGE:-binance}"
QUOTE_ASSET="${LLM_ROTATE_QUOTE:-USDT}"
MAX_CANDIDATES="${LLM_ROTATE_MAX_CANDIDATES:-20}"
MIN_QUOTE_VOLUME="${LLM_ROTATE_MIN_QUOTE_VOLUME:-20000000}"
EXCLUDE_REGEX="${LLM_ROTATE_EXCLUDE_REGEX:-(UP|DOWN|BULL|BEAR|1000|[0-9][0-9][0-9]+L|[0-9][0-9][0-9]+S)}"
WHITELIST_ONLY="${LLM_ROTATE_WHITELIST_ONLY:-false}"
SYNC_WHITELIST="${LLM_ROTATE_SYNC_WHITELIST:-true}"
LOG_PATH="${LLM_ROTATE_LOG_PATH:-${ROOT_DIR}/freqtrade/user_data/logs/llm-pair-rotation.log}"
USE_SPIKE_BIAS="${LLM_ROTATE_USE_SPIKE_BIAS:-}"
SPIKE_DB_PATH="${LLM_ROTATE_SPIKE_DB_PATH:-}"
SPIKE_LOOKBACK_HOURS="${LLM_ROTATE_SPIKE_LOOKBACK_HOURS:-}"
SPIKE_TOP_N="${LLM_ROTATE_SPIKE_TOP_N:-}"
SPIKE_MIN_SCORE="${LLM_ROTATE_SPIKE_MIN_SCORE:-}"
SPIKE_REQUIRE_LLM_ALLOWED="${LLM_ROTATE_SPIKE_REQUIRE_LLM_ALLOWED:-}"
USE_SMART_MONEY_BIAS="${LLM_ROTATE_USE_SMART_MONEY_BIAS:-}"
SMART_MONEY_TOP_N="${LLM_ROTATE_SMART_MONEY_TOP_N:-}"
SMART_MONEY_MIN_SCORE="${LLM_ROTATE_SMART_MONEY_MIN_SCORE:-}"
SMART_MONEY_REQUIRE_BUY="${LLM_ROTATE_SMART_MONEY_REQUIRE_BUY:-}"
SMART_MONEY_FORCE_REFRESH="${LLM_ROTATE_SMART_MONEY_FORCE_REFRESH:-}"
SMART_MONEY_FORCE_SLOT="${LLM_ROTATE_SMART_MONEY_FORCE_SLOT:-}"
MODE="${STRATEGY_MODE:-conservative}"
APPLY=false
RESTART=false

usage() {
  cat <<'EOF'
Usage:
  ./scripts/rotate-risk-pairs.sh
  ./scripts/rotate-risk-pairs.sh --apply
  ./scripts/rotate-risk-pairs.sh --apply --restart --mode aggressive
  ./scripts/rotate-risk-pairs.sh --top 3 --min-confidence 0.60
  ./scripts/rotate-risk-pairs.sh --auto-discover --max-candidates 25

Options:
  --top N                   Number of risk pairs to select.
  --min-confidence VALUE    Minimum LLM confidence [0..1].
  --candidates "PAIRS"      Space/comma separated candidate pairs (manual mode).
  --auto-discover           Discover candidates from exchange markets.
  --no-auto-discover        Disable exchange discovery and use provided/manual candidates.
  --data-source VALUE       local | exchange | auto  (default: auto).
  --exchange VALUE          Exchange id for discovery/data (default: binance).
  --quote VALUE             Quote asset for discovery (default: USDT).
  --max-candidates N        Max discovered candidates before ranking.
  --min-quote-volume VALUE  Min 24h quote volume for discovered pairs.
  --exclude-regex REGEX     Regex filter for symbols/base (leveraged token guard).
  --allowed-risk "LIST"     Space/comma list from: low medium high.
  --allowed-regimes "LIST"  Space/comma list from: trend_pullback breakout mean_reversion.
  --whitelist-only          Restrict ranking to existing pair_whitelist.
  --sync-whitelist          Add selected pairs to pair_whitelist on --apply.
  --no-sync-whitelist       Do not edit pair_whitelist on --apply.
  --log-path PATH           Log file path for rotation decisions.
  --bot-api-url URL         Default: http://localhost:8000
  --mode VALUE              Strategy mode for optional restart.
  --apply                   Write selected pairs to .env (RISK_PAIRS).
  --restart                 Restart freqtrade after apply.
  --help                    Show this help.

Notes:
  - LLM ranks; hard filters still apply.
  - With --apply and sync enabled, selected pairs are auto-added to pair_whitelist.
  - If local data is missing and --data-source=auto, exchange OHLCV is used.
  - Optional: set LLM_ROTATE_USE_SPIKE_BIAS=true to bias candidates with recent scanner winners.
  - Optional: set LLM_ROTATE_USE_SMART_MONEY_BIAS=true to prepend Binance-spot tradable smart-money pairs.
  - Optional: set LLM_ROTATE_SMART_MONEY_FORCE_SLOT=true to guarantee at least one selected smart-money pair.
EOF
}

get_env_file_value() {
  local key="$1"
  python3 - "$ENV_FILE" "$key" <<'PY'
import sys
from pathlib import Path

env_path = Path(sys.argv[1])
key = sys.argv[2]
if not env_path.exists():
    raise SystemExit(0)

for raw in env_path.read_text().splitlines():
    line = raw.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    if k.strip() == key:
        print(v.strip())
        break
PY
}

is_true() {
  case "$(printf '%s' "${1}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

spike_bias_candidates() {
  local db_path="$1"
  local quote_asset="$2"
  local lookback_hours="$3"
  local top_n="$4"
  local min_score="$5"
  local require_llm_allowed="$6"
  python3 - "$db_path" "$quote_asset" "$lookback_hours" "$top_n" "$min_score" "$require_llm_allowed" <<'PY'
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

db_path = Path(sys.argv[1])
quote = str(sys.argv[2] or "USDT").strip().upper()
lookback_hours = int(float(sys.argv[3]))
top_n = int(float(sys.argv[4]))
min_score = float(sys.argv[5])
require_allowed = str(sys.argv[6]).strip().lower() in {"1", "true", "yes", "on"}

if not db_path.exists() or top_n <= 0:
    print("")
    raise SystemExit(0)

cutoff = datetime.now(timezone.utc) - timedelta(hours=max(1, lookback_hours))
best_by_pair: dict[str, float] = {}

conn = sqlite3.connect(str(db_path))
conn.row_factory = sqlite3.Row
rows = conn.execute(
    "SELECT ts, symbol, score, llm_allowed FROM alerts ORDER BY id DESC LIMIT 2000"
).fetchall()
conn.close()

for row in rows:
    symbol = str(row["symbol"] or "").strip().upper()
    if not symbol.endswith(quote):
        continue
    if require_allowed and int(row["llm_allowed"] or 0) != 1:
        continue
    try:
        score = float(row["score"])
    except (TypeError, ValueError):
        continue
    if score < min_score:
        continue
    ts_raw = str(row["ts"] or "").strip()
    if not ts_raw:
        continue
    try:
        ts = datetime.fromisoformat(ts_raw)
    except ValueError:
        continue
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    if ts < cutoff:
        continue

    base = symbol[: -len(quote)].strip()
    if not base:
        continue
    pair = f"{base}/{quote}"
    prev = best_by_pair.get(pair)
    if prev is None or score > prev:
        best_by_pair[pair] = score

ordered = sorted(best_by_pair.items(), key=lambda item: item[1], reverse=True)[:top_n]
print(" ".join(pair for pair, _ in ordered))
PY
}

smart_money_bias_candidates() {
  local bot_api_url="$1"
  local binance_rest_base="$2"
  local quote_asset="$3"
  local top_n="$4"
  local min_score="$5"
  local require_buy="$6"
  local force_refresh="$7"
  local exclude_regex="$8"
  python3 - "$bot_api_url" "$binance_rest_base" "$quote_asset" "$top_n" "$min_score" "$require_buy" "$force_refresh" "$exclude_regex" <<'PY'
import json
import re
import sys
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "on"}


def fetch_json(url: str, timeout: float = 12.0):
    req = Request(url=url, method="GET", headers={"Accept": "application/json"})
    with urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="replace")
    return json.loads(body)


bot_api_url = str(sys.argv[1]).strip().rstrip("/")
binance_rest_base = str(sys.argv[2]).strip().rstrip("/")
quote = str(sys.argv[3] or "USDT").strip().upper()
top_n = int(float(sys.argv[4]))
min_score = float(sys.argv[5])
require_buy = as_bool(sys.argv[6])
force_refresh = as_bool(sys.argv[7])
exclude_regex = str(sys.argv[8] or "").strip()

if top_n <= 0:
    print("")
    raise SystemExit(0)

pattern = None
if exclude_regex:
    try:
        pattern = re.compile(exclude_regex, re.IGNORECASE)
    except re.error:
        pattern = None

query = urlencode({"limit": max(50, top_n * 4), "force_refresh": "true" if force_refresh else "false"})
skill_url = f"{bot_api_url}/skills/trading-signal?{query}"
items = []
try:
    payload = fetch_json(skill_url, timeout=15.0)
    raw_items = payload.get("items", []) if isinstance(payload, dict) else []
    if isinstance(raw_items, list):
        items = [row for row in raw_items if isinstance(row, dict)]
except Exception:
    print("")
    raise SystemExit(0)

if not items:
    print("")
    raise SystemExit(0)

spot_symbols = set()
try:
    exchange_info = fetch_json(f"{binance_rest_base}/api/v3/exchangeInfo", timeout=20.0)
    symbols = exchange_info.get("symbols", []) if isinstance(exchange_info, dict) else []
    if isinstance(symbols, list):
        for row in symbols:
            if not isinstance(row, dict):
                continue
            if str(row.get("status", "")).upper() != "TRADING":
                continue
            if not bool(row.get("isSpotTradingAllowed", False)):
                continue
            quote_asset = str(row.get("quoteAsset", "")).upper()
            if quote_asset != quote:
                continue
            symbol = str(row.get("symbol", "")).upper()
            if symbol:
                spot_symbols.add(symbol)
except Exception:
    print("")
    raise SystemExit(0)

if not spot_symbols:
    print("")
    raise SystemExit(0)

picked = []
seen = set()
for item in items:
    pair = str(item.get("pair", "")).strip().upper()
    if "/" not in pair:
        continue
    base, pair_quote = pair.split("/", 1)
    if pair_quote != quote or not base:
        continue
    symbol = f"{base}{quote}"
    if symbol not in spot_symbols:
        continue

    if pattern and (pattern.search(base) or pattern.search(pair)):
        continue

    side = str(item.get("side", "")).strip().lower()
    if require_buy and side != "buy":
        continue

    try:
        score = float(item.get("score", 0.0) or 0.0)
    except (TypeError, ValueError):
        score = 0.0
    if score < min_score:
        continue

    norm_pair = f"{base}/{quote}"
    if norm_pair in seen:
        continue
    seen.add(norm_pair)
    picked.append(norm_pair)
    if len(picked) >= top_n:
        break

print(" ".join(picked))
PY
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --top)
      TOP_N="${2:-}"
      shift 2
      ;;
    --min-confidence)
      MIN_CONFIDENCE="${2:-}"
      shift 2
      ;;
    --candidates)
      CANDIDATES="${2:-}"
      shift 2
      ;;
    --auto-discover)
      AUTO_DISCOVER=true
      shift
      ;;
    --no-auto-discover)
      AUTO_DISCOVER=false
      shift
      ;;
    --data-source)
      DATA_SOURCE="${2:-}"
      shift 2
      ;;
    --exchange)
      EXCHANGE_ID="${2:-}"
      shift 2
      ;;
    --quote)
      QUOTE_ASSET="${2:-}"
      shift 2
      ;;
    --max-candidates)
      MAX_CANDIDATES="${2:-}"
      shift 2
      ;;
    --min-quote-volume)
      MIN_QUOTE_VOLUME="${2:-}"
      shift 2
      ;;
    --exclude-regex)
      EXCLUDE_REGEX="${2:-}"
      shift 2
      ;;
    --allowed-risk)
      ALLOWED_RISK="${2:-}"
      shift 2
      ;;
    --allowed-regimes)
      ALLOWED_REGIMES="${2:-}"
      shift 2
      ;;
    --whitelist-only)
      WHITELIST_ONLY=true
      shift
      ;;
    --sync-whitelist)
      SYNC_WHITELIST=true
      shift
      ;;
    --no-sync-whitelist)
      SYNC_WHITELIST=false
      shift
      ;;
    --log-path)
      LOG_PATH="${2:-}"
      shift 2
      ;;
    --bot-api-url)
      BOT_API_URL="${2:-}"
      shift 2
      ;;
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --apply)
      APPLY=true
      shift
      ;;
    --restart)
      RESTART=true
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if ! [[ "${TOP_N}" =~ ^[0-9]+$ ]]; then
  echo "--top must be an integer." >&2
  exit 1
fi
if ! [[ "${LOOKBACK_CANDLES}" =~ ^[0-9]+$ ]]; then
  echo "LLM_ROTATE_LOOKBACK_CANDLES must be an integer." >&2
  exit 1
fi
if ! [[ "${MAX_CANDIDATES}" =~ ^[0-9]+$ ]]; then
  echo "--max-candidates must be an integer." >&2
  exit 1
fi
if ! [[ "${MIN_CONFIDENCE}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "--min-confidence must be numeric." >&2
  exit 1
fi
if ! [[ "${MIN_QUOTE_VOLUME}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "--min-quote-volume must be numeric." >&2
  exit 1
fi
if [[ "${RESTART}" == "true" && "${APPLY}" != "true" ]]; then
  echo "--restart requires --apply." >&2
  exit 1
fi

MODE="$(printf '%s' "${MODE}" | tr '[:upper:]' '[:lower:]')"
if [[ "${MODE}" != "conservative" && "${MODE}" != "aggressive" ]]; then
  echo "--mode must be either conservative or aggressive." >&2
  exit 1
fi

if [[ -z "${ALLOWED_REGIMES}" ]]; then
  if [[ "${MODE}" == "aggressive" ]]; then
    ALLOWED_REGIMES="trend_pullback breakout mean_reversion"
  else
    ALLOWED_REGIMES="trend_pullback"
  fi
fi

DATA_SOURCE="$(printf '%s' "${DATA_SOURCE}" | tr '[:upper:]' '[:lower:]')"
if [[ "${DATA_SOURCE}" != "local" && "${DATA_SOURCE}" != "exchange" && "${DATA_SOURCE}" != "auto" ]]; then
  echo "--data-source must be one of: local, exchange, auto." >&2
  exit 1
fi

core_pairs="${CORE_PAIRS:-}"
if [[ -z "${core_pairs}" ]]; then
  core_pairs="$(get_env_file_value CORE_PAIRS || true)"
fi
core_pairs="${core_pairs:-BTC/USDT ETH/USDT BNB/USDT}"

current_risk_pairs="${RISK_PAIRS:-}"
if [[ -z "${current_risk_pairs}" ]]; then
  current_risk_pairs="$(get_env_file_value RISK_PAIRS || true)"
fi

if [[ -z "${CANDIDATES}" ]]; then
  CANDIDATES="$(get_env_file_value LLM_ROTATE_CANDIDATES || true)"
fi
binance_rest_base_from_file="$(get_env_file_value BINANCE_REST_BASE || true)"
if [[ -n "${binance_rest_base_from_file}" && "${BINANCE_REST_BASE_URL}" == "https://api.binance.com" ]]; then
  BINANCE_REST_BASE_URL="${binance_rest_base_from_file}"
fi
if [[ -z "${USE_SPIKE_BIAS}" ]]; then
  USE_SPIKE_BIAS="$(get_env_file_value LLM_ROTATE_USE_SPIKE_BIAS || true)"
fi
if [[ -z "${SPIKE_DB_PATH}" ]]; then
  SPIKE_DB_PATH="$(get_env_file_value LLM_ROTATE_SPIKE_DB_PATH || true)"
fi
if [[ -z "${SPIKE_LOOKBACK_HOURS}" ]]; then
  SPIKE_LOOKBACK_HOURS="$(get_env_file_value LLM_ROTATE_SPIKE_LOOKBACK_HOURS || true)"
fi
if [[ -z "${SPIKE_TOP_N}" ]]; then
  SPIKE_TOP_N="$(get_env_file_value LLM_ROTATE_SPIKE_TOP_N || true)"
fi
if [[ -z "${SPIKE_MIN_SCORE}" ]]; then
  SPIKE_MIN_SCORE="$(get_env_file_value LLM_ROTATE_SPIKE_MIN_SCORE || true)"
fi
if [[ -z "${SPIKE_REQUIRE_LLM_ALLOWED}" ]]; then
  SPIKE_REQUIRE_LLM_ALLOWED="$(get_env_file_value LLM_ROTATE_SPIKE_REQUIRE_LLM_ALLOWED || true)"
fi
if [[ -z "${USE_SMART_MONEY_BIAS}" ]]; then
  USE_SMART_MONEY_BIAS="$(get_env_file_value LLM_ROTATE_USE_SMART_MONEY_BIAS || true)"
fi
if [[ -z "${SMART_MONEY_TOP_N}" ]]; then
  SMART_MONEY_TOP_N="$(get_env_file_value LLM_ROTATE_SMART_MONEY_TOP_N || true)"
fi
if [[ -z "${SMART_MONEY_MIN_SCORE}" ]]; then
  SMART_MONEY_MIN_SCORE="$(get_env_file_value LLM_ROTATE_SMART_MONEY_MIN_SCORE || true)"
fi
if [[ -z "${SMART_MONEY_REQUIRE_BUY}" ]]; then
  SMART_MONEY_REQUIRE_BUY="$(get_env_file_value LLM_ROTATE_SMART_MONEY_REQUIRE_BUY || true)"
fi
if [[ -z "${SMART_MONEY_FORCE_REFRESH}" ]]; then
  SMART_MONEY_FORCE_REFRESH="$(get_env_file_value LLM_ROTATE_SMART_MONEY_FORCE_REFRESH || true)"
fi
if [[ -z "${SMART_MONEY_FORCE_SLOT}" ]]; then
  SMART_MONEY_FORCE_SLOT="$(get_env_file_value LLM_ROTATE_SMART_MONEY_FORCE_SLOT || true)"
fi
USE_SPIKE_BIAS="${USE_SPIKE_BIAS:-false}"
SPIKE_DB_PATH="${SPIKE_DB_PATH:-${ROOT_DIR}/freqtrade/user_data/logs/spike-scanner.sqlite}"
SPIKE_LOOKBACK_HOURS="${SPIKE_LOOKBACK_HOURS:-48}"
SPIKE_TOP_N="${SPIKE_TOP_N:-4}"
SPIKE_MIN_SCORE="${SPIKE_MIN_SCORE:-0.80}"
SPIKE_REQUIRE_LLM_ALLOWED="${SPIKE_REQUIRE_LLM_ALLOWED:-false}"
USE_SMART_MONEY_BIAS="${USE_SMART_MONEY_BIAS:-false}"
SMART_MONEY_TOP_N="${SMART_MONEY_TOP_N:-4}"
SMART_MONEY_MIN_SCORE="${SMART_MONEY_MIN_SCORE:-0.60}"
SMART_MONEY_REQUIRE_BUY="${SMART_MONEY_REQUIRE_BUY:-true}"
SMART_MONEY_FORCE_REFRESH="${SMART_MONEY_FORCE_REFRESH:-false}"
SMART_MONEY_FORCE_SLOT="${SMART_MONEY_FORCE_SLOT:-true}"
if ! [[ "${SPIKE_LOOKBACK_HOURS}" =~ ^[0-9]+$ ]]; then
  echo "LLM_ROTATE_SPIKE_LOOKBACK_HOURS must be an integer." >&2
  exit 1
fi
if ! [[ "${SPIKE_TOP_N}" =~ ^[0-9]+$ ]]; then
  echo "LLM_ROTATE_SPIKE_TOP_N must be an integer." >&2
  exit 1
fi
if ! [[ "${SPIKE_MIN_SCORE}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "LLM_ROTATE_SPIKE_MIN_SCORE must be numeric." >&2
  exit 1
fi
if ! [[ "${SMART_MONEY_TOP_N}" =~ ^[0-9]+$ ]]; then
  echo "LLM_ROTATE_SMART_MONEY_TOP_N must be an integer." >&2
  exit 1
fi
if ! [[ "${SMART_MONEY_MIN_SCORE}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "LLM_ROTATE_SMART_MONEY_MIN_SCORE must be numeric." >&2
  exit 1
fi
if ! is_true "${AUTO_DISCOVER}"; then
  if [[ -z "${CANDIDATES}" ]]; then
    CANDIDATES="${current_risk_pairs}"
  fi
  if [[ -z "${CANDIDATES}" ]]; then
    CANDIDATES="$(get_env_file_value SCHED_DOWNLOAD_PAIRS || true)"
  fi
fi

if [[ ! -f "${CONFIG_FILE}" ]]; then
  echo "Missing config file: ${CONFIG_FILE}" >&2
  echo "Create it from freqtrade/user_data/config.json.example first." >&2
  exit 1
fi

if [[ "${LOG_PATH}" != /* ]]; then
  LOG_PATH="${ROOT_DIR}/${LOG_PATH#./}"
fi
if [[ "${SPIKE_DB_PATH}" != /* ]]; then
  SPIKE_DB_PATH="${ROOT_DIR}/${SPIKE_DB_PATH#./}"
fi

mkdir -p "$(dirname "${LOG_PATH}")"

SPIKE_BIAS_CANDIDATES=""
if is_true "${USE_SPIKE_BIAS}"; then
  SPIKE_BIAS_CANDIDATES="$(
    spike_bias_candidates \
      "${SPIKE_DB_PATH}" \
      "${QUOTE_ASSET}" \
      "${SPIKE_LOOKBACK_HOURS}" \
      "${SPIKE_TOP_N}" \
      "${SPIKE_MIN_SCORE}" \
      "${SPIKE_REQUIRE_LLM_ALLOWED}"
  )"
  if [[ -n "${SPIKE_BIAS_CANDIDATES}" ]]; then
    echo "Spike bias candidates: ${SPIKE_BIAS_CANDIDATES}"
  else
    echo "Spike bias enabled, but no recent qualifying scanner symbols were found."
  fi
fi

SMART_MONEY_BIAS_CANDIDATES=""
if is_true "${USE_SMART_MONEY_BIAS}"; then
  SMART_MONEY_BIAS_CANDIDATES="$(
    smart_money_bias_candidates \
      "${BOT_API_URL}" \
      "${BINANCE_REST_BASE_URL}" \
      "${QUOTE_ASSET}" \
      "${SMART_MONEY_TOP_N}" \
      "${SMART_MONEY_MIN_SCORE}" \
      "${SMART_MONEY_REQUIRE_BUY}" \
      "${SMART_MONEY_FORCE_REFRESH}" \
      "${EXCLUDE_REGEX}"
  )"
  if [[ -n "${SMART_MONEY_BIAS_CANDIDATES}" ]]; then
    echo "Smart-money bias candidates (spot-tradable): ${SMART_MONEY_BIAS_CANDIDATES}"
  else
    echo "Smart-money bias enabled, but no qualifying Binance-spot symbols were found."
  fi
fi

if ! is_true "${AUTO_DISCOVER}" && [[ -z "${CANDIDATES}" ]] && [[ -z "${SPIKE_BIAS_CANDIDATES}" ]] && [[ -z "${SMART_MONEY_BIAS_CANDIDATES}" ]]; then
  echo "No candidates provided and auto-discovery disabled." >&2
  echo "Use --auto-discover, set --candidates / LLM_ROTATE_CANDIDATES, or enable spike/smart-money bias." >&2
  exit 1
fi

if ! is_true "${ROTATE_SKIP_BOOTSTRAP_SERVICES:-false}"; then
  docker compose up -d ollama bot-api >/dev/null
fi

echo "Preparing candidate metrics..."

if ! docker ps --format '{{.Names}}' | grep -qx 'freqtrade'; then
  echo "freqtrade container is not running. Start it before rotating pairs." >&2
  exit 1
fi

metrics_json="$(
  docker exec -i \
    -e ROTATE_CANDIDATES="${CANDIDATES}" \
    -e ROTATE_SPIKE_CANDIDATES="${SPIKE_BIAS_CANDIDATES}" \
    -e ROTATE_SMART_MONEY_CANDIDATES="${SMART_MONEY_BIAS_CANDIDATES}" \
    -e ROTATE_AUTO_DISCOVER="${AUTO_DISCOVER}" \
    -e ROTATE_DATA_SOURCE="${DATA_SOURCE}" \
    -e ROTATE_EXCHANGE="${EXCHANGE_ID}" \
    -e ROTATE_QUOTE="${QUOTE_ASSET}" \
    -e ROTATE_MAX_CANDIDATES="${MAX_CANDIDATES}" \
    -e ROTATE_MIN_QUOTE_VOLUME="${MIN_QUOTE_VOLUME}" \
    -e ROTATE_EXCLUDE_REGEX="${EXCLUDE_REGEX}" \
    -e ROTATE_WHITELIST_ONLY="${WHITELIST_ONLY}" \
    -e ROTATE_CORE_PAIRS="${core_pairs}" \
    -e ROTATE_TIMEFRAME="${TIMEFRAME}" \
    -e ROTATE_LOOKBACK_CANDLES="${LOOKBACK_CANDLES}" \
    -e ROTATE_CONFIG_PATH="/freqtrade/user_data/config.json" \
    freqtrade /bin/sh -lc 'python - <<'"'"'PY'"'"'
import json
import math
import os
import re
from pathlib import Path

import pandas as pd
import talib.abstract as ta

try:
    import ccxt
except Exception:
    ccxt = None


def as_bool(raw: str, default: bool = False) -> bool:
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def parse_pairs(raw: str):
    return [part.strip().upper() for part in raw.replace(",", " ").split() if part.strip()]


def pair_to_filename(pair: str, timeframe: str) -> str:
    return f"{pair.replace(chr(47), chr(95))}-{timeframe}.feather"


def finite(value):
    try:
        val = float(value)
    except Exception:
        return None
    if not math.isfinite(val):
        return None
    return val


def deterministic_score(row, trend_4h: str) -> float:
    score = 0.0
    if row["close"] > row["ema200"]:
        score += 2.0
    if row["ema20"] > row["ema50"]:
        score += 1.5
    if row["ema50"] > row["ema200"]:
        score += 1.0
    if trend_4h == "bullish":
        score += 1.5
    if 45.0 <= row["rsi"] <= 62.0:
        score += 1.5
    elif 40.0 <= row["rsi"] <= 68.0:
        score += 0.75
    if row["adx"] >= 20.0:
        score += 1.5
    elif row["adx"] >= 14.0:
        score += 0.75
    if 0.8 <= row["atr_pct"] <= 5.5:
        score += 1.0
    if row["volume_z"] >= -0.2:
        score += 0.75
    elif row["volume_z"] >= -1.0:
        score += 0.35
    if row["close"] >= row["ema20"]:
        score += 0.5
    return round(score, 2)


def add_indicators(df):
    if df is None or df.empty:
        return None
    out = df.copy()
    out["ema20"] = ta.EMA(out, timeperiod=20)
    out["ema50"] = ta.EMA(out, timeperiod=50)
    out["ema200"] = ta.EMA(out, timeperiod=200)
    out["rsi"] = ta.RSI(out, timeperiod=14)
    out["adx"] = ta.ADX(out, timeperiod=14)
    out["atr"] = ta.ATR(out, timeperiod=14)
    out["atr_pct"] = (out["atr"] / out["close"]) * 100.0
    vol_ma20 = out["volume"].rolling(20).mean()
    vol_std = out["volume"].rolling(20).std()
    out["volume_z"] = (out["volume"] - vol_ma20) / vol_std
    return out


def load_local_ohlcv(data_dir: Path, pair: str, timeframe: str, lookback: int):
    path = data_dir / pair_to_filename(pair, timeframe)
    if not path.exists():
        return None
    df = pd.read_feather(path)
    if df is None or df.empty:
        return None
    min_rows = max(220, lookback)
    if len(df) < min_rows:
        return None
    return df.tail(max(260, lookback)).copy()


def build_exchange(exchange_id: str):
    if ccxt is None:
        return None
    exchange_class = getattr(ccxt, exchange_id, None)
    if exchange_class is None:
        return None
    return exchange_class({"enableRateLimit": True, "options": {"defaultType": "spot"}})


def load_exchange_ohlcv(exchange, pair: str, timeframe: str, lookback: int):
    if exchange is None:
        return None
    limit = max(260, lookback) + 20
    try:
        bars = exchange.fetch_ohlcv(pair, timeframe=timeframe, limit=limit)
    except Exception:
        return None
    if not bars:
        return None
    df = pd.DataFrame(bars, columns=["date", "open", "high", "low", "close", "volume"])
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"], unit="ms", utc=True)
    return df.tail(max(260, lookback)).copy()


def discover_candidates(exchange, core_pairs, quote_asset, max_candidates, min_quote_volume, exclude_regex):
    if exchange is None:
        return [], ["ccxt_exchange_unavailable"]
    try:
        markets = exchange.load_markets()
    except Exception:
        return [], ["ccxt_load_markets_error"]
    tickers = {}
    if exchange.has.get("fetchTickers"):
        try:
            tickers = exchange.fetch_tickers()
        except Exception:
            tickers = {}

    pattern = re.compile(exclude_regex, re.IGNORECASE) if exclude_regex else None
    scored = []
    skipped = []

    for symbol, market in markets.items():
        pair = str(symbol).upper()
        base = str(market.get("base", "")).upper()
        quote = str(market.get("quote", "")).upper()
        active = market.get("active", True)
        spot = bool(market.get("spot", False))
        contract = bool(market.get("contract", False))

        if not spot or contract:
            continue
        if quote != quote_asset:
            continue
        if not active:
            continue
        if ":" in pair:
            continue
        if pair in core_pairs:
            continue
        if pattern and (pattern.search(pair) or pattern.search(base)):
            continue

        ticker = tickers.get(symbol) or tickers.get(pair) or {}
        quote_volume = finite(ticker.get("quoteVolume"))
        if quote_volume is None:
            info = ticker.get("info") if isinstance(ticker, dict) else None
            if isinstance(info, dict):
                quote_volume = finite(info.get("quoteVolume") or info.get("quote_volume"))
        quote_volume = quote_volume or 0.0

        if quote_volume < min_quote_volume:
            skipped.append(f"low_volume:{pair}")
            continue

        scored.append((pair, quote_volume))

    scored.sort(key=lambda item: item[1], reverse=True)
    return [p for p, _ in scored[:max_candidates]], skipped


manual_candidates = parse_pairs(os.getenv("ROTATE_CANDIDATES", ""))
spike_candidates = parse_pairs(os.getenv("ROTATE_SPIKE_CANDIDATES", ""))
smart_money_candidates = parse_pairs(os.getenv("ROTATE_SMART_MONEY_CANDIDATES", ""))
core_pairs = set(parse_pairs(os.getenv("ROTATE_CORE_PAIRS", "")))
auto_discover = as_bool(os.getenv("ROTATE_AUTO_DISCOVER", "true"), default=True)
data_source = str(os.getenv("ROTATE_DATA_SOURCE", "auto")).strip().lower()
exchange_id = str(os.getenv("ROTATE_EXCHANGE", "binance")).strip().lower()
quote_asset = str(os.getenv("ROTATE_QUOTE", "USDT")).strip().upper()
max_candidates = int(os.getenv("ROTATE_MAX_CANDIDATES", "20"))
min_quote_volume = float(os.getenv("ROTATE_MIN_QUOTE_VOLUME", "20000000"))
exclude_regex = str(os.getenv("ROTATE_EXCLUDE_REGEX", "")).strip()
whitelist_only = as_bool(os.getenv("ROTATE_WHITELIST_ONLY", "false"), default=False)
timeframe = os.getenv("ROTATE_TIMEFRAME", "1h")
lookback = int(os.getenv("ROTATE_LOOKBACK_CANDLES", "240"))
config_path = Path(os.getenv("ROTATE_CONFIG_PATH", "/freqtrade/user_data/config.json"))
data_dir = Path("/freqtrade/user_data/data/binance")

pair_whitelist = set()
if config_path.exists():
    try:
        cfg = json.loads(config_path.read_text())
        pair_whitelist = {str(p).upper() for p in cfg.get("exchange", {}).get("pair_whitelist", [])}
    except Exception:
        pair_whitelist = set()

exchange = build_exchange(exchange_id)

candidates = []
discovery_notes = []
if manual_candidates:
    candidates.extend(manual_candidates)
    discovery_notes.append("source=manual")
if auto_discover:
    discovered, notes = discover_candidates(
        exchange=exchange,
        core_pairs=core_pairs,
        quote_asset=quote_asset,
        max_candidates=max_candidates,
        min_quote_volume=min_quote_volume,
        exclude_regex=exclude_regex,
    )
    candidates.extend(discovered)
    discovery_notes.append("source=exchange_discovery")
    for note in notes[:20]:
        discovery_notes.append(note)
elif not candidates:
    fallback = sorted(pair_whitelist) if pair_whitelist else []
    candidates = [p for p in fallback if p not in core_pairs][:max_candidates]
    discovery_notes.append("source=whitelist_fallback")

if spike_candidates:
    candidates = spike_candidates + candidates
    discovery_notes.append(f"source=spike_bias count={len(spike_candidates)}")
if smart_money_candidates:
    candidates = smart_money_candidates + candidates
    discovery_notes.append(f"source=smart_money_bias count={len(smart_money_candidates)}")

# Deduplicate while preserving order.
seen = set()
ordered_candidates = []
for pair in candidates:
    if pair in seen:
        continue
    seen.add(pair)
    ordered_candidates.append(pair)

if len(ordered_candidates) > 40:
    ordered_candidates = ordered_candidates[:40]
    discovery_notes.append("candidate_cap=40")

result = {
    "candidates": [],
    "skipped": [],
    "whitelist_missing": [],
    "discovery_notes": discovery_notes,
}


def get_df(pair: str, tf: str, lb: int):
    if data_source == "local":
        local_df = load_local_ohlcv(data_dir, pair, tf, lb)
        return local_df, "local"
    if data_source == "exchange":
        ex_df = load_exchange_ohlcv(exchange, pair, tf, lb)
        return ex_df, "exchange"

    # auto mode
    local_df = load_local_ohlcv(data_dir, pair, tf, lb)
    if local_df is not None:
        return local_df, "local"
    ex_df = load_exchange_ohlcv(exchange, pair, tf, lb)
    return ex_df, "exchange"


for pair in ordered_candidates:
    if pair in core_pairs:
        continue

    in_whitelist = (not pair_whitelist) or (pair in pair_whitelist)
    if not in_whitelist and whitelist_only:
        result["whitelist_missing"].append(pair)
        continue
    if not in_whitelist:
        result["whitelist_missing"].append(pair)

    raw_df, source_used = get_df(pair, timeframe, lookback)
    df = add_indicators(raw_df)
    if df is None or len(df) < 220:
        result["skipped"].append({"pair": pair, "reason": f"missing_or_short_{timeframe}_data"})
        continue

    row = df.iloc[-1]
    values = {
        "price": finite(row.get("close")),
        "ema_20": finite(row.get("ema20")),
        "ema_50": finite(row.get("ema50")),
        "ema_200": finite(row.get("ema200")),
        "rsi_14": finite(row.get("rsi")),
        "adx_14": finite(row.get("adx")),
        "atr_pct": finite(row.get("atr_pct")),
        "volume_zscore": finite(row.get("volume_z")),
    }
    if any(v is None for v in values.values()):
        result["skipped"].append({"pair": pair, "reason": "invalid_indicator_values"})
        continue

    info_raw, _ = get_df(pair, "4h", 220)
    info_df = add_indicators(info_raw)
    trend_4h = "bearish"
    if info_df is not None and not info_df.empty:
        info_row = info_df.iloc[-1]
        ema50_4h = finite(info_row.get("ema50"))
        ema200_4h = finite(info_row.get("ema200"))
        if ema50_4h is not None and ema200_4h is not None and ema50_4h > ema200_4h:
            trend_4h = "bullish"

    market_structure = "higher_highs"
    if not (values["price"] > values["ema_20"] > values["ema_50"] > values["ema_200"]):
        market_structure = "mixed"

    score_row = {
        "close": values["price"],
        "ema20": values["ema_20"],
        "ema50": values["ema_50"],
        "ema200": values["ema_200"],
        "rsi": values["rsi_14"],
        "adx": values["adx_14"],
        "atr_pct": values["atr_pct"],
        "volume_z": values["volume_zscore"],
    }
    score = deterministic_score(score_row, trend_4h)

    result["candidates"].append(
        {
            "pair": pair,
            "timeframe": timeframe,
            "price": values["price"],
            "ema_20": values["ema_20"],
            "ema_50": values["ema_50"],
            "ema_200": values["ema_200"],
            "rsi_14": values["rsi_14"],
            "adx_14": values["adx_14"],
            "atr_pct": values["atr_pct"],
            "volume_zscore": values["volume_zscore"],
            "trend_4h": trend_4h,
            "market_structure": market_structure,
            "deterministic_score": score,
            "data_source": source_used,
        }
    )

print(json.dumps(result, separators=(",", ":")))
PY'
)"

candidate_count="$(
  python3 - "${metrics_json}" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
print(len(payload.get("candidates", [])))
PY
)"

if [[ "${candidate_count}" -eq 0 ]]; then
  echo "No eligible candidates found."
  python3 - "${metrics_json}" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
for note in payload.get("discovery_notes", []):
    print(f"- {note}")
for row in payload.get("skipped", []):
    print(f"- skipped {row.get('pair')}: {row.get('reason')}")
for pair in payload.get("whitelist_missing", []):
    print(f"- not in pair_whitelist: {pair}")
PY
  python3 - "${metrics_json}" "${LOG_PATH}" "${TOP_N}" "${MIN_CONFIDENCE}" "${ALLOWED_RISK}" "${ALLOWED_REGIMES}" "${DATA_SOURCE}" "${AUTO_DISCOVER}" <<'PY'
import json
import sys
from datetime import datetime, timezone

meta = json.loads(sys.argv[1])
log_path = sys.argv[2]
top_n = int(sys.argv[3])
min_conf = float(sys.argv[4])
allowed_risk = [x.strip().lower() for x in sys.argv[5].replace(",", " ").split() if x.strip()]
allowed_regimes = [x.strip().lower() for x in sys.argv[6].replace(",", " ").split() if x.strip()]
data_source = sys.argv[7]
auto_discover = str(sys.argv[8]).lower() in {"1", "true", "yes", "on"}

entry = {
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "event": "rotation_no_candidates",
    "top_n": top_n,
    "min_confidence": min_conf,
    "allowed_risk_levels": allowed_risk,
    "allowed_regimes": allowed_regimes,
    "data_source": data_source,
    "auto_discover": auto_discover,
    "selected_pairs": [],
    "discovery_notes": meta.get("discovery_notes", []),
    "whitelist_missing": meta.get("whitelist_missing", []),
    "skipped": meta.get("skipped", []),
}

with open(log_path, "a", encoding="utf-8") as f:
    f.write(json.dumps(entry, separators=(",", ":")) + "\n")
PY
  echo "Rotation log appended: ${LOG_PATH}"
  exit 1
fi

rank_request="$(
  python3 - "${metrics_json}" "${TOP_N}" "${MIN_CONFIDENCE}" "${ALLOWED_RISK}" "${ALLOWED_REGIMES}" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
top_n = int(sys.argv[2])
min_conf = float(sys.argv[3])
allowed = [x.strip().lower() for x in sys.argv[4].replace(",", " ").split() if x.strip()]
allowed_regimes = [x.strip().lower() for x in sys.argv[5].replace(",", " ").split() if x.strip()]

for item in payload.get("candidates", []):
    item.pop("data_source", None)

body = {
    "candidates": payload["candidates"],
    "top_n": top_n,
    "min_confidence": min_conf,
    "allowed_risk_levels": allowed or ["low", "medium"],
    "allowed_regimes": allowed_regimes or ["trend_pullback"],
}
print(json.dumps(body, separators=(",", ":")))
PY
)"

rank_response="$(
  curl -fsS -X POST "${BOT_API_URL%/}/rank-pairs" \
    -H "Content-Type: application/json" \
    -d "${rank_request}"
)"

echo ""
echo "LLM ranking result:"
python3 - "${rank_response}" "${metrics_json}" <<'PY'
import json
import sys

ranked = json.loads(sys.argv[1])
meta = json.loads(sys.argv[2])
sources = {
    item.get("pair"): item.get("data_source", "?")
    for item in meta.get("candidates", [])
}

print(f"source={ranked.get('source')} selected={', '.join(ranked.get('selected_pairs', [])) or 'none'}")
print(f"reason={ranked.get('reason', 'n/a')}")
print(
    "skill sources: market_rank={market} trading_signal={signal}".format(
        market=ranked.get("market_rank_source") or "n/a",
        signal=ranked.get("trading_signal_source") or "n/a",
    )
)
if ranked.get("market_rank_errors"):
    print(f"market_rank_errors={','.join([str(x) for x in ranked.get('market_rank_errors', [])])}")
if ranked.get("trading_signal_errors"):
    print(f"trading_signal_errors={','.join([str(x) for x in ranked.get('trading_signal_errors', [])])}")
print("pair        src      final  det  conf  sig(side/score)   risk    regime          note")
for item in ranked.get("decisions", []):
    pair_name = item.get("pair", "")
    pair = f"{pair_name:<10}"
    src = f"{sources.get(pair_name, '?'):<7}"
    final = f"{float(item.get('final_score', 0.0)):>5.2f}"
    det = f"{float(item.get('deterministic_score', 0.0)):>4.2f}"
    conf = f"{float(item.get('confidence', 0.0)):>4.2f}"
    sig_side = str(item.get("trading_signal_side", "neutral"))[:7]
    sig_score = float(item.get("trading_signal_score", 0.0) or 0.0)
    sig = f"{sig_side}/{sig_score:.2f}"
    sig = f"{sig:<16}"
    risk = f"{str(item.get('risk_level', '')):<7}"
    regime = f"{str(item.get('regime', '')):<15}"
    note = str(item.get("note", ""))[:60]
    print(f"{pair}  {src}  {final}  {det}  {conf}  {sig} {risk} {regime} {note}")

if meta.get("discovery_notes"):
    print("")
    print("Discovery notes:")
    for note in meta["discovery_notes"][:20]:
        print(f"- {note}")

if meta.get("whitelist_missing"):
    print("")
    print("Not currently in pair_whitelist:")
    for pair in meta["whitelist_missing"]:
        print(f"- {pair}")
PY

selected_pairs="$(
  python3 - "${rank_response}" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
selected = [str(x).upper() for x in payload.get("selected_pairs", []) if str(x).strip()]
print(" ".join(selected))
PY
)"

selected_pairs_before_force="${selected_pairs}"
selected_pairs="$(
  python3 - "${selected_pairs}" "${SMART_MONEY_BIAS_CANDIDATES}" "${TOP_N}" "${USE_SMART_MONEY_BIAS}" "${SMART_MONEY_FORCE_SLOT}" <<'PY'
import sys


def parse_pairs(raw: str):
    seen = set()
    out = []
    for part in str(raw or "").replace(",", " ").split():
        pair = part.strip().upper()
        if not pair or pair in seen:
            continue
        seen.add(pair)
        out.append(pair)
    return out


selected = parse_pairs(sys.argv[1])
smart = parse_pairs(sys.argv[2])
top_n = int(float(sys.argv[3]))
use_smart = str(sys.argv[4]).strip().lower() in {"1", "true", "yes", "on"}
force_slot = str(sys.argv[5]).strip().lower() in {"1", "true", "yes", "on"}

if use_smart and force_slot and top_n > 0 and smart:
    smart_set = set(smart)
    if not any(pair in smart_set for pair in selected):
        pick = smart[0]
        if pick not in selected:
            if len(selected) >= top_n:
                selected = selected[: max(0, top_n - 1)] + [pick]
            else:
                selected.append(pick)

if len(selected) > top_n:
    selected = selected[:top_n]

print(" ".join(selected))
PY
)"

if [[ "${selected_pairs}" != "${selected_pairs_before_force}" ]]; then
  echo "Applied smart-money slot enforcement -> selected=${selected_pairs}"
fi

python3 - "${metrics_json}" "${rank_response}" "${LOG_PATH}" "${TOP_N}" "${MIN_CONFIDENCE}" "${ALLOWED_RISK}" "${ALLOWED_REGIMES}" "${DATA_SOURCE}" "${AUTO_DISCOVER}" "${APPLY}" "${RESTART}" "${SYNC_WHITELIST}" "${MODE}" "${selected_pairs}" <<'PY'
import json
import sys
from datetime import datetime, timezone

meta = json.loads(sys.argv[1])
ranked = json.loads(sys.argv[2])
log_path = sys.argv[3]
top_n = int(sys.argv[4])
min_conf = float(sys.argv[5])
allowed_risk = [x.strip().lower() for x in sys.argv[6].replace(",", " ").split() if x.strip()]
allowed_regimes = [x.strip().lower() for x in sys.argv[7].replace(",", " ").split() if x.strip()]
data_source = sys.argv[8]
auto_discover = str(sys.argv[9]).lower() in {"1", "true", "yes", "on"}
apply_mode = str(sys.argv[10]).lower() in {"1", "true", "yes", "on"}
restart_mode = str(sys.argv[11]).lower() in {"1", "true", "yes", "on"}
sync_whitelist = str(sys.argv[12]).lower() in {"1", "true", "yes", "on"}
mode = sys.argv[13]
selected_pairs_override = [x.strip().upper() for x in str(sys.argv[14]).split() if x.strip()]

sources = {
    item.get("pair"): item.get("data_source", "?")
    for item in meta.get("candidates", [])
}
decisions = []
for item in ranked.get("decisions", []):
    pair = str(item.get("pair", ""))
    decisions.append(
        {
            "pair": pair,
            "data_source": sources.get(pair, "?"),
            "regime": item.get("regime"),
            "risk_level": item.get("risk_level"),
            "confidence": item.get("confidence"),
            "deterministic_score": item.get("deterministic_score"),
            "market_rank_score": item.get("market_rank_score"),
            "trading_signal_side": item.get("trading_signal_side"),
            "trading_signal_score": item.get("trading_signal_score"),
            "final_score": item.get("final_score"),
            "note": item.get("note"),
        }
    )

entry = {
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "event": "rotation_decision",
    "source": ranked.get("source"),
    "reason": ranked.get("reason"),
    "selected_pairs": selected_pairs_override or ranked.get("selected_pairs", []),
    "market_rank_source": ranked.get("market_rank_source"),
    "market_rank_errors": ranked.get("market_rank_errors", []),
    "trading_signal_source": ranked.get("trading_signal_source"),
    "trading_signal_errors": ranked.get("trading_signal_errors", []),
    "top_n": top_n,
    "min_confidence": min_conf,
    "allowed_risk_levels": allowed_risk,
    "allowed_regimes": allowed_regimes,
    "data_source": data_source,
    "auto_discover": auto_discover,
    "apply": apply_mode,
    "restart": restart_mode,
    "sync_whitelist": sync_whitelist,
    "strategy_mode": mode,
    "discovery_notes": meta.get("discovery_notes", []),
    "whitelist_missing": meta.get("whitelist_missing", []),
    "skipped": meta.get("skipped", []),
    "decisions": decisions,
}

with open(log_path, "a", encoding="utf-8") as f:
    f.write(json.dumps(entry, separators=(",", ":")) + "\n")
PY
echo "Rotation log appended: ${LOG_PATH}"

if [[ -z "${selected_pairs}" ]]; then
  echo ""
  echo "No pair passed your filters. Keeping current RISK_PAIRS unchanged."
  exit 0
fi

echo ""
echo "Selected risk pairs: ${selected_pairs}"

risk_changed="$(
  python3 - "${current_risk_pairs}" "${selected_pairs}" <<'PY'
import sys

def norm(raw: str):
    parts = [p.strip().upper() for p in raw.replace(",", " ").split() if p.strip()]
    seen = set()
    ordered = []
    for part in parts:
        if part in seen:
            continue
        seen.add(part)
        ordered.append(part)
    return ordered

old = norm(sys.argv[1])
new = norm(sys.argv[2])
print("true" if old != new else "false")
PY
)"

if [[ "${APPLY}" != "true" ]]; then
  echo "Preview only. Re-run with --apply to write .env."
  exit 0
fi

if [[ "${risk_changed}" == "true" ]]; then
  python3 - "${ENV_FILE}" "RISK_PAIRS" "${selected_pairs}" <<'PY'
import sys
from pathlib import Path

env_path = Path(sys.argv[1])
key = sys.argv[2]
value = sys.argv[3]

lines = []
found = False
if env_path.exists():
    lines = env_path.read_text().splitlines()

for idx, raw in enumerate(lines):
    stripped = raw.strip()
    if stripped.startswith("#") or "=" not in raw:
        continue
    k, _ = raw.split("=", 1)
    if k.strip() == key:
        lines[idx] = f"{key}={value}"
        found = True
        break

if not found:
    if lines and lines[-1].strip():
        lines.append("")
    lines.append(f"{key}={value}")

env_path.write_text("\n".join(lines) + "\n")
PY
  echo "Updated .env -> RISK_PAIRS=${selected_pairs}"
else
  echo "RISK_PAIRS unchanged -> ${selected_pairs}"
fi

if is_true "${SYNC_WHITELIST}"; then
  python3 - "${CONFIG_FILE}" "${core_pairs}" "${selected_pairs}" <<'PY'
import json
import sys
from pathlib import Path

config_path = Path(sys.argv[1])
core_pairs = [p.strip().upper() for p in sys.argv[2].replace(",", " ").split() if p.strip()]
risk_pairs = [p.strip().upper() for p in sys.argv[3].replace(",", " ").split() if p.strip()]

if not config_path.exists():
    print(f"Config not found: {config_path}", file=sys.stderr)
    raise SystemExit(1)

cfg = json.loads(config_path.read_text())
exchange = cfg.setdefault("exchange", {})
whitelist = exchange.get("pair_whitelist") or []

ordered = []
seen = set()
for pair in whitelist + core_pairs + risk_pairs:
    up = str(pair).upper()
    if up in seen:
        continue
    seen.add(up)
    ordered.append(up)

exchange["pair_whitelist"] = ordered
config_path.write_text(json.dumps(cfg, indent=2) + "\n")
print("Synced config pair_whitelist with selected/core pairs.")
PY
fi

if [[ "${RESTART}" == "true" ]]; then
  if [[ "${risk_changed}" == "true" ]]; then
    echo "Restarting freqtrade with mode=${MODE}..."
    if docker restart freqtrade >/dev/null 2>&1; then
      echo "freqtrade container restarted."
    else
      STRATEGY_MODE="${MODE}" docker compose up -d freqtrade
    fi
  else
    echo "Skipping freqtrade restart (selected pairs unchanged)."
  fi
else
  echo "Tip: restart freqtrade to apply updated pair configuration."
fi
