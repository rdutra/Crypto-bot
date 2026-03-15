#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"
CONFIG_FILE="${ROOT_DIR}/freqtrade/user_data/config.json"

BOT_API_URL="${LLM_BOT_API_URL:-http://localhost:8000}"
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

mkdir -p "$(dirname "${LOG_PATH}")"

if ! is_true "${AUTO_DISCOVER}" && [[ -z "${CANDIDATES}" ]]; then
  echo "No candidates provided and auto-discovery disabled." >&2
  echo "Use --auto-discover or set --candidates / LLM_ROTATE_CANDIDATES." >&2
  exit 1
fi

docker compose up -d ollama bot-api >/dev/null

echo "Preparing candidate metrics..."

metrics_json="$(
  docker compose run --rm --no-deps \
    -e ROTATE_CANDIDATES="${CANDIDATES}" \
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
    --entrypoint /bin/sh freqtrade \
    -lc 'python - <<'"'"'PY'"'"'
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
    bars = exchange.fetch_ohlcv(pair, timeframe=timeframe, limit=limit)
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
    markets = exchange.load_markets()
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
    candidates = manual_candidates
    discovery_notes.append("source=manual")
elif auto_discover:
    discovered, notes = discover_candidates(
        exchange=exchange,
        core_pairs=core_pairs,
        quote_asset=quote_asset,
        max_candidates=max_candidates,
        min_quote_volume=min_quote_volume,
        exclude_regex=exclude_regex,
    )
    candidates = discovered
    discovery_notes.append("source=exchange_discovery")
    for note in notes[:20]:
        discovery_notes.append(note)
else:
    fallback = sorted(pair_whitelist) if pair_whitelist else []
    candidates = [p for p in fallback if p not in core_pairs][:max_candidates]
    discovery_notes.append("source=whitelist_fallback")

# Deduplicate while preserving order.
seen = set()
ordered_candidates = []
for pair in candidates:
    if pair in seen:
        continue
    seen.add(pair)
    ordered_candidates.append(pair)

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
print("pair        src      final  det  conf  risk    regime          note")
for item in ranked.get("decisions", []):
    pair_name = item.get("pair", "")
    pair = f"{pair_name:<10}"
    src = f"{sources.get(pair_name, '?'):<7}"
    final = f"{float(item.get('final_score', 0.0)):>5.2f}"
    det = f"{float(item.get('deterministic_score', 0.0)):>4.2f}"
    conf = f"{float(item.get('confidence', 0.0)):>4.2f}"
    risk = f"{str(item.get('risk_level', '')):<7}"
    regime = f"{str(item.get('regime', '')):<15}"
    note = str(item.get("note", ""))[:60]
    print(f"{pair}  {src}  {final}  {det}  {conf}  {risk} {regime} {note}")

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

python3 - "${metrics_json}" "${rank_response}" "${LOG_PATH}" "${TOP_N}" "${MIN_CONFIDENCE}" "${ALLOWED_RISK}" "${ALLOWED_REGIMES}" "${DATA_SOURCE}" "${AUTO_DISCOVER}" "${APPLY}" "${RESTART}" "${SYNC_WHITELIST}" "${MODE}" <<'PY'
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
            "final_score": item.get("final_score"),
            "note": item.get("note"),
        }
    )

entry = {
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "event": "rotation_decision",
    "source": ranked.get("source"),
    "reason": ranked.get("reason"),
    "selected_pairs": ranked.get("selected_pairs", []),
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

if [[ "${APPLY}" != "true" ]]; then
  echo "Preview only. Re-run with --apply to write .env."
  exit 0
fi

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
  echo "Restarting freqtrade with mode=${MODE}..."
  STRATEGY_MODE="${MODE}" docker compose up -d freqtrade
else
  echo "Tip: restart freqtrade to apply updated pair configuration."
fi
