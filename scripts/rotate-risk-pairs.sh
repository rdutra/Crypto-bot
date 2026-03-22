#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"
CONFIG_FILE="${ROOT_DIR}/freqtrade/user_data/config.json"
SHELL_HELPER="${ROOT_DIR}/scripts/shell_helpers.py"
ROTATE_HELPER="${ROOT_DIR}/scripts/rotate_risk_pairs.py"

BOT_API_URL="${LLM_BOT_API_URL:-http://localhost:8000}"
BINANCE_REST_BASE_URL="${BINANCE_REST_BASE:-https://api.binance.com}"
TIMEFRAME="${LLM_ROTATE_TIMEFRAME:-1h}"
LOOKBACK_CANDLES="${LLM_ROTATE_LOOKBACK_CANDLES:-240}"
TOP_N="${LLM_ROTATE_TOP_N:-3}"
MIN_CONFIDENCE="${LLM_ROTATE_MIN_CONFIDENCE:-0.60}"
ALLOWED_RISK="${LLM_ROTATE_ALLOWED_RISK:-}"
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
SPIKE_DB_TARGET="${LLM_ROTATE_SPIKE_DB_URL:-${LLM_ROTATE_SPIKE_DB_PATH:-}}"
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
SOURCE_DIVERSITY_ENABLED="${LLM_ROTATE_SOURCE_DIVERSITY_ENABLED:-}"
MIN_BINANCE_SKILL_PAIRS="${LLM_ROTATE_MIN_BINANCE_SKILL_PAIRS:-}"
MIN_ALGO_PAIRS="${LLM_ROTATE_MIN_ALGO_PAIRS:-}"
MIN_SPIKE_PAIRS="${LLM_ROTATE_MIN_SPIKE_PAIRS:-}"
RESERVE_SPIKE_SLOT="${LLM_ROTATE_RESERVE_SPIKE_SLOT:-}"
RESERVE_SPIKE_MIN_CONFIDENCE="${LLM_ROTATE_RESERVE_SPIKE_MIN_CONFIDENCE:-}"
USE_TOKEN_INFO_PREFILTER="${LLM_ROTATE_USE_TOKEN_INFO_PREFILTER:-}"
TOKEN_INFO_MIN_LIQUIDITY_USD="${LLM_ROTATE_TOKEN_INFO_MIN_LIQUIDITY_USD:-}"
TOKEN_INFO_MIN_HOLDERS="${LLM_ROTATE_TOKEN_INFO_MIN_HOLDERS:-}"
TOKEN_INFO_MAX_TOP10_SHARE="${LLM_ROTATE_TOKEN_INFO_MAX_TOP10_SHARE:-}"
TOKEN_INFO_REQUIRE_BINANCE_SPOT_TRADABLE="${LLM_ROTATE_TOKEN_INFO_REQUIRE_BINANCE_SPOT_TRADABLE:-}"
TOKEN_INFO_FAIL_OPEN="${LLM_ROTATE_TOKEN_INFO_FAIL_OPEN:-}"
USE_TOKEN_AUDIT_PREFILTER="${LLM_ROTATE_USE_TOKEN_AUDIT_PREFILTER:-}"
TOKEN_AUDIT_BLOCK_LEVELS="${LLM_ROTATE_TOKEN_AUDIT_BLOCK_LEVELS:-}"
TOKEN_AUDIT_FAIL_OPEN="${LLM_ROTATE_TOKEN_AUDIT_FAIL_OPEN:-}"
EXCLUDED_BASES="${LLM_ROTATE_EXCLUDED_BASES:-}"
EXCLUDED_PAIRS="${LLM_ROTATE_EXCLUDED_PAIRS:-}"
MIN_ATR_PCT="${LLM_ROTATE_MIN_ATR_PCT:-}"
MIN_ATR_PCT_AGGRESSIVE="${LLM_ROTATE_MIN_ATR_PCT_AGGRESSIVE:-}"
ACTIVE_MIN_ATR_PCT=""
ROTATION_OUTCOME_DB_PATH="${LLM_ROTATE_OUTCOME_DB_URL:-${LLM_ROTATE_OUTCOME_DB_PATH:-}}"
ROTATION_OUTCOME_HORIZON_MINUTES="${LLM_ROTATE_OUTCOME_HORIZON_MINUTES:-}"
ROTATION_OUTCOME_SUCCESS_PCT="${LLM_ROTATE_OUTCOME_SUCCESS_PCT:-}"
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
  - Optional token-info/token-audit prefilters can reject candidates before /rank-pairs.
  - With --apply and sync enabled, selected pairs are auto-added to pair_whitelist.
  - If local data is missing and --data-source=auto, exchange OHLCV is used.
  - Optional: set LLM_ROTATE_USE_SPIKE_BIAS=true to bias candidates with recent scanner winners.
  - Optional: set LLM_ROTATE_USE_SMART_MONEY_BIAS=true to prepend Binance-spot tradable smart-money pairs.
  - Optional: set LLM_ROTATE_SMART_MONEY_FORCE_SLOT=true to guarantee at least one selected smart-money pair.
  - Optional: set LLM_ROTATE_SOURCE_DIVERSITY_ENABLED=true to reserve selected slots for Binance-skill, algo, and spike sources.
  - Optional: set LLM_ROTATE_MIN_ATR_PCT / LLM_ROTATE_MIN_ATR_PCT_AGGRESSIVE to reject low-volatility pairs before ranking.
  - Optional: set LLM_ROTATE_EXCLUDED_PAIRS to hard-block specific symbols before ranking.
  - Optional: set LLM_ROTATE_OUTCOME_DB_PATH to track ranked-pair outcomes independently from executed trades.
EOF
}

get_env_file_value() {
  local key="$1"
  python3 "${SHELL_HELPER}" env-value "${ENV_FILE}" "${key}"
}

is_true() {
  case "$(printf '%s' "${1}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

spike_bias_candidates() {
  local db_target="$1"
  local quote_asset="$2"
  local lookback_hours="$3"
  local top_n="$4"
  local min_score="$5"
  local require_llm_allowed="$6"
  local args=(
    "${ROTATE_HELPER}" spike-bias-candidates
    --db-target "${db_target}"
    --quote-asset "${quote_asset}"
    --lookback-hours "${lookback_hours}"
    --top-n "${top_n}"
    --min-score "${min_score}"
  )
  if is_true "${require_llm_allowed}"; then
    args+=(--require-llm-allowed)
  fi
  python3 "${args[@]}"
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
  local args=(
    "${ROTATE_HELPER}" smart-money-bias-candidates
    --bot-api-url "${bot_api_url}"
    --binance-rest-base "${binance_rest_base}"
    --quote-asset "${quote_asset}"
    --top-n "${top_n}"
    --min-score "${min_score}"
    --exclude-regex "${exclude_regex}"
  )
  if is_true "${require_buy}"; then
    args+=(--require-buy)
  fi
  if is_true "${force_refresh}"; then
    args+=(--force-refresh)
  fi
  python3 "${args[@]}"
}

apply_skill_prefilters() {
  local bot_api_url="$1"
  local metrics_json="$2"
  local use_token_info="$3"
  local min_liquidity_usd="$4"
  local min_holders="$5"
  local max_top10_share="$6"
  local require_spot_tradable="$7"
  local token_info_fail_open="$8"
  local use_token_audit="$9"
  local token_audit_block_levels="${10}"
  local token_audit_fail_open="${11}"
  local args=(
    "${ROTATE_HELPER}" apply-skill-prefilters
    --bot-api-url "${bot_api_url}"
    --metrics-json "${metrics_json}"
    --min-liquidity-usd "${min_liquidity_usd}"
    --min-holders "${min_holders}"
    --max-top10-share "${max_top10_share}"
    --token-audit-block-levels "${token_audit_block_levels}"
  )
  if is_true "${use_token_info}"; then
    args+=(--use-token-info)
  fi
  if is_true "${require_spot_tradable}"; then
    args+=(--require-spot-tradable)
  fi
  if is_true "${token_info_fail_open}"; then
    args+=(--token-info-fail-open)
  fi
  if is_true "${use_token_audit}"; then
    args+=(--use-token-audit)
  fi
  if is_true "${token_audit_fail_open}"; then
    args+=(--token-audit-fail-open)
  fi
  python3 "${args[@]}"
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

core_pairs="${CORE_PAIRS:-}"
if [[ -z "${core_pairs}" ]]; then
  core_pairs="$(get_env_file_value CORE_PAIRS || true)"
fi
core_pairs="${core_pairs:-BTC/USDT ETH/USDT BNB/USDT}"

freqtrade_strategy="${FREQTRADE_STRATEGY:-}"
if [[ -z "${freqtrade_strategy}" ]]; then
  freqtrade_strategy="$(get_env_file_value FREQTRADE_STRATEGY || true)"
fi
freqtrade_strategy="${freqtrade_strategy:-LlmTrendPullbackStrategy}"

if [[ -z "${ALLOWED_REGIMES}" ]]; then
  if [[ "${freqtrade_strategy}" == "LlmRotationAlignedStrategy" || "${MODE}" == "aggressive" ]]; then
    ALLOWED_REGIMES="trend_pullback breakout mean_reversion"
  else
    ALLOWED_REGIMES="trend_pullback"
  fi
fi

if [[ -z "${ALLOWED_RISK}" ]]; then
  if [[ "${freqtrade_strategy}" == "LlmRotationAlignedStrategy" && "${MODE}" == "aggressive" ]]; then
    ALLOWED_RISK="low medium high"
  else
    ALLOWED_RISK="low medium"
  fi
fi

DATA_SOURCE="$(printf '%s' "${DATA_SOURCE}" | tr '[:upper:]' '[:lower:]')"
if [[ "${DATA_SOURCE}" != "local" && "${DATA_SOURCE}" != "exchange" && "${DATA_SOURCE}" != "auto" ]]; then
  echo "--data-source must be one of: local, exchange, auto." >&2
  exit 1
fi

current_risk_pairs="${RISK_PAIRS:-}"
if [[ -z "${current_risk_pairs}" ]]; then
  current_risk_pairs="$(get_env_file_value RISK_PAIRS || true)"
fi

current_spike_pairs="${SPIKE_PAIRS:-}"
if [[ -z "${current_spike_pairs}" ]]; then
  current_spike_pairs="$(get_env_file_value SPIKE_PAIRS || true)"
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
if [[ -z "${SPIKE_DB_TARGET}" ]]; then
  SPIKE_DB_TARGET="$(get_env_file_value LLM_ROTATE_SPIKE_DB_URL || true)"
fi
if [[ -z "${SPIKE_DB_TARGET}" ]]; then
  SPIKE_DB_TARGET="$(get_env_file_value LLM_ROTATE_SPIKE_DB_PATH || true)"
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
if [[ -z "${SOURCE_DIVERSITY_ENABLED}" ]]; then
  SOURCE_DIVERSITY_ENABLED="$(get_env_file_value LLM_ROTATE_SOURCE_DIVERSITY_ENABLED || true)"
fi
if [[ -z "${MIN_BINANCE_SKILL_PAIRS}" ]]; then
  MIN_BINANCE_SKILL_PAIRS="$(get_env_file_value LLM_ROTATE_MIN_BINANCE_SKILL_PAIRS || true)"
fi
if [[ -z "${MIN_ALGO_PAIRS}" ]]; then
  MIN_ALGO_PAIRS="$(get_env_file_value LLM_ROTATE_MIN_ALGO_PAIRS || true)"
fi
if [[ -z "${MIN_SPIKE_PAIRS}" ]]; then
  MIN_SPIKE_PAIRS="$(get_env_file_value LLM_ROTATE_MIN_SPIKE_PAIRS || true)"
fi
if [[ -z "${RESERVE_SPIKE_SLOT}" ]]; then
  RESERVE_SPIKE_SLOT="$(get_env_file_value LLM_ROTATE_RESERVE_SPIKE_SLOT || true)"
fi
if [[ -z "${RESERVE_SPIKE_MIN_CONFIDENCE}" ]]; then
  RESERVE_SPIKE_MIN_CONFIDENCE="$(get_env_file_value LLM_ROTATE_RESERVE_SPIKE_MIN_CONFIDENCE || true)"
fi
if [[ -z "${USE_TOKEN_INFO_PREFILTER}" ]]; then
  USE_TOKEN_INFO_PREFILTER="$(get_env_file_value LLM_ROTATE_USE_TOKEN_INFO_PREFILTER || true)"
fi
if [[ -z "${TOKEN_INFO_MIN_LIQUIDITY_USD}" ]]; then
  TOKEN_INFO_MIN_LIQUIDITY_USD="$(get_env_file_value LLM_ROTATE_TOKEN_INFO_MIN_LIQUIDITY_USD || true)"
fi
if [[ -z "${TOKEN_INFO_MIN_HOLDERS}" ]]; then
  TOKEN_INFO_MIN_HOLDERS="$(get_env_file_value LLM_ROTATE_TOKEN_INFO_MIN_HOLDERS || true)"
fi
if [[ -z "${TOKEN_INFO_MAX_TOP10_SHARE}" ]]; then
  TOKEN_INFO_MAX_TOP10_SHARE="$(get_env_file_value LLM_ROTATE_TOKEN_INFO_MAX_TOP10_SHARE || true)"
fi
if [[ -z "${TOKEN_INFO_REQUIRE_BINANCE_SPOT_TRADABLE}" ]]; then
  TOKEN_INFO_REQUIRE_BINANCE_SPOT_TRADABLE="$(get_env_file_value LLM_ROTATE_TOKEN_INFO_REQUIRE_BINANCE_SPOT_TRADABLE || true)"
fi
if [[ -z "${TOKEN_INFO_FAIL_OPEN}" ]]; then
  TOKEN_INFO_FAIL_OPEN="$(get_env_file_value LLM_ROTATE_TOKEN_INFO_FAIL_OPEN || true)"
fi
if [[ -z "${USE_TOKEN_AUDIT_PREFILTER}" ]]; then
  USE_TOKEN_AUDIT_PREFILTER="$(get_env_file_value LLM_ROTATE_USE_TOKEN_AUDIT_PREFILTER || true)"
fi
if [[ -z "${TOKEN_AUDIT_BLOCK_LEVELS}" ]]; then
  TOKEN_AUDIT_BLOCK_LEVELS="$(get_env_file_value LLM_ROTATE_TOKEN_AUDIT_BLOCK_LEVELS || true)"
fi
if [[ -z "${TOKEN_AUDIT_FAIL_OPEN}" ]]; then
  TOKEN_AUDIT_FAIL_OPEN="$(get_env_file_value LLM_ROTATE_TOKEN_AUDIT_FAIL_OPEN || true)"
fi
if [[ -z "${EXCLUDED_BASES}" ]]; then
  EXCLUDED_BASES="$(get_env_file_value LLM_ROTATE_EXCLUDED_BASES || true)"
fi
if [[ -z "${MIN_ATR_PCT}" ]]; then
  MIN_ATR_PCT="$(get_env_file_value LLM_ROTATE_MIN_ATR_PCT || true)"
fi
if [[ -z "${MIN_ATR_PCT_AGGRESSIVE}" ]]; then
  MIN_ATR_PCT_AGGRESSIVE="$(get_env_file_value LLM_ROTATE_MIN_ATR_PCT_AGGRESSIVE || true)"
fi
if [[ -z "${ROTATION_OUTCOME_DB_PATH}" ]]; then
  ROTATION_OUTCOME_DB_PATH="$(get_env_file_value LLM_ROTATE_OUTCOME_DB_URL || true)"
fi
if [[ -z "${ROTATION_OUTCOME_DB_PATH}" ]]; then
  ROTATION_OUTCOME_DB_PATH="$(get_env_file_value LLM_ROTATE_OUTCOME_DB_PATH || true)"
fi
if [[ -z "${ROTATION_OUTCOME_HORIZON_MINUTES}" ]]; then
  ROTATION_OUTCOME_HORIZON_MINUTES="$(get_env_file_value LLM_ROTATE_OUTCOME_HORIZON_MINUTES || true)"
fi
if [[ -z "${ROTATION_OUTCOME_SUCCESS_PCT}" ]]; then
  ROTATION_OUTCOME_SUCCESS_PCT="$(get_env_file_value LLM_ROTATE_OUTCOME_SUCCESS_PCT || true)"
fi
USE_SPIKE_BIAS="${USE_SPIKE_BIAS:-false}"
SPIKE_DB_TARGET="${SPIKE_DB_TARGET:-${ROOT_DIR}/freqtrade/user_data/logs/spike-scanner.sqlite}"
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
SOURCE_DIVERSITY_ENABLED="${SOURCE_DIVERSITY_ENABLED:-true}"
MIN_BINANCE_SKILL_PAIRS="${MIN_BINANCE_SKILL_PAIRS:-2}"
MIN_ALGO_PAIRS="${MIN_ALGO_PAIRS:-2}"
MIN_SPIKE_PAIRS="${MIN_SPIKE_PAIRS:-1}"
RESERVE_SPIKE_SLOT="${RESERVE_SPIKE_SLOT:-false}"
RESERVE_SPIKE_MIN_CONFIDENCE="${RESERVE_SPIKE_MIN_CONFIDENCE:-0.80}"
USE_TOKEN_INFO_PREFILTER="${USE_TOKEN_INFO_PREFILTER:-true}"
TOKEN_INFO_MIN_LIQUIDITY_USD="${TOKEN_INFO_MIN_LIQUIDITY_USD:-1000000}"
TOKEN_INFO_MIN_HOLDERS="${TOKEN_INFO_MIN_HOLDERS:-1000}"
TOKEN_INFO_MAX_TOP10_SHARE="${TOKEN_INFO_MAX_TOP10_SHARE:-0.90}"
TOKEN_INFO_REQUIRE_BINANCE_SPOT_TRADABLE="${TOKEN_INFO_REQUIRE_BINANCE_SPOT_TRADABLE:-true}"
TOKEN_INFO_FAIL_OPEN="${TOKEN_INFO_FAIL_OPEN:-true}"
USE_TOKEN_AUDIT_PREFILTER="${USE_TOKEN_AUDIT_PREFILTER:-true}"
TOKEN_AUDIT_BLOCK_LEVELS="${TOKEN_AUDIT_BLOCK_LEVELS:-avoid}"
TOKEN_AUDIT_FAIL_OPEN="${TOKEN_AUDIT_FAIL_OPEN:-true}"
EXCLUDED_BASES="${EXCLUDED_BASES:-USDC USDT FDUSD TUSD USDP BUSD DAI EUR USD1}"
EXCLUDED_PAIRS="${EXCLUDED_PAIRS:-}"
MIN_ATR_PCT="${MIN_ATR_PCT:-0}"
MIN_ATR_PCT_AGGRESSIVE="${MIN_ATR_PCT_AGGRESSIVE:-0.35}"
ROTATION_OUTCOME_DB_PATH="${ROTATION_OUTCOME_DB_PATH:-${ROOT_DIR}/freqtrade/user_data/logs/rotation-outcomes.sqlite}"
ROTATION_OUTCOME_HORIZON_MINUTES="${ROTATION_OUTCOME_HORIZON_MINUTES:-60}"
ROTATION_OUTCOME_SUCCESS_PCT="${ROTATION_OUTCOME_SUCCESS_PCT:-1.0}"
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
if ! [[ "${MIN_BINANCE_SKILL_PAIRS}" =~ ^[0-9]+$ ]]; then
  echo "LLM_ROTATE_MIN_BINANCE_SKILL_PAIRS must be an integer." >&2
  exit 1
fi
if ! [[ "${MIN_ALGO_PAIRS}" =~ ^[0-9]+$ ]]; then
  echo "LLM_ROTATE_MIN_ALGO_PAIRS must be an integer." >&2
  exit 1
fi
if ! [[ "${MIN_SPIKE_PAIRS}" =~ ^[0-9]+$ ]]; then
  echo "LLM_ROTATE_MIN_SPIKE_PAIRS must be an integer." >&2
  exit 1
fi
if ! [[ "${RESERVE_SPIKE_MIN_CONFIDENCE}" =~ ^([0-9]+([.][0-9]+)?|[.][0-9]+)$ ]]; then
  echo "LLM_ROTATE_RESERVE_SPIKE_MIN_CONFIDENCE must be a number." >&2
  exit 1
fi
if ! [[ "${TOKEN_INFO_MIN_LIQUIDITY_USD}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "LLM_ROTATE_TOKEN_INFO_MIN_LIQUIDITY_USD must be numeric." >&2
  exit 1
fi
if ! [[ "${TOKEN_INFO_MIN_HOLDERS}" =~ ^[0-9]+$ ]]; then
  echo "LLM_ROTATE_TOKEN_INFO_MIN_HOLDERS must be an integer." >&2
  exit 1
fi
if ! [[ "${TOKEN_INFO_MAX_TOP10_SHARE}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "LLM_ROTATE_TOKEN_INFO_MAX_TOP10_SHARE must be numeric." >&2
  exit 1
fi
if ! [[ "${MIN_ATR_PCT}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "LLM_ROTATE_MIN_ATR_PCT must be numeric." >&2
  exit 1
fi
if ! [[ "${MIN_ATR_PCT_AGGRESSIVE}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "LLM_ROTATE_MIN_ATR_PCT_AGGRESSIVE must be numeric." >&2
  exit 1
fi
if ! [[ "${ROTATION_OUTCOME_HORIZON_MINUTES}" =~ ^[0-9]+$ ]]; then
  echo "LLM_ROTATE_OUTCOME_HORIZON_MINUTES must be an integer." >&2
  exit 1
fi
if ! [[ "${ROTATION_OUTCOME_SUCCESS_PCT}" =~ ^-?[0-9]+([.][0-9]+)?$ ]]; then
  echo "LLM_ROTATE_OUTCOME_SUCCESS_PCT must be numeric." >&2
  exit 1
fi
if [[ "${MODE}" == "aggressive" ]]; then
  ACTIVE_MIN_ATR_PCT="${MIN_ATR_PCT_AGGRESSIVE}"
else
  ACTIVE_MIN_ATR_PCT="${MIN_ATR_PCT}"
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
if [[ "${SPIKE_DB_TARGET}" != *"://"* && "${SPIKE_DB_TARGET}" != /* ]]; then
  SPIKE_DB_TARGET="${ROOT_DIR}/${SPIKE_DB_TARGET#./}"
fi
if [[ "${ROTATION_OUTCOME_DB_PATH}" != *"://"* && "${ROTATION_OUTCOME_DB_PATH}" != /* ]]; then
  ROTATION_OUTCOME_DB_PATH="${ROOT_DIR}/${ROTATION_OUTCOME_DB_PATH#./}"
fi

mkdir -p "$(dirname "${LOG_PATH}")"

SPIKE_BIAS_CANDIDATES=""
if is_true "${USE_SPIKE_BIAS}"; then
  SPIKE_BIAS_CANDIDATES="$(
    spike_bias_candidates \
      "${SPIKE_DB_TARGET}" \
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
  read -r -a llm_services <<<"$(./scripts/llm-runtime.sh services)"
  docker compose up -d "${llm_services[@]}" >/dev/null
fi

echo "Preparing candidate metrics..."

if ! docker ps --format '{{.Names}}' | grep -qx 'freqtrade'; then
  echo "freqtrade container is not running. Start it before rotating pairs." >&2
  exit 1
fi

metrics_json="$(
  prepare_cmd=(
    docker exec -i
    -e ROTATE_CANDIDATES="${CANDIDATES}" \
    -e ROTATE_SPIKE_CANDIDATES="${SPIKE_BIAS_CANDIDATES}"
    -e ROTATE_SMART_MONEY_CANDIDATES="${SMART_MONEY_BIAS_CANDIDATES}"
    -e ROTATE_AUTO_DISCOVER="${AUTO_DISCOVER}"
    -e ROTATE_DATA_SOURCE="${DATA_SOURCE}"
    -e ROTATE_EXCHANGE="${EXCHANGE_ID}"
    -e ROTATE_QUOTE="${QUOTE_ASSET}"
    -e ROTATE_MAX_CANDIDATES="${MAX_CANDIDATES}"
    -e ROTATE_MIN_QUOTE_VOLUME="${MIN_QUOTE_VOLUME}"
    -e ROTATE_EXCLUDE_REGEX="${EXCLUDE_REGEX}"
    -e ROTATE_WHITELIST_ONLY="${WHITELIST_ONLY}"
    -e ROTATE_CORE_PAIRS="${core_pairs}"
    -e ROTATE_EXCLUDED_BASES="${EXCLUDED_BASES}"
    -e ROTATE_EXCLUDED_PAIRS="${EXCLUDED_PAIRS}"
    -e ROTATE_MIN_ATR_PCT="${ACTIVE_MIN_ATR_PCT}"
    -e ROTATE_TIMEFRAME="${TIMEFRAME}"
    -e ROTATE_LOOKBACK_CANDLES="${LOOKBACK_CANDLES}"
    -e ROTATE_CONFIG_PATH="/freqtrade/user_data/config.json"
    freqtrade
    python
    /freqtrade/scripts/rotate_risk_pairs.py
    prepare-candidates
    --rotate-candidates "${CANDIDATES}"
    --rotate-spike-candidates "${SPIKE_BIAS_CANDIDATES}"
    --rotate-smart-money-candidates "${SMART_MONEY_BIAS_CANDIDATES}"
    --rotate-data-source "${DATA_SOURCE}"
    --rotate-exchange "${EXCHANGE_ID}"
    --rotate-quote "${QUOTE_ASSET}"
    --rotate-max-candidates "${MAX_CANDIDATES}"
    --rotate-min-quote-volume "${MIN_QUOTE_VOLUME}"
    --rotate-exclude-regex "${EXCLUDE_REGEX}"
    --rotate-core-pairs "${core_pairs}"
    --rotate-excluded-bases "${EXCLUDED_BASES}"
    --rotate-excluded-pairs "${EXCLUDED_PAIRS}"
    --rotate-min-atr-pct "${ACTIVE_MIN_ATR_PCT}"
    --rotate-timeframe "${TIMEFRAME}"
    --rotate-lookback-candles "${LOOKBACK_CANDLES}"
    --rotate-config-path /freqtrade/user_data/config.json
  )
  if is_true "${AUTO_DISCOVER}"; then
    prepare_cmd+=(--rotate-auto-discover)
  fi
  if is_true "${WHITELIST_ONLY}"; then
    prepare_cmd+=(--rotate-whitelist-only)
  fi
  "${prepare_cmd[@]}"
)"

metrics_json="$(
  apply_skill_prefilters \
    "${BOT_API_URL}" \
    "${metrics_json}" \
    "${USE_TOKEN_INFO_PREFILTER}" \
    "${TOKEN_INFO_MIN_LIQUIDITY_USD}" \
    "${TOKEN_INFO_MIN_HOLDERS}" \
    "${TOKEN_INFO_MAX_TOP10_SHARE}" \
    "${TOKEN_INFO_REQUIRE_BINANCE_SPOT_TRADABLE}" \
    "${TOKEN_INFO_FAIL_OPEN}" \
    "${USE_TOKEN_AUDIT_PREFILTER}" \
    "${TOKEN_AUDIT_BLOCK_LEVELS}" \
    "${TOKEN_AUDIT_FAIL_OPEN}"
)"

current_prices_json="$(
  python3 "${ROTATE_HELPER}" current-prices-json --metrics-json "${metrics_json}"
)"

python3 "${ROOT_DIR}/scripts/rotation_outcomes.py" \
  resolve \
  --db-path "${ROTATION_OUTCOME_DB_PATH}" \
  --exchange "${EXCHANGE_ID}" \
  --rest-base-url "${BINANCE_REST_BASE_URL}" \
  --success-pct "${ROTATION_OUTCOME_SUCCESS_PCT}" \
  --limit 200 \
  --current-prices-json "${current_prices_json}" >/dev/null || true

candidate_count="$(
  python3 "${ROTATE_HELPER}" candidate-count --metrics-json "${metrics_json}"
)"

if [[ "${candidate_count}" -eq 0 ]]; then
  echo "No eligible candidates found."
  python3 "${ROTATE_HELPER}" print-metrics-summary --metrics-json "${metrics_json}"
  no_candidates_cmd=(
    python3 "${ROTATE_HELPER}" log-no-candidates
    --metrics-json "${metrics_json}"
    --log-path "${LOG_PATH}"
    --top-n "${TOP_N}"
    --min-confidence "${MIN_CONFIDENCE}"
    --allowed-risk "${ALLOWED_RISK}"
    --allowed-regimes "${ALLOWED_REGIMES}"
    --data-source "${DATA_SOURCE}"
  )
  if is_true "${AUTO_DISCOVER}"; then
    no_candidates_cmd+=(--auto-discover)
  fi
  "${no_candidates_cmd[@]}"
  echo "Rotation log appended: ${LOG_PATH}"
  exit 1
fi

rank_request="$(
  python3 "${ROTATE_HELPER}" build-rank-request \
    --metrics-json "${metrics_json}" \
    --top-n "${TOP_N}" \
    --min-confidence "${MIN_CONFIDENCE}" \
    --allowed-risk "${ALLOWED_RISK}" \
    --allowed-regimes "${ALLOWED_REGIMES}"
)"

rank_response="$(
  curl -fsS -X POST "${BOT_API_URL%/}/rank-pairs" \
    -H "Content-Type: application/json" \
    -d "${rank_request}"
)"

echo ""
echo "LLM ranking result:"
python3 "${ROTATE_HELPER}" print-ranking-summary --rank-response "${rank_response}" --metrics-json "${metrics_json}"

selected_pairs="$(
  python3 "${ROTATE_HELPER}" selected-pairs --rank-response "${rank_response}"
)"

selected_pairs_before_force="${selected_pairs}"
selected_pairs="$(
  diversity_cmd=(
    python3 "${ROTATE_HELPER}" enforce-source-diversity
    --rank-response "${rank_response}"
    --metrics-json "${metrics_json}"
    --top-n "${TOP_N}"
    --min-confidence "${MIN_CONFIDENCE}"
    --allowed-risk "${ALLOWED_RISK}"
    --allowed-regimes "${ALLOWED_REGIMES}"
    --min-binance-pairs "${MIN_BINANCE_SKILL_PAIRS}"
    --min-algo-pairs "${MIN_ALGO_PAIRS}"
    --min-spike-pairs "${MIN_SPIKE_PAIRS}"
    --reserve-spike-min-confidence "${RESERVE_SPIKE_MIN_CONFIDENCE}"
  )
  if is_true "${USE_SMART_MONEY_BIAS}"; then
    diversity_cmd+=(--use-smart-money)
  fi
  if is_true "${SMART_MONEY_FORCE_SLOT}"; then
    diversity_cmd+=(--force-smart-money-slot)
  fi
  if is_true "${SOURCE_DIVERSITY_ENABLED}"; then
    diversity_cmd+=(--diversity-enabled)
  fi
  if is_true "${RESERVE_SPIKE_SLOT}"; then
    diversity_cmd+=(--reserve-spike-slot)
  fi
  "${diversity_cmd[@]}"
)"

if [[ "${selected_pairs}" != "${selected_pairs_before_force}" ]]; then
  echo "Applied source diversity enforcement -> selected=${selected_pairs}"
fi

rotation_entry_json="$(
  entry_cmd=(
    python3 "${ROTATE_HELPER}" build-rotation-entry
    --metrics-json "${metrics_json}"
    --rank-response "${rank_response}"
    --top-n "${TOP_N}"
    --min-confidence "${MIN_CONFIDENCE}"
    --allowed-risk "${ALLOWED_RISK}"
    --allowed-regimes "${ALLOWED_REGIMES}"
    --data-source "${DATA_SOURCE}"
    --mode "${MODE}"
    --selected-pairs-override "${selected_pairs}"
  )
  if is_true "${AUTO_DISCOVER}"; then
    entry_cmd+=(--auto-discover)
  fi
  if is_true "${APPLY}"; then
    entry_cmd+=(--apply-mode)
  fi
  if is_true "${RESTART}"; then
    entry_cmd+=(--restart-mode)
  fi
  if is_true "${SYNC_WHITELIST}"; then
    entry_cmd+=(--sync-whitelist)
  fi
  "${entry_cmd[@]}"
)"
selected_spike_pairs="$(
  python3 "${ROTATE_HELPER}" selected-source-pairs \
    --rotation-entry-json "${rotation_entry_json}" \
    --source spike
)"
printf '%s\n' "${rotation_entry_json}" >> "${LOG_PATH}"
python3 "${ROOT_DIR}/scripts/rotation_outcomes.py" \
  record \
  --db-path "${ROTATION_OUTCOME_DB_PATH}" \
  --horizon-minutes "${ROTATION_OUTCOME_HORIZON_MINUTES}" \
  --event-json "${rotation_entry_json}" >/dev/null || true
echo "Rotation log appended: ${LOG_PATH}"

if [[ -z "${selected_pairs}" ]]; then
  echo ""
  echo "No pair passed your filters."
  risk_changed="$(
    python3 "${ROTATE_HELPER}" risk-changed --current-risk-pairs "${current_risk_pairs}" --selected-pairs ""
  )"
  if [[ "${APPLY}" != "true" ]]; then
    echo "Preview only. Re-run with --apply to clear RISK_PAIRS and reduce the whitelist to core pairs."
    exit 0
  fi
  if [[ "${risk_changed}" == "true" ]]; then
    python3 "${ROTATE_HELPER}" set-env-value --env-path "${ENV_FILE}" --key "RISK_PAIRS" --value ""
    echo "Updated .env -> RISK_PAIRS="
  else
    echo "RISK_PAIRS already empty."
  fi
  spike_changed="$(
    python3 "${ROTATE_HELPER}" risk-changed --current-risk-pairs "${current_spike_pairs}" --selected-pairs ""
  )"
  if [[ "${spike_changed}" == "true" ]]; then
    python3 "${ROTATE_HELPER}" set-env-value --env-path "${ENV_FILE}" --key "SPIKE_PAIRS" --value ""
    echo "Updated .env -> SPIKE_PAIRS="
  else
    echo "SPIKE_PAIRS already empty."
  fi
  if is_true "${SYNC_WHITELIST}"; then
    python3 "${ROTATE_HELPER}" sync-whitelist --config-path "${CONFIG_FILE}" --core-pairs "${core_pairs}" --selected-pairs ""
  fi
  if [[ "${RESTART}" == "true" ]]; then
    echo "Restarting freqtrade with mode=${MODE}..."
    if STRATEGY_MODE="${MODE}" docker compose up -d --force-recreate freqtrade >/dev/null 2>&1; then
      echo "freqtrade container recreated."
    else
      docker restart freqtrade >/dev/null 2>&1 || true
    fi
  else
    echo "Tip: restart freqtrade to apply the cleared risk-pair configuration."
  fi
  exit 0
fi

echo ""
echo "Selected risk pairs: ${selected_pairs}"

risk_changed="$(
  python3 "${ROTATE_HELPER}" risk-changed --current-risk-pairs "${current_risk_pairs}" --selected-pairs "${selected_pairs}"
)"
spike_changed="$(
  python3 "${ROTATE_HELPER}" risk-changed --current-risk-pairs "${current_spike_pairs}" --selected-pairs "${selected_spike_pairs}"
)"

if [[ "${APPLY}" != "true" ]]; then
  echo "Preview only. Re-run with --apply to write .env."
  exit 0
fi

if [[ "${risk_changed}" == "true" ]]; then
  python3 "${ROTATE_HELPER}" set-env-value --env-path "${ENV_FILE}" --key "RISK_PAIRS" --value "${selected_pairs}"
  echo "Updated .env -> RISK_PAIRS=${selected_pairs}"
else
  echo "RISK_PAIRS unchanged -> ${selected_pairs}"
fi

if [[ "${spike_changed}" == "true" ]]; then
  python3 "${ROTATE_HELPER}" set-env-value --env-path "${ENV_FILE}" --key "SPIKE_PAIRS" --value "${selected_spike_pairs}"
  echo "Updated .env -> SPIKE_PAIRS=${selected_spike_pairs}"
else
  echo "SPIKE_PAIRS unchanged -> ${selected_spike_pairs}"
fi

if is_true "${SYNC_WHITELIST}"; then
  python3 "${ROTATE_HELPER}" sync-whitelist --config-path "${CONFIG_FILE}" --core-pairs "${core_pairs}" --selected-pairs "${selected_pairs}"
fi

if [[ "${RESTART}" == "true" ]]; then
  if [[ "${risk_changed}" == "true" || "${spike_changed}" == "true" ]]; then
    echo "Restarting freqtrade with mode=${MODE}..."
    if STRATEGY_MODE="${MODE}" docker compose up -d --force-recreate freqtrade >/dev/null 2>&1; then
      echo "freqtrade container recreated."
    else
      docker restart freqtrade >/dev/null 2>&1 || true
    fi
  else
    echo "Skipping freqtrade restart (selected pairs unchanged)."
  fi
else
  echo "Tip: restart freqtrade to apply updated pair configuration."
fi
