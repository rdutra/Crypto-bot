#!/usr/bin/env sh
set -eu

LOG_DIR="/freqtrade/user_data/logs"
STATE_DIR="${SCHED_STATE_DIR:-/freqtrade/user_data/logs/scheduler_state}"
DATA_DIR="/freqtrade/user_data/data"
SHELL_HELPER="/workspace/scripts/shell_helpers.py"
CONFIG_PATH="/freqtrade/user_data/config.json"
ROTATION_LOG_PATH="/freqtrade/user_data/logs/llm-pair-rotation.log"

SCHED_DOWNLOAD_ENABLED="${SCHED_DOWNLOAD_ENABLED:-true}"
SCHED_DOWNLOAD_TIME="${SCHED_DOWNLOAD_TIME:-02:15}"
SCHED_DOWNLOAD_TIMERANGE="${SCHED_DOWNLOAD_TIMERANGE:-20230101-}"
SCHED_DOWNLOAD_PAIRS="${SCHED_DOWNLOAD_PAIRS:-BTC/USDT ETH/USDT BNB/USDT SOL/USDT XRP/USDT AVAX/USDT LINK/USDT INJ/USDT DOGE/USDT ADA/USDT SUI/USDT TRX/USDT TAO/USDT ZEC/USDT WLD/USDT PEPE/USDT}"

SCHED_PRUNE_ENABLED="${SCHED_PRUNE_ENABLED:-true}"
SCHED_PRUNE_TIME="${SCHED_PRUNE_TIME:-03:00}"
SCHED_PRUNE_WEEKDAY="${SCHED_PRUNE_WEEKDAY:-0}"
SCHED_PRUNE_DAYS="${SCHED_PRUNE_DAYS:-180}"

DOWNLOAD_LOG="${LOG_DIR}/scheduler-download.log"
PRUNE_LOG="${LOG_DIR}/scheduler-prune.log"

mkdir -p "${LOG_DIR}" "${STATE_DIR}"

log() {
  printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

is_true() {
  case "$(printf '%s' "${1}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

run_download() {
  log "[scheduler] Running download-data"
  WHITELIST_PAIRS="$(python3 "${SHELL_HELPER}" current-whitelist-pairs "${CONFIG_PATH}")"
  RECENT_ROTATION_PAIRS="$(python3 "${SHELL_HELPER}" recent-rotation-pairs "${ROTATION_LOG_PATH}" 8 20)"
  ALL_PAIRS="$(python3 "${SHELL_HELPER}" unique-pairs "${SCHED_DOWNLOAD_PAIRS} ${WHITELIST_PAIRS} ${RECENT_ROTATION_PAIRS}")"
  # Intentional word splitting for pair list.
  # shellcheck disable=SC2086
  if freqtrade download-data \
    --config /freqtrade/user_data/config.json \
    --timeframes 1h 4h \
    --pairs ${ALL_PAIRS} \
    --timerange "${SCHED_DOWNLOAD_TIMERANGE}" \
    >>"${DOWNLOAD_LOG}" 2>&1; then
    date +%F >"${STATE_DIR}/last_download_date"
    log "[scheduler] download-data completed"
  else
    log "[scheduler] download-data failed (check ${DOWNLOAD_LOG})"
  fi
}

run_prune() {
  if [ ! -d "${DATA_DIR}" ]; then
    log "[scheduler] Data directory not found, skipping prune"
    date +%F >"${STATE_DIR}/last_prune_date"
    return
  fi

  log "[scheduler] Pruning files older than ${SCHED_PRUNE_DAYS} days"
  if find "${DATA_DIR}" -type f \( -name "*.feather" -o -name "*.parquet" \) -mtime +"${SCHED_PRUNE_DAYS}" -print -delete >>"${PRUNE_LOG}" 2>&1; then
    date +%F >"${STATE_DIR}/last_prune_date"
    log "[scheduler] Prune completed"
  else
    log "[scheduler] Prune failed (check ${PRUNE_LOG})"
  fi
}

log "[scheduler] started"
log "[scheduler] download enabled=${SCHED_DOWNLOAD_ENABLED} time=${SCHED_DOWNLOAD_TIME} timerange=${SCHED_DOWNLOAD_TIMERANGE}"
log "[scheduler] prune enabled=${SCHED_PRUNE_ENABLED} time=${SCHED_PRUNE_TIME} weekday=${SCHED_PRUNE_WEEKDAY} days=${SCHED_PRUNE_DAYS}"

while true; do
  NOW_HM="$(date +%H:%M)"
  TODAY="$(date +%F)"
  WEEKDAY="$(date +%w)"

  if is_true "${SCHED_DOWNLOAD_ENABLED}" && [ "${NOW_HM}" = "${SCHED_DOWNLOAD_TIME}" ]; then
    LAST_DOWNLOAD="$(cat "${STATE_DIR}/last_download_date" 2>/dev/null || true)"
    if [ "${LAST_DOWNLOAD}" != "${TODAY}" ]; then
      run_download
    fi
  fi

  if is_true "${SCHED_PRUNE_ENABLED}" && [ "${NOW_HM}" = "${SCHED_PRUNE_TIME}" ] && [ "${WEEKDAY}" = "${SCHED_PRUNE_WEEKDAY}" ]; then
    LAST_PRUNE="$(cat "${STATE_DIR}/last_prune_date" 2>/dev/null || true)"
    if [ "${LAST_PRUNE}" != "${TODAY}" ]; then
      run_prune
    fi
  fi

  sleep 30
done
