#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

INTERVAL_MINUTES="${LLM_ROTATE_LOOP_INTERVAL_MINUTES:-60}"
MODE="${STRATEGY_MODE:-conservative}"
WITH_RESTART=true
JITTER_SECONDS="${LLM_ROTATE_LOOP_JITTER_SECONDS:-0}"
RUN_ONCE=false

usage() {
  cat <<'EOF'
Usage:
  ./scripts/rotate-risk-pairs-loop.sh
  ./scripts/rotate-risk-pairs-loop.sh --mode aggressive --interval-minutes 45
  ./scripts/rotate-risk-pairs-loop.sh --mode aggressive --no-restart
  ./scripts/rotate-risk-pairs-loop.sh --once --mode aggressive

Options:
  --interval-minutes N  Rotation interval in minutes. Default: 60.
  --mode VALUE          Strategy profile: conservative|aggressive.
  --restart             Recreate freqtrade after each apply (default).
  --no-restart          Do not recreate freqtrade.
  --jitter-seconds N    Add random jitter [0..N] seconds before each cycle.
  --once                Run one cycle and exit.
  --help                Show help.

Env defaults:
  LLM_ROTATE_LOOP_INTERVAL_MINUTES
  LLM_ROTATE_LOOP_JITTER_SECONDS
  STRATEGY_MODE
EOF
}

log() {
  printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

cleanup() {
  if [[ -n "${LOCK_DIR:-}" && -d "${LOCK_DIR}" ]]; then
    rmdir "${LOCK_DIR}" >/dev/null 2>&1 || true
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --interval-minutes)
      INTERVAL_MINUTES="${2:-}"
      shift 2
      ;;
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --restart)
      WITH_RESTART=true
      shift
      ;;
    --no-restart)
      WITH_RESTART=false
      shift
      ;;
    --jitter-seconds)
      JITTER_SECONDS="${2:-}"
      shift 2
      ;;
    --once)
      RUN_ONCE=true
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

if ! [[ "${INTERVAL_MINUTES}" =~ ^[0-9]+$ ]] || [[ "${INTERVAL_MINUTES}" -lt 1 ]]; then
  echo "--interval-minutes must be an integer >= 1." >&2
  exit 1
fi
if ! [[ "${JITTER_SECONDS}" =~ ^[0-9]+$ ]]; then
  echo "--jitter-seconds must be an integer >= 0." >&2
  exit 1
fi

MODE="$(printf '%s' "${MODE}" | tr '[:upper:]' '[:lower:]')"
if [[ "${MODE}" != "conservative" && "${MODE}" != "aggressive" ]]; then
  echo "--mode must be either conservative or aggressive." >&2
  exit 1
fi

cd "${ROOT_DIR}"

LOCK_DIR="${ROOT_DIR}/freqtrade/user_data/logs/.rotate-risk-pairs-loop.lock"
if ! mkdir "${LOCK_DIR}" 2>/dev/null; then
  echo "Another rotate-risk-pairs loop appears to be running. Lock: ${LOCK_DIR}" >&2
  exit 1
fi

trap cleanup EXIT
trap 'exit 130' INT TERM

cycle=1
while true; do
  if [[ "${JITTER_SECONDS}" -gt 0 ]]; then
    jitter=$((RANDOM % (JITTER_SECONDS + 1)))
    if [[ "${jitter}" -gt 0 ]]; then
      log "[rotate-loop] cycle=${cycle} jitter_sleep=${jitter}s"
      sleep "${jitter}"
    fi
  fi

  rotate_cmd=(./scripts/rotate-risk-pairs.sh --apply --mode "${MODE}")
  if [[ "${WITH_RESTART}" == "true" ]]; then
    rotate_cmd+=(--restart)
  fi

  log "[rotate-loop] cycle=${cycle} running: ${rotate_cmd[*]}"
  if "${rotate_cmd[@]}"; then
    log "[rotate-loop] cycle=${cycle} completed successfully"
  else
    log "[rotate-loop] cycle=${cycle} failed"
  fi

  if [[ "${RUN_ONCE}" == "true" ]]; then
    break
  fi

  sleep_seconds=$((INTERVAL_MINUTES * 60))
  log "[rotate-loop] cycle=${cycle} sleeping ${sleep_seconds}s"
  sleep "${sleep_seconds}"
  cycle=$((cycle + 1))
done

