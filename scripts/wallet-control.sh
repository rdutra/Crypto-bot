#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/wallet-control.sh
  ./scripts/wallet-control.sh --watch 30
  ./scripts/wallet-control.sh --stop
  ./scripts/wallet-control.sh --watch 30 --stop-below 290

Options:
  --watch SECONDS      Refresh wallet every N seconds.
  --stop               Send POST /stop to freqtrade API immediately.
  --stop-below VALUE   Auto-stop when total_bot <= VALUE (stake currency).
  --help               Show this help.

Environment (or .env fallback):
  FREQTRADE_API_URL       Default: http://localhost:8080/api/v1
  FREQTRADE_API_USERNAME  Default: freqtrade
  FREQTRADE_API_PASSWORD  Required for API login.
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

api_url_default="http://localhost:8080/api/v1"
api_url="${FREQTRADE_API_URL:-}"
if [[ -z "${api_url}" ]]; then
  api_url="$(get_env_file_value FREQTRADE_API_URL || true)"
fi
api_url="${api_url:-$api_url_default}"

api_user="${FREQTRADE_API_USERNAME:-}"
if [[ -z "${api_user}" ]]; then
  api_user="$(get_env_file_value FREQTRADE_API_USERNAME || true)"
fi
api_user="${api_user:-freqtrade}"

api_pass="${FREQTRADE_API_PASSWORD:-}"
if [[ -z "${api_pass}" ]]; then
  api_pass="$(get_env_file_value FREQTRADE_API_PASSWORD || true)"
fi

watch_seconds=0
stop_now=false
stop_below=""
login_retries="${WALLET_LOGIN_RETRIES:-20}"
login_retry_delay="${WALLET_LOGIN_RETRY_DELAY:-2}"
run_retries="${WALLET_RUN_RETRIES:-5}"
run_retry_delay="${WALLET_RUN_RETRY_DELAY:-2}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --watch)
      watch_seconds="${2:-}"
      shift 2
      ;;
    --stop)
      stop_now=true
      shift
      ;;
    --stop-below)
      stop_below="${2:-}"
      shift 2
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

if [[ -z "${api_pass}" ]]; then
  echo "FREQTRADE_API_PASSWORD is missing. Set it in environment or .env." >&2
  exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required." >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required." >&2
  exit 1
fi

if [[ -n "${watch_seconds}" ]] && ! [[ "${watch_seconds}" =~ ^[0-9]+$ ]]; then
  echo "--watch must be an integer number of seconds." >&2
  exit 1
fi
if [[ -n "${stop_below}" ]] && ! [[ "${stop_below}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "--stop-below must be numeric." >&2
  exit 1
fi
if ! [[ "${login_retries}" =~ ^[0-9]+$ ]]; then
  echo "WALLET_LOGIN_RETRIES must be an integer." >&2
  exit 1
fi
if ! [[ "${login_retry_delay}" =~ ^[0-9]+$ ]]; then
  echo "WALLET_LOGIN_RETRY_DELAY must be an integer." >&2
  exit 1
fi
if ! [[ "${run_retries}" =~ ^[0-9]+$ ]]; then
  echo "WALLET_RUN_RETRIES must be an integer." >&2
  exit 1
fi
if ! [[ "${run_retry_delay}" =~ ^[0-9]+$ ]]; then
  echo "WALLET_RUN_RETRY_DELAY must be an integer." >&2
  exit 1
fi

api_post_form() {
  local path="$1"
  shift
  curl -fsS -X POST "${api_url}${path}" "$@"
}

api_post_json() {
  local path="$1"
  local token="$2"
  curl -fsS -X POST "${api_url}${path}" \
    -H "Authorization: Bearer ${token}" \
    -H "Content-Type: application/json"
}

api_get_json() {
  local path="$1"
  local token="$2"
  curl -fsS "${api_url}${path}" -H "Authorization: Bearer ${token}"
}

extract_access_token() {
  local response="$1"
  python3 - "${response}" <<'PY' 2>/dev/null || true
import json
import sys

try:
    payload = json.loads(sys.argv[1])
    token = payload.get("access_token")
    if token:
        print(token)
except Exception:
    pass
PY
}

extract_error_detail() {
  local response="$1"
  python3 - "${response}" <<'PY' 2>/dev/null || true
import json
import sys

try:
    payload = json.loads(sys.argv[1])
    detail = payload.get("detail")
    if detail:
        print(detail)
except Exception:
    pass
PY
}

get_access_token() {
  local response token detail attempt
  for ((attempt = 1; attempt <= login_retries; attempt++)); do
    # Freqtrade 2026.x expects HTTP Basic on /token/login
    response="$(
      curl -sS -X POST "${api_url}/token/login" \
        -u "${api_user}:${api_pass}" \
      2>/dev/null || true
    )"
    token="$(extract_access_token "${response}")"
    if [[ -n "${token}" ]]; then
      echo "${token}"
      return 0
    fi
    detail="$(extract_error_detail "${response}")"
    if [[ "${detail}" == "Incorrect username or password" ]]; then
      echo "Freqtrade API rejected credentials (username/password mismatch)." >&2
      return 1
    fi

    # Backward compatibility for older Freqtrade versions that used form auth.
    response="$(
      api_post_form "/token/login" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        --data-urlencode "username=${api_user}" \
        --data-urlencode "password=${api_pass}" \
      2>/dev/null || true
    )"
    token="$(extract_access_token "${response}")"
    if [[ -n "${token}" ]]; then
      echo "${token}"
      return 0
    fi
    detail="$(extract_error_detail "${response}")"
    if [[ "${detail}" == "Incorrect username or password" ]]; then
      echo "Freqtrade API rejected credentials (username/password mismatch)." >&2
      return 1
    fi

    if [[ "${attempt}" -lt "${login_retries}" ]]; then
      echo "Waiting for Freqtrade API login endpoint (${attempt}/${login_retries})..."
      sleep "${login_retry_delay}"
    fi
  done

  echo "Could not authenticate to Freqtrade API after ${login_retries} attempts." >&2
  echo "Check that freqtrade is up and API credentials in .env are correct." >&2
  return 1
}

print_wallet_snapshot() {
  local balance_json="$1"
  local count_json="$2"
  python3 - "${balance_json}" "${count_json}" <<'PY'
import json
import sys

balance = json.loads(sys.argv[1])
count = json.loads(sys.argv[2])

stake = balance.get("stake", "?")
total_bot = float(balance.get("total_bot", 0.0))
total = float(balance.get("total", 0.0))
starting = float(balance.get("starting_capital", 0.0))
profit = total_bot - starting
profit_pct = (profit / starting * 100.0) if starting else 0.0

print("")
print("Wallet Snapshot")
print(f"  Stake currency     : {stake}")
print(f"  Bot wallet         : {total_bot:.3f} {stake}")
print(f"  Exchange total est : {total:.3f} {stake}")
print(f"  Since start        : {profit:+.3f} {stake} ({profit_pct:+.2f}%)")
print(f"  Open trades        : {count.get('current', 0)} / {count.get('max', 0)}")
print(f"  Open stake         : {float(count.get('total_stake', 0.0)):.3f} {stake}")

currencies = balance.get("currencies", [])
currencies = sorted(currencies, key=lambda x: float(x.get("est_stake_bot") or x.get("est_stake") or 0.0), reverse=True)
top = [c for c in currencies if float(c.get("est_stake_bot") or c.get("est_stake") or 0.0) > 0][:8]
if top:
    print("  Top holdings:")
    for c in top:
        name = c.get("currency", "?")
        est = float(c.get("est_stake_bot") or c.get("est_stake") or 0.0)
        free = float(c.get("free", 0.0))
        used = float(c.get("used", 0.0))
        print(f"    - {name:>8s}  est={est:10.3f} {stake}  free={free:12.6f}  used={used:12.6f}")
print("")
PY
}

extract_total_bot() {
  local balance_json="$1"
  python3 - "${balance_json}" <<'PY'
import json
import sys
payload = json.loads(sys.argv[1])
print(float(payload.get("total_bot", 0.0)))
PY
}

send_stop() {
  local token="$1"
  local response
  response="$(api_post_json "/stop" "${token}")"
  python3 - "${response}" <<'PY'
import json
import sys
payload = json.loads(sys.argv[1])
print(payload.get("status", "stop command sent"))
PY
}

run_once() {
  local token balance_json count_json
  token="$(get_access_token)"

  if [[ "${stop_now}" == "true" ]]; then
    echo "Sending stop command..."
    send_stop "${token}"
    exit 0
  fi

  if ! balance_json="$(api_get_json "/balance" "${token}" 2>/dev/null)"; then
    echo "Failed to fetch /balance from Freqtrade API." >&2
    return 1
  fi
  if ! count_json="$(api_get_json "/count" "${token}" 2>/dev/null)"; then
    echo "Failed to fetch /count from Freqtrade API." >&2
    return 1
  fi
  if ! print_wallet_snapshot "${balance_json}" "${count_json}"; then
    echo "Failed to parse wallet API response." >&2
    return 1
  fi

  if [[ -n "${stop_below}" ]]; then
    local total_bot
    if ! total_bot="$(extract_total_bot "${balance_json}")"; then
      echo "Failed to parse wallet total from /balance response." >&2
      return 1
    fi
    if python3 - "${total_bot}" "${stop_below}" <<'PY'
import sys
total = float(sys.argv[1])
floor = float(sys.argv[2])
raise SystemExit(0 if total <= floor else 1)
PY
    then
      echo "Wallet threshold reached (${total_bot} <= ${stop_below}). Sending stop..."
      send_stop "${token}"
      exit 0
    fi
  fi

}

run_once_with_retry() {
  local attempt
  for ((attempt = 1; attempt <= run_retries; attempt++)); do
    if run_once; then
      return 0
    fi
    if [[ "${attempt}" -lt "${run_retries}" ]]; then
      echo "Wallet API request failed. Retrying (${attempt}/${run_retries})..."
      sleep "${run_retry_delay}"
    fi
  done
  return 1
}

if [[ "${watch_seconds}" -gt 0 ]]; then
  while true; do
    run_once_with_retry
    sleep "${watch_seconds}"
  done
else
  run_once_with_retry
fi
