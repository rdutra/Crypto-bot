#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="${ROOT_DIR}/freqtrade/user_data/config.json"
WATCH_SECONDS=30
STOP_BELOW=""
MODE="${STRATEGY_MODE:-conservative}"
ROTATE_RISK_PAIRS=false

usage() {
  cat <<'EOF'
Usage:
  ./scripts/run-dry-watch.sh
  ./scripts/run-dry-watch.sh --mode aggressive
  ./scripts/run-dry-watch.sh --mode aggressive --rotate-risk-pairs
  ./scripts/run-dry-watch.sh --watch 20
  ./scripts/run-dry-watch.sh --watch 30 --stop-below 290 --mode conservative

Options:
  --watch SECONDS      Wallet refresh interval. Default: 30.
  --stop-below VALUE   Auto-stop when bot wallet <= VALUE.
  --mode VALUE         Strategy profile: conservative|aggressive.
  --rotate-risk-pairs  Refresh RISK_PAIRS with LLM ranking before start.
  --help               Show help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --watch)
      WATCH_SECONDS="${2:-}"
      shift 2
      ;;
    --stop-below)
      STOP_BELOW="${2:-}"
      shift 2
      ;;
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --rotate-risk-pairs)
      ROTATE_RISK_PAIRS=true
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

if ! [[ "${WATCH_SECONDS}" =~ ^[0-9]+$ ]]; then
  echo "--watch must be an integer." >&2
  exit 1
fi
if [[ -n "${STOP_BELOW}" ]] && ! [[ "${STOP_BELOW}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "--stop-below must be numeric." >&2
  exit 1
fi
MODE="$(printf '%s' "${MODE}" | tr '[:upper:]' '[:lower:]')"
if [[ "${MODE}" != "conservative" && "${MODE}" != "aggressive" ]]; then
  echo "--mode must be either conservative or aggressive." >&2
  exit 1
fi

python3 - <<'PY' "${CONFIG_PATH}"
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.exists():
    print(f"Missing config file: {path}", file=sys.stderr)
    raise SystemExit(1)

cfg = json.loads(path.read_text())
if not cfg.get("dry_run", False):
    print("Refusing to run: config has dry_run=false. Use run-live-watch.sh for real trading.", file=sys.stderr)
    raise SystemExit(1)
PY

cd "${ROOT_DIR}"

cleaned_up=false
cleanup() {
  if [[ "${cleaned_up}" == "true" ]]; then
    return
  fi
  cleaned_up=true
  echo ""
  echo "Stopping freqtrade..."
  WALLET_LOGIN_RETRIES=1 ./scripts/wallet-control.sh --stop >/dev/null 2>&1 || true
  docker compose stop freqtrade >/dev/null 2>&1 || true
}

trap cleanup EXIT
trap 'exit 130' INT TERM

echo "Starting dry-run in ${MODE} mode..."
if [[ "${ROTATE_RISK_PAIRS}" == "true" ]]; then
  echo "Rotating risk pairs (LLM advisor)..."
  docker compose up -d ollama bot-api spike-scanner >/dev/null
  if ./scripts/rotate-risk-pairs.sh --apply --mode "${MODE}"; then
    rotate_log="${LLM_ROTATE_LOG_PATH:-./freqtrade/user_data/logs/llm-pair-rotation.log}"
    if [[ "${rotate_log}" != /* ]]; then
      rotate_log="${ROOT_DIR}/${rotate_log#./}"
    fi
    if [[ -f "${rotate_log}" ]]; then
      python3 - "${rotate_log}" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.exists():
    raise SystemExit(0)
lines = [ln for ln in path.read_text().splitlines() if ln.strip()]
if not lines:
    raise SystemExit(0)
last = json.loads(lines[-1])
selected = " ".join(last.get("selected_pairs", [])) or "none"
source = last.get("source", "unknown")
reason = last.get("reason", "n/a")
print(f"Rotation summary: source={source} reason={reason} selected={selected}")
PY
    fi
  else
    echo "Risk-pair rotation failed; continuing with current RISK_PAIRS."
  fi
fi
STRATEGY_MODE="${MODE}" docker compose up -d ollama bot-api spike-scanner scheduler pair-rotator policy-pivot freqtrade

wallet_cmd=(./scripts/wallet-control.sh --watch "${WATCH_SECONDS}")
if [[ -n "${STOP_BELOW}" ]]; then
  wallet_cmd+=(--stop-below "${STOP_BELOW}")
fi

"${wallet_cmd[@]}"
