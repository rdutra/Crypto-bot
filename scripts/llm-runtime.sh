#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

resolve_provider() {
  if [[ -n "${LLM_PROVIDER:-}" ]]; then
    printf '%s\n' "${LLM_PROVIDER}"
    return
  fi

  python3 - "${ROOT_DIR}/.env" <<'PY'
import sys
from pathlib import Path

env_path = Path(sys.argv[1])
provider = ""
if env_path.exists():
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() != "LLM_PROVIDER":
            continue
        provider = value.strip().strip('"').strip("'")
        break
print(provider or "ollama")
PY
}

provider="$(resolve_provider)"
provider="$(printf '%s' "${provider}" | tr '[:upper:]' '[:lower:]')"
if [[ "${provider}" != "openai_compatible" ]]; then
  provider="ollama"
fi

command="${1:-provider}"
case "${command}" in
  provider)
    printf '%s\n' "${provider}"
    ;;
  services)
    if [[ "${provider}" == "ollama" ]]; then
      printf '%s\n' "ollama bot-api spike-scanner"
    else
      printf '%s\n' "bot-api spike-scanner"
    fi
    ;;
  *)
    echo "Usage: ./scripts/llm-runtime.sh [provider|services]" >&2
    exit 1
    ;;
esac
