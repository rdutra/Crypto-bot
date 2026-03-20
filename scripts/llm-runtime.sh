#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SHELL_HELPER="${ROOT_DIR}/scripts/shell_helpers.py"

read_env_value() {
  local key="$1"
  if [[ -n "${!key:-}" ]]; then
    printf '%s\n' "${!key}"
    return
  fi

  python3 "${SHELL_HELPER}" env-value "${ROOT_DIR}/.env" "${key}"
}

resolve_provider() {
  local provider
  provider="$(read_env_value "LLM_PROVIDER")"
  printf '%s\n' "${provider:-ollama}"
}

resolve_ollama_mode() {
  local provider base_url normalized
  provider="$(resolve_provider)"
  provider="$(printf '%s' "${provider}" | tr '[:upper:]' '[:lower:]')"
  if [[ "${provider}" != "ollama" ]]; then
    printf '%s\n' "external"
    return
  fi

  base_url="$(read_env_value "OLLAMA_BASE_URL")"
  normalized="$(printf '%s' "${base_url:-http://ollama:11434}" | tr '[:upper:]' '[:lower:]')"
  case "${normalized}" in
    http://ollama:11434|https://ollama:11434|http://ollama|https://ollama)
      printf '%s\n' "docker"
      ;;
    *)
      printf '%s\n' "external"
      ;;
  esac
}

provider="$(resolve_provider)"
provider="$(printf '%s' "${provider}" | tr '[:upper:]' '[:lower:]')"
if [[ "${provider}" != "openai_compatible" ]]; then
  provider="ollama"
fi
ollama_mode="$(resolve_ollama_mode)"

command="${1:-provider}"
case "${command}" in
  provider)
    printf '%s\n' "${provider}"
    ;;
  ollama-mode)
    printf '%s\n' "${ollama_mode}"
    ;;
  services)
    if [[ "${provider}" == "ollama" && "${ollama_mode}" == "docker" ]]; then
      printf '%s\n' "ollama bot-api spike-scanner"
    else
      printf '%s\n' "bot-api spike-scanner"
    fi
    ;;
  *)
    echo "Usage: ./scripts/llm-runtime.sh [provider|ollama-mode|services]" >&2
    exit 1
    ;;
esac
