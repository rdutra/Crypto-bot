#!/bin/sh
set -eu

case "$(printf '%s' "${BOT_API_ACCESS_LOG_ENABLED:-false}" | tr '[:upper:]' '[:lower:]')" in
  1|true|yes|on)
    exec uvicorn main:app --host 0.0.0.0 --port 8000
    ;;
  *)
    exec uvicorn main:app --host 0.0.0.0 --port 8000 --no-access-log
    ;;
esac
