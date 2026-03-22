import os
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()


def _env_bool(key: str, default: bool) -> bool:
    value = os.getenv(key)
    if value is None or not value.strip():
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_str(key: str, default: str) -> str:
    value = os.getenv(key)
    if value is None:
        return default
    value = value.strip()
    return value if value else default


def _env_int(key: str, default: int) -> int:
    raw = _env_str(key, str(default))
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(key: str, default: float) -> float:
    raw = _env_str(key, str(default))
    try:
        return float(raw)
    except ValueError:
        return default


def _shared_bot_api_url() -> str:
    return _env_str("LLM_BOT_API_URL", _env_str("BOT_API_URL", "http://bot-api:8000"))


class Settings(BaseModel):
    rest_base: str = _env_str("BINANCE_REST_BASE", "https://api.binance.com")
    ws_base: str = _env_str("BINANCE_WS_BASE", "wss://stream.binance.com:9443/stream")
    quote_asset: str = _env_str("SPIKE_QUOTE_ASSET", _env_str("LLM_ROTATE_QUOTE", "USDT")).upper()
    min_quote_volume: float = _env_float(
        "SPIKE_MIN_QUOTE_VOLUME",
        _env_float("LLM_ROTATE_MIN_QUOTE_VOLUME", 20000000.0),
    )
    exclude_regex: str = _env_str(
        "SPIKE_EXCLUDE_REGEX",
        _env_str(
            "LLM_ROTATE_EXCLUDE_REGEX",
            r"(UP|DOWN|BULL|BEAR|1000|[0-9][0-9][0-9]+L|[0-9][0-9][0-9]+S)",
        ),
    )
    include_symbols: str = os.getenv("SPIKE_INCLUDE_SYMBOLS", "")
    exclude_symbols: str = _env_str("SPIKE_EXCLUDE_SYMBOLS", "BTCUSDT ETHUSDT")
    universe_max_symbols: int = _env_int("SPIKE_UNIVERSE_MAX_SYMBOLS", 60)
    ws_symbols_per_conn: int = _env_int("SPIKE_WS_SYMBOLS_PER_CONN", 25)

    top_n_alerts: int = _env_int("SPIKE_TOP_N_ALERTS", 5)
    min_score: float = _env_float("SPIKE_MIN_SCORE", 0.80)
    max_spread_pct: float = _env_float("SPIKE_MAX_SPREAD_PCT", 0.30)
    min_breakout_pct: float = _env_float("SPIKE_MIN_BREAKOUT_PCT", 0.003)
    min_buy_ratio: float = _env_float("SPIKE_MIN_BUY_RATIO", 0.60)
    min_rel_quote: float = _env_float("SPIKE_MIN_REL_QUOTE", 8.0)
    cooldown_minutes: int = _env_int("SPIKE_ALERT_COOLDOWN_MINUTES", 30)
    loop_seconds: int = _env_int("SPIKE_LOOP_SECONDS", 5)
    alert_log_path: str = _env_str("SPIKE_LOG_PATH", "/data/spike-alerts.jsonl")
    db_path: str = _env_str("SPIKE_DB_URL", _env_str("SPIKE_DB_PATH", "/data/spike-scanner.sqlite"))
    outcome_horizons_minutes: str = _env_str("SPIKE_OUTCOME_HORIZONS_MINUTES", "60,240,1440,2880")
    outcome_loop_seconds: int = _env_int("SPIKE_OUTCOME_LOOP_SECONDS", 30)
    outcome_batch_size: int = _env_int("SPIKE_OUTCOME_BATCH_SIZE", 200)
    llm_shadow_enabled: bool = _env_bool("SPIKE_LLM_SHADOW_ENABLED", False)
    llm_shadow_bot_api_url: str = _env_str("SPIKE_LLM_SHADOW_BOT_API_URL", _shared_bot_api_url())
    llm_shadow_timeout_seconds: int = _env_int("SPIKE_LLM_SHADOW_TIMEOUT_SECONDS", 45)
    llm_shadow_min_confidence: float = _env_float("SPIKE_LLM_SHADOW_MIN_CONFIDENCE", 0.65)
    llm_shadow_allowed_regimes: str = _env_str("SPIKE_LLM_SHADOW_ALLOWED_REGIMES", "trend_pullback,breakout")
    llm_shadow_allowed_risk_levels: str = _env_str("SPIKE_LLM_SHADOW_ALLOWED_RISK_LEVELS", "low,medium")
    llm_shadow_eval_top_n: int = _env_int("SPIKE_LLM_SHADOW_EVAL_TOP_N", 5)
    llm_shadow_eval_min_score: float = _env_float("SPIKE_LLM_SHADOW_EVAL_MIN_SCORE", 0.70)
    llm_shadow_eval_cache_seconds: int = _env_int("SPIKE_LLM_SHADOW_EVAL_CACHE_SECONDS", 300)

    web_enabled: bool = _env_bool("SPIKE_WEB_ENABLED", True)
    web_host: str = _env_str("SPIKE_WEB_HOST", "0.0.0.0")
    web_port: int = _env_int("SPIKE_WEB_PORT", 8090)
    notify_enabled: bool = _env_bool("SPIKE_NOTIFY_ENABLED", True)
    notify_timeout_seconds: int = _env_int("SPIKE_NOTIFY_TIMEOUT_SECONDS", 10)
    telegram_bot_token: str = _env_str("SPIKE_TELEGRAM_BOT_TOKEN", _env_str("TELEGRAM_BOT_TOKEN", ""))
    telegram_chat_id: str = _env_str("SPIKE_TELEGRAM_CHAT_ID", _env_str("TELEGRAM_CHAT_ID", ""))
    discord_webhook_url: str = _env_str("SPIKE_DISCORD_WEBHOOK_URL", _env_str("DISCORD_WEBHOOK_URL", ""))

    def include_set(self) -> set[str]:
        return {s.strip().upper() for s in self.include_symbols.replace(",", " ").split() if s.strip()}

    def exclude_set(self) -> set[str]:
        return {s.strip().upper() for s in self.exclude_symbols.replace(",", " ").split() if s.strip()}

    def has_notifier_targets(self) -> bool:
        return bool(
            (self.telegram_bot_token and self.telegram_chat_id)
            or self.discord_webhook_url
        )

    def parsed_outcome_horizons(self) -> list[int]:
        values: list[int] = []
        for part in self.outcome_horizons_minutes.replace(";", ",").split(","):
            part = part.strip()
            if not part:
                continue
            try:
                minute = int(part)
            except ValueError:
                continue
            if minute > 0:
                values.append(minute)
        unique_sorted = sorted(set(values))
        return unique_sorted or [60, 240, 1440, 2880]

    def parsed_llm_shadow_allowed_regimes(self) -> set[str]:
        values = {part.strip().lower() for part in self.llm_shadow_allowed_regimes.replace(";", ",").split(",")}
        return {value for value in values if value}

    def parsed_llm_shadow_allowed_risk_levels(self) -> set[str]:
        values = {part.strip().lower() for part in self.llm_shadow_allowed_risk_levels.replace(";", ",").split(",")}
        return {value for value in values if value}
