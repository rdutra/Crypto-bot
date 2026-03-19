from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Any, Dict, Literal, Protocol
from uuid import uuid4

import httpx

SkillMeta = Dict[str, Any]


def _env_bool(key: str, default: bool) -> bool:
    value = os.getenv(key)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(key: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(key, str(default)))
    except ValueError:
        value = default
    return max(minimum, min(maximum, value))


def _env_float(key: str, default: float, minimum: float, maximum: float) -> float:
    try:
        value = float(os.getenv(key, str(default)))
    except ValueError:
        value = default
    return max(minimum, min(maximum, value))


def _ticker_to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _float_like(value: Any) -> float:
    if isinstance(value, str):
        parsed = value.strip().replace(",", "")
        if not parsed:
            return 0.0
        if parsed.endswith("%"):
            parsed = parsed[:-1]
            try:
                return float(parsed) / 100.0
            except ValueError:
                return 0.0
        try:
            num = float(parsed)
        except ValueError:
            return 0.0
        if num > 1.0:
            return num / 100.0
        return num

    try:
        num = float(value)
    except (TypeError, ValueError):
        return 0.0
    if num > 1.0:
        return num / 100.0
    return num


def _normalize_signal_side(value: Any) -> Literal["buy", "sell", "neutral"]:
    text = str(value).strip().lower()
    if any(token in text for token in ("buy", "long", "bull", "up")):
        return "buy"
    if any(token in text for token in ("sell", "short", "bear", "down")):
        return "sell"
    return "neutral"


def _to_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "on", "tradable", "supported"}:
        return True
    if text in {"0", "false", "no", "off", "unsupported"}:
        return False
    return None


def _to_int(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _normalize_chain(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text.upper() if len(text) <= 8 else text


def _normalize_symbol(value: Any) -> str:
    text = re.sub(r"[^A-Z0-9]", "", str(value or "").strip().upper())
    return text


def _normalize_contract(value: Any) -> str:
    text = str(value or "").strip()
    return text[:128]


def _pick_first(row: dict[str, Any], keys: tuple[str, ...], default: Any = None) -> Any:
    for key in keys:
        if key in row and row.get(key) not in {None, ""}:
            return row.get(key)
    return default


@dataclass(frozen=True)
class SkillSettings:
    provider_name: str = field(default_factory=lambda: (os.getenv("LLM_SKILL_PROVIDER", "direct").strip().lower() or "direct"))
    allow_official_fallback: bool = field(default_factory=lambda: _env_bool("LLM_OFFICIAL_SKILL_FALLBACK_TO_DIRECT", True))
    official_base_url: str = field(default_factory=lambda: os.getenv("LLM_OFFICIAL_SKILL_BASE_URL", "").strip().rstrip("/"))
    official_user_agent: str = field(default_factory=lambda: os.getenv("LLM_OFFICIAL_SKILL_USER_AGENT", "crypto-bot/1.0 (official-skill-provider)").strip())

    market_rank_enabled: bool = field(default_factory=lambda: _env_bool("LLM_MARKET_RANK_SKILL_ENABLED", False))
    market_rank_base_url: str = field(default_factory=lambda: os.getenv("LLM_MARKET_RANK_BASE_URL", "https://web3.binance.com").rstrip("/"))
    market_rank_timeout: float = field(default_factory=lambda: _env_float("LLM_MARKET_RANK_TIMEOUT_SECONDS", 6.0, 1.0, 30.0))
    market_rank_cache_seconds: int = field(default_factory=lambda: _env_int("LLM_MARKET_RANK_CACHE_SECONDS", 300, 10, 3600))
    market_rank_max_tokens: int = field(default_factory=lambda: _env_int("LLM_MARKET_RANK_MAX_TOKENS", 120, 20, 200))
    market_rank_period: int = field(default_factory=lambda: _env_int("LLM_MARKET_RANK_PERIOD", 50, 10, 50))
    market_rank_quote: str = field(default_factory=lambda: os.getenv("LLM_MARKET_RANK_QUOTE", "USDT").strip().upper() or "USDT")
    market_rank_chain_id: str = field(default_factory=lambda: os.getenv("LLM_MARKET_RANK_CHAIN_ID", "").strip())
    market_rank_user_agent: str = field(default_factory=lambda: os.getenv("LLM_MARKET_RANK_USER_AGENT", "binance-web3/2.0 (Skill)").strip())
    market_rank_unified_endpoint: str = field(default_factory=lambda: os.getenv(
        "LLM_MARKET_RANK_UNIFIED_ENDPOINT",
        "/bapi/defi/v1/public/wallet-direct/buw/wallet/market/token/pulse/unified/rank/list",
    ).strip())
    market_rank_exclude_regex: str = field(default_factory=lambda: os.getenv(
        "LLM_MARKET_RANK_EXCLUDE_REGEX",
        r"(UP|DOWN|BULL|BEAR|1000|[0-9][0-9][0-9]+L|[0-9][0-9][0-9]+S)",
    ).strip())
    official_market_rank_path: str = field(default_factory=lambda: os.getenv(
        "LLM_OFFICIAL_MARKET_RANK_PATH",
        "/bapi/defi/v1/public/wallet-direct/buw/wallet/market/token/pulse/unified/rank/list",
    ).strip())

    trading_signal_enabled: bool = field(default_factory=lambda: _env_bool("LLM_TRADING_SIGNAL_SKILL_ENABLED", False))
    trading_signal_base_url: str = field(default_factory=lambda: os.getenv("LLM_TRADING_SIGNAL_BASE_URL", "https://web3.binance.com").rstrip("/"))
    trading_signal_timeout: float = field(default_factory=lambda: _env_float("LLM_TRADING_SIGNAL_TIMEOUT_SECONDS", 6.0, 1.0, 30.0))
    trading_signal_cache_seconds: int = field(default_factory=lambda: _env_int("LLM_TRADING_SIGNAL_CACHE_SECONDS", 180, 10, 3600))
    trading_signal_max_items: int = field(default_factory=lambda: _env_int("LLM_TRADING_SIGNAL_MAX_ITEMS", 120, 20, 300))
    trading_signal_quote: str = field(default_factory=lambda: os.getenv("LLM_TRADING_SIGNAL_QUOTE", "USDT").strip().upper() or "USDT")
    trading_signal_min_score: float = field(default_factory=lambda: _env_float("LLM_TRADING_SIGNAL_MIN_SCORE", 0.25, 0.0, 1.0))
    trading_signal_chain_id: str = field(default_factory=lambda: os.getenv("LLM_TRADING_SIGNAL_CHAIN_ID", "56").strip() or "56")
    trading_signal_user_agent: str = field(default_factory=lambda: os.getenv("LLM_TRADING_SIGNAL_USER_AGENT", "binance-web3/1.0 (Skill)").strip())
    trading_signal_exclude_regex: str = field(default_factory=lambda: os.getenv(
        "LLM_TRADING_SIGNAL_EXCLUDE_REGEX",
        r"(UP|DOWN|BULL|BEAR|1000|[0-9][0-9][0-9]+L|[0-9][0-9][0-9]+S)",
    ).strip())
    trading_signal_endpoints: list[str] = field(default_factory=lambda: [
        part.strip()
        for part in os.getenv(
            "LLM_TRADING_SIGNAL_ENDPOINTS",
            "/bapi/defi/v1/public/wallet-direct/buw/wallet/web/signal/smart-money,"
            "/friendly/gateway/v1/public/alpha-trade/market/signal/list,"
            "/friendly/gateway/v1/public/alpha-trade/trading-signal/list,"
            "/friendly/gateway/v1/public/alpha-trade/whitelist/trading-signal/list",
        ).split(",")
        if part.strip()
    ])
    official_trading_signal_path: str = field(default_factory=lambda: os.getenv(
        "LLM_OFFICIAL_TRADING_SIGNAL_PATH",
        "/bapi/defi/v1/public/wallet-direct/buw/wallet/web/signal/smart-money",
    ).strip())

    binance_rest_base: str = field(default_factory=lambda: os.getenv("BINANCE_REST_BASE", "https://api.binance.com").rstrip("/"))

    token_info_enabled: bool = field(default_factory=lambda: _env_bool("LLM_TOKEN_INFO_SKILL_ENABLED", False))
    token_info_timeout: float = field(default_factory=lambda: _env_float("LLM_TOKEN_INFO_TIMEOUT_SECONDS", 6.0, 1.0, 30.0))
    token_info_cache_seconds: int = field(default_factory=lambda: _env_int("LLM_TOKEN_INFO_CACHE_SECONDS", 300, 10, 3600))
    token_info_path: str = field(default_factory=lambda: os.getenv(
        "LLM_TOKEN_INFO_PATH",
        "/bapi/defi/v5/public/wallet-direct/buw/wallet/market/token/search/ai",
    ).strip())

    token_audit_enabled: bool = field(default_factory=lambda: _env_bool("LLM_TOKEN_AUDIT_SKILL_ENABLED", False))
    token_audit_timeout: float = field(default_factory=lambda: _env_float("LLM_TOKEN_AUDIT_TIMEOUT_SECONDS", 6.0, 1.0, 30.0))
    token_audit_cache_seconds: int = field(default_factory=lambda: _env_int("LLM_TOKEN_AUDIT_CACHE_SECONDS", 300, 10, 3600))
    token_audit_path: str = field(default_factory=lambda: os.getenv(
        "LLM_TOKEN_AUDIT_PATH",
        "/bapi/defi/v1/public/wallet-direct/security/token/audit",
    ).strip())

    address_info_enabled: bool = field(default_factory=lambda: _env_bool("LLM_ADDRESS_INFO_SKILL_ENABLED", False))
    address_info_timeout: float = field(default_factory=lambda: _env_float("LLM_ADDRESS_INFO_TIMEOUT_SECONDS", 6.0, 1.0, 30.0))
    address_info_cache_seconds: int = field(default_factory=lambda: _env_int("LLM_ADDRESS_INFO_CACHE_SECONDS", 300, 10, 3600))
    address_info_path: str = field(default_factory=lambda: os.getenv(
        "LLM_ADDRESS_INFO_PATH",
        "/bapi/defi/v3/public/wallet-direct/buw/wallet/address/pnl/active-position-list",
    ).strip())

    def market_rank_exclude_re(self) -> re.Pattern[str]:
        try:
            return re.compile(self.market_rank_exclude_regex, re.IGNORECASE)
        except re.error:
            return re.compile(r"(UP|DOWN|BULL|BEAR)", re.IGNORECASE)

    def trading_signal_exclude_re(self) -> re.Pattern[str]:
        try:
            return re.compile(self.trading_signal_exclude_regex, re.IGNORECASE)
        except re.error:
            return re.compile(r"(UP|DOWN|BULL|BEAR)", re.IGNORECASE)


MARKET_RANK_TYPES: Dict[int, str] = {
    10: "trending_rank",
    11: "top_search_rank",
    20: "alpha_rank",
}


def pair_from_token_symbol(symbol: str, quote: str) -> str:
    cleaned = re.sub(r"[^A-Z0-9]", "", str(symbol).strip().upper())
    if not cleaned:
        return ""
    if cleaned.endswith(quote) and len(cleaned) > len(quote):
        cleaned = cleaned[: -len(quote)]
    return f"{cleaned}/{quote}"


def rank_strength(rank_value: Any, max_tokens: int) -> float:
    try:
        rank = int(rank_value)
    except (TypeError, ValueError):
        return 0.0
    if rank <= 0:
        return 0.0
    return max(0.0, 1.0 - ((rank - 1) / float(max(1, max_tokens))))


def extract_rank_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []

    data = payload.get("data")
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        for key in ("tokens", "rows", "list", "items", "data"):
            rows = data.get(key)
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
    items = payload.get("items")
    if isinstance(items, list):
        return [row for row in items if isinstance(row, dict)]
    return []


def extract_signal_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []

    for key in ("data", "rows", "list", "items", "signals", "result"):
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
        if isinstance(value, dict):
            for nested in ("rows", "list", "items", "signals", "data"):
                inner = value.get(nested)
                if isinstance(inner, list):
                    return [row for row in inner if isinstance(row, dict)]
    return []


def _signal_score_from_row(row: dict[str, Any]) -> float:
    for key in (
        "score",
        "confidence",
        "signalScore",
        "signal_score",
        "strength",
        "successRate",
        "winRate",
        "probability",
        "buyScore",
        "hotScore",
    ):
        if key in row:
            score = _float_like(row.get(key))
            if score > 0:
                return max(0.0, min(1.0, score))
    return 0.0


def is_supported_spot_symbol(symbol: str, quote: str, exclude_re: re.Pattern[str]) -> bool:
    sym = str(symbol).strip().upper()
    if not sym.endswith(quote):
        return False
    base = sym[: -len(quote)]
    if len(base) < 2:
        return False
    if exclude_re.search(base):
        return False
    return True


def is_supported_signal_symbol(symbol: str, quote: str, exclude_re: re.Pattern[str]) -> bool:
    sym = str(symbol).strip().upper()
    if sym.endswith(quote):
        base = sym[: -len(quote)]
    else:
        base = sym
    if len(base) < 2:
        return False
    if exclude_re.search(base):
        return False
    return True


def build_market_rank_context(rows_by_type: dict[int, list[dict[str, Any]]], settings: SkillSettings) -> dict[str, dict[str, Any]]:
    pair_map: dict[str, dict[str, Any]] = {}

    for rank_type, rows in rows_by_type.items():
        field_name = MARKET_RANK_TYPES.get(rank_type)
        if not field_name:
            continue
        for idx, row in enumerate(rows, start=1):
            symbol = (
                row.get("symbol")
                or row.get("pair")
                or row.get("tokenSymbol")
                or row.get("baseAsset")
                or row.get("ticker")
                or row.get("symbolName")
                or ""
            )
            pair = pair_from_token_symbol(str(symbol).split("/")[0], settings.market_rank_quote)
            if not pair:
                continue
            existing = pair_map.setdefault(
                pair,
                {
                    "pair": pair,
                    "trending_rank": None,
                    "top_search_rank": None,
                    "alpha_rank": None,
                    "rank_score": 0.0,
                    "hits": 0,
                },
            )
            current_rank = existing.get(field_name)
            if current_rank is None or idx < int(current_rank):
                existing[field_name] = idx

    for entry in pair_map.values():
        weights = (
            (entry.get("trending_rank"), 0.45),
            (entry.get("top_search_rank"), 0.35),
            (entry.get("alpha_rank"), 0.20),
        )
        score = 0.0
        hits = 0
        for rank_value, weight in weights:
            strength = rank_strength(rank_value, settings.market_rank_max_tokens)
            if strength > 0:
                hits += 1
            score += strength * weight
        entry["rank_score"] = round(max(0.0, min(1.0, score)), 4)
        entry["hits"] = hits

    return pair_map


def build_spot_fallback_rows(tickers: list[dict[str, Any]], settings: SkillSettings) -> dict[int, list[dict[str, Any]]]:
    normalized: list[dict[str, Any]] = []
    exclude_re = settings.market_rank_exclude_re()
    for row in tickers:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).upper()
        if not is_supported_spot_symbol(symbol, settings.market_rank_quote, exclude_re):
            continue
        quote_volume = _ticker_to_float(row.get("quoteVolume"), 0.0)
        if quote_volume <= 0.0:
            continue
        price_change_pct = _ticker_to_float(row.get("priceChangePercent"), 0.0)
        momentum = max(0.0, price_change_pct) * quote_volume
        normalized.append(
            {
                "symbol": symbol,
                "quote_volume": quote_volume,
                "price_change_pct": price_change_pct,
                "momentum": momentum,
            }
        )

    if not normalized:
        return {}

    by_volume = sorted(normalized, key=lambda item: item["quote_volume"], reverse=True)[: settings.market_rank_max_tokens]
    by_abs_change = sorted(by_volume, key=lambda item: abs(item["price_change_pct"]), reverse=True)
    by_momentum = sorted(by_volume, key=lambda item: item["momentum"], reverse=True)

    return {
        10: [{"symbol": item["symbol"]} for item in by_volume],
        11: [{"symbol": item["symbol"]} for item in by_abs_change],
        20: [{"symbol": item["symbol"]} for item in by_momentum],
    }


def build_trading_signal_context(rows: list[dict[str, Any]], settings: SkillSettings) -> dict[str, dict[str, Any]]:
    context: dict[str, dict[str, Any]] = {}
    exclude_re = settings.trading_signal_exclude_re()

    for idx, row in enumerate(rows, start=1):
        symbol = (
            row.get("symbol")
            or row.get("pair")
            or row.get("tokenSymbol")
            or row.get("baseAsset")
            or row.get("ticker")
            or ""
        )
        symbol_raw = str(symbol).strip().upper()
        if "/" in symbol_raw:
            symbol_raw = symbol_raw.replace("/", "")
        if not is_supported_signal_symbol(symbol_raw, settings.trading_signal_quote, exclude_re):
            continue
        pair = pair_from_token_symbol(symbol_raw, settings.trading_signal_quote)
        if not pair:
            continue

        side = _normalize_signal_side(
            row.get("signal")
            or row.get("signalType")
            or row.get("direction")
            or row.get("action")
            or row.get("type")
            or ""
        )
        score = _signal_score_from_row(row)
        if score <= 0.0:
            score = max(0.0, 1.0 - ((idx - 1) / float(max(1, settings.trading_signal_max_items))))
        confidence = max(0.0, min(1.0, _float_like(row.get("confidence")) or score))
        note = str(row.get("note") or row.get("reason") or row.get("tag") or "trading_signal")[:160]

        if score < settings.trading_signal_min_score:
            continue

        previous = context.get(pair)
        if previous is None or float(previous.get("score", 0.0)) < score:
            context[pair] = {
                "pair": pair,
                "side": side,
                "score": round(score, 4),
                "confidence": round(confidence, 4),
                "note": note,
            }

    return context


def build_spot_trading_signal_rows(tickers: list[dict[str, Any]], settings: SkillSettings) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    exclude_re = settings.trading_signal_exclude_re()
    for row in tickers:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).upper()
        if not is_supported_signal_symbol(symbol, settings.trading_signal_quote, exclude_re):
            continue
        quote_volume = _ticker_to_float(row.get("quoteVolume"), 0.0)
        if quote_volume <= 0.0:
            continue
        change_pct = _ticker_to_float(row.get("priceChangePercent"), 0.0)
        normalized.append({"symbol": symbol, "quote_volume": quote_volume, "change_pct": change_pct})

    if not normalized:
        return []

    max_quote_volume = max(item["quote_volume"] for item in normalized) or 1.0
    signal_rows: list[dict[str, Any]] = []
    for item in normalized:
        momentum = max(-1.0, min(1.0, item["change_pct"] / 12.0))
        vol_norm = max(0.0, min(1.0, item["quote_volume"] / max_quote_volume))
        base_score = (abs(momentum) * 0.7) + (vol_norm * 0.3)

        side: Literal["buy", "sell", "neutral"] = "neutral"
        if momentum >= 0.08:
            side = "buy"
        elif momentum <= -0.08:
            side = "sell"

        score = base_score if side != "neutral" else base_score * 0.4
        signal_rows.append(
            {
                "symbol": item["symbol"],
                "signal": side,
                "score": round(max(0.0, min(1.0, score)), 4),
                "confidence": round(max(0.0, min(1.0, score * 0.9)), 4),
                "reason": f"spot_proxy_change:{item['change_pct']:.2f}",
            }
        )

    signal_rows.sort(key=lambda row: float(row.get("score", 0.0) or 0.0), reverse=True)
    return signal_rows[: settings.trading_signal_max_items]


def extract_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("items", "data", "rows", "list", "result"):
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
        if isinstance(value, dict):
            for nested in ("items", "rows", "list", "data"):
                inner = value.get(nested)
                if isinstance(inner, list):
                    return [row for row in inner if isinstance(row, dict)]
    return []


def normalize_token_info_row(row: dict[str, Any], *, symbol: str = "", chain_id: str = "", address: str = "") -> dict[str, Any] | None:
    normalized_symbol = _normalize_symbol(_pick_first(row, ("symbol", "tokenSymbol", "ticker", "baseAsset", "asset")))
    normalized_address = _normalize_contract(_pick_first(row, ("contract_address", "contractAddress", "tokenAddress", "address", "contract")))
    normalized_chain = _normalize_chain(_pick_first(row, ("chain", "network", "chainId", "chain_id")))

    if symbol and normalized_symbol and normalized_symbol != _normalize_symbol(symbol):
        return None
    if address and normalized_address and normalized_address.lower() != str(address).strip().lower():
        return None
    if chain_id and normalized_chain and normalized_chain != _normalize_chain(chain_id):
        return None

    price = _ticker_to_float(_pick_first(row, ("price", "priceUsd", "lastPrice", "currentPrice")), 0.0)
    change_24h_pct = _ticker_to_float(_pick_first(row, ("change_24h_pct", "priceChangePercent", "change24h", "priceChange24h")), 0.0)
    volume_24h_usd = _ticker_to_float(_pick_first(row, ("volume_24h_usd", "volumeUsd", "volume24h", "quoteVolume")), 0.0)
    liquidity_usd = _ticker_to_float(_pick_first(row, ("liquidity_usd", "liquidityUsd", "liquidity", "tvlUsd", "tvl")), 0.0)
    market_cap_usd = _ticker_to_float(_pick_first(row, ("market_cap_usd", "marketCapUsd", "marketCap", "fdvUsd", "fdv")), 0.0)
    holders = _to_int(_pick_first(row, ("holders", "holderCount", "holdersCount")))
    top10_holder_share = _float_like(_pick_first(row, ("top10_holder_share", "top10HolderShare", "top10HolderPct", "topHoldersPct")))
    spot_tradable = _to_bool(_pick_first(row, ("is_binance_spot_tradable", "isBinanceSpotTradable", "spotTradable", "tradable")))

    return {
        "symbol": normalized_symbol,
        "name": str(_pick_first(row, ("name", "tokenName", "assetName"), "")).strip()[:120],
        "chain": normalized_chain,
        "contract_address": normalized_address,
        "price": round(price, 12) if price else 0.0,
        "change_24h_pct": round(change_24h_pct, 6) if change_24h_pct else 0.0,
        "volume_24h_usd": round(volume_24h_usd, 4) if volume_24h_usd else 0.0,
        "liquidity_usd": round(liquidity_usd, 4) if liquidity_usd else 0.0,
        "market_cap_usd": round(market_cap_usd, 4) if market_cap_usd else 0.0,
        "holders": holders or 0,
        "top10_holder_share": round(max(0.0, min(1.0, top10_holder_share)), 6),
        "is_binance_spot_tradable": bool(spot_tradable) if spot_tradable is not None else False,
        "risk_level": str(_pick_first(row, ("riskLevel", "risk_level"), "")).strip().lower(),
    }


def _audit_classification_from_row(row: dict[str, Any]) -> str:
    explicit = str(_pick_first(row, ("classification", "rating", "level", "riskLevel", "status"), "")).strip().lower()
    mapping = {
        "safe": "safe",
        "low": "safe",
        "ok": "safe",
        "watch": "watch",
        "medium": "watch",
        "moderate": "watch",
        "caution": "caution",
        "warning": "caution",
        "high": "caution",
        "avoid": "avoid",
        "critical": "avoid",
        "danger": "avoid",
        "scam": "avoid",
    }
    if explicit in mapping:
        return mapping[explicit]

    score = _ticker_to_float(_pick_first(row, ("score", "riskScore", "securityScore")), 0.0)
    if score > 1.0:
        score = score / 100.0
    score = max(0.0, min(1.0, score))
    if score >= 0.85:
        return "safe"
    if score >= 0.65:
        return "watch"
    if score >= 0.40:
        return "caution"
    return "avoid"


def _audit_flags(row: dict[str, Any]) -> list[str]:
    raw = _pick_first(row, ("flags", "issues", "warnings", "risks"), [])
    flags: list[str] = []
    if isinstance(raw, list):
        for value in raw:
            text = str(value).strip()
            if text:
                flags.append(text[:80])
    elif isinstance(raw, dict):
        for key, value in raw.items():
            if _to_bool(value):
                flags.append(str(key)[:80])
    elif raw:
        flags.append(str(raw)[:80])
    return sorted(set(flags))[:20]


def normalize_token_audit_row(row: dict[str, Any], *, symbol: str = "", chain_id: str = "", address: str = "") -> dict[str, Any] | None:
    normalized_address = _normalize_contract(_pick_first(row, ("contract_address", "contractAddress", "tokenAddress", "address", "contract")))
    normalized_chain = _normalize_chain(_pick_first(row, ("chain", "network", "chainId", "chain_id")))
    normalized_symbol = _normalize_symbol(_pick_first(row, ("symbol", "tokenSymbol", "ticker", "baseAsset", "asset")))
    if symbol and normalized_symbol and normalized_symbol != _normalize_symbol(symbol):
        return None
    if address and normalized_address and normalized_address.lower() != str(address).strip().lower():
        return None
    if chain_id and normalized_chain and normalized_chain != _normalize_chain(chain_id):
        return None

    score = _ticker_to_float(_pick_first(row, ("score", "riskScore", "securityScore")), 0.0)
    if score > 1.0:
        score = score / 100.0
    score = max(0.0, min(1.0, score))

    return {
        "contract_address": normalized_address,
        "chain": normalized_chain,
        "classification": _audit_classification_from_row(row),
        "flags": _audit_flags(row),
        "score": round(score, 6),
    }


def _normalize_position_row(row: dict[str, Any]) -> dict[str, Any] | None:
    symbol = _normalize_symbol(_pick_first(row, ("symbol", "tokenSymbol", "asset", "ticker")))
    value_usd = _ticker_to_float(_pick_first(row, ("value_usd", "valueUsd", "usdValue", "notionalUsd", "amountUsd")), 0.0)
    share = _float_like(_pick_first(row, ("share", "allocation", "weight", "portfolioShare")))
    if value_usd <= 0.0 and share <= 0.0 and not symbol:
        return None
    return {"symbol": symbol, "value_usd": round(value_usd, 4), "share": round(max(0.0, min(1.0, share)), 6)}


def _concentration_risk(top1_share: float, top3_share: float, top10_share: float) -> str:
    if top1_share >= 0.6 or top3_share >= 0.85:
        return "high"
    if top1_share >= 0.35 or top3_share >= 0.65 or top10_share >= 0.9:
        return "medium"
    return "low"


def normalize_address_info_row(row: dict[str, Any], *, address: str = "", chain_id: str = "") -> dict[str, Any] | None:
    normalized_address = _normalize_contract(_pick_first(row, ("address", "wallet", "ownerAddress")))
    normalized_chain = _normalize_chain(_pick_first(row, ("chain", "network", "chainId", "chain_id")))
    if address and normalized_address and normalized_address.lower() != str(address).strip().lower():
        return None
    if chain_id and normalized_chain and normalized_chain != _normalize_chain(chain_id):
        return None

    positions_raw = _pick_first(row, ("top_positions", "positions", "holdings", "assets"), [])
    top_positions: list[dict[str, Any]] = []
    if isinstance(positions_raw, list):
        for item in positions_raw:
            if not isinstance(item, dict):
                continue
            normalized = _normalize_position_row(item)
            if normalized is not None:
                top_positions.append(normalized)
    top_positions = top_positions[:10]

    total_value_usd = _ticker_to_float(_pick_first(row, ("total_value_usd", "totalValueUsd", "portfolioUsd", "balanceUsd")), 0.0)
    top1_share = _float_like(_pick_first(row, ("top1_share", "top1Share", "largestHoldingShare")))
    top3_share = _float_like(_pick_first(row, ("top3_share", "top3Share")))
    top10_share = _float_like(_pick_first(row, ("top10_share", "top10Share", "concentration")))

    if not top1_share and top_positions:
        top1_share = float(top_positions[0].get("share", 0.0) or 0.0)
    if not top3_share and top_positions:
        top3_share = sum(float(item.get("share", 0.0) or 0.0) for item in top_positions[:3])
    if not top10_share and top_positions:
        top10_share = sum(float(item.get("share", 0.0) or 0.0) for item in top_positions[:10])

    return {
        "address": normalized_address,
        "chain": normalized_chain,
        "total_value_usd": round(total_value_usd, 4),
        "top_positions": top_positions,
        "top1_share": round(max(0.0, min(1.0, top1_share)), 6),
        "top3_share": round(max(0.0, min(1.0, top3_share)), 6),
        "top10_share": round(max(0.0, min(1.0, top10_share)), 6),
        "concentration_risk": _concentration_risk(top1_share, top3_share, top10_share),
    }


class SkillProvider(Protocol):
    name: str

    async def market_rank(self) -> tuple[dict[str, dict[str, Any]], SkillMeta]: ...
    async def trading_signal(self) -> tuple[dict[str, dict[str, Any]], SkillMeta]: ...
    async def token_info(self, *, symbol: str = "", chain_id: str = "", address: str = "") -> tuple[list[dict[str, Any]], SkillMeta]: ...
    async def token_audit(self, *, symbol: str = "", chain_id: str = "", address: str = "") -> tuple[list[dict[str, Any]], SkillMeta]: ...
    async def address_info(self, *, address: str = "", chain_id: str = "") -> tuple[list[dict[str, Any]], SkillMeta]: ...


class DirectBinanceProvider:
    name = "direct"

    def __init__(self, settings: SkillSettings):
        self.settings = settings

    async def market_rank(self) -> tuple[dict[str, dict[str, Any]], SkillMeta]:
        rows_by_type: dict[int, list[dict[str, Any]]] = {}
        errors: list[str] = []
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "identity",
            "User-Agent": self.settings.market_rank_user_agent,
        }
        source = "error"
        try:
            async with httpx.AsyncClient(timeout=self.settings.market_rank_timeout, headers=headers) as client:
                for rank_type in MARKET_RANK_TYPES:
                    try:
                        rows_by_type[rank_type] = await self._fetch_rank_type(client, rank_type)
                    except Exception as exc:
                        rows_by_type[rank_type] = []
                        errors.append(f"rank_type_{rank_type}:{type(exc).__name__}")
                pair_map = build_market_rank_context(rows_by_type, self.settings)
                source = "binance_web3"
                if not pair_map:
                    errors.append("web3_empty")
                    try:
                        rows_by_type = await self._fetch_spot_fallback_rows(client)
                        pair_map = build_market_rank_context(rows_by_type, self.settings)
                        if pair_map:
                            source = "binance_spot_fallback"
                            errors.append("using_spot_fallback")
                    except Exception as exc:
                        errors.append(f"spot_fallback:{type(exc).__name__}")
        except Exception as exc:
            errors.append(f"client:{type(exc).__name__}")
            pair_map = {}
            source = "error"

        meta = {
            "provider": self.name,
            "upstream_source": source,
            "source": source,
            "errors": errors,
            "upstream_errors": list(errors),
            "fetched_types": {str(k): len(v) for k, v in rows_by_type.items()},
            "cache_seconds": self.settings.market_rank_cache_seconds,
        }
        return pair_map, meta

    async def trading_signal(self) -> tuple[dict[str, dict[str, Any]], SkillMeta]:
        errors: list[str] = []
        rows: list[dict[str, Any]] = []
        source = "error"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "identity",
            "User-Agent": self.settings.trading_signal_user_agent,
        }
        try:
            async with httpx.AsyncClient(timeout=self.settings.trading_signal_timeout, headers=headers) as client:
                for path in self.settings.trading_signal_endpoints:
                    path_rows = await self._fetch_trading_signal_rows_from_path(client, path)
                    if path_rows:
                        rows = path_rows
                        source = f"binance_web3:{path}"
                        break
                    errors.append(f"empty:{path}")
                pair_map = build_trading_signal_context(rows, self.settings)
                if not pair_map:
                    errors.append("web3_empty")
                    try:
                        rows = await self._fetch_spot_trading_signal_rows(client)
                        pair_map = build_trading_signal_context(rows, self.settings)
                        if pair_map:
                            source = "binance_spot_fallback"
                            errors.append("using_spot_fallback")
                    except Exception as exc:
                        errors.append(f"spot_fallback:{type(exc).__name__}")
        except Exception as exc:
            errors.append(f"client:{type(exc).__name__}")
            pair_map = {}

        meta = {
            "provider": self.name,
            "upstream_source": source,
            "source": source,
            "errors": errors,
            "upstream_errors": list(errors),
            "cache_seconds": self.settings.trading_signal_cache_seconds,
            "rows_seen": len(rows),
        }
        return pair_map, meta

    async def token_info(self, *, symbol: str = "", chain_id: str = "", address: str = "") -> tuple[list[dict[str, Any]], SkillMeta]:
        return [], {
            "provider": self.name,
            "source": "unsupported",
            "upstream_source": "unsupported",
            "errors": ["unsupported_by_direct_provider"],
            "upstream_errors": ["unsupported_by_direct_provider"],
            "cache_seconds": self.settings.token_info_cache_seconds,
        }

    async def token_audit(self, *, symbol: str = "", chain_id: str = "", address: str = "") -> tuple[list[dict[str, Any]], SkillMeta]:
        return [], {
            "provider": self.name,
            "source": "unsupported",
            "upstream_source": "unsupported",
            "errors": ["unsupported_by_direct_provider"],
            "upstream_errors": ["unsupported_by_direct_provider"],
            "cache_seconds": self.settings.token_audit_cache_seconds,
        }

    async def address_info(self, *, address: str = "", chain_id: str = "") -> tuple[list[dict[str, Any]], SkillMeta]:
        return [], {
            "provider": self.name,
            "source": "unsupported",
            "upstream_source": "unsupported",
            "errors": ["unsupported_by_direct_provider"],
            "upstream_errors": ["unsupported_by_direct_provider"],
            "cache_seconds": self.settings.address_info_cache_seconds,
        }

    async def _fetch_rank_type(self, client: httpx.AsyncClient, rank_type: int) -> list[dict[str, Any]]:
        url = f"{self.settings.market_rank_base_url}{self.settings.market_rank_unified_endpoint}"
        request_payload: dict[str, Any] = {
            "rankType": rank_type,
            "period": self.settings.market_rank_period,
            "sortBy": 70,
            "orderAsc": False,
            "page": 1,
            "size": self.settings.market_rank_max_tokens,
        }
        if self.settings.market_rank_chain_id:
            request_payload["chainId"] = self.settings.market_rank_chain_id
        response = await client.post(url, json=request_payload)
        response.raise_for_status()
        return extract_rank_rows(response.json())

    async def _fetch_spot_fallback_rows(self, client: httpx.AsyncClient) -> dict[int, list[dict[str, Any]]]:
        response = await client.get(f"{self.settings.binance_rest_base}/api/v3/ticker/24hr")
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            return {}
        return build_spot_fallback_rows(payload, self.settings)

    async def _fetch_trading_signal_rows_from_path(self, client: httpx.AsyncClient, path: str) -> list[dict[str, Any]]:
        url = f"{self.settings.trading_signal_base_url}{path}"
        max_items = max(1, min(100, int(self.settings.trading_signal_max_items)))
        if "wallet/web/signal/smart-money" in path:
            methods: list[tuple[str, dict[str, Any] | None]] = [
                (
                    "POST",
                    {
                        "smartSignalType": "",
                        "page": 1,
                        "pageSize": max_items,
                        "chainId": self.settings.trading_signal_chain_id,
                    },
                )
            ]
        else:
            methods = [
                ("GET", None),
                ("POST", {"page": 1, "size": self.settings.trading_signal_max_items}),
                ("POST", {"limit": self.settings.trading_signal_max_items}),
            ]

        for method, payload in methods:
            try:
                if method == "GET":
                    response = await client.get(url, params={"page": 1, "size": self.settings.trading_signal_max_items})
                else:
                    response = await client.post(url, json=payload)
                response.raise_for_status()
                rows = extract_signal_rows(response.json())
                if rows:
                    return rows
            except Exception:
                continue
        return []

    async def _fetch_spot_trading_signal_rows(self, client: httpx.AsyncClient) -> list[dict[str, Any]]:
        response = await client.get(f"{self.settings.binance_rest_base}/api/v3/ticker/24hr")
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            return []
        return build_spot_trading_signal_rows(payload, self.settings)


class OfficialSkillProvider:
    name = "official_skill"

    def __init__(self, settings: SkillSettings, fallback_provider: DirectBinanceProvider):
        self.settings = settings
        self.fallback_provider = fallback_provider

    async def market_rank(self) -> tuple[dict[str, dict[str, Any]], SkillMeta]:
        if not self.settings.official_base_url:
            return await self._fallback("official_base_url_missing", self.fallback_provider.market_rank)
        rows_by_type, upstream_source, errors = await self._fetch_official_rank_rows()
        pair_map = build_market_rank_context(rows_by_type, self.settings)
        if pair_map:
            return pair_map, {
                "provider": self.name,
                "source": f"official_skill:{upstream_source}",
                "upstream_source": upstream_source,
                "errors": errors,
                "upstream_errors": list(errors),
                "fetched_types": {str(k): len(v) for k, v in rows_by_type.items()},
                "cache_seconds": self.settings.market_rank_cache_seconds,
            }
        if self.settings.allow_official_fallback:
            return await self._fallback("official_market_rank_empty", self.fallback_provider.market_rank, errors)
        return {}, {
            "provider": self.name,
            "source": "error",
            "upstream_source": upstream_source,
            "errors": errors + ["official_market_rank_empty"],
            "upstream_errors": errors + ["official_market_rank_empty"],
            "cache_seconds": self.settings.market_rank_cache_seconds,
            "fetched_types": {str(k): len(v) for k, v in rows_by_type.items()},
        }

    async def trading_signal(self) -> tuple[dict[str, dict[str, Any]], SkillMeta]:
        if not self.settings.official_base_url:
            return await self._fallback("official_base_url_missing", self.fallback_provider.trading_signal)
        rows, upstream_source, errors = await self._fetch_official_signal_rows()
        pair_map = build_trading_signal_context(rows, self.settings)
        if pair_map:
            return pair_map, {
                "provider": self.name,
                "source": f"official_skill:{upstream_source}",
                "upstream_source": upstream_source,
                "errors": errors,
                "upstream_errors": list(errors),
                "cache_seconds": self.settings.trading_signal_cache_seconds,
                "rows_seen": len(rows),
            }
        if self.settings.allow_official_fallback:
            return await self._fallback("official_trading_signal_empty", self.fallback_provider.trading_signal, errors)
        return {}, {
            "provider": self.name,
            "source": "error",
            "upstream_source": upstream_source,
            "errors": errors + ["official_trading_signal_empty"],
            "upstream_errors": errors + ["official_trading_signal_empty"],
            "cache_seconds": self.settings.trading_signal_cache_seconds,
            "rows_seen": len(rows),
        }

    async def token_info(self, *, symbol: str = "", chain_id: str = "", address: str = "") -> tuple[list[dict[str, Any]], SkillMeta]:
        if not self.settings.official_base_url:
            return [], self._unsupported_meta(self.settings.token_info_cache_seconds, "official_base_url_missing")
        payload, upstream_source, errors = await self._fetch_json(
            path=self.settings.token_info_path,
            timeout=self.settings.token_info_timeout,
            params=self._token_search_params(symbol=symbol, chain_id=chain_id, address=address),
        )
        items = []
        raw_items = extract_items(payload)
        for row in raw_items:
            enriched = await self._enrich_token_info_row(row)
            normalized = normalize_token_info_row(enriched, symbol=symbol, chain_id=chain_id, address=address)
            if normalized is not None:
                items.append(normalized)
        return items, {
            "provider": self.name,
            "source": "official_skill" if items else "error",
            "upstream_source": upstream_source,
            "errors": errors,
            "upstream_errors": list(errors),
            "cache_seconds": self.settings.token_info_cache_seconds,
        }

    async def token_audit(self, *, symbol: str = "", chain_id: str = "", address: str = "") -> tuple[list[dict[str, Any]], SkillMeta]:
        if not self.settings.official_base_url:
            return [], self._unsupported_meta(self.settings.token_audit_cache_seconds, "official_base_url_missing")
        payload: Any = {}
        upstream_source = self.settings.token_audit_path
        errors: list[str] = []
        if address and chain_id:
            payload, upstream_source, errors = await self._post_json(
                path=self.settings.token_audit_path,
                timeout=self.settings.token_audit_timeout,
                payload={
                    "chainId": chain_id,
                    "contractAddress": address,
                    "requestId": str(uuid4()),
                },
            )
        items = []
        for row in extract_items(payload):
            normalized = normalize_token_audit_row(row, symbol=symbol, chain_id=chain_id, address=address)
            if normalized is not None:
                items.append(normalized)
        if not items:
            fallback_items, fallback_errors = await self._derive_token_audit(symbol=symbol, chain_id=chain_id, address=address)
            if fallback_items:
                items = fallback_items
                errors.extend(fallback_errors)
                upstream_source = f"{upstream_source}|derived_from_token_info"
        return items, {
            "provider": self.name,
            "source": "official_skill" if items else "error",
            "upstream_source": upstream_source,
            "errors": errors,
            "upstream_errors": list(errors),
            "cache_seconds": self.settings.token_audit_cache_seconds,
        }

    async def address_info(self, *, address: str = "", chain_id: str = "") -> tuple[list[dict[str, Any]], SkillMeta]:
        if not self.settings.official_base_url:
            return [], self._unsupported_meta(self.settings.address_info_cache_seconds, "official_base_url_missing")
        payload, upstream_source, errors = await self._fetch_json(
            path=self.settings.address_info_path,
            timeout=self.settings.address_info_timeout,
            params=self._query_params(address=address, chain_id=chain_id),
        )
        items = []
        for row in extract_items(payload):
            normalized = normalize_address_info_row(row, address=address, chain_id=chain_id)
            if normalized is not None:
                items.append(normalized)
        return items, {
            "provider": self.name,
            "source": "official_skill" if items else "error",
            "upstream_source": upstream_source,
            "errors": errors,
            "upstream_errors": list(errors),
            "cache_seconds": self.settings.address_info_cache_seconds,
        }

    async def _fetch_json(self, *, path: str, timeout: float, params: dict[str, Any] | None = None) -> tuple[Any, str, list[str]]:
        url = f"{self.settings.official_base_url}{path}"
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "User-Agent": self.settings.official_user_agent,
        }
        errors: list[str] = []
        try:
            async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
                response = await client.get(url, params=params or {})
                response.raise_for_status()
                return response.json(), path, errors
        except Exception as exc:
            errors.append(f"client:{type(exc).__name__}")
            return {}, path, errors

    async def _post_json(self, *, path: str, timeout: float, payload: dict[str, Any]) -> tuple[Any, str, list[str]]:
        url = f"{self.settings.official_base_url}{path}"
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Accept-Encoding": "identity",
            "User-Agent": self.settings.official_user_agent,
        }
        errors: list[str] = []
        try:
            async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
                return response.json(), path, errors
        except Exception as exc:
            errors.append(f"client:{type(exc).__name__}")
            return {}, path, errors

    async def _fallback(self, reason: str, loader, existing_errors: list[str] | None = None):
        pair_map, meta = await loader()
        errors = list(existing_errors or [])
        errors.append(reason)
        fallback_meta = dict(meta)
        fallback_meta["provider"] = self.name
        fallback_meta["source"] = f"official_skill_fallback:{meta.get('source', 'direct')}"
        fallback_meta["upstream_source"] = meta.get("source", "direct")
        fallback_meta["errors"] = errors
        fallback_meta["upstream_errors"] = errors + list(meta.get("errors", []) or [])
        return pair_map, fallback_meta

    def _unsupported_meta(self, cache_seconds: int, reason: str) -> SkillMeta:
        return {
            "provider": self.name,
            "source": "unsupported",
            "upstream_source": "unsupported",
            "errors": [reason],
            "upstream_errors": [reason],
            "cache_seconds": cache_seconds,
        }

    def _official_rank_payload_to_rows(self, payload: Any) -> dict[int, list[dict[str, Any]]]:
        if isinstance(payload, dict):
            items = payload.get("items")
            if isinstance(items, list):
                ranked = [row for row in items if isinstance(row, dict)]
                if ranked and any("rank_score" in row or "trending_rank" in row for row in ranked):
                    rows_by_type: dict[int, list[dict[str, Any]]] = {10: [], 11: [], 20: []}
                    for row in ranked:
                        symbol = row.get("pair") or row.get("symbol") or row.get("tokenSymbol") or ""
                        if not symbol:
                            continue
                        if row.get("trending_rank"):
                            rows_by_type[10].append({"symbol": symbol})
                        if row.get("top_search_rank"):
                            rows_by_type[11].append({"symbol": symbol})
                        if row.get("alpha_rank"):
                            rows_by_type[20].append({"symbol": symbol})
                    if any(rows_by_type.values()):
                        return rows_by_type
            data = payload.get("data")
            if isinstance(data, dict):
                rows_by_type: dict[int, list[dict[str, Any]]] = {}
                for rank_type, field_name in MARKET_RANK_TYPES.items():
                    rows = data.get(field_name) or data.get(str(rank_type))
                    if isinstance(rows, list):
                        rows_by_type[rank_type] = [row for row in rows if isinstance(row, dict)]
                if rows_by_type:
                    return rows_by_type
        rows = extract_rank_rows(payload)
        return {rank_type: rows for rank_type in MARKET_RANK_TYPES} if rows else {}

    def _official_signal_payload_to_rows(self, payload: Any) -> list[dict[str, Any]]:
        rows = extract_signal_rows(payload)
        if rows:
            return rows
        if isinstance(payload, dict):
            items = payload.get("items")
            if isinstance(items, list):
                return [row for row in items if isinstance(row, dict)]
        return []

    def _query_params(self, *, symbol: str = "", chain_id: str = "", address: str = "") -> dict[str, Any]:
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        if chain_id:
            params["chain_id"] = chain_id
        if address:
            params["address"] = address
        return params

    async def _fetch_official_rank_rows(self) -> tuple[dict[int, list[dict[str, Any]]], str, list[str]]:
        rows_by_type: dict[int, list[dict[str, Any]]] = {}
        errors: list[str] = []
        for rank_type in MARKET_RANK_TYPES:
            payload, _, payload_errors = await self._post_json(
                path=self.settings.official_market_rank_path,
                timeout=self.settings.market_rank_timeout,
                payload={
                    "rankType": rank_type,
                    "period": self.settings.market_rank_period,
                    "sortBy": 70,
                    "orderAsc": False,
                    "page": 1,
                    "size": self.settings.market_rank_max_tokens,
                    **({"chainId": self.settings.market_rank_chain_id} if self.settings.market_rank_chain_id else {}),
                },
            )
            rows_by_type[rank_type] = extract_rank_rows(payload)
            if payload_errors:
                errors.extend([f"rank_type_{rank_type}:{err}" for err in payload_errors])
        return rows_by_type, self.settings.official_market_rank_path, errors

    async def _fetch_official_signal_rows(self) -> tuple[list[dict[str, Any]], str, list[str]]:
        path = self.settings.official_trading_signal_path
        if "smart-money" in path:
            payload, _, errors = await self._post_json(
                path=path,
                timeout=self.settings.trading_signal_timeout,
                payload={
                    "smartSignalType": "",
                    "page": 1,
                    "pageSize": max(1, min(100, int(self.settings.trading_signal_max_items))),
                    "chainId": self.settings.trading_signal_chain_id,
                },
            )
            return self._official_signal_payload_to_rows(payload), path, errors
        payload, _, errors = await self._fetch_json(
            path=path,
            timeout=self.settings.trading_signal_timeout,
            params={"page": 1, "size": self.settings.trading_signal_max_items},
        )
        return self._official_signal_payload_to_rows(payload), path, errors

    def _token_search_params(self, *, symbol: str = "", chain_id: str = "", address: str = "") -> dict[str, Any]:
        params: dict[str, Any] = {}
        keyword = address or symbol
        if keyword:
            params["keyword"] = keyword
        if chain_id:
            params["chainIds"] = chain_id
        params["orderBy"] = "volume24h"
        return params

    async def _enrich_token_info_row(self, row: dict[str, Any]) -> dict[str, Any]:
        enriched = dict(row)
        chain = str(row.get("chainId") or row.get("chain") or "").strip()
        contract = str(row.get("contractAddress") or row.get("contract_address") or "").strip()
        symbol = _normalize_symbol(row.get("symbol") or row.get("ticker") or "")
        if chain and contract:
            meta_payload, _, _ = await self._fetch_json(
                path="/bapi/defi/v1/public/wallet-direct/buw/wallet/dex/market/token/meta/info/ai",
                timeout=self.settings.token_info_timeout,
                params={"chainId": chain, "contractAddress": contract},
            )
            dynamic_payload, _, _ = await self._fetch_json(
                path="/bapi/defi/v4/public/wallet-direct/buw/wallet/market/token/dynamic/info/ai",
                timeout=self.settings.token_info_timeout,
                params={"chainId": chain, "contractAddress": contract},
            )
            if isinstance(meta_payload, dict) and isinstance(meta_payload.get("data"), dict):
                enriched.update(meta_payload.get("data") or {})
            if isinstance(dynamic_payload, dict) and isinstance(dynamic_payload.get("data"), dict):
                enriched.update(dynamic_payload.get("data") or {})
        if symbol:
            enriched["isBinanceSpotTradable"] = await self._is_binance_spot_tradable(symbol)
        return enriched

    async def _is_binance_spot_tradable(self, symbol: str) -> bool:
        pair_symbol = f"{symbol.upper()}USDT"
        headers = {
            "Accept": "application/json, text/plain, */*",
            "User-Agent": self.settings.official_user_agent,
        }
        try:
            async with httpx.AsyncClient(timeout=self.settings.token_info_timeout, headers=headers) as client:
                response = await client.get(f"{self.settings.binance_rest_base}/api/v3/exchangeInfo", params={"symbol": pair_symbol})
                response.raise_for_status()
                payload = response.json()
        except Exception:
            return False
        if not isinstance(payload, dict):
            return False
        rows = payload.get("symbols")
        if not isinstance(rows, list) or not rows:
            return False
        row = rows[0]
        if not isinstance(row, dict):
            return False
        return bool(row.get("isSpotTradingAllowed")) and str(row.get("status", "")).upper() == "TRADING"

    async def _derive_token_audit(self, *, symbol: str = "", chain_id: str = "", address: str = "") -> tuple[list[dict[str, Any]], list[str]]:
        items, meta = await self.token_info(symbol=symbol, chain_id=chain_id, address=address)
        if not items:
            return [], ["token_info_empty_for_audit"]
        derived: list[dict[str, Any]] = []
        for item in items:
            risk_level_raw = item.get("riskLevel") or item.get("risk_level")
            top10 = _float_like(item.get("top10_holder_share"))
            classification = "watch"
            flags: list[str] = []
            score = 0.65
            if str(risk_level_raw).strip().lower() in {"3", "high"}:
                classification = "caution"
                score = 0.35
                flags.append("risk_level_high")
            elif str(risk_level_raw).strip().lower() in {"2", "medium"}:
                classification = "watch"
                score = 0.55
                flags.append("risk_level_medium")
            elif str(risk_level_raw).strip().lower() in {"1", "low"}:
                classification = "safe"
                score = 0.85
            if top10 >= 0.9:
                classification = "avoid"
                score = min(score, 0.2)
                flags.append("holder_concentration_extreme")
            elif top10 >= 0.75 and classification == "safe":
                classification = "watch"
                score = min(score, 0.6)
                flags.append("holder_concentration_elevated")
            derived.append(
                {
                    "contract_address": item.get("contract_address", ""),
                    "chain": item.get("chain", ""),
                    "classification": classification,
                    "flags": flags,
                    "score": round(score, 6),
                }
            )
        return derived, [f"derived_from_token_info:{meta.get('source', '')}"]


class SkillService:
    def __init__(self, settings: SkillSettings | None = None):
        self.settings = settings or SkillSettings()
        self.direct_provider = DirectBinanceProvider(self.settings)
        self.official_provider = OfficialSkillProvider(self.settings, self.direct_provider)
        self.provider_name = self.settings.provider_name if self.settings.provider_name in {"direct", "official_skill"} else "direct"
        self._market_rank_cache: dict[str, Any] = {"ts": 0.0, "pairs": {}, "meta": {"source": "disabled", "errors": []}}
        self._trading_signal_cache: dict[str, Any] = {"ts": 0.0, "pairs": {}, "meta": {"source": "disabled", "errors": []}}
        self._token_info_cache: dict[str, Any] = {"items": {}, "meta": {}}
        self._token_audit_cache: dict[str, Any] = {"items": {}, "meta": {}}
        self._address_info_cache: dict[str, Any] = {"items": {}, "meta": {}}
        self._market_rank_lock = Lock()
        self._trading_signal_lock = Lock()
        self._token_info_lock = Lock()
        self._token_audit_lock = Lock()
        self._address_info_lock = Lock()

    @property
    def active_provider(self) -> SkillProvider:
        if self.provider_name == "official_skill":
            return self.official_provider
        return self.direct_provider

    async def load_market_rank(self, force_refresh: bool = False) -> tuple[dict[str, dict[str, Any]], SkillMeta]:
        if not self.settings.market_rank_enabled:
            return {}, {"provider": self.provider_name, "source": "disabled", "errors": [], "upstream_errors": []}

        now = time.time()
        with self._market_rank_lock:
            cached_ts = float(self._market_rank_cache.get("ts", 0.0) or 0.0)
            cached_pairs = dict(self._market_rank_cache.get("pairs", {}) or {})
            cached_meta = dict(self._market_rank_cache.get("meta", {}) or {})
            if (not force_refresh) and cached_pairs and (now - cached_ts) < self.settings.market_rank_cache_seconds:
                return cached_pairs, cached_meta

        pair_map, meta = await self.active_provider.market_rank()
        if not pair_map and cached_pairs:
            meta = dict(meta)
            meta["source"] = "stale_cache"
            pair_map = cached_pairs
        with self._market_rank_lock:
            self._market_rank_cache["ts"] = now
            self._market_rank_cache["pairs"] = pair_map
            self._market_rank_cache["meta"] = meta
        return pair_map, meta

    async def load_trading_signal(self, force_refresh: bool = False) -> tuple[dict[str, dict[str, Any]], SkillMeta]:
        if not self.settings.trading_signal_enabled:
            return {}, {"provider": self.provider_name, "source": "disabled", "errors": [], "upstream_errors": []}

        now = time.time()
        with self._trading_signal_lock:
            cached_ts = float(self._trading_signal_cache.get("ts", 0.0) or 0.0)
            cached_pairs = dict(self._trading_signal_cache.get("pairs", {}) or {})
            cached_meta = dict(self._trading_signal_cache.get("meta", {}) or {})
            if (not force_refresh) and cached_pairs and (now - cached_ts) < self.settings.trading_signal_cache_seconds:
                return cached_pairs, cached_meta

        pair_map, meta = await self.active_provider.trading_signal()
        if not pair_map and cached_pairs:
            meta = dict(meta)
            meta["source"] = "stale_cache"
            pair_map = cached_pairs
        with self._trading_signal_lock:
            self._trading_signal_cache["ts"] = now
            self._trading_signal_cache["pairs"] = pair_map
            self._trading_signal_cache["meta"] = meta
        return pair_map, meta

    async def load_token_info(self, *, symbol: str = "", chain_id: str = "", address: str = "", force_refresh: bool = False) -> tuple[list[dict[str, Any]], SkillMeta]:
        if not self.settings.token_info_enabled:
            return [], {"provider": self.provider_name, "source": "disabled", "errors": [], "upstream_errors": []}
        return await self._load_items(
            cache=self._token_info_cache,
            lock=self._token_info_lock,
            cache_seconds=self.settings.token_info_cache_seconds,
            key=self._cache_key(symbol=symbol, chain_id=chain_id, address=address),
            loader=lambda: self.official_provider.token_info(symbol=symbol, chain_id=chain_id, address=address),
            force_refresh=force_refresh,
        )

    async def load_token_audit(self, *, symbol: str = "", chain_id: str = "", address: str = "", force_refresh: bool = False) -> tuple[list[dict[str, Any]], SkillMeta]:
        if not self.settings.token_audit_enabled:
            return [], {"provider": self.provider_name, "source": "disabled", "errors": [], "upstream_errors": []}
        return await self._load_items(
            cache=self._token_audit_cache,
            lock=self._token_audit_lock,
            cache_seconds=self.settings.token_audit_cache_seconds,
            key=self._cache_key(symbol=symbol, chain_id=chain_id, address=address),
            loader=lambda: self.official_provider.token_audit(symbol=symbol, chain_id=chain_id, address=address),
            force_refresh=force_refresh,
        )

    async def load_address_info(self, *, address: str = "", chain_id: str = "", force_refresh: bool = False) -> tuple[list[dict[str, Any]], SkillMeta]:
        if not self.settings.address_info_enabled:
            return [], {"provider": self.provider_name, "source": "disabled", "errors": [], "upstream_errors": []}
        return await self._load_items(
            cache=self._address_info_cache,
            lock=self._address_info_lock,
            cache_seconds=self.settings.address_info_cache_seconds,
            key=self._cache_key(chain_id=chain_id, address=address),
            loader=lambda: self.official_provider.address_info(address=address, chain_id=chain_id),
            force_refresh=force_refresh,
        )

    async def _load_items(self, *, cache: dict[str, Any], lock: Lock, cache_seconds: int, key: str, loader, force_refresh: bool) -> tuple[list[dict[str, Any]], SkillMeta]:
        now = time.time()
        with lock:
            bucket = dict(cache.get("items", {}) or {})
            item = bucket.get(key)
            if item and (not force_refresh) and (now - float(item.get("ts", 0.0) or 0.0)) < cache_seconds:
                return list(item.get("value", []) or []), dict(item.get("meta", {}) or {})
        items, meta = await loader()
        with lock:
            bucket = dict(cache.get("items", {}) or {})
            bucket[key] = {"ts": now, "value": items, "meta": meta}
            cache["items"] = bucket
        return items, meta

    def _cache_key(self, *, symbol: str = "", chain_id: str = "", address: str = "") -> str:
        return f"symbol={symbol.strip().upper()}|chain={chain_id.strip().upper()}|address={address.strip().lower()}"
