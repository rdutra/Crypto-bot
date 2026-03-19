import logging
import json
import os
import re
import time
from collections import deque
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict, List, Literal

import httpx
from fastapi import FastAPI
from pydantic import BaseModel, Field

from debug_store import LlmDebugStore

app = FastAPI(title="bot-api", version="1.0.0")
logger = logging.getLogger("bot-api")

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1:8b")
OLLAMA_TIMEOUT = float(os.getenv("OLLAMA_TIMEOUT", "30"))


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


LLM_DEBUG_ENABLED = str(os.getenv("LLM_DEBUG_ENABLED", "true")).strip().lower() in {"1", "true", "yes", "on"}
LLM_DEBUG_MAX_ENTRIES = _env_int("LLM_DEBUG_MAX_ENTRIES", 250, 20, 2000)
LLM_DEBUG_PROMPT_MAX_CHARS = _env_int("LLM_DEBUG_PROMPT_MAX_CHARS", 8000, 200, 50000)
LLM_DEBUG_RESPONSE_MAX_CHARS = _env_int("LLM_DEBUG_RESPONSE_MAX_CHARS", 8000, 200, 50000)
LLM_DEBUG_DB_PATH = os.getenv("LLM_DEBUG_DB_PATH", "/app/data/llm-debug.sqlite").strip() or "/app/data/llm-debug.sqlite"
LLM_DEBUG_DB_MAX_ROWS = _env_int("LLM_DEBUG_DB_MAX_ROWS", 50000, 1000, 2000000)
LLM_CALL_LOG: deque[dict] = deque(maxlen=LLM_DEBUG_MAX_ENTRIES)
LLM_CALL_LOCK = Lock()
LLM_DEBUG_STORE = LlmDebugStore(
    enabled=LLM_DEBUG_ENABLED,
    db_path=LLM_DEBUG_DB_PATH,
    max_rows=LLM_DEBUG_DB_MAX_ROWS,
)

MARKET_RANK_SKILL_ENABLED = str(os.getenv("LLM_MARKET_RANK_SKILL_ENABLED", "false")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
MARKET_RANK_BASE_URL = os.getenv("LLM_MARKET_RANK_BASE_URL", "https://web3.binance.com").rstrip("/")
BINANCE_REST_BASE = os.getenv("BINANCE_REST_BASE", "https://api.binance.com").rstrip("/")
MARKET_RANK_TIMEOUT = _env_float("LLM_MARKET_RANK_TIMEOUT_SECONDS", 6.0, 1.0, 30.0)
MARKET_RANK_CACHE_SECONDS = _env_int("LLM_MARKET_RANK_CACHE_SECONDS", 300, 10, 3600)
MARKET_RANK_MAX_TOKENS = _env_int("LLM_MARKET_RANK_MAX_TOKENS", 120, 20, 200)
MARKET_RANK_PERIOD = _env_int("LLM_MARKET_RANK_PERIOD", 50, 10, 50)
MARKET_RANK_QUOTE = os.getenv("LLM_MARKET_RANK_QUOTE", "USDT").strip().upper() or "USDT"
MARKET_RANK_CHAIN_ID = os.getenv("LLM_MARKET_RANK_CHAIN_ID", "").strip()
MARKET_RANK_USER_AGENT = os.getenv("LLM_MARKET_RANK_USER_AGENT", "binance-web3/2.0 (Skill)").strip()
MARKET_RANK_UNIFIED_ENDPOINT = os.getenv(
    "LLM_MARKET_RANK_UNIFIED_ENDPOINT",
    "/bapi/defi/v1/public/wallet-direct/buw/wallet/market/token/pulse/unified/rank/list",
).strip()
MARKET_RANK_EXCLUDE_REGEX = os.getenv(
    "LLM_MARKET_RANK_EXCLUDE_REGEX",
    r"(UP|DOWN|BULL|BEAR|1000|[0-9][0-9][0-9]+L|[0-9][0-9][0-9]+S)",
).strip()
try:
    MARKET_RANK_EXCLUDE_RE = re.compile(MARKET_RANK_EXCLUDE_REGEX, re.IGNORECASE)
except re.error:
    MARKET_RANK_EXCLUDE_RE = re.compile(r"(UP|DOWN|BULL|BEAR)", re.IGNORECASE)

MARKET_RANK_TYPES: Dict[int, str] = {
    10: "trending_rank",
    11: "top_search_rank",
    20: "alpha_rank",
}
MARKET_RANK_CACHE: Dict[str, Any] = {
    "ts": 0.0,
    "pairs": {},
    "meta": {"source": "disabled", "errors": []},
}
MARKET_RANK_CACHE_LOCK = Lock()

TRADING_SIGNAL_SKILL_ENABLED = str(os.getenv("LLM_TRADING_SIGNAL_SKILL_ENABLED", "false")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
TRADING_SIGNAL_BASE_URL = os.getenv("LLM_TRADING_SIGNAL_BASE_URL", "https://web3.binance.com").rstrip("/")
TRADING_SIGNAL_TIMEOUT = _env_float("LLM_TRADING_SIGNAL_TIMEOUT_SECONDS", 6.0, 1.0, 30.0)
TRADING_SIGNAL_CACHE_SECONDS = _env_int("LLM_TRADING_SIGNAL_CACHE_SECONDS", 180, 10, 3600)
TRADING_SIGNAL_MAX_ITEMS = _env_int("LLM_TRADING_SIGNAL_MAX_ITEMS", 120, 20, 300)
TRADING_SIGNAL_QUOTE = os.getenv("LLM_TRADING_SIGNAL_QUOTE", "USDT").strip().upper() or "USDT"
TRADING_SIGNAL_MIN_SCORE = _env_float("LLM_TRADING_SIGNAL_MIN_SCORE", 0.25, 0.0, 1.0)
TRADING_SIGNAL_BUY_BONUS_MULT = _env_float("LLM_TRADING_SIGNAL_BUY_BONUS_MULT", 0.9, 0.0, 2.0)
TRADING_SIGNAL_SELL_PENALTY_MULT = _env_float("LLM_TRADING_SIGNAL_SELL_PENALTY_MULT", 0.0, 0.0, 2.0)
TRADING_SIGNAL_CHAIN_ID = os.getenv("LLM_TRADING_SIGNAL_CHAIN_ID", "56").strip() or "56"
TRADING_SIGNAL_USER_AGENT = os.getenv("LLM_TRADING_SIGNAL_USER_AGENT", "binance-web3/1.0 (Skill)").strip()
TRADING_SIGNAL_EXCLUDE_REGEX = os.getenv(
    "LLM_TRADING_SIGNAL_EXCLUDE_REGEX",
    r"(UP|DOWN|BULL|BEAR|1000|[0-9][0-9][0-9]+L|[0-9][0-9][0-9]+S)",
).strip()
try:
    TRADING_SIGNAL_EXCLUDE_RE = re.compile(TRADING_SIGNAL_EXCLUDE_REGEX, re.IGNORECASE)
except re.error:
    TRADING_SIGNAL_EXCLUDE_RE = re.compile(r"(UP|DOWN|BULL|BEAR)", re.IGNORECASE)

TRADING_SIGNAL_ENDPOINTS_RAW = os.getenv(
    "LLM_TRADING_SIGNAL_ENDPOINTS",
    "/bapi/defi/v1/public/wallet-direct/buw/wallet/web/signal/smart-money,"
    "/friendly/gateway/v1/public/alpha-trade/market/signal/list,"
    "/friendly/gateway/v1/public/alpha-trade/trading-signal/list,"
    "/friendly/gateway/v1/public/alpha-trade/whitelist/trading-signal/list",
)
TRADING_SIGNAL_ENDPOINTS = [part.strip() for part in TRADING_SIGNAL_ENDPOINTS_RAW.split(",") if part.strip()]
TRADING_SIGNAL_CACHE: Dict[str, Any] = {
    "ts": 0.0,
    "pairs": {},
    "meta": {"source": "disabled", "errors": []},
}
TRADING_SIGNAL_CACHE_LOCK = Lock()

RegimeLiteral = Literal["trend_pullback", "breakout", "mean_reversion", "chaotic", "no_trade"]
RiskLiteral = Literal["low", "medium", "high"]
PolicyProfileLiteral = Literal["defensive", "normal", "offensive"]


class RegimeRequest(BaseModel):
    pair: str
    timeframe: str
    price: float
    ema_20: float
    ema_50: float
    ema_200: float
    rsi_14: float
    adx_14: float
    atr_pct: float
    volume_zscore: float
    trend_4h: str
    market_structure: str


class RegimeDecision(BaseModel):
    regime: RegimeLiteral
    risk_level: RiskLiteral
    confidence: float = Field(ge=0.0, le=1.0)
    note: str = Field(min_length=1, max_length=220)


class PairCandidate(BaseModel):
    pair: str
    timeframe: str
    price: float
    ema_20: float
    ema_50: float
    ema_200: float
    rsi_14: float
    adx_14: float
    atr_pct: float
    volume_zscore: float
    trend_4h: str
    market_structure: str
    deterministic_score: float = Field(default=0.0, ge=0.0, le=100.0)


class RankPairsRequest(BaseModel):
    candidates: List[PairCandidate] = Field(min_length=1, max_length=40)
    top_n: int = Field(default=3, ge=1, le=20)
    min_confidence: float = Field(default=0.6, ge=0.0, le=1.0)
    allowed_risk_levels: List[RiskLiteral] = Field(default_factory=lambda: ["low", "medium"])
    allowed_regimes: List[RegimeLiteral] = Field(default_factory=lambda: ["trend_pullback"])


class LlmRankDecision(BaseModel):
    pair: str
    regime: RegimeLiteral
    risk_level: RiskLiteral
    confidence: float = Field(ge=0.0, le=1.0)
    note: str = Field(min_length=1, max_length=220)


class RankedPair(BaseModel):
    pair: str
    regime: RegimeLiteral
    risk_level: RiskLiteral
    confidence: float = Field(ge=0.0, le=1.0)
    note: str = Field(min_length=1, max_length=220)
    deterministic_score: float = Field(ge=0.0)
    market_rank_score: float = Field(default=0.0, ge=0.0, le=1.0)
    trading_signal_side: Literal["buy", "sell", "neutral"] = "neutral"
    trading_signal_score: float = Field(default=0.0, ge=0.0, le=1.0)
    final_score: float


class RankPairsResponse(BaseModel):
    selected_pairs: List[str]
    decisions: List[RankedPair]
    source: Literal["llm", "fallback"]
    reason: str | None = None
    market_rank_source: str | None = None
    market_rank_errors: List[str] = Field(default_factory=list)
    trading_signal_source: str | None = None
    trading_signal_errors: List[str] = Field(default_factory=list)


class RuntimePolicyRequest(BaseModel):
    lookback_hours: float = Field(default=24.0, ge=1.0, le=336.0)
    closed_trades: int = Field(default=0, ge=0)
    win_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    avg_profit_pct: float = Field(default=0.0, ge=-100.0, le=100.0)
    net_profit_pct: float = Field(default=0.0, ge=-100.0, le=100.0)
    max_drawdown_pct: float = Field(default=0.0, ge=-100.0, le=0.0)
    open_trades: int = Field(default=0, ge=0, le=50)
    spike_allowed_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    market_note: str = Field(default="", max_length=160)


class RuntimePolicyDecision(BaseModel):
    profile: PolicyProfileLiteral
    confidence: float = Field(ge=0.0, le=1.0)
    note: str = Field(min_length=1, max_length=220)
    aggr_entry_strictness: Literal["strict", "normal"]
    risk_stake_multiplier: float = Field(ge=0.1, le=1.0)
    risk_max_open_trades: int = Field(ge=1, le=5)
    source: Literal["llm", "fallback"] = "llm"
    reason: str | None = None


def _clip_text(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return f"{value[:limit]}...[truncated]"


def _record_llm_call(
    *,
    endpoint: str,
    prompt: str,
    raw_response: str,
    parsed_ok: bool,
    error: str | None = None,
) -> None:
    if not LLM_DEBUG_ENABLED:
        return

    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "endpoint": endpoint,
        "model": OLLAMA_MODEL,
        "parsed_ok": bool(parsed_ok),
        "error": (error or "")[:240],
        "prompt": _clip_text(prompt, LLM_DEBUG_PROMPT_MAX_CHARS),
        "response": _clip_text(raw_response, LLM_DEBUG_RESPONSE_MAX_CHARS),
    }
    with LLM_CALL_LOCK:
        LLM_CALL_LOG.append(entry)
    LLM_DEBUG_STORE.insert(entry)


def _dump_llm_calls(limit: int, endpoint: str | None = None) -> tuple[list[dict], int]:
    if LLM_DEBUG_STORE.available:
        items, total = LLM_DEBUG_STORE.fetch(limit=limit, endpoint=endpoint)
        if total > 0:
            return items, total

    with LLM_CALL_LOCK:
        snapshot = list(LLM_CALL_LOG)

    if endpoint:
        endpoint_norm = endpoint.strip().lower()
        filtered = [item for item in snapshot if str(item.get("endpoint", "")).lower() == endpoint_norm]
    else:
        filtered = snapshot
    total = len(filtered)
    return list(reversed(filtered[-limit:])), total


@app.on_event("shutdown")
def _on_shutdown() -> None:
    LLM_DEBUG_STORE.close()


def _fallback(note: str) -> RegimeDecision:
    return RegimeDecision(
        regime="no_trade",
        risk_level="high",
        confidence=0.0,
        note=note[:220],
    )


def _extract_json_candidates(raw_text: str) -> List[str]:
    candidates = [raw_text.strip()]

    # Some model outputs wrap JSON with prose; extract the first object block.
    match = re.search(r"\{.*\}", raw_text, re.DOTALL)
    if match:
        candidates.append(match.group(0).strip())

    match_arr = re.search(r"\[.*\]", raw_text, re.DOTALL)
    if match_arr:
        candidates.append(match_arr.group(0).strip())

    return candidates


def _parse_ollama_json(raw_text: str) -> RegimeDecision | None:
    candidates = _extract_json_candidates(raw_text)
    for candidate in candidates:
        if not candidate:
            continue
        try:
            data = json.loads(candidate)
            return RegimeDecision.model_validate(data)
        except Exception:
            continue
    return None


def _parse_rank_output(raw_text: str) -> Dict[str, LlmRankDecision] | None:
    for candidate in _extract_json_candidates(raw_text):
        if not candidate:
            continue
        try:
            data = json.loads(candidate)
        except Exception:
            continue

        if isinstance(data, dict):
            items = data.get("decisions")
        elif isinstance(data, list):
            items = data
        else:
            continue

        if not isinstance(items, list):
            continue

        parsed: Dict[str, LlmRankDecision] = {}
        for item in items:
            try:
                decision = LlmRankDecision.model_validate(item)
            except Exception:
                continue
            parsed[decision.pair.upper()] = decision

        if parsed:
            return parsed

    return None


def _parse_policy_output(raw_text: str) -> RuntimePolicyDecision | None:
    for candidate in _extract_json_candidates(raw_text):
        if not candidate:
            continue
        try:
            data = json.loads(candidate)
            decision = RuntimePolicyDecision.model_validate(data)
            return decision
        except Exception:
            continue
    return None


def _pair_from_token_symbol(symbol: str) -> str:
    cleaned = re.sub(r"[^A-Z0-9]", "", str(symbol).strip().upper())
    if not cleaned:
        return ""
    if cleaned.endswith(MARKET_RANK_QUOTE) and len(cleaned) > len(MARKET_RANK_QUOTE):
        cleaned = cleaned[: -len(MARKET_RANK_QUOTE)]
    return f"{cleaned}/{MARKET_RANK_QUOTE}"


def _rank_strength(rank_value: Any) -> float:
    try:
        rank = int(rank_value)
    except (TypeError, ValueError):
        return 0.0
    if rank <= 0:
        return 0.0
    return max(0.0, 1.0 - ((rank - 1) / float(max(1, MARKET_RANK_MAX_TOKENS))))


def _extract_rank_rows(payload: Any) -> List[dict]:
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
    return []


async def _fetch_rank_type(client: httpx.AsyncClient, rank_type: int) -> List[dict]:
    url = f"{MARKET_RANK_BASE_URL}{MARKET_RANK_UNIFIED_ENDPOINT}"
    request_payload: Dict[str, Any] = {
        "rankType": rank_type,
        "period": MARKET_RANK_PERIOD,
        "sortBy": 70,
        "orderAsc": False,
        "page": 1,
        "size": MARKET_RANK_MAX_TOKENS,
    }
    if MARKET_RANK_CHAIN_ID:
        request_payload["chainId"] = MARKET_RANK_CHAIN_ID

    response = await client.post(url, json=request_payload)
    response.raise_for_status()
    return _extract_rank_rows(response.json())


def _ticker_to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _is_supported_spot_symbol(symbol: str) -> bool:
    sym = str(symbol).strip().upper()
    if not sym.endswith(MARKET_RANK_QUOTE):
        return False
    base = sym[: -len(MARKET_RANK_QUOTE)]
    if len(base) < 2:
        return False
    if MARKET_RANK_EXCLUDE_RE.search(base):
        return False
    return True


def _build_spot_fallback_rows(tickers: List[dict]) -> Dict[int, List[dict]]:
    normalized: List[dict] = []
    for row in tickers:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).upper()
        if not _is_supported_spot_symbol(symbol):
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

    by_volume = sorted(normalized, key=lambda item: item["quote_volume"], reverse=True)[:MARKET_RANK_MAX_TOKENS]
    by_abs_change = sorted(by_volume, key=lambda item: abs(item["price_change_pct"]), reverse=True)
    by_momentum = sorted(by_volume, key=lambda item: item["momentum"], reverse=True)

    return {
        10: [{"symbol": item["symbol"]} for item in by_volume],
        11: [{"symbol": item["symbol"]} for item in by_abs_change],
        20: [{"symbol": item["symbol"]} for item in by_momentum],
    }


async def _fetch_spot_fallback_rows(client: httpx.AsyncClient) -> Dict[int, List[dict]]:
    response = await client.get(f"{BINANCE_REST_BASE}/api/v3/ticker/24hr")
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        return {}
    return _build_spot_fallback_rows(payload)


def _trading_signal_pair_from_symbol(symbol: str) -> str:
    cleaned = re.sub(r"[^A-Z0-9]", "", str(symbol).strip().upper())
    if not cleaned:
        return ""
    if cleaned.endswith(TRADING_SIGNAL_QUOTE) and len(cleaned) > len(TRADING_SIGNAL_QUOTE):
        cleaned = cleaned[: -len(TRADING_SIGNAL_QUOTE)]
    return f"{cleaned}/{TRADING_SIGNAL_QUOTE}"


def _is_supported_signal_symbol(symbol: str) -> bool:
    sym = str(symbol).strip().upper()
    if sym.endswith(TRADING_SIGNAL_QUOTE):
        base = sym[: -len(TRADING_SIGNAL_QUOTE)]
    else:
        base = sym
    if len(base) < 2:
        return False
    if TRADING_SIGNAL_EXCLUDE_RE.search(base):
        return False
    return True


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


def _signal_score_from_row(row: dict) -> float:
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


def _extract_signal_rows(payload: Any) -> List[dict]:
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


async def _fetch_trading_signal_rows_from_path(client: httpx.AsyncClient, path: str) -> List[dict]:
    url = f"{TRADING_SIGNAL_BASE_URL}{path}"
    max_items = max(1, min(100, int(TRADING_SIGNAL_MAX_ITEMS)))
    if "wallet/web/signal/smart-money" in path:
        methods: List[tuple[str, Dict[str, Any] | None]] = [
            (
                "POST",
                {
                    "smartSignalType": "",
                    "page": 1,
                    "pageSize": max_items,
                    "chainId": TRADING_SIGNAL_CHAIN_ID,
                },
            )
        ]
    else:
        methods = [
            ("GET", None),
            ("POST", {"page": 1, "size": TRADING_SIGNAL_MAX_ITEMS}),
            ("POST", {"limit": TRADING_SIGNAL_MAX_ITEMS}),
        ]

    for method, payload in methods:
        try:
            if method == "GET":
                response = await client.get(url, params={"page": 1, "size": TRADING_SIGNAL_MAX_ITEMS})
            else:
                response = await client.post(url, json=payload)
            response.raise_for_status()
            rows = _extract_signal_rows(response.json())
            if rows:
                return rows
        except Exception:
            continue

    return []


def _build_trading_signal_context(rows: List[dict]) -> Dict[str, dict]:
    context: Dict[str, dict] = {}
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
        if not _is_supported_signal_symbol(symbol_raw):
            continue
        pair = _trading_signal_pair_from_symbol(symbol_raw)
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
            score = max(0.0, 1.0 - ((idx - 1) / float(max(1, TRADING_SIGNAL_MAX_ITEMS))))
        confidence = max(0.0, min(1.0, _float_like(row.get("confidence")) or score))
        note = str(row.get("note") or row.get("reason") or row.get("tag") or "trading_signal")[:160]

        if score < TRADING_SIGNAL_MIN_SCORE:
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


def _build_spot_trading_signal_rows(tickers: List[dict]) -> List[dict]:
    normalized: List[dict] = []
    for row in tickers:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).upper()
        if not _is_supported_signal_symbol(symbol):
            continue
        quote_volume = _ticker_to_float(row.get("quoteVolume"), 0.0)
        if quote_volume <= 0.0:
            continue
        change_pct = _ticker_to_float(row.get("priceChangePercent"), 0.0)
        normalized.append({"symbol": symbol, "quote_volume": quote_volume, "change_pct": change_pct})

    if not normalized:
        return []

    max_quote_volume = max(item["quote_volume"] for item in normalized) or 1.0
    signal_rows: List[dict] = []
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
    return signal_rows[:TRADING_SIGNAL_MAX_ITEMS]


async def _fetch_spot_trading_signal_rows(client: httpx.AsyncClient) -> List[dict]:
    response = await client.get(f"{BINANCE_REST_BASE}/api/v3/ticker/24hr")
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        return []
    return _build_spot_trading_signal_rows(payload)


async def _load_trading_signal_context(force_refresh: bool = False) -> tuple[Dict[str, dict], Dict[str, Any]]:
    if not TRADING_SIGNAL_SKILL_ENABLED:
        return {}, {"source": "disabled", "errors": []}

    now = time.time()
    with TRADING_SIGNAL_CACHE_LOCK:
        cached_ts = float(TRADING_SIGNAL_CACHE.get("ts", 0.0) or 0.0)
        cached_pairs = dict(TRADING_SIGNAL_CACHE.get("pairs", {}) or {})
        cached_meta = dict(TRADING_SIGNAL_CACHE.get("meta", {}) or {})
        if (not force_refresh) and cached_pairs and (now - cached_ts) < TRADING_SIGNAL_CACHE_SECONDS:
            return cached_pairs, cached_meta

    errors: List[str] = []
    rows: List[dict] = []
    source = "error"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "identity",
        "User-Agent": TRADING_SIGNAL_USER_AGENT,
    }

    try:
        async with httpx.AsyncClient(timeout=TRADING_SIGNAL_TIMEOUT, headers=headers) as client:
            for path in TRADING_SIGNAL_ENDPOINTS:
                path_rows = await _fetch_trading_signal_rows_from_path(client, path)
                if path_rows:
                    rows = path_rows
                    source = f"binance_web3:{path}"
                    break
                errors.append(f"empty:{path}")

            pair_map = _build_trading_signal_context(rows)
            if not pair_map:
                errors.append("web3_empty")
                try:
                    rows = await _fetch_spot_trading_signal_rows(client)
                    pair_map = _build_trading_signal_context(rows)
                    if pair_map:
                        source = "binance_spot_fallback"
                        errors.append("using_spot_fallback")
                except Exception as exc:
                    errors.append(f"spot_fallback:{type(exc).__name__}")
    except Exception as exc:
        errors.append(f"client:{type(exc).__name__}")
        pair_map = {}

    if errors and not pair_map:
        source = "error"
        with TRADING_SIGNAL_CACHE_LOCK:
            cached_pairs = dict(TRADING_SIGNAL_CACHE.get("pairs", {}) or {})
        if cached_pairs:
            source = "stale_cache"
            pair_map = cached_pairs

    meta = {
        "source": source,
        "errors": errors,
        "cache_seconds": TRADING_SIGNAL_CACHE_SECONDS,
        "rows_seen": len(rows),
    }

    with TRADING_SIGNAL_CACHE_LOCK:
        TRADING_SIGNAL_CACHE["ts"] = now
        TRADING_SIGNAL_CACHE["pairs"] = pair_map
        TRADING_SIGNAL_CACHE["meta"] = meta

    return pair_map, meta


def _build_market_rank_context(rows_by_type: Dict[int, List[dict]]) -> Dict[str, dict]:
    pair_map: Dict[str, dict] = {}

    for rank_type, rows in rows_by_type.items():
        field_name = MARKET_RANK_TYPES.get(rank_type)
        if not field_name:
            continue
        for idx, row in enumerate(rows, start=1):
            symbol = (
                row.get("symbol")
                or row.get("tokenSymbol")
                or row.get("baseAsset")
                or row.get("ticker")
                or row.get("symbolName")
                or ""
            )
            pair = _pair_from_token_symbol(str(symbol).split("/")[0])
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
            strength = _rank_strength(rank_value)
            if strength > 0:
                hits += 1
            score += strength * weight
        entry["rank_score"] = round(max(0.0, min(1.0, score)), 4)
        entry["hits"] = hits

    return pair_map


async def _load_market_rank_context(force_refresh: bool = False) -> tuple[Dict[str, dict], Dict[str, Any]]:
    if not MARKET_RANK_SKILL_ENABLED:
        return {}, {"source": "disabled", "errors": []}

    now = time.time()
    with MARKET_RANK_CACHE_LOCK:
        cached_ts = float(MARKET_RANK_CACHE.get("ts", 0.0) or 0.0)
        cached_pairs = dict(MARKET_RANK_CACHE.get("pairs", {}) or {})
        cached_meta = dict(MARKET_RANK_CACHE.get("meta", {}) or {})
        if (not force_refresh) and cached_pairs and (now - cached_ts) < MARKET_RANK_CACHE_SECONDS:
            return cached_pairs, cached_meta

    rows_by_type: Dict[int, List[dict]] = {}
    errors: List[str] = []
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "identity",
        "User-Agent": MARKET_RANK_USER_AGENT,
    }

    try:
        async with httpx.AsyncClient(timeout=MARKET_RANK_TIMEOUT, headers=headers) as client:
            for rank_type in MARKET_RANK_TYPES:
                try:
                    rows_by_type[rank_type] = await _fetch_rank_type(client, rank_type)
                except Exception as exc:
                    rows_by_type[rank_type] = []
                    errors.append(f"rank_type_{rank_type}:{type(exc).__name__}")

            pair_map = _build_market_rank_context(rows_by_type)
            source = "binance_web3"
            if not pair_map:
                errors.append("web3_empty")
                try:
                    rows_by_type = await _fetch_spot_fallback_rows(client)
                    pair_map = _build_market_rank_context(rows_by_type)
                    if pair_map:
                        source = "binance_spot_fallback"
                        errors.append("using_spot_fallback")
                except Exception as exc:
                    errors.append(f"spot_fallback:{type(exc).__name__}")
    except Exception as exc:
        errors.append(f"client:{type(exc).__name__}")
        pair_map = {}
        source = "error"

    if errors and not pair_map:
        source = "error"
        if cached_pairs:
            source = "stale_cache"
            pair_map = cached_pairs

    meta = {
        "source": source,
        "errors": errors,
        "fetched_types": {str(k): len(v) for k, v in rows_by_type.items()},
        "cache_seconds": MARKET_RANK_CACHE_SECONDS,
    }

    with MARKET_RANK_CACHE_LOCK:
        MARKET_RANK_CACHE["ts"] = now
        MARKET_RANK_CACHE["pairs"] = pair_map
        MARKET_RANK_CACHE["meta"] = meta

    return pair_map, meta


def _build_regime_prompt(req: RegimeRequest) -> str:
    payload = req.model_dump()
    return (
        "You are a crypto trading classifier.\n"
        "Return only valid JSON with this exact schema:\n"
        '{"regime":"trend_pullback|breakout|mean_reversion|chaotic|no_trade",'
        '"risk_level":"low|medium|high","confidence":0.0,"note":"short sentence"}\n'
        "Rules:\n"
        "- confidence must be between 0 and 1.\n"
        "- If uncertain, return no_trade with high risk.\n"
        "- Keep note under 140 characters.\n"
        f"Input JSON:\n{json.dumps(payload, separators=(',', ':'))}"
    )


def _build_rank_prompt(
    req: RankPairsRequest,
    market_rank_context: Dict[str, dict] | None = None,
    trading_signal_context: Dict[str, dict] | None = None,
) -> str:
    market_rank_context = market_rank_context or {}
    trading_signal_context = trading_signal_context or {}
    compact_candidates = []
    for candidate in req.candidates:
        rank_info = market_rank_context.get(candidate.pair.upper(), {})
        signal_info = trading_signal_context.get(candidate.pair.upper(), {})
        compact_candidates.append(
            {
                "pair": candidate.pair,
                "timeframe": candidate.timeframe,
                "price": candidate.price,
                "ema_20": candidate.ema_20,
                "ema_50": candidate.ema_50,
                "ema_200": candidate.ema_200,
                "rsi_14": candidate.rsi_14,
                "adx_14": candidate.adx_14,
                "atr_pct": candidate.atr_pct,
                "volume_zscore": candidate.volume_zscore,
                "trend_4h": candidate.trend_4h,
                "market_structure": candidate.market_structure,
                "deterministic_score": candidate.deterministic_score,
                "market_rank_score": rank_info.get("rank_score", 0.0),
                "market_rank_hits": rank_info.get("hits", 0),
                "market_rank_trending": rank_info.get("trending_rank"),
                "market_rank_top_search": rank_info.get("top_search_rank"),
                "market_rank_alpha": rank_info.get("alpha_rank"),
                "trading_signal_side": signal_info.get("side", "neutral"),
                "trading_signal_score": signal_info.get("score", 0.0),
                "trading_signal_confidence": signal_info.get("confidence", 0.0),
                "trading_signal_note": signal_info.get("note", ""),
            }
        )

    return (
        "You are a crypto pair-selection assistant.\n"
        "Return only valid JSON with this schema:\n"
        '{"decisions":[{"pair":"BTC/USDT","regime":"trend_pullback|breakout|mean_reversion|chaotic|no_trade",'
        '"risk_level":"low|medium|high","confidence":0.0,"note":"short sentence"}]}\n'
        "Rules:\n"
        "- Keep one decision per input pair.\n"
        "- confidence must be between 0 and 1.\n"
        "- Prefer trend_pullback when trend and pullback metrics align.\n"
        "- market_rank_score is a secondary prior from Binance market ranks (0..1).\n"
        "- trading_signal_side/score is an extra prior from Binance trading-signal skill.\n"
        "- If uncertain, use no_trade with high risk.\n"
        "- Keep note under 140 characters.\n"
        f"Input JSON:\n{json.dumps({'candidates': compact_candidates}, separators=(',', ':'))}"
    )


def _build_policy_prompt(req: RuntimePolicyRequest) -> str:
    payload = req.model_dump()
    return (
        "You are a risk controller for an automated crypto trading bot.\n"
        "Return only valid JSON with this exact schema:\n"
        '{"profile":"defensive|normal|offensive","confidence":0.0,"note":"short sentence",'
        '"aggr_entry_strictness":"strict|normal","risk_stake_multiplier":0.55,"risk_max_open_trades":2}\n'
        "Rules:\n"
        "- Keep confidence between 0 and 1.\n"
        "- Use defensive when recent performance is weak or unstable.\n"
        "- Keep risk_stake_multiplier between 0.1 and 1.0.\n"
        "- Keep risk_max_open_trades between 1 and 5.\n"
        "- Keep note under 140 characters.\n"
        f"Input JSON:\n{json.dumps(payload, separators=(',', ':'))}"
    )


def _skill_meta_source(meta: Dict[str, Any] | None) -> str:
    if not isinstance(meta, dict):
        return ""
    return str(meta.get("source") or "")


def _skill_meta_errors(meta: Dict[str, Any] | None) -> List[str]:
    if not isinstance(meta, dict):
        return []
    errors = meta.get("errors", [])
    if errors is None:
        return []
    if isinstance(errors, list):
        return [str(err) for err in errors]
    return [str(errors)]


def _rank_fallback(
    req: RankPairsRequest,
    reason: str = "deterministic_fallback",
    market_rank_meta: Dict[str, Any] | None = None,
    trading_signal_meta: Dict[str, Any] | None = None,
    market_rank_context: Dict[str, dict] | None = None,
    trading_signal_context: Dict[str, dict] | None = None,
) -> RankPairsResponse:
    market_rank_context = market_rank_context or {}
    trading_signal_context = trading_signal_context or {}
    fallback_smart_buy_min_score = _env_float("LLM_FALLBACK_SMART_BUY_MIN_SCORE", 0.75, 0.0, 1.0)

    ordered = sorted(req.candidates, key=lambda item: item.deterministic_score, reverse=True)
    decisions: List[RankedPair] = []
    selected: List[str] = []

    for candidate in ordered:
        key = candidate.pair.upper()
        rank_info = market_rank_context.get(key, {})
        signal_info = trading_signal_context.get(key, {})
        market_rank_score = max(0.0, min(1.0, float(rank_info.get("rank_score", 0.0) or 0.0)))
        trading_signal_side = str(signal_info.get("side", "neutral")).strip().lower()
        if trading_signal_side not in {"buy", "sell", "neutral"}:
            trading_signal_side = "neutral"
        trading_signal_score = max(0.0, min(1.0, float(signal_info.get("score", 0.0) or 0.0)))

        trendish = candidate.deterministic_score >= 6.0
        smart_buy = trading_signal_side == "buy" and trading_signal_score >= fallback_smart_buy_min_score
        tradable = trendish or smart_buy

        risk_level: RiskLiteral = "medium" if tradable else "high"
        if trendish:
            regime: RegimeLiteral = "trend_pullback"
        elif smart_buy:
            # Smart-money buy can qualify as a tradable fallback even when deterministic trend score is weaker.
            regime = "mean_reversion"
        else:
            regime = "no_trade"

        confidence = min(0.95, max(0.0, candidate.deterministic_score / 10.0))
        if smart_buy:
            confidence = max(confidence, min(0.95, 0.60 + (trading_signal_score * 0.35)))

        final_score = candidate.deterministic_score + (confidence * 3.0) + (market_rank_score * 0.75)
        if trading_signal_side == "buy":
            final_score += trading_signal_score * TRADING_SIGNAL_BUY_BONUS_MULT
        elif trading_signal_side == "sell" and TRADING_SIGNAL_SELL_PENALTY_MULT > 0.0:
            final_score -= trading_signal_score * TRADING_SIGNAL_SELL_PENALTY_MULT

        note = "fallback_det_rank_smart_buy" if smart_buy and not trendish else "fallback_det_rank"

        decisions.append(
            RankedPair(
                pair=candidate.pair,
                regime=regime,
                risk_level=risk_level,
                confidence=confidence,
                note=note,
                deterministic_score=candidate.deterministic_score,
                market_rank_score=market_rank_score,
                trading_signal_side=trading_signal_side,
                trading_signal_score=trading_signal_score,
                final_score=final_score,
            )
        )

    decisions.sort(key=lambda item: item.final_score, reverse=True)

    allowed_risk = set(req.allowed_risk_levels)
    allowed_regimes = set(req.allowed_regimes)
    for decision in decisions:
        if len(selected) >= req.top_n:
            break
        if (
            decision.regime in allowed_regimes
            and decision.risk_level in allowed_risk
            and decision.confidence >= req.min_confidence
        ):
            selected.append(decision.pair)

    market_rank_meta = market_rank_meta or {}
    trading_signal_meta = trading_signal_meta or {}

    return RankPairsResponse(
        selected_pairs=selected,
        decisions=decisions,
        source="fallback",
        reason=reason,
        market_rank_source=_skill_meta_source(market_rank_meta),
        market_rank_errors=_skill_meta_errors(market_rank_meta),
        trading_signal_source=_skill_meta_source(trading_signal_meta),
        trading_signal_errors=_skill_meta_errors(trading_signal_meta),
    )


def _policy_fallback(req: RuntimePolicyRequest, reason: str) -> RuntimePolicyDecision:
    risk_flags = 0
    if req.closed_trades >= 4 and req.win_rate < 0.45:
        risk_flags += 1
    if req.net_profit_pct <= -1.5:
        risk_flags += 1
    if req.max_drawdown_pct <= -2.0:
        risk_flags += 1
    if req.spike_allowed_rate is not None and req.spike_allowed_rate < 0.25:
        risk_flags += 1

    if risk_flags >= 2:
        return RuntimePolicyDecision(
            profile="defensive",
            confidence=0.7,
            note="fallback:defensive due to weak recent stats",
            aggr_entry_strictness="strict",
            risk_stake_multiplier=0.35,
            risk_max_open_trades=1,
            source="fallback",
            reason=reason,
        )

    if req.closed_trades >= 6 and req.win_rate >= 0.58 and req.net_profit_pct >= 1.0:
        return RuntimePolicyDecision(
            profile="offensive",
            confidence=0.68,
            note="fallback:offensive due to strong recent stats",
            aggr_entry_strictness="normal",
            risk_stake_multiplier=0.75,
            risk_max_open_trades=2,
            source="fallback",
            reason=reason,
        )

    return RuntimePolicyDecision(
        profile="normal",
        confidence=0.62,
        note="fallback:normal balanced profile",
        aggr_entry_strictness="strict",
        risk_stake_multiplier=0.55,
        risk_max_open_trades=2,
        source="fallback",
        reason=reason,
    )


def _to_regime_request(candidate: PairCandidate) -> RegimeRequest:
    return RegimeRequest(
        pair=candidate.pair,
        timeframe=candidate.timeframe,
        price=candidate.price,
        ema_20=candidate.ema_20,
        ema_50=candidate.ema_50,
        ema_200=candidate.ema_200,
        rsi_14=candidate.rsi_14,
        adx_14=candidate.adx_14,
        atr_pct=candidate.atr_pct,
        volume_zscore=candidate.volume_zscore,
        trend_4h=candidate.trend_4h,
        market_structure=candidate.market_structure,
    )


async def _rank_via_single_classify(req: RankPairsRequest) -> tuple[Dict[str, LlmRankDecision], int]:
    parsed: Dict[str, LlmRankDecision] = {}
    success_count = 0
    ollama_failure_count = 0
    max_ollama_failures = _env_int("LLM_RANK_SINGLE_MAX_OLLAMA_FAILURES", 2, 1, 10)

    for candidate in req.candidates:
        single_req = _to_regime_request(candidate)
        prompt = _build_regime_prompt(single_req)
        raw_text = ""
        try:
            raw_text = await _run_ollama(prompt)
            decision = _parse_ollama_json(raw_text)
        except Exception as exc:
            decision = None
            ollama_failure_count += 1
            _record_llm_call(
                endpoint="rank-pairs-single",
                prompt=prompt,
                raw_response=raw_text,
                parsed_ok=False,
                error=f"ollama_error:{exc}",
            )
            # If the model path is repeatedly failing, stop early and let caller use deterministic fallback.
            if ollama_failure_count >= max_ollama_failures and success_count == 0:
                return {}, 0

        if decision is None:
            if raw_text:
                _record_llm_call(
                    endpoint="rank-pairs-single",
                    prompt=prompt,
                    raw_response=raw_text,
                    parsed_ok=False,
                    error="invalid_model_output",
                )
            fallback = _fallback("invalid_model_output")
            parsed[candidate.pair.upper()] = LlmRankDecision(
                pair=candidate.pair,
                regime=fallback.regime,
                risk_level=fallback.risk_level,
                confidence=fallback.confidence,
                note="single:invalid_model_output",
            )
            continue

        _record_llm_call(
            endpoint="rank-pairs-single",
            prompt=prompt,
            raw_response=raw_text,
            parsed_ok=True,
        )

        parsed[candidate.pair.upper()] = LlmRankDecision(
            pair=candidate.pair,
            regime=decision.regime,
            risk_level=decision.risk_level,
            confidence=decision.confidence,
            note=f"single:{decision.note}"[:220],
        )
        success_count += 1

    return parsed, success_count


async def _run_ollama(prompt: str) -> str:
    request_body = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.1},
    }

    async with httpx.AsyncClient(timeout=OLLAMA_TIMEOUT) as client:
        response = await client.post(f"{OLLAMA_BASE_URL}/api/generate", json=request_body)
        response.raise_for_status()
        ollama_json = response.json()
        return str(ollama_json.get("response", "")).strip()


@app.get("/healthz")
async def healthz():
    return {
        "status": "ok",
        "ollama_base_url": OLLAMA_BASE_URL,
        "ollama_model": OLLAMA_MODEL,
        "market_rank_skill_enabled": MARKET_RANK_SKILL_ENABLED,
        "trading_signal_skill_enabled": TRADING_SIGNAL_SKILL_ENABLED,
    }


@app.get("/skills/crypto-market-rank")
async def crypto_market_rank(limit: int = 30, force_refresh: bool = False):
    limit = max(1, min(200, int(limit)))
    pair_map, meta = await _load_market_rank_context(force_refresh=bool(force_refresh))

    ranked = sorted(
        pair_map.values(),
        key=lambda row: (float(row.get("rank_score", 0.0) or 0.0), int(row.get("hits", 0) or 0)),
        reverse=True,
    )

    return {
        "enabled": MARKET_RANK_SKILL_ENABLED,
        "quote": MARKET_RANK_QUOTE,
        "count": len(pair_map),
        "meta": meta,
        "items": ranked[:limit],
    }


@app.get("/skills/trading-signal")
async def trading_signal(limit: int = 30, force_refresh: bool = False):
    limit = max(1, min(200, int(limit)))
    pair_map, meta = await _load_trading_signal_context(force_refresh=bool(force_refresh))

    ranked = sorted(
        pair_map.values(),
        key=lambda row: float(row.get("score", 0.0) or 0.0),
        reverse=True,
    )

    return {
        "enabled": TRADING_SIGNAL_SKILL_ENABLED,
        "quote": TRADING_SIGNAL_QUOTE,
        "count": len(pair_map),
        "meta": meta,
        "items": ranked[:limit],
    }


@app.post("/classify", response_model=RegimeDecision)
async def classify(req: RegimeRequest):
    prompt = _build_regime_prompt(req)
    raw_text = ""
    try:
        raw_text = await _run_ollama(prompt)
    except Exception as exc:
        _record_llm_call(
            endpoint="classify",
            prompt=prompt,
            raw_response=raw_text,
            parsed_ok=False,
            error=f"ollama_unavailable:{exc}",
        )
        return _fallback("ollama_unavailable")

    decision = _parse_ollama_json(raw_text)
    if decision is None:
        _record_llm_call(
            endpoint="classify",
            prompt=prompt,
            raw_response=raw_text,
            parsed_ok=False,
            error="invalid_model_output",
        )
        return _fallback("invalid_model_output")
    _record_llm_call(endpoint="classify", prompt=prompt, raw_response=raw_text, parsed_ok=True)
    return decision


@app.post("/rank-pairs", response_model=RankPairsResponse)
async def rank_pairs(req: RankPairsRequest):
    market_rank_context: Dict[str, dict] = {}
    trading_signal_context: Dict[str, dict] = {}
    market_rank_meta: Dict[str, Any] = {"source": "disabled", "errors": []}
    trading_signal_meta: Dict[str, Any] = {"source": "disabled", "errors": []}
    if MARKET_RANK_SKILL_ENABLED:
        market_rank_context, market_rank_meta = await _load_market_rank_context()
        if market_rank_meta.get("source") == "error":
            logger.warning("market-rank skill unavailable: %s", ",".join(market_rank_meta.get("errors", [])))
    if TRADING_SIGNAL_SKILL_ENABLED:
        trading_signal_context, trading_signal_meta = await _load_trading_signal_context()
        if trading_signal_meta.get("source") == "error":
            logger.warning("trading-signal skill unavailable: %s", ",".join(trading_signal_meta.get("errors", [])))
    prompt = _build_rank_prompt(
        req,
        market_rank_context=market_rank_context,
        trading_signal_context=trading_signal_context,
    )
    parse_reason = "batch_ok"
    raw_text = ""

    try:
        raw_text = await _run_ollama(prompt)
        parsed = _parse_rank_output(raw_text)
    except Exception as exc:
        parsed = None
        parse_reason = "batch_ollama_error"
        _record_llm_call(
            endpoint="rank-pairs-batch",
            prompt=prompt,
            raw_response=raw_text,
            parsed_ok=False,
            error=f"ollama_error:{exc}",
        )

    if raw_text and parsed is not None:
        _record_llm_call(endpoint="rank-pairs-batch", prompt=prompt, raw_response=raw_text, parsed_ok=True)
    elif raw_text and parsed is None:
        _record_llm_call(
            endpoint="rank-pairs-batch",
            prompt=prompt,
            raw_response=raw_text,
            parsed_ok=False,
            error="invalid_model_output",
        )

    if parsed is None:
        parse_reason = "batch_invalid_model_output"
        parsed, success_count = await _rank_via_single_classify(req)
        if not parsed:
            logger.warning("rank-pairs using deterministic fallback: single_classify_empty")
            return _rank_fallback(
                req,
                reason="single_classify_empty",
                market_rank_meta=market_rank_meta,
                trading_signal_meta=trading_signal_meta,
                market_rank_context=market_rank_context,
                trading_signal_context=trading_signal_context,
            )
        if success_count <= 0:
            parse_reason = "single_classify_invalid_all"
            logger.warning("rank-pairs using deterministic fallback: %s", parse_reason)
            return _rank_fallback(
                req,
                reason=parse_reason,
                market_rank_meta=market_rank_meta,
                trading_signal_meta=trading_signal_meta,
                market_rank_context=market_rank_context,
                trading_signal_context=trading_signal_context,
            )
        else:
            parse_reason = f"single_classify_ok:{success_count}/{len(req.candidates)}"
            logger.info("rank-pairs recovered via single classify: %s", parse_reason)
    else:
        logger.info("rank-pairs batch parse success: %s decisions", len(parsed))

    allowed_risk = set(req.allowed_risk_levels)
    allowed_regimes = set(req.allowed_regimes)
    by_pair = {candidate.pair.upper(): candidate for candidate in req.candidates}
    ranked: List[RankedPair] = []
    selected: List[str] = []

    for candidate in req.candidates:
        key = candidate.pair.upper()
        decision = parsed.get(key)
        rank_info = market_rank_context.get(key, {})
        signal_info = trading_signal_context.get(key, {})
        market_rank_score = max(0.0, min(1.0, float(rank_info.get("rank_score", 0.0) or 0.0)))
        trading_signal_side = str(signal_info.get("side", "neutral")).strip().lower()
        if trading_signal_side not in {"buy", "sell", "neutral"}:
            trading_signal_side = "neutral"
        trading_signal_score = max(0.0, min(1.0, float(signal_info.get("score", 0.0) or 0.0)))
        if decision is None:
            regime: RegimeLiteral = "no_trade"
            risk_level: RiskLiteral = "high"
            confidence = 0.0
            note = "missing_pair_decision"
        else:
            regime = decision.regime
            risk_level = decision.risk_level
            confidence = decision.confidence
            note = decision.note

        deterministic_score = by_pair[key].deterministic_score
        final_score = deterministic_score + (confidence * 3.0) + (market_rank_score * 0.75)
        if trading_signal_side == "buy":
            final_score += trading_signal_score * TRADING_SIGNAL_BUY_BONUS_MULT
        elif trading_signal_side == "sell" and TRADING_SIGNAL_SELL_PENALTY_MULT > 0.0:
            final_score -= trading_signal_score * TRADING_SIGNAL_SELL_PENALTY_MULT
        if regime in allowed_regimes:
            final_score += 0.75
        if risk_level == "low":
            final_score += 0.25

        ranked.append(
            RankedPair(
                pair=candidate.pair,
                regime=regime,
                risk_level=risk_level,
                confidence=confidence,
                note=note,
                deterministic_score=deterministic_score,
                market_rank_score=market_rank_score,
                trading_signal_side=trading_signal_side,
                trading_signal_score=trading_signal_score,
                final_score=final_score,
            )
        )

    ranked.sort(key=lambda item: item.final_score, reverse=True)

    for item in ranked:
        if len(selected) >= req.top_n:
            break
        if (
            item.regime in allowed_regimes
            and item.risk_level in allowed_risk
            and item.confidence >= req.min_confidence
        ):
            selected.append(item.pair)

    return RankPairsResponse(
        selected_pairs=selected,
        decisions=ranked,
        source="llm",
        reason=parse_reason,
        market_rank_source=_skill_meta_source(market_rank_meta),
        market_rank_errors=_skill_meta_errors(market_rank_meta),
        trading_signal_source=_skill_meta_source(trading_signal_meta),
        trading_signal_errors=_skill_meta_errors(trading_signal_meta),
    )


@app.post("/policy", response_model=RuntimePolicyDecision)
async def policy(req: RuntimePolicyRequest):
    prompt = _build_policy_prompt(req)
    raw_text = ""
    try:
        raw_text = await _run_ollama(prompt)
        parsed = _parse_policy_output(raw_text)
        if parsed is None:
            logger.warning("policy fallback: invalid_model_output")
            _record_llm_call(
                endpoint="policy",
                prompt=prompt,
                raw_response=raw_text,
                parsed_ok=False,
                error="invalid_model_output",
            )
            return _policy_fallback(req, reason="invalid_model_output")
        _record_llm_call(endpoint="policy", prompt=prompt, raw_response=raw_text, parsed_ok=True)
        parsed.source = "llm"
        parsed.reason = "llm_ok"
        return parsed
    except Exception as exc:
        logger.warning("policy fallback: ollama_unavailable")
        _record_llm_call(
            endpoint="policy",
            prompt=prompt,
            raw_response=raw_text,
            parsed_ok=False,
            error=f"ollama_unavailable:{exc}",
        )
        return _policy_fallback(req, reason="ollama_unavailable")


@app.get("/debug/llm-calls")
async def debug_llm_calls(limit: int = 50, endpoint: str | None = None):
    if not LLM_DEBUG_ENABLED:
        return {"items": [], "total": 0, "enabled": False, "storage": "disabled"}

    limit = max(1, min(500, int(limit)))
    items, total = _dump_llm_calls(limit=limit, endpoint=endpoint)
    storage = "sqlite" if LLM_DEBUG_STORE.available else "memory"
    return {"items": items, "total": total, "enabled": True, "storage": storage}
