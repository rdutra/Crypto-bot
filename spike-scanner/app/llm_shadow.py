import math
import time
from statistics import mean, pstdev
from typing import Any

import aiohttp

from app.config import Settings
from app.state import SymbolState


def _ema(values: list[float], period: int) -> float | None:
    if period <= 0 or len(values) < period:
        return None
    multiplier = 2.0 / (period + 1.0)
    ema_value = mean(values[:period])
    for value in values[period:]:
        ema_value = ((value - ema_value) * multiplier) + ema_value
    return float(ema_value)


def _rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) <= period:
        return None
    gains: list[float] = []
    losses: list[float] = []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(0.0, delta))
        losses.append(max(0.0, -delta))
    recent_gains = gains[-period:]
    recent_losses = losses[-period:]
    avg_gain = mean(recent_gains) if recent_gains else 0.0
    avg_loss = mean(recent_losses) if recent_losses else 0.0
    if avg_loss <= 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _atr_pct(klines: list[dict[str, float]], period: int = 14) -> float | None:
    if len(klines) <= period:
        return None
    true_ranges: list[float] = []
    for i in range(1, len(klines)):
        high = float(klines[i]["high"])
        low = float(klines[i]["low"])
        prev_close = float(klines[i - 1]["close"])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        true_ranges.append(tr)
    atr = mean(true_ranges[-period:]) if true_ranges else 0.0
    close = float(klines[-1]["close"])
    if close <= 0:
        return 0.0
    return (atr / close) * 100.0


def _adx(klines: list[dict[str, float]], period: int = 14) -> float | None:
    if len(klines) <= period:
        return None

    plus_dm: list[float] = []
    minus_dm: list[float] = []
    true_ranges: list[float] = []

    for i in range(1, len(klines)):
        cur = klines[i]
        prev = klines[i - 1]

        up_move = float(cur["high"]) - float(prev["high"])
        down_move = float(prev["low"]) - float(cur["low"])
        plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0.0)
        minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0.0)

        high = float(cur["high"])
        low = float(cur["low"])
        prev_close = float(prev["close"])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        true_ranges.append(tr)

    tr_sum = sum(true_ranges[-period:])
    if tr_sum <= 0:
        return 0.0

    pdi = (100.0 * sum(plus_dm[-period:])) / tr_sum
    mdi = (100.0 * sum(minus_dm[-period:])) / tr_sum
    denom = pdi + mdi
    if denom <= 0:
        return 0.0
    dx = (100.0 * abs(pdi - mdi)) / denom
    return float(dx)


def _volume_zscore(quote_volumes: list[float], lookback: int = 60) -> float:
    if not quote_volumes:
        return 0.0
    window = quote_volumes[-lookback:]
    if len(window) < 2:
        return 0.0
    mu = mean(window)
    sigma = pstdev(window)
    if sigma <= 0:
        return 0.0
    return (window[-1] - mu) / sigma


def _safe(value: float | None, default: float = 0.0) -> float:
    if value is None:
        return default
    if math.isnan(value) or math.isinf(value):
        return default
    return float(value)


class LlmShadowDecider:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._cache: dict[str, tuple[float, dict[str, Any]]] = {}

    def enabled(self) -> bool:
        return bool(self.settings.llm_shadow_enabled)

    def _build_payload(self, symbol: str, state: SymbolState) -> dict[str, Any] | None:
        klines = list(state.kline_1m)
        if len(klines) < 30:
            return None

        closes = [float(k["close"]) for k in klines]
        quote_volumes = [float(k["quote_vol"]) for k in klines]
        price = float(state.last_price if state.last_price > 0 else closes[-1])

        ema20 = _safe(_ema(closes, 20), price)
        ema50 = _safe(_ema(closes, 50), ema20 if ema20 > 0 else price)
        ema200 = _safe(_ema(closes, 200), ema50 if ema50 > 0 else price)
        rsi14 = _safe(_rsi(closes, 14), 50.0)
        adx14 = _safe(_adx(klines, 14), 20.0)
        atr_pct = _safe(_atr_pct(klines, 14), 1.0)
        volume_z = _safe(_volume_zscore(quote_volumes), 0.0)

        trend_4h = "bullish" if ema50 > ema200 else "bearish"
        market_structure = "higher_highs" if price > ema20 > ema50 > ema200 else "mixed"

        return {
            "pair": symbol.upper(),
            "timeframe": "1m",
            "price": price,
            "ema_20": ema20,
            "ema_50": ema50,
            "ema_200": ema200,
            "rsi_14": rsi14,
            "adx_14": adx14,
            "atr_pct": atr_pct,
            "volume_zscore": volume_z,
            "trend_4h": trend_4h,
            "market_structure": market_structure,
        }

    async def evaluate(self, symbol: str, state: SymbolState, current_ts: float | None = None) -> dict[str, Any]:
        if not self.enabled():
            return {}

        now = float(current_ts) if current_ts is not None else time.monotonic()
        cache_seconds = max(0, int(self.settings.llm_shadow_eval_cache_seconds))
        cache_key = symbol.upper()
        cached = self._cache.get(cache_key)
        if cached is not None and cache_seconds > 0:
            cached_ts, cached_value = cached
            if (now - cached_ts) <= cache_seconds:
                result = dict(cached_value)
                result["cached"] = True
                return result

        payload = self._build_payload(symbol, state)
        if payload is None:
            return {
                "allowed": None,
                "reason": "insufficient_data",
                "cached": False,
            }

        url = self.settings.llm_shadow_bot_api_url.rstrip("/") + "/classify"
        timeout = aiohttp.ClientTimeout(total=max(2, self.settings.llm_shadow_timeout_seconds))
        started = time.monotonic()

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=payload) as resp:
                    status = int(resp.status)
                    body = await resp.json(content_type=None)
                    if status >= 300:
                        return {
                            "allowed": None,
                            "reason": f"http_{status}",
                            "cached": False,
                        }
        except Exception:
            return {
                "allowed": None,
                "reason": "llm_error",
                "cached": False,
            }

        if not isinstance(body, dict):
            return {
                "allowed": None,
                "reason": "invalid_response",
                "cached": False,
            }

        regime = str(body.get("regime", "")).lower()
        risk_level = str(body.get("risk_level", "")).lower()
        raw_conf = body.get("confidence", 0.0)
        try:
            confidence = _safe(float(raw_conf), 0.0)
        except (TypeError, ValueError):
            confidence = 0.0
        note = str(body.get("note", ""))[:220]
        allowed_regimes = self.settings.parsed_llm_shadow_allowed_regimes()
        allowed_risk = self.settings.parsed_llm_shadow_allowed_risk_levels()
        allowed = (
            regime in allowed_regimes
            and risk_level in allowed_risk
            and confidence >= float(self.settings.llm_shadow_min_confidence)
        )

        elapsed_ms = int((time.monotonic() - started) * 1000)
        result = {
            "allowed": bool(allowed),
            "regime": regime,
            "risk_level": risk_level,
            "confidence": round(confidence, 4),
            "note": note,
            "reason": "ok",
            "latency_ms": elapsed_ms,
            "cached": False,
        }
        self._cache[cache_key] = (now, result)
        return result
