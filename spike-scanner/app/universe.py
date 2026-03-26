import re
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiohttp

from app.config import Settings


async def _fetch_json(session: aiohttp.ClientSession, url: str) -> Any:
    async with session.get(url, timeout=30) as resp:
        resp.raise_for_status()
        return await resp.json()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _load_cached_universe_payload(cache_path: str) -> tuple[list[str], datetime | None]:
    path = Path(cache_path)
    if not path.exists():
        return [], None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return [], None
    fetched_at_raw = str(payload.get("fetched_at", "") or "").strip()
    symbols = payload.get("symbols", [])
    if not fetched_at_raw or not isinstance(symbols, list):
        return [], None
    try:
        fetched_at = datetime.fromisoformat(fetched_at_raw.replace("Z", "+00:00"))
    except ValueError:
        return [], None
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)
    else:
        fetched_at = fetched_at.astimezone(timezone.utc)
    normalized = [str(symbol).lower() for symbol in symbols if str(symbol).strip()]
    return normalized, fetched_at


def _write_cached_universe(cache_path: str, symbols: list[str]) -> None:
    path = Path(cache_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"fetched_at": _utc_now().isoformat(), "symbols": [str(symbol).lower() for symbol in symbols if str(symbol).strip()]}
    path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")


async def load_universe(session: aiohttp.ClientSession, settings: Settings) -> list[str]:
    cached_symbols, cached_at = _load_cached_universe_payload(settings.universe_cache_path)
    if cached_symbols and cached_at and cached_at >= (_utc_now() - timedelta(minutes=max(1, int(settings.universe_cache_ttl_minutes)))):
        return cached_symbols

    exchange_url = f"{settings.rest_base}/api/v3/exchangeInfo?permissions=SPOT"
    tickers_url = f"{settings.rest_base}/api/v3/ticker/24hr"

    try:
        exchange_info, tickers = await _fetch_json(session, exchange_url), await _fetch_json(session, tickers_url)
    except Exception:
        if cached_symbols:
            return cached_symbols
        raise

    quote_volume_map: dict[str, float] = {}
    for t in tickers:
        symbol = str(t.get("symbol", "")).upper()
        try:
            quote_volume_map[symbol] = float(t.get("quoteVolume", 0.0))
        except (TypeError, ValueError):
            quote_volume_map[symbol] = 0.0

    include_set = settings.include_set()
    exclude_set = settings.exclude_set()
    exclude_re = re.compile(settings.exclude_regex, re.IGNORECASE) if settings.exclude_regex else None

    candidates: list[tuple[str, float]] = []
    for s in exchange_info.get("symbols", []):
        symbol = str(s.get("symbol", "")).upper()
        if not symbol:
            continue
        if include_set and symbol not in include_set:
            continue
        if symbol in exclude_set:
            continue
        if s.get("status") != "TRADING":
            continue
        if s.get("quoteAsset") != settings.quote_asset:
            continue
        if not s.get("isSpotTradingAllowed", True):
            continue
        if exclude_re and exclude_re.search(symbol):
            continue

        quote_vol = quote_volume_map.get(symbol, 0.0)
        if quote_vol < settings.min_quote_volume:
            continue
        candidates.append((symbol, quote_vol))

    candidates.sort(key=lambda x: x[1], reverse=True)
    selected = [symbol.lower() for symbol, _ in candidates[: settings.universe_max_symbols]]
    _write_cached_universe(settings.universe_cache_path, selected)
    return selected
