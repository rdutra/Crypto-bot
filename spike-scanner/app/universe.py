import re
from typing import Any

import aiohttp

from app.config import Settings


async def _fetch_json(session: aiohttp.ClientSession, url: str) -> Any:
    async with session.get(url, timeout=30) as resp:
        resp.raise_for_status()
        return await resp.json()


async def load_universe(session: aiohttp.ClientSession, settings: Settings) -> list[str]:
    exchange_url = f"{settings.rest_base}/api/v3/exchangeInfo?permissions=SPOT"
    tickers_url = f"{settings.rest_base}/api/v3/ticker/24hr"

    exchange_info, tickers = await _fetch_json(session, exchange_url), await _fetch_json(session, tickers_url)

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
    return selected
