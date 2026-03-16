import asyncio
import json
import logging
from typing import Iterable

import websockets

from app.state import STATE

LOGGER = logging.getLogger(__name__)


async def stream_symbols(ws_base: str, symbols: list[str], symbols_per_conn: int = 25) -> None:
    tasks = []
    for chunk in _chunks(symbols, max(1, symbols_per_conn)):
        tasks.append(asyncio.create_task(_stream_chunk(ws_base, list(chunk))))
    await asyncio.gather(*tasks)


def _chunks(items: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


async def _stream_chunk(ws_base: str, symbols: list[str]) -> None:
    stream_names = []
    for s in symbols:
        stream_names.append(f"{s}@aggTrade")
        stream_names.append(f"{s}@bookTicker")
        stream_names.append(f"{s}@kline_1m")

    url = f"{ws_base}?streams={'/'.join(stream_names)}"

    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                LOGGER.info("Connected stream chunk: symbols=%s", len(symbols))
                async for raw in ws:
                    msg = json.loads(raw)
                    data = msg.get("data", {})
                    stream = msg.get("stream", "")
                    if "@aggTrade" in stream:
                        await handle_aggtrade(data)
                    elif "@bookTicker" in stream:
                        await handle_bookticker(data)
                    elif "@kline_1m" in stream:
                        await handle_kline(data)
        except Exception as exc:
            LOGGER.warning("Stream chunk reconnect in 2s (%s)", exc)
            await asyncio.sleep(2)


async def handle_aggtrade(data: dict) -> None:
    symbol = str(data.get("s", "")).lower()
    if not symbol:
        return

    price = float(data.get("p", 0.0))
    qty = float(data.get("q", 0.0))
    quote = price * qty

    STATE[symbol].last_price = price
    STATE[symbol].trade_events.append(
        {
            "ts": float(data.get("T", 0)) / 1000.0,
            "quote": quote,
            "maker_sell": bool(data.get("m", False)),
            "age_s": 0.0,
        }
    )


async def handle_bookticker(data: dict) -> None:
    symbol = str(data.get("s", "")).lower()
    if not symbol:
        return

    STATE[symbol].bid = float(data.get("b", 0.0))
    STATE[symbol].ask = float(data.get("a", 0.0))


async def handle_kline(data: dict) -> None:
    k = data.get("k", {})
    if not k or not k.get("x"):
        return

    symbol = str(k.get("s", "")).lower()
    if not symbol:
        return

    high_ = float(k.get("h", 0.0))
    low_ = float(k.get("l", 0.0))
    close_ = float(k.get("c", 0.0))
    open_ = float(k.get("o", 0.0))
    range_pct = ((high_ - low_) / close_ * 100.0) if close_ > 0 else 0.0

    STATE[symbol].kline_1m.append(
        {
            "open": open_,
            "high": high_,
            "low": low_,
            "close": close_,
            "quote_vol": float(k.get("q", 0.0)),
            "trade_count": int(k.get("n", 0)),
            "range_pct": range_pct,
        }
    )
