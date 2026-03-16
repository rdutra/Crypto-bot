from collections import defaultdict, deque
from dataclasses import dataclass, field
from time import time


@dataclass
class SymbolState:
    last_price: float = 0.0
    bid: float = 0.0
    ask: float = 0.0
    trade_events: deque = field(default_factory=lambda: deque(maxlen=5000))
    kline_1m: deque = field(default_factory=lambda: deque(maxlen=240))
    last_alert_ts: float = 0.0


STATE: dict[str, SymbolState] = defaultdict(SymbolState)


def now_ts() -> float:
    return time()
