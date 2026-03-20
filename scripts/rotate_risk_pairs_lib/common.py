from __future__ import annotations

import json
import math
from typing import Any
from urllib.request import Request, urlopen


def as_bool(raw: str | None, default: bool = False) -> bool:
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def parse_pairs(raw: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for part in str(raw or "").replace(",", " ").split():
        pair = part.strip().upper()
        if not pair or pair in seen:
            continue
        seen.add(pair)
        out.append(pair)
    return out


def finite(value: Any) -> float | None:
    try:
        parsed = float(value)
    except Exception:
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def fetch_json(url: str, timeout: float = 20.0) -> Any:
    req = Request(url=url, method="GET", headers={"Accept": "application/json"})
    with urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8", errors="replace"))
