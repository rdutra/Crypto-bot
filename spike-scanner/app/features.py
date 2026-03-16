def spread_pct(bid: float, ask: float) -> float:
    if bid <= 0 or ask <= 0:
        return 999.0
    mid = (bid + ask) / 2.0
    return ((ask - bid) / mid) * 100.0


def close_to_high_score(close_: float, high_: float, low_: float) -> float:
    if high_ <= low_:
        return 0.0
    return max(0.0, min(1.0, (close_ - low_) / (high_ - low_)))


def norm(value: float, low: float, high: float) -> float:
    if high <= low:
        return 0.0
    return max(0.0, min(1.0, (value - low) / (high - low)))
