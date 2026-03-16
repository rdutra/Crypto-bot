from app.features import close_to_high_score, norm, spread_pct


def compute_score(symbol_state) -> tuple[float, dict]:
    trades = list(symbol_state.trade_events)
    klines = list(symbol_state.kline_1m)

    if len(trades) < 20 or len(klines) < 20:
        return 0.0, {}

    quote_vol_1m = sum(t["quote"] for t in trades if t["age_s"] <= 60)
    quote_vol_5m = sum(t["quote"] for t in trades if t["age_s"] <= 300)
    trade_count_5m = sum(1 for t in trades if t["age_s"] <= 300)

    buy_quote_1m = sum(t["quote"] for t in trades if t["age_s"] <= 60 and not t["maker_sell"])
    sell_quote_1m = sum(t["quote"] for t in trades if t["age_s"] <= 60 and t["maker_sell"])
    buy_ratio = buy_quote_1m / max(1.0, buy_quote_1m + sell_quote_1m)

    baseline_quote = max(1.0, sum(k["quote_vol"] for k in klines[-60:]) / min(60, len(klines[-60:])))
    rel_quote = quote_vol_5m / baseline_quote

    baseline_trades = max(1.0, sum(k["trade_count"] for k in klines[-60:]) / min(60, len(klines[-60:])))
    rel_trades = trade_count_5m / baseline_trades

    recent = klines[-1]
    prev_15_high = max(k["high"] for k in klines[-16:-1]) if len(klines) >= 16 else recent["high"]
    breakout = max(0.0, (recent["close"] / prev_15_high) - 1.0) if prev_15_high > 0 else 0.0

    cth = close_to_high_score(recent["close"], recent["high"], recent["low"])
    spr = spread_pct(symbol_state.bid, symbol_state.ask)

    score = (
        0.22 * norm(rel_quote, 1.0, 8.0)
        + 0.18 * norm(rel_trades, 1.0, 6.0)
        + 0.15 * norm(breakout, 0.0, 0.03)
        + 0.12 * cth
        + 0.10 * (1.0 - norm(spr, 0.05, 0.60))
        + 0.10 * norm(buy_ratio, 0.45, 0.75)
        + 0.08 * norm(recent["range_pct"], 0.2, 4.0)
        + 0.05 * norm(quote_vol_1m, 1.0, baseline_quote * 3.0)
    )

    return max(0.0, min(1.0, score)), {
        "rel_quote": rel_quote,
        "rel_trades": rel_trades,
        "breakout": breakout,
        "spread_pct": spr,
        "close_to_high": cth,
        "buy_ratio": buy_ratio,
        "quote_vol_1m": quote_vol_1m,
    }
