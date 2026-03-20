from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from .common import finite, parse_pairs


def _load_prepare_dependencies():
    import pandas as pd  # noqa: PLC0415
    import talib.abstract as ta  # noqa: PLC0415

    try:
        import ccxt  # noqa: PLC0415
    except Exception:
        ccxt = None
    return pd, ta, ccxt



def prepare_candidates(args: argparse.Namespace) -> int:
    pd, ta, ccxt = _load_prepare_dependencies()

    def pair_to_filename(pair: str, timeframe: str) -> str:
        return f"{pair.replace('/', '_')}-{timeframe}.feather"

    def deterministic_score(row: dict[str, float], trend_4h: str) -> float:
        score = 0.0
        if row["close"] > row["ema200"]:
            score += 2.0
        if row["ema20"] > row["ema50"]:
            score += 1.5
        if row["ema50"] > row["ema200"]:
            score += 1.0
        if trend_4h == "bullish":
            score += 1.5
        if 45.0 <= row["rsi"] <= 62.0:
            score += 1.5
        elif 40.0 <= row["rsi"] <= 68.0:
            score += 0.75
        if row["adx"] >= 20.0:
            score += 1.5
        elif row["adx"] >= 14.0:
            score += 0.75
        if 0.8 <= row["atr_pct"] <= 5.5:
            score += 1.0
        if row["volume_z"] >= -0.2:
            score += 0.75
        elif row["volume_z"] >= -1.0:
            score += 0.35
        if row["close"] >= row["ema20"]:
            score += 0.5
        return round(score, 2)

    def add_indicators(df):
        if df is None or df.empty:
            return None
        out = df.copy()
        out["ema20"] = ta.EMA(out, timeperiod=20)
        out["ema50"] = ta.EMA(out, timeperiod=50)
        out["ema200"] = ta.EMA(out, timeperiod=200)
        out["rsi"] = ta.RSI(out, timeperiod=14)
        out["adx"] = ta.ADX(out, timeperiod=14)
        out["atr"] = ta.ATR(out, timeperiod=14)
        out["atr_pct"] = (out["atr"] / out["close"]) * 100.0
        vol_ma20 = out["volume"].rolling(20).mean()
        vol_std = out["volume"].rolling(20).std()
        out["volume_z"] = (out["volume"] - vol_ma20) / vol_std
        return out

    def resample_ohlcv(df, rule: str):
        if df is None or df.empty or "date" not in df.columns:
            return None
        frame = df.copy()
        try:
            frame["date"] = pd.to_datetime(frame["date"], utc=True)
        except Exception:
            return None
        frame = frame.sort_values("date").set_index("date")
        out = frame.resample(rule, label="right", closed="right").agg(
            {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
        ).dropna()
        if out.empty:
            return None
        return out.reset_index()

    def load_local_ohlcv(data_dir: Path, pair: str, timeframe: str, lookback: int):
        path = data_dir / pair_to_filename(pair, timeframe)
        if not path.exists():
            return None
        df = pd.read_feather(path)
        if df is None or df.empty:
            return None
        min_rows = max(220, lookback)
        if len(df) < min_rows:
            return None
        return df.tail(max(260, lookback)).copy()

    def build_exchange(exchange_id: str):
        if ccxt is None:
            return None
        exchange_class = getattr(ccxt, exchange_id, None)
        if exchange_class is None:
            return None
        return exchange_class({"enableRateLimit": True, "options": {"defaultType": "spot"}})

    def load_exchange_ohlcv(exchange, pair: str, timeframe: str, lookback: int):
        if exchange is None:
            return None
        limit = max(260, lookback) + 20
        try:
            bars = exchange.fetch_ohlcv(pair, timeframe=timeframe, limit=limit)
        except Exception:
            return None
        if not bars:
            return None
        df = pd.DataFrame(bars, columns=["date", "open", "high", "low", "close", "volume"])
        if df.empty:
            return None
        df["date"] = pd.to_datetime(df["date"], unit="ms", utc=True)
        return df.tail(max(260, lookback)).copy()

    def discover_candidates(exchange, core_pairs, quote_asset, max_candidates, min_quote_volume, exclude_regex, excluded_bases):
        if exchange is None:
            return [], ["ccxt_exchange_unavailable"]
        try:
            markets = exchange.load_markets()
        except Exception:
            return [], ["ccxt_load_markets_error"]
        tickers = {}
        if exchange.has.get("fetchTickers"):
            try:
                tickers = exchange.fetch_tickers()
            except Exception:
                tickers = {}
        pattern = re.compile(exclude_regex, re.IGNORECASE) if exclude_regex else None
        scored = []
        skipped = []
        for symbol, market in markets.items():
            pair = str(symbol).upper()
            base = str(market.get("base", "")).upper()
            quote = str(market.get("quote", "")).upper()
            if not bool(market.get("spot", False)) or bool(market.get("contract", False)):
                continue
            if quote != quote_asset or not market.get("active", True) or ":" in pair or pair in core_pairs:
                continue
            if base in excluded_bases:
                skipped.append(f"excluded_base:{pair}")
                continue
            if pattern and (pattern.search(pair) or pattern.search(base)):
                continue
            ticker = tickers.get(symbol) or tickers.get(pair) or {}
            quote_volume = finite(ticker.get("quoteVolume"))
            if quote_volume is None:
                info = ticker.get("info") if isinstance(ticker, dict) else None
                if isinstance(info, dict):
                    quote_volume = finite(info.get("quoteVolume") or info.get("quote_volume"))
            quote_volume = quote_volume or 0.0
            if quote_volume < min_quote_volume:
                skipped.append(f"low_volume:{pair}")
                continue
            scored.append((pair, quote_volume))
        scored.sort(key=lambda item: item[1], reverse=True)
        return [pair for pair, _ in scored[:max_candidates]], skipped

    manual_candidates = parse_pairs(args.rotate_candidates)
    spike_candidates = parse_pairs(args.rotate_spike_candidates)
    smart_money_candidates = parse_pairs(args.rotate_smart_money_candidates)
    core_pairs = set(parse_pairs(args.rotate_core_pairs))
    pair_whitelist: set[str] = set()
    if args.rotate_config_path.exists():
        try:
            cfg = json.loads(args.rotate_config_path.read_text())
            pair_whitelist = {str(item).upper() for item in cfg.get("exchange", {}).get("pair_whitelist", [])}
        except Exception:
            pair_whitelist = set()

    excluded_bases = {item.strip().upper() for item in args.rotate_excluded_bases.replace(",", " ").split() if item.strip()}
    excluded_pairs = {item.strip().upper() for item in args.rotate_excluded_pairs.replace(",", " ").split() if item.strip()}
    exchange = build_exchange(args.rotate_exchange)
    df_cache: dict[tuple[str, str], tuple[Any, str]] = {}
    candidates: list[str] = []
    candidate_sources: dict[str, set[str]] = {}
    discovery_notes: list[str] = []

    def add_candidates(items: list[str], source_label: str) -> None:
        for pair in items:
            normalized = pair.upper()
            candidates.append(normalized)
            candidate_sources.setdefault(normalized, set()).add(source_label)

    if manual_candidates:
        add_candidates(manual_candidates, "manual")
        discovery_notes.append("source=manual")
    if args.rotate_auto_discover:
        discovered, notes = discover_candidates(
            exchange=exchange,
            core_pairs=core_pairs,
            quote_asset=args.rotate_quote.upper(),
            max_candidates=args.rotate_max_candidates,
            min_quote_volume=args.rotate_min_quote_volume,
            exclude_regex=args.rotate_exclude_regex,
            excluded_bases=excluded_bases,
        )
        add_candidates(discovered, "algo")
        discovery_notes.append("source=exchange_discovery")
        discovery_notes.extend(notes[:20])
    elif not candidates:
        fallback = sorted(pair_whitelist) if pair_whitelist else []
        candidates = [pair for pair in fallback if pair not in core_pairs][: args.rotate_max_candidates]
        for pair in candidates:
            candidate_sources.setdefault(pair, set()).add("algo")
        discovery_notes.append("source=whitelist_fallback")

    if spike_candidates:
        for pair in reversed(spike_candidates):
            candidates.insert(0, pair)
            candidate_sources.setdefault(pair.upper(), set()).add("spike")
        discovery_notes.append(f"source=spike_bias count={len(spike_candidates)}")
    if smart_money_candidates:
        for pair in reversed(smart_money_candidates):
            candidates.insert(0, pair)
            candidate_sources.setdefault(pair.upper(), set()).add("binance_skill")
        discovery_notes.append(f"source=smart_money_bias count={len(smart_money_candidates)}")

    ordered_candidates: list[str] = []
    seen: set[str] = set()
    for pair in candidates:
        if pair in seen:
            continue
        seen.add(pair)
        ordered_candidates.append(pair)
    if len(ordered_candidates) > 40:
        ordered_candidates = ordered_candidates[:40]
        discovery_notes.append("candidate_cap=40")

    result: dict[str, Any] = {
        "candidates": [],
        "skipped": [],
        "whitelist_missing": [],
        "discovery_notes": discovery_notes,
    }

    def get_df(pair: str, tf: str, lb: int):
        key = (pair, tf)
        if key in df_cache:
            return df_cache[key]
        if args.rotate_data_source == "local":
            local_df = load_local_ohlcv(args.data_dir, pair, tf, lb)
            df_cache[key] = (local_df, "local")
            return df_cache[key]
        if args.rotate_data_source == "exchange":
            ex_df = load_exchange_ohlcv(exchange, pair, tf, lb)
            df_cache[key] = (ex_df, "exchange")
            return df_cache[key]
        local_df = load_local_ohlcv(args.data_dir, pair, tf, lb)
        if local_df is not None:
            df_cache[key] = (local_df, "local")
            return df_cache[key]
        ex_df = load_exchange_ohlcv(exchange, pair, tf, lb)
        df_cache[key] = (ex_df, "exchange")
        return df_cache[key]

    for pair in ordered_candidates:
        if pair in core_pairs:
            continue
        in_whitelist = (not pair_whitelist) or (pair in pair_whitelist)
        if not in_whitelist and args.rotate_whitelist_only:
            result["whitelist_missing"].append(pair)
            continue
        if not in_whitelist:
            result["whitelist_missing"].append(pair)

        raw_df, source_used = get_df(pair, args.rotate_timeframe, args.rotate_lookback_candles)
        df = add_indicators(raw_df)
        if df is None or len(df) < 220:
            result["skipped"].append({"pair": pair, "reason": f"missing_or_short_{args.rotate_timeframe}_data"})
            continue
        row = df.iloc[-1]
        values = {
            "price": finite(row.get("close")),
            "ema_20": finite(row.get("ema20")),
            "ema_50": finite(row.get("ema50")),
            "ema_200": finite(row.get("ema200")),
            "rsi_14": finite(row.get("rsi")),
            "adx_14": finite(row.get("adx")),
            "atr_pct": finite(row.get("atr_pct")),
            "volume_zscore": finite(row.get("volume_z")),
        }
        if any(value is None for value in values.values()):
            result["skipped"].append({"pair": pair, "reason": "invalid_indicator_values"})
            continue
        base = pair.split("/", 1)[0]
        if pair in excluded_pairs:
            result["skipped"].append({"pair": pair, "reason": "excluded_pair"})
            continue
        if base in excluded_bases:
            result["skipped"].append({"pair": pair, "reason": "excluded_base"})
            continue
        if args.rotate_min_atr_pct > 0.0 and float(values["atr_pct"]) < args.rotate_min_atr_pct:
            atr_pct_value = float(values["atr_pct"])
            result["skipped"].append({"pair": pair, "reason": f"atr_below:{atr_pct_value:.4f}<{args.rotate_min_atr_pct:.4f}"})
            continue

        info_raw = resample_ohlcv(raw_df, "4h") if args.rotate_timeframe == "1h" else None
        if info_raw is None or len(info_raw) < 220:
            info_raw, _ = get_df(pair, "4h", 220)
        info_df = add_indicators(info_raw)
        trend_4h = "bearish"
        if info_df is not None and not info_df.empty:
            info_row = info_df.iloc[-1]
            ema50_4h = finite(info_row.get("ema50"))
            ema200_4h = finite(info_row.get("ema200"))
            if ema50_4h is not None and ema200_4h is not None and ema50_4h > ema200_4h:
                trend_4h = "bullish"

        market_structure = "higher_highs"
        if not (values["price"] > values["ema_20"] > values["ema_50"] > values["ema_200"]):
            market_structure = "mixed"

        score = deterministic_score(
            {
                "close": float(values["price"]),
                "ema20": float(values["ema_20"]),
                "ema50": float(values["ema_50"]),
                "ema200": float(values["ema_200"]),
                "rsi": float(values["rsi_14"]),
                "adx": float(values["adx_14"]),
                "atr_pct": float(values["atr_pct"]),
                "volume_z": float(values["volume_zscore"]),
            },
            trend_4h,
        )

        result["candidates"].append(
            {
                "pair": pair,
                "timeframe": args.rotate_timeframe,
                "price": values["price"],
                "ema_20": values["ema_20"],
                "ema_50": values["ema_50"],
                "ema_200": values["ema_200"],
                "rsi_14": values["rsi_14"],
                "adx_14": values["adx_14"],
                "atr_pct": values["atr_pct"],
                "volume_zscore": values["volume_zscore"],
                "trend_4h": trend_4h,
                "market_structure": market_structure,
                "deterministic_score": score,
                "data_source": source_used,
                "candidate_sources": sorted(candidate_sources.get(pair, set())),
            }
        )
    print(json.dumps(result, separators=(",", ":")))
    return 0
