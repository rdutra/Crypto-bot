from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .common import finite, parse_pairs
from .news import load_coin_news_contexts


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
    exchange_timeout_ms = max(
        1000,
        int(float(os.getenv("ROTATE_EXCHANGE_TIMEOUT_MS", os.getenv("LLM_ROTATE_EXCHANGE_TIMEOUT_MS", "8000")))),
    )
    max_exchange_fallbacks = max(
        0,
        int(float(os.getenv("ROTATE_MAX_EXCHANGE_FALLBACKS", os.getenv("LLM_ROTATE_MAX_EXCHANGE_FALLBACKS", "6")))),
    )
    trades_db_target = (
        str(
            os.getenv(
                "ROTATE_TRADES_DB_URL",
                os.getenv("FREQTRADE_DB_URL", os.getenv("ROTATE_TRADES_DB_PATH", "")),
            )
        )
        .strip()
    )
    pair_stats_lookback_hours = 168.0
    pair_stats_min_trades = 2
    coin_news_enabled = str(os.getenv("COIN_NEWS_ENABLED", "true")).strip().lower() in {"1", "true", "yes", "on"}
    coin_news_db_target = str(
        os.getenv(
            "COIN_NEWS_DB_URL",
            os.getenv("COIN_NEWS_DB_PATH", os.getenv("LLM_ROTATE_OUTCOME_DB_URL", "")),
        )
    ).strip()
    coin_news_max_age_minutes = max(30, int(float(os.getenv("COIN_NEWS_MAX_AGE_MINUTES", "180"))))

    def _is_postgres_target(target: str) -> bool:
        return urlparse(str(target).strip()).scheme.lower().startswith("postgres")

    def _normalize_postgres_target(target: str) -> str:
        normalized = str(target).strip()
        if normalized.startswith("postgresql+psycopg2://"):
            return "postgresql://" + normalized[len("postgresql+psycopg2://") :]
        if normalized.startswith("postgresql+psycopg://"):
            return "postgresql://" + normalized[len("postgresql+psycopg://") :]
        return normalized

    def collect_pair_trade_stats(target: str, lookback_hours: float) -> dict[str, dict[str, float]]:
        if not target:
            return {}
        if _is_postgres_target(target):
            try:
                import psycopg2
                import psycopg2.extras
                from datetime import datetime, timedelta, timezone

                cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
                conn = psycopg2.connect(_normalize_postgres_target(target))
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(
                    """
                    SELECT
                        pair,
                        COUNT(*) AS closed_trades,
                        COALESCE(AVG(close_profit), 0.0) AS avg_profit_ratio,
                        COALESCE(SUM(close_profit), 0.0) AS net_profit_ratio,
                        COALESCE(SUM(CASE WHEN close_profit > 0 THEN 1 ELSE 0 END), 0) AS wins
                    FROM trades
                    WHERE is_open = false
                      AND close_date IS NOT NULL
                      AND close_date >= %s
                    GROUP BY pair
                    """
                    ,
                    (cutoff,),
                )
                rows = cur.fetchall() or []
                cur.close()
                conn.close()
            except Exception:
                return {}
            result: dict[str, dict[str, float]] = {}
            for row in rows:
                pair = str(row.get("pair", "")).strip().upper()
                if not pair:
                    continue
                closed = int(row.get("closed_trades") or 0)
                wins = int(row.get("wins") or 0)
                result[pair] = {
                    "closed_trades": float(closed),
                    "win_rate": (wins / closed) if closed > 0 else 0.0,
                    "avg_profit_pct": float(row.get("avg_profit_ratio") or 0.0) * 100.0,
                    "net_profit_pct": float(row.get("net_profit_ratio") or 0.0) * 100.0,
                }
            return result

        db_path = Path(target)
        if not db_path.exists():
            return {}
        try:
            import sqlite3
            from datetime import datetime, timedelta, timezone

            cutoff = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT
                    pair,
                    COUNT(*) AS closed_trades,
                    COALESCE(AVG(close_profit), 0.0) AS avg_profit_ratio,
                    COALESCE(SUM(close_profit), 0.0) AS net_profit_ratio,
                    COALESCE(SUM(CASE WHEN close_profit > 0 THEN 1 ELSE 0 END), 0) AS wins
                FROM trades
                WHERE is_open = 0
                  AND close_date IS NOT NULL
                  AND julianday(close_date) >= julianday(?)
                GROUP BY pair
                """,
                (cutoff,),
            ).fetchall()
            conn.close()
        except Exception:
            return {}
        result = {}
        for row in rows:
            pair = str(row["pair"] or "").strip().upper()
            if not pair:
                continue
            closed = int(row["closed_trades"] or 0)
            wins = int(row["wins"] or 0)
            result[pair] = {
                "closed_trades": float(closed),
                "win_rate": (wins / closed) if closed > 0 else 0.0,
                "avg_profit_pct": float(row["avg_profit_ratio"] or 0.0) * 100.0,
                "net_profit_pct": float(row["net_profit_ratio"] or 0.0) * 100.0,
            }
        return result

    def historical_penalty(stats: dict[str, float] | None) -> float:
        if not stats:
            return 0.0
        closed_trades = int(stats.get("closed_trades", 0.0) or 0)
        if closed_trades < pair_stats_min_trades:
            return 0.0
        avg_profit_pct = float(stats.get("avg_profit_pct", 0.0) or 0.0)
        net_profit_pct = float(stats.get("net_profit_pct", 0.0) or 0.0)
        win_rate = float(stats.get("win_rate", 0.0) or 0.0)
        penalty = 0.0
        if avg_profit_pct < 0.0:
            penalty += min(1.75, abs(avg_profit_pct) * 1.5)
        if net_profit_pct < -0.5:
            penalty += min(1.25, abs(net_profit_pct) * 0.6)
        if closed_trades >= 3 and win_rate < 0.4:
            penalty += 0.75
        return round(min(3.0, penalty), 2)

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

    def change_pct(df) -> float:
        if df is None or len(df) < 2:
            return 0.0
        try:
            close_now = float(df.iloc[-1]["close"])
            close_prev = float(df.iloc[-2]["close"])
        except Exception:
            return 0.0
        if close_prev == 0.0:
            return 0.0
        return ((close_now / close_prev) - 1.0) * 100.0

    def session_label(now_utc: datetime) -> str:
        hour = now_utc.hour
        if 0 <= hour < 7:
            return "asia"
        if 7 <= hour < 12:
            return "europe"
        if 12 <= hour < 13:
            return "us_preopen"
        if 13 <= hour < 20:
            return "us_cash"
        return "offhours"

    def build_market_context(candidate_rows: list[dict[str, Any]]) -> dict[str, Any]:
        now_utc = datetime.now(timezone.utc)
        btc_df, _ = get_df("BTC/USDT", args.rotate_timeframe, args.rotate_lookback_candles)
        eth_df, _ = get_df("ETH/USDT", args.rotate_timeframe, args.rotate_lookback_candles)
        btc_ind = add_indicators(btc_df)
        eth_ind = add_indicators(eth_df)

        btc_change = change_pct(btc_df)
        eth_change = change_pct(eth_df)
        btc_rsi = finite(btc_ind.iloc[-1].get("rsi")) if btc_ind is not None and not btc_ind.empty else 0.0
        eth_rsi = finite(eth_ind.iloc[-1].get("rsi")) if eth_ind is not None and not eth_ind.empty else 0.0

        alt_rows = [row for row in candidate_rows if row.get("pair") not in {"BTC/USDT", "ETH/USDT", "BNB/USDT"}]
        if not alt_rows:
            alt_rows = candidate_rows
        alt_count = max(1, len(alt_rows))
        above_ema20 = sum(1 for row in alt_rows if float(row.get("price") or 0.0) > float(row.get("ema_20") or 0.0))
        momentum = sum(
            1
            for row in alt_rows
            if float(row.get("price") or 0.0) > float(row.get("ema_20") or 0.0)
            and float(row.get("rsi_14") or 0.0) >= 55.0
        )
        alt_above_ema20_ratio = above_ema20 / float(alt_count)
        alt_momentum_ratio = momentum / float(alt_count)
        overextended = (btc_rsi or 0.0) >= 72.0 and (eth_rsi or 0.0) >= 70.0 and alt_momentum_ratio >= 0.6

        broad_move = "mixed"
        if btc_change >= 0.8 and eth_change >= 0.8 and alt_above_ema20_ratio >= 0.6:
            broad_move = "risk_on"
        elif btc_change <= -0.8 and eth_change <= -0.8 and alt_above_ema20_ratio <= 0.4:
            broad_move = "risk_off"

        note = "mixed market backdrop"
        if broad_move == "risk_on" and overextended:
            note = "broad risk-on move with overextended majors"
        elif broad_move == "risk_on":
            note = "broad risk-on move"
        elif broad_move == "risk_off":
            note = "broad risk-off move"

        return {
            "broad_move": broad_move,
            "session_label": session_label(now_utc),
            "btc_change_pct": round(btc_change, 4),
            "eth_change_pct": round(eth_change, 4),
            "btc_rsi_1h": round(float(btc_rsi or 0.0), 2),
            "eth_rsi_1h": round(float(eth_rsi or 0.0), 2),
            "alt_above_ema20_ratio": round(alt_above_ema20_ratio, 4),
            "alt_momentum_ratio": round(alt_momentum_ratio, 4),
            "overextended": overextended,
            "note": note,
        }

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
        return exchange_class(
            {
                "enableRateLimit": True,
                "timeout": exchange_timeout_ms,
                "options": {"defaultType": "spot"},
            }
        )

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
    exchange_fallback_state = {"used": 0, "skipped_budget": 0}

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
        "market_context": None,
    }
    pair_trade_stats = collect_pair_trade_stats(trades_db_target, pair_stats_lookback_hours)
    coin_news_by_pair: dict[str, dict[str, Any]] = {}
    news_status = "disabled"

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
        if exchange_fallback_state["used"] >= max_exchange_fallbacks:
            exchange_fallback_state["skipped_budget"] += 1
            df_cache[key] = (None, "exchange_budget_exhausted")
            return df_cache[key]
        exchange_fallback_state["used"] += 1
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
            if source_used == "exchange_budget_exhausted":
                result["skipped"].append({"pair": pair, "reason": "exchange_budget_exhausted"})
                continue
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
        trade_stats = pair_trade_stats.get(pair, {})
        pair_penalty = historical_penalty(trade_stats)
        if pair_penalty > 0.0:
            result["discovery_notes"].append(
                f"historical_penalty:{pair}:{pair_penalty:.2f}:avg={float(trade_stats.get('avg_profit_pct', 0.0)):.2f}%"
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
                "recent_closed_trades": int(trade_stats.get("closed_trades", 0.0) or 0),
                "recent_win_rate": float(trade_stats.get("win_rate", 0.0) or 0.0),
                "recent_avg_profit_pct": float(trade_stats.get("avg_profit_pct", 0.0) or 0.0),
                "recent_net_profit_pct": float(trade_stats.get("net_profit_pct", 0.0) or 0.0),
                "historical_penalty": pair_penalty,
            }
        )
    if exchange_fallback_state["used"] > 0:
        result["discovery_notes"].append(
            f"exchange_fallback_used={exchange_fallback_state['used']}/{max_exchange_fallbacks} timeout_ms={exchange_timeout_ms}"
        )
    if exchange_fallback_state["skipped_budget"] > 0:
        result["discovery_notes"].append(f"exchange_budget_exhausted_count={exchange_fallback_state['skipped_budget']}")
    if coin_news_enabled and result["candidates"]:
        candidate_pairs = [str(item.get("pair", "")).upper() for item in result["candidates"] if str(item.get("pair", "")).strip()]
        coin_news_by_pair, news_status = load_coin_news_contexts(
            db_target=coin_news_db_target,
            pairs=candidate_pairs,
            max_age_minutes=coin_news_max_age_minutes,
        )
        result["discovery_notes"].append(f"coin_news:{news_status}:pairs={len(coin_news_by_pair)}")
        for item in result["candidates"]:
            pair = str(item.get("pair", "")).upper()
            item["coin_news_context"] = coin_news_by_pair.get(
                pair,
                {
                    "news_count_24h": 0,
                    "sentiment": "neutral",
                    "sentiment_score": 0.0,
                    "major_catalyst": False,
                    "risk_flags": [],
                    "last_news_age_minutes": None,
                    "note": "no_cached_news",
                    "top_headlines": [],
                },
            )
    if result["candidates"]:
        result["market_context"] = build_market_context(result["candidates"])
    print(json.dumps(result, separators=(",", ":")))
    return 0
