from __future__ import annotations

import argparse
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

from .common import fetch_json, finite
from spike_db import fetch_spike_bias_rows


def spike_bias_candidates(args: argparse.Namespace) -> int:
    db_target = str(args.db_target).strip()
    quote = str(args.quote_asset or "USDT").strip().upper()
    top_n = int(float(args.top_n))
    if not db_target or top_n <= 0:
        print("")
        return 0

    cutoff = datetime.now(timezone.utc) - timedelta(hours=max(1, int(float(args.lookback_hours))))
    best_by_pair: dict[str, tuple[int, int, float]] = {}
    try:
        rows = fetch_spike_bias_rows(db_target)
    except Exception:
        print("")
        return 0

    for row in rows:
        symbol = str(row["symbol"] or "").strip().upper()
        if not symbol.endswith(quote):
            continue
        if args.require_llm_allowed and int(row["llm_allowed"] or 0) != 1:
            continue
        score = finite(row["score"])
        if score is None or score < args.min_score:
            continue
        ts_raw = str(row["ts"] or "").strip()
        if not ts_raw:
            continue
        try:
            ts = datetime.fromisoformat(ts_raw)
        except ValueError:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ts < cutoff:
            continue

        base = symbol[: -len(quote)].strip()
        if not base:
            continue
        pair = f"{base}/{quote}"
        current = (int(row["eligible_rank"] or 0), int(row["source_rank"] or 0), score)
        previous = best_by_pair.get(pair)
        if previous is None or current > previous:
            best_by_pair[pair] = current

    ordered = sorted(best_by_pair.items(), key=lambda item: item[1], reverse=True)[:top_n]
    print(" ".join(pair for pair, _ in ordered))
    return 0



def smart_money_bias_candidates(args: argparse.Namespace) -> int:
    if args.top_n <= 0:
        print("")
        return 0

    pattern = None
    if args.exclude_regex:
        try:
            pattern = re.compile(args.exclude_regex, re.IGNORECASE)
        except re.error:
            pattern = None

    query = urlencode(
        {
            "limit": max(50, args.top_n * 4),
            "force_refresh": "true" if args.force_refresh else "false",
        }
    )
    skill_url = f"{args.bot_api_url.rstrip('/')}/skills/trading-signal?{query}"
    try:
        payload = fetch_json(skill_url, timeout=max(1.0, float(args.bot_api_timeout_seconds)))
    except Exception:
        print("")
        return 0
    raw_items = payload.get("items", []) if isinstance(payload, dict) else []
    items = [row for row in raw_items if isinstance(row, dict)]
    if not items:
        print("")
        return 0

    try:
        exchange_info = fetch_json(
            f"{args.binance_rest_base.rstrip('/')}/api/v3/exchangeInfo",
            timeout=max(1.0, float(args.exchange_timeout_seconds)),
        )
    except Exception:
        print("")
        return 0
    symbols = exchange_info.get("symbols", []) if isinstance(exchange_info, dict) else []
    spot_symbols: set[str] = set()
    for row in symbols:
        if not isinstance(row, dict):
            continue
        if str(row.get("status", "")).upper() != "TRADING":
            continue
        if not bool(row.get("isSpotTradingAllowed", False)):
            continue
        if str(row.get("quoteAsset", "")).upper() != args.quote_asset.upper():
            continue
        symbol = str(row.get("symbol", "")).upper()
        if symbol:
            spot_symbols.add(symbol)
    if not spot_symbols:
        print("")
        return 0

    picked: list[str] = []
    seen: set[str] = set()
    for item in items:
        pair = str(item.get("pair", "")).strip().upper()
        if "/" not in pair:
            continue
        base, pair_quote = pair.split("/", 1)
        if pair_quote != args.quote_asset.upper() or not base:
            continue
        if f"{base}{pair_quote}" not in spot_symbols:
            continue
        if pattern and (pattern.search(base) or pattern.search(pair)):
            continue
        side = str(item.get("side", "")).strip().lower()
        if args.require_buy and side != "buy":
            continue
        score = finite(item.get("score", 0.0)) or 0.0
        if score < args.min_score:
            continue
        normalized = f"{base}/{pair_quote}"
        if normalized in seen:
            continue
        seen.add(normalized)
        picked.append(normalized)
        if len(picked) >= args.top_n:
            break

    print(" ".join(picked))
    return 0
