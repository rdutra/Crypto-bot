#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from rotate_risk_pairs_lib.common import parse_pairs
from rotate_risk_pairs_lib.news import DEFAULT_FEED_URLS, refresh_coin_news_summaries


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch curated coin news asynchronously into a DB-backed summary cache.")
    parser.add_argument("--db-target", required=True)
    parser.add_argument("--pairs", required=True, help="Space/comma separated pair list")
    parser.add_argument("--cache-path", default="freqtrade/user_data/logs/coin-news-cache.json")
    parser.add_argument("--cache-ttl-seconds", type=int, default=900)
    parser.add_argument("--lookback-hours", type=float, default=24.0)
    parser.add_argument("--timeout-seconds", type=float, default=8.0)
    parser.add_argument("--feed-urls", default=" ".join(DEFAULT_FEED_URLS))
    args = parser.parse_args()

    feed_urls = [item.strip() for item in str(args.feed_urls).replace(",", " ").split() if item.strip()]
    result = refresh_coin_news_summaries(
        db_target=args.db_target,
        pairs=parse_pairs(args.pairs),
        feed_urls=feed_urls or list(DEFAULT_FEED_URLS),
        cache_path=Path(args.cache_path),
        cache_ttl_seconds=int(args.cache_ttl_seconds),
        lookback_hours=float(args.lookback_hours),
        timeout_seconds=float(args.timeout_seconds),
    )
    print(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
