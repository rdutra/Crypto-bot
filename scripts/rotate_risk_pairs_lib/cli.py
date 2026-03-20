from __future__ import annotations

import argparse
from pathlib import Path

from .bias import smart_money_bias_candidates, spike_bias_candidates
from .prefilters import apply_skill_prefilters
from .prepare import prepare_candidates
from .reporting import (
    build_rank_request,
    build_rotation_entry,
    candidate_count,
    current_prices_json,
    enforce_source_diversity,
    log_no_candidates,
    print_metrics_summary,
    print_ranking_summary,
    selected_pairs,
)
from .state import risk_changed, set_env_value, sync_whitelist


def _add_shared_metrics_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--metrics-json", required=True)



def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    spike = subparsers.add_parser("spike-bias-candidates")
    spike.add_argument("--db-path", type=Path, required=True)
    spike.add_argument("--quote-asset", required=True)
    spike.add_argument("--lookback-hours", type=float, required=True)
    spike.add_argument("--top-n", type=int, required=True)
    spike.add_argument("--min-score", type=float, required=True)
    spike.add_argument("--require-llm-allowed", action="store_true")

    smart = subparsers.add_parser("smart-money-bias-candidates")
    smart.add_argument("--bot-api-url", required=True)
    smart.add_argument("--binance-rest-base", required=True)
    smart.add_argument("--quote-asset", required=True)
    smart.add_argument("--top-n", type=int, required=True)
    smart.add_argument("--min-score", type=float, required=True)
    smart.add_argument("--require-buy", action="store_true")
    smart.add_argument("--force-refresh", action="store_true")
    smart.add_argument("--exclude-regex", default="")

    prefilter = subparsers.add_parser("apply-skill-prefilters")
    prefilter.add_argument("--bot-api-url", required=True)
    prefilter.add_argument("--metrics-json", required=True)
    prefilter.add_argument("--use-token-info", action="store_true")
    prefilter.add_argument("--min-liquidity-usd", type=float, required=True)
    prefilter.add_argument("--min-holders", type=int, required=True)
    prefilter.add_argument("--max-top10-share", type=float, required=True)
    prefilter.add_argument("--require-spot-tradable", action="store_true")
    prefilter.add_argument("--token-info-fail-open", action="store_true")
    prefilter.add_argument("--use-token-audit", action="store_true")
    prefilter.add_argument("--token-audit-block-levels", default="")
    prefilter.add_argument("--token-audit-fail-open", action="store_true")

    prepare = subparsers.add_parser("prepare-candidates")
    prepare.add_argument("--rotate-candidates", default="")
    prepare.add_argument("--rotate-spike-candidates", default="")
    prepare.add_argument("--rotate-smart-money-candidates", default="")
    prepare.add_argument("--rotate-auto-discover", action="store_true")
    prepare.add_argument("--rotate-data-source", default="auto")
    prepare.add_argument("--rotate-exchange", default="binance")
    prepare.add_argument("--rotate-quote", default="USDT")
    prepare.add_argument("--rotate-max-candidates", type=int, required=True)
    prepare.add_argument("--rotate-min-quote-volume", type=float, required=True)
    prepare.add_argument("--rotate-exclude-regex", default="")
    prepare.add_argument("--rotate-whitelist-only", action="store_true")
    prepare.add_argument("--rotate-core-pairs", default="")
    prepare.add_argument("--rotate-excluded-bases", default="")
    prepare.add_argument("--rotate-excluded-pairs", default="")
    prepare.add_argument("--rotate-min-atr-pct", type=float, required=True)
    prepare.add_argument("--rotate-timeframe", default="1h")
    prepare.add_argument("--rotate-lookback-candles", type=int, required=True)
    prepare.add_argument("--rotate-config-path", type=Path, required=True)
    prepare.add_argument("--data-dir", type=Path, default=Path("/freqtrade/user_data/data/binance"))

    prices = subparsers.add_parser("current-prices-json")
    _add_shared_metrics_args(prices)

    count = subparsers.add_parser("candidate-count")
    _add_shared_metrics_args(count)

    metrics_summary = subparsers.add_parser("print-metrics-summary")
    _add_shared_metrics_args(metrics_summary)

    no_candidates = subparsers.add_parser("log-no-candidates")
    _add_shared_metrics_args(no_candidates)
    no_candidates.add_argument("--log-path", type=Path, required=True)
    no_candidates.add_argument("--top-n", type=int, required=True)
    no_candidates.add_argument("--min-confidence", type=float, required=True)
    no_candidates.add_argument("--allowed-risk", required=True)
    no_candidates.add_argument("--allowed-regimes", required=True)
    no_candidates.add_argument("--data-source", required=True)
    no_candidates.add_argument("--auto-discover", action="store_true")

    rank_request = subparsers.add_parser("build-rank-request")
    _add_shared_metrics_args(rank_request)
    rank_request.add_argument("--top-n", type=int, required=True)
    rank_request.add_argument("--min-confidence", type=float, required=True)
    rank_request.add_argument("--allowed-risk", required=True)
    rank_request.add_argument("--allowed-regimes", required=True)

    ranking_summary = subparsers.add_parser("print-ranking-summary")
    ranking_summary.add_argument("--rank-response", required=True)
    ranking_summary.add_argument("--metrics-json", required=True)

    selected = subparsers.add_parser("selected-pairs")
    selected.add_argument("--rank-response", required=True)

    diversity = subparsers.add_parser("enforce-source-diversity")
    diversity.add_argument("--rank-response", required=True)
    diversity.add_argument("--metrics-json", required=True)
    diversity.add_argument("--top-n", type=int, required=True)
    diversity.add_argument("--min-confidence", type=float, required=True)
    diversity.add_argument("--allowed-risk", required=True)
    diversity.add_argument("--allowed-regimes", required=True)
    diversity.add_argument("--use-smart-money", action="store_true")
    diversity.add_argument("--force-smart-money-slot", action="store_true")
    diversity.add_argument("--diversity-enabled", action="store_true")
    diversity.add_argument("--min-binance-pairs", type=int, required=True)
    diversity.add_argument("--min-algo-pairs", type=int, required=True)
    diversity.add_argument("--min-spike-pairs", type=int, required=True)

    entry = subparsers.add_parser("build-rotation-entry")
    entry.add_argument("--metrics-json", required=True)
    entry.add_argument("--rank-response", required=True)
    entry.add_argument("--top-n", type=int, required=True)
    entry.add_argument("--min-confidence", type=float, required=True)
    entry.add_argument("--allowed-risk", required=True)
    entry.add_argument("--allowed-regimes", required=True)
    entry.add_argument("--data-source", required=True)
    entry.add_argument("--auto-discover", action="store_true")
    entry.add_argument("--apply-mode", action="store_true")
    entry.add_argument("--restart-mode", action="store_true")
    entry.add_argument("--sync-whitelist", action="store_true")
    entry.add_argument("--mode", required=True)
    entry.add_argument("--selected-pairs-override", default="")

    changed = subparsers.add_parser("risk-changed")
    changed.add_argument("--current-risk-pairs", required=True)
    changed.add_argument("--selected-pairs", required=True)

    set_env = subparsers.add_parser("set-env-value")
    set_env.add_argument("--env-path", type=Path, required=True)
    set_env.add_argument("--key", required=True)
    set_env.add_argument("--value", required=True)

    sync = subparsers.add_parser("sync-whitelist")
    sync.add_argument("--config-path", type=Path, required=True)
    sync.add_argument("--core-pairs", required=True)
    sync.add_argument("--selected-pairs", required=True)

    return parser



def main() -> int:
    args = build_parser().parse_args()
    handlers = {
        "spike-bias-candidates": spike_bias_candidates,
        "smart-money-bias-candidates": smart_money_bias_candidates,
        "apply-skill-prefilters": apply_skill_prefilters,
        "prepare-candidates": prepare_candidates,
        "current-prices-json": current_prices_json,
        "candidate-count": candidate_count,
        "print-metrics-summary": print_metrics_summary,
        "log-no-candidates": log_no_candidates,
        "build-rank-request": build_rank_request,
        "print-ranking-summary": print_ranking_summary,
        "selected-pairs": selected_pairs,
        "enforce-source-diversity": enforce_source_diversity,
        "build-rotation-entry": build_rotation_entry,
        "risk-changed": risk_changed,
        "set-env-value": set_env_value,
        "sync-whitelist": sync_whitelist,
    }
    return handlers[args.command](args)
