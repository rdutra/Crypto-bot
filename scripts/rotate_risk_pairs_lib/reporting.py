from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from .common import parse_pairs


def current_prices_json(args: argparse.Namespace) -> int:
    payload = json.loads(args.metrics_json)
    prices = {}
    for item in payload.get("candidates", []):
        pair = str(item.get("pair", "")).strip().upper()
        price = item.get("price")
        if pair and isinstance(price, (int, float)):
            prices[pair] = float(price)
    print(json.dumps(prices, separators=(",", ":")))
    return 0



def candidate_count(args: argparse.Namespace) -> int:
    payload = json.loads(args.metrics_json)
    print(len(payload.get("candidates", [])))
    return 0



def print_metrics_summary(args: argparse.Namespace) -> int:
    payload = json.loads(args.metrics_json)
    for note in payload.get("discovery_notes", []):
        print(f"- {note}")
    for note in payload.get("prefilter_notes", []):
        print(f"- {note}")
    for row in payload.get("skipped", []):
        print(f"- skipped {row.get('pair')}: {row.get('reason')}")
    for row in payload.get("prefilter_rejected", []):
        reasons = ", ".join(str(reason) for reason in row.get("reasons", []))
        print(f"- prefilter {row.get('pair')}: {reasons}")
    for pair in payload.get("whitelist_missing", []):
        print(f"- not in pair_whitelist: {pair}")
    return 0



def log_no_candidates(args: argparse.Namespace) -> int:
    meta = json.loads(args.metrics_json)
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event": "rotation_no_candidates",
        "top_n": args.top_n,
        "min_confidence": args.min_confidence,
        "allowed_risk_levels": [item.strip().lower() for item in args.allowed_risk.replace(",", " ").split() if item.strip()],
        "allowed_regimes": [item.strip().lower() for item in args.allowed_regimes.replace(",", " ").split() if item.strip()],
        "data_source": args.data_source,
        "auto_discover": args.auto_discover,
        "selected_pairs": [],
        "candidate_count": len(meta.get("candidates", [])),
        "ranked_count": 0,
        "selected_count": 0,
        "prefilter_rejected_count": len(meta.get("prefilter_rejected", [])),
        "selected_ratio": 0.0,
        "avg_ranked_confidence": None,
        "avg_ranked_final_score": None,
        "avg_selected_confidence": None,
        "avg_selected_final_score": None,
        "discovery_notes": meta.get("discovery_notes", []),
        "prefilter_notes": meta.get("prefilter_notes", []),
        "prefilter_rejected": meta.get("prefilter_rejected", []),
        "whitelist_missing": meta.get("whitelist_missing", []),
        "skipped": meta.get("skipped", []),
    }
    with args.log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, separators=(",", ":")) + "\n")
    return 0



def build_rank_request(args: argparse.Namespace) -> int:
    payload = json.loads(args.metrics_json)
    body = {
        "candidates": payload["candidates"],
        "market_context": payload.get("market_context"),
        "top_n": args.top_n,
        "min_confidence": args.min_confidence,
        "allowed_risk_levels": [item.strip().lower() for item in args.allowed_risk.replace(",", " ").split() if item.strip()] or ["low", "medium"],
        "allowed_regimes": [item.strip().lower() for item in args.allowed_regimes.replace(",", " ").split() if item.strip()] or ["trend_pullback"],
    }
    print(json.dumps(body, separators=(",", ":")))
    return 0



def print_ranking_summary(args: argparse.Namespace) -> int:
    ranked = json.loads(args.rank_response)
    meta = json.loads(args.metrics_json)
    sources = {item.get("pair"): item.get("data_source", "?") for item in meta.get("candidates", [])}
    origins = {
        item.get("pair"): ",".join(str(x) for x in item.get("candidate_sources", []) if str(x).strip()) or "-"
        for item in meta.get("candidates", [])
    }
    print(f"source={ranked.get('source')} selected={', '.join(ranked.get('selected_pairs', [])) or 'none'}")
    print(f"reason={ranked.get('reason', 'n/a')}")
    print(
        "skill sources: market_rank={market} trading_signal={signal}".format(
            market=ranked.get("market_rank_source") or "n/a",
            signal=ranked.get("trading_signal_source") or "n/a",
        )
    )
    if ranked.get("market_rank_errors"):
        print(f"market_rank_errors={','.join(str(x) for x in ranked.get('market_rank_errors', []))}")
    if ranked.get("trading_signal_errors"):
        print(f"trading_signal_errors={','.join(str(x) for x in ranked.get('trading_signal_errors', []))}")
    print("pair        src      origin               final  det  conf  sig(side/score)   risk    regime          note")
    for item in ranked.get("decisions", []):
        pair_name = item.get("pair", "")
        sig_side = str(item.get("trading_signal_side", "neutral"))[:7]
        sig_score = float(item.get("trading_signal_score", 0.0) or 0.0)
        print(
            f"{pair_name:<10}  "
            f"{sources.get(pair_name, '?'):<7}  "
            f"{origins.get(pair_name, '-'):<19} "
            f"{float(item.get('final_score', 0.0)):>5.2f}  "
            f"{float(item.get('deterministic_score', 0.0)):>4.2f}  "
            f"{float(item.get('confidence', 0.0)):>4.2f}  "
            f"{f'{sig_side}/{sig_score:.2f}':<16} "
            f"{str(item.get('risk_level', '')):<7} "
            f"{str(item.get('regime', '')):<15} "
            f"{str(item.get('note', ''))[:60]}"
        )
    if meta.get("discovery_notes"):
        print("\nDiscovery notes:")
        for note in meta["discovery_notes"][:20]:
            print(f"- {note}")
    if meta.get("prefilter_notes"):
        print("\nPrefilter notes:")
        for note in meta["prefilter_notes"][:20]:
            print(f"- {note}")
    if meta.get("prefilter_rejected"):
        print("\nPrefilter rejected:")
        for row in meta["prefilter_rejected"][:20]:
            print(f"- {row.get('pair')}: {', '.join(str(x) for x in row.get('reasons', []))}")
    if meta.get("whitelist_missing"):
        print("\nNot currently in pair_whitelist:")
        for pair in meta["whitelist_missing"]:
            print(f"- {pair}")
    return 0



def selected_pairs(args: argparse.Namespace) -> int:
    payload = json.loads(args.rank_response)
    print(" ".join(str(item).upper() for item in payload.get("selected_pairs", []) if str(item).strip()))
    return 0



def selected_source_pairs(args: argparse.Namespace) -> int:
    payload = json.loads(args.rotation_entry_json)
    target_source = str(args.source or "").strip().lower()
    pairs: list[str] = []
    for row in payload.get("decisions", []):
        if not isinstance(row, dict):
            continue
        if not bool(row.get("selected")):
            continue
        sources = {str(item).strip().lower() for item in row.get("candidate_sources", []) if str(item).strip()}
        if target_source and target_source not in sources:
            continue
        pair = str(row.get("pair", "")).strip().upper()
        if pair:
            pairs.append(pair)
    print(" ".join(pairs))
    return 0


def enforce_source_diversity(args: argparse.Namespace) -> int:
    ranked = json.loads(args.rank_response)
    meta = json.loads(args.metrics_json)
    origins_by_pair = {
        str(item.get("pair", "")).upper(): {str(x).strip().lower() for x in item.get("candidate_sources", []) if str(x).strip()}
        for item in meta.get("candidates", [])
        if str(item.get("pair", "")).strip()
    }
    eligible_pairs: set[str] = set()
    allowed_risk = {item.strip().lower() for item in args.allowed_risk.replace(",", " ").split() if item.strip()}
    allowed_regimes = {item.strip().lower() for item in args.allowed_regimes.replace(",", " ").split() if item.strip()}
    for item in ranked.get("decisions", []):
        pair = str(item.get("pair", "")).upper()
        if not pair:
            continue
        confidence = float(item.get("confidence") or 0.0)
        regime = str(item.get("regime", "")).strip().lower()
        risk_level = str(item.get("risk_level", "")).strip().lower()
        if confidence < args.min_confidence:
            continue
        if allowed_regimes and regime and regime not in allowed_regimes:
            continue
        if allowed_risk and risk_level and risk_level not in allowed_risk:
            continue
        eligible_pairs.add(pair)

    confidence_by_pair = {
        str(item.get("pair", "")).upper(): float(item.get("confidence") or 0.0)
        for item in ranked.get("decisions", [])
        if str(item.get("pair", "")).strip()
    }

    ranked_order: list[str] = []
    seen: set[str] = set()
    for pair in parse_pairs(" ".join(str(x) for x in ranked.get("selected_pairs", []))):
        if pair not in seen and pair in eligible_pairs:
            ranked_order.append(pair)
            seen.add(pair)
    for item in ranked.get("decisions", []):
        pair = str(item.get("pair", "")).upper()
        if not pair or pair in seen or pair not in eligible_pairs:
            continue
        ranked_order.append(pair)
        seen.add(pair)
    if not ranked_order or args.top_n <= 0:
        print("")
        return 0

    quotas = {"binance_skill": 0, "algo": 0, "spike": 0}
    if args.diversity_enabled:
        quotas["binance_skill"] = max(0, args.min_binance_pairs)
        quotas["algo"] = max(0, args.min_algo_pairs)
        quotas["spike"] = max(0, args.min_spike_pairs)
    if args.use_smart_money and args.force_smart_money_slot:
        quotas["binance_skill"] = max(quotas["binance_skill"], 1)

    selected: list[str] = []
    selected_set: set[str] = set()

    if args.reserve_spike_slot and args.top_n > 0:
        for pair in ranked_order:
            if pair not in eligible_pairs:
                continue
            if "spike" not in origins_by_pair.get(pair, set()):
                continue
            if confidence_by_pair.get(pair, 0.0) < args.reserve_spike_min_confidence:
                continue
            selected.append(pair)
            selected_set.add(pair)
            break

    def try_pick(source_name: str, quota: int) -> None:
        if quota <= 0:
            return
        picked = 0
        for pair in ranked_order:
            if picked >= quota or len(selected) >= args.top_n:
                return
            if pair in selected_set or source_name not in origins_by_pair.get(pair, set()):
                continue
            selected.append(pair)
            selected_set.add(pair)
            picked += 1

    for source_name in ("binance_skill", "algo", "spike"):
        try_pick(source_name, quotas.get(source_name, 0))
    for pair in ranked_order:
        if len(selected) >= args.top_n:
            break
        if pair in selected_set:
            continue
        selected.append(pair)
        selected_set.add(pair)
    print(" ".join(selected))
    return 0



def build_rotation_entry(args: argparse.Namespace) -> int:
    meta = json.loads(args.metrics_json)
    ranked = json.loads(args.rank_response)
    allowed_risk = [item.strip().lower() for item in args.allowed_risk.replace(",", " ").split() if item.strip()]
    allowed_regimes = [item.strip().lower() for item in args.allowed_regimes.replace(",", " ").split() if item.strip()]
    selected_set = set(parse_pairs(args.selected_pairs_override) or [str(item).upper() for item in ranked.get("selected_pairs", []) if str(item).strip()])
    sources = {item.get("pair"): item.get("data_source", "?") for item in meta.get("candidates", [])}
    candidate_origins = {
        item.get("pair"): [str(x) for x in item.get("candidate_sources", []) if str(x).strip()]
        for item in meta.get("candidates", [])
    }
    candidate_prices = {item.get("pair"): item.get("price") for item in meta.get("candidates", [])}
    candidate_atr_pct = {item.get("pair"): item.get("atr_pct") for item in meta.get("candidates", [])}
    candidate_news = {
        str(item.get("pair", "")).upper(): item.get("coin_news_context", {})
        for item in meta.get("candidates", [])
        if str(item.get("pair", "")).strip()
    }
    prefilter_rejected = meta.get("prefilter_rejected", [])
    if not isinstance(prefilter_rejected, list):
        prefilter_rejected = []

    decisions = []
    ranked_confidences = []
    ranked_final_scores = []
    selected_confidences = []
    selected_final_scores = []
    for item in ranked.get("decisions", []):
        pair = str(item.get("pair", ""))
        confidence_value = float(item.get("confidence") or 0.0)
        final_score_value = float(item.get("final_score") or 0.0)
        regime = str(item.get("regime", "")).strip().lower()
        risk_level = str(item.get("risk_level", "")).strip().lower()
        ranked_confidences.append(confidence_value)
        ranked_final_scores.append(final_score_value)
        selected = pair.upper() in selected_set
        selection_reasons = []
        if selected:
            selection_status = "selected_for_bot"
            selection_reasons.append("selected")
            if confidence_value < args.min_confidence:
                selection_reasons.append(f"confidence_below:{args.min_confidence:.2f}")
            if regime and regime not in allowed_regimes:
                selection_reasons.append(f"regime_blocked:{regime}")
            if risk_level and risk_level not in allowed_risk:
                selection_reasons.append(f"risk_blocked:{risk_level}")
            selected_confidences.append(confidence_value)
            selected_final_scores.append(final_score_value)
        else:
            if confidence_value < args.min_confidence:
                selection_reasons.append(f"confidence_below:{args.min_confidence:.2f}")
            if regime and regime not in allowed_regimes:
                selection_reasons.append(f"regime_blocked:{regime}")
            if risk_level and risk_level not in allowed_risk:
                selection_reasons.append(f"risk_blocked:{risk_level}")
            if not selection_reasons:
                selection_reasons.append("not_in_final_selection")
            selection_status = "ranked_rejected"
        decisions.append(
            {
                "pair": pair,
                "data_source": sources.get(pair, "?"),
                "candidate_sources": candidate_origins.get(pair, []),
                "regime": item.get("regime"),
                "risk_level": item.get("risk_level"),
                "confidence": confidence_value,
                "deterministic_score": item.get("deterministic_score"),
                "market_rank_score": item.get("market_rank_score"),
                "trading_signal_side": item.get("trading_signal_side"),
                "trading_signal_score": item.get("trading_signal_score"),
                "final_score": final_score_value,
                "price": candidate_prices.get(pair),
                "atr_pct": candidate_atr_pct.get(pair),
                "coin_news_context": candidate_news.get(pair, {}),
                "note": item.get("note"),
                "selected": selected,
                "selection_status": selection_status,
                "selection_reason": ", ".join(selection_reasons),
            }
        )

    def avg(values: list[float]) -> float | None:
        if not values:
            return None
        return sum(values) / float(len(values))

    selected_source_counts: dict[str, int] = {"binance_skill": 0, "algo": 0, "spike": 0}
    for item in decisions:
        if not item.get("selected"):
            continue
        for source_name in item.get("candidate_sources", []):
            selected_source_counts[source_name] = selected_source_counts.get(source_name, 0) + 1

    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event": "rotation_decision",
        "source": ranked.get("source"),
        "reason": ranked.get("reason"),
        "market_context": meta.get("market_context"),
        "selected_pairs": parse_pairs(args.selected_pairs_override) or ranked.get("selected_pairs", []),
        "market_rank_source": ranked.get("market_rank_source"),
        "market_rank_errors": ranked.get("market_rank_errors", []),
        "trading_signal_source": ranked.get("trading_signal_source"),
        "trading_signal_errors": ranked.get("trading_signal_errors", []),
        "top_n": args.top_n,
        "min_confidence": args.min_confidence,
        "allowed_risk_levels": allowed_risk,
        "allowed_regimes": allowed_regimes,
        "data_source": args.data_source,
        "auto_discover": args.auto_discover,
        "apply": args.apply_mode,
        "restart": args.restart_mode,
        "sync_whitelist": args.sync_whitelist,
        "strategy_mode": args.mode,
        "candidate_count": len(meta.get("candidates", [])),
        "ranked_count": len(decisions),
        "selected_count": len(selected_set),
        "prefilter_rejected_count": len(prefilter_rejected),
        "selected_ratio": (len(selected_set) / float(len(meta.get("candidates", [])))) if meta.get("candidates") else None,
        "avg_ranked_confidence": avg(ranked_confidences),
        "avg_ranked_final_score": avg(ranked_final_scores),
        "avg_selected_confidence": avg(selected_confidences),
        "avg_selected_final_score": avg(selected_final_scores),
        "selected_source_counts": selected_source_counts,
        "discovery_notes": meta.get("discovery_notes", []),
        "prefilter_notes": meta.get("prefilter_notes", []),
        "candidate_news": candidate_news,
        "prefilter_rejected": prefilter_rejected,
        "whitelist_missing": meta.get("whitelist_missing", []),
        "skipped": meta.get("skipped", []),
        "decisions": decisions,
    }
    print(json.dumps(entry, separators=(",", ":")))
    return 0
