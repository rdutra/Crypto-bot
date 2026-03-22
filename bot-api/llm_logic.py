from __future__ import annotations

import json
import re
from typing import Any, Dict, List

from api_models import (
    LlmRankDecision,
    RankPairsRequest,
    RegimeDecision,
    RegimeRequest,
    RuntimePolicyDecision,
)


def extract_json_candidates(raw_text: str) -> List[str]:
    candidates = [raw_text.strip()]

    match = re.search(r"\{.*\}", raw_text, re.DOTALL)
    if match:
        candidates.append(match.group(0).strip())

    match_arr = re.search(r"\[.*\]", raw_text, re.DOTALL)
    if match_arr:
        candidates.append(match_arr.group(0).strip())

    return candidates


def parse_regime_output(raw_text: str) -> RegimeDecision | None:
    for candidate in extract_json_candidates(raw_text):
        if not candidate:
            continue
        try:
            data = json.loads(candidate)
            return RegimeDecision.model_validate(data)
        except Exception:
            continue
    return None


def parse_rank_output(raw_text: str) -> Dict[str, LlmRankDecision] | None:
    for candidate in extract_json_candidates(raw_text):
        if not candidate:
            continue
        try:
            data = json.loads(candidate)
        except Exception:
            continue

        if isinstance(data, dict):
            items = data.get("decisions")
        elif isinstance(data, list):
            items = data
        else:
            continue

        if not isinstance(items, list):
            continue

        parsed: Dict[str, LlmRankDecision] = {}
        for item in items:
            try:
                decision = LlmRankDecision.model_validate(item)
            except Exception:
                continue
            parsed[decision.pair.upper()] = decision

        if parsed:
            return parsed

    return None


def parse_policy_output(raw_text: str) -> RuntimePolicyDecision | None:
    for candidate in extract_json_candidates(raw_text):
        if not candidate:
            continue
        try:
            data = json.loads(candidate)
            return RuntimePolicyDecision.model_validate(data)
        except Exception:
            continue
    return None


def build_regime_prompt(req: RegimeRequest) -> str:
    payload = req.model_dump()
    return (
        "You are a crypto trading classifier.\n"
        "Return only valid JSON with this exact schema:\n"
        '{"regime":"trend_pullback|breakout|mean_reversion|chaotic|no_trade",'
        '"risk_level":"low|medium|high","confidence":0.0,"note":"short sentence"}\n'
        "Rules:\n"
        "- confidence must be between 0 and 1.\n"
        "- If uncertain, return no_trade with high risk.\n"
        "- Keep note under 140 characters.\n"
        f"Input JSON:\n{json.dumps(payload, separators=(',', ':'))}"
    )


def build_rank_prompt(
    req: RankPairsRequest,
    market_rank_context: Dict[str, dict] | None = None,
    trading_signal_context: Dict[str, dict] | None = None,
) -> str:
    market_rank_context = market_rank_context or {}
    trading_signal_context = trading_signal_context or {}
    compact_candidates = []
    for candidate in req.candidates:
        rank_info = market_rank_context.get(candidate.pair.upper(), {})
        signal_info = trading_signal_context.get(candidate.pair.upper(), {})
        compact_candidates.append(
            {
                "pair": candidate.pair,
                "data_source": candidate.data_source,
                "candidate_sources": candidate.candidate_sources,
                "timeframe": candidate.timeframe,
                "price": candidate.price,
                "ema_20": candidate.ema_20,
                "ema_50": candidate.ema_50,
                "ema_200": candidate.ema_200,
                "rsi_14": candidate.rsi_14,
                "adx_14": candidate.adx_14,
                "atr_pct": candidate.atr_pct,
                "volume_zscore": candidate.volume_zscore,
                "trend_4h": candidate.trend_4h,
                "market_structure": candidate.market_structure,
                "deterministic_score": candidate.deterministic_score,
                "market_rank_score": rank_info.get("rank_score", 0.0),
                "market_rank_hits": rank_info.get("hits", 0),
                "market_rank_trending": rank_info.get("trending_rank"),
                "market_rank_top_search": rank_info.get("top_search_rank"),
                "market_rank_alpha": rank_info.get("alpha_rank"),
                "trading_signal_side": signal_info.get("side", "neutral"),
                "trading_signal_score": signal_info.get("score", 0.0),
                "trading_signal_confidence": signal_info.get("confidence", 0.0),
                "trading_signal_note": signal_info.get("note", ""),
            }
        )

    return (
        "You are a crypto pair-selection assistant.\n"
        "Return only valid JSON with this schema:\n"
        '{"decisions":[{"pair":"BTC/USDT","regime":"trend_pullback|breakout|mean_reversion|chaotic|no_trade",'
        '"risk_level":"low|medium|high","confidence":0.0,"note":"short sentence"}]}\n'
        "Rules:\n"
        "- Keep one decision per input pair.\n"
        "- confidence must be between 0 and 1.\n"
        "- Prefer trend_pullback when trend and pullback metrics align.\n"
        "- candidate_sources can include spike, algo, or binance_skill.\n"
        "- If candidate_sources includes spike, evaluate it as a scanner-detected momentum candidate.\n"
        "- For spike candidates, breakout or mean_reversion with high risk is acceptable when ATR/volume are strong, even if the 4h trend is mixed or weak.\n"
        "- Do not default a spike candidate to no_trade solely because the higher timeframe trend is not cleanly bullish.\n"
        "- market_rank_score is a secondary prior from Binance market ranks (0..1).\n"
        "- trading_signal_side/score is an extra prior from Binance trading-signal skill.\n"
        "- If uncertain, use no_trade with high risk.\n"
        "- Keep note under 140 characters.\n"
        f"Input JSON:\n{json.dumps({'candidates': compact_candidates}, separators=(',', ':'))}"
    )


def build_policy_prompt(req: Any) -> str:
    payload = req.model_dump()
    return (
        "You are a risk controller for an automated crypto trading bot.\n"
        "Return only valid JSON with this exact schema:\n"
        '{"profile":"defensive|normal|offensive","confidence":0.0,"note":"short sentence",'
        '"aggr_entry_strictness":"strict|normal","risk_stake_multiplier":0.55,"risk_max_open_trades":2}\n'
        "Rules:\n"
        "- Keep confidence between 0 and 1.\n"
        "- Use defensive when recent performance is weak or unstable.\n"
        "- Use defensive when the recent rotation funnel is weak: few viable candidates, low selected confidence, or high prefilter rejection.\n"
        "- Use offensive only when both recent performance and recent rotation quality are strong.\n"
        "- Keep risk_stake_multiplier between 0.1 and 1.0.\n"
        "- Keep risk_max_open_trades between 1 and 5.\n"
        "- Keep note under 140 characters.\n"
        f"Input JSON:\n{json.dumps(payload, separators=(',', ':'))}"
    )


def skill_meta_source(meta: Dict[str, Any] | None) -> str:
    if not isinstance(meta, dict):
        return ""
    return str(meta.get("source") or "")


def skill_meta_errors(meta: Dict[str, Any] | None) -> List[str]:
    if not isinstance(meta, dict):
        return []
    errors = meta.get("errors", [])
    if errors is None:
        return []
    if isinstance(errors, list):
        return [str(err) for err in errors]
    return [str(errors)]


def skill_meta_provider(meta: Dict[str, Any] | None) -> str:
    if not isinstance(meta, dict):
        return ""
    return str(meta.get("provider") or "")


def skill_meta_upstream_source(meta: Dict[str, Any] | None) -> str:
    if not isinstance(meta, dict):
        return ""
    return str(meta.get("upstream_source") or "")


def skill_meta_upstream_errors(meta: Dict[str, Any] | None) -> List[str]:
    if not isinstance(meta, dict):
        return []
    errors = meta.get("upstream_errors", [])
    if errors is None:
        return []
    if isinstance(errors, list):
        return [str(err) for err in errors]
    return [str(errors)]
