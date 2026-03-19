from __future__ import annotations

import json
import logging
import os
from collections import deque
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException

from api_models import (
    LlmRankDecision,
    PairCandidate,
    RankPairsRequest,
    RankPairsResponse,
    RankedPair,
    RegimeDecision,
    RegimeLiteral,
    RegimeRequest,
    RiskLiteral,
    RuntimePolicyDecision,
    RuntimePolicyRequest,
    SkillItemsResponse,
)
from debug_store import LlmDebugStore
from llm_client import LlmClient, LlmClientSettings
from llm_logic import (
    build_policy_prompt,
    build_rank_prompt,
    build_regime_prompt,
    parse_policy_output,
    parse_rank_output,
    parse_regime_output,
    skill_meta_errors,
    skill_meta_provider,
    skill_meta_source,
    skill_meta_upstream_errors,
    skill_meta_upstream_source,
)
from skill_providers import (
    SkillService,
    build_trading_signal_context,
    extract_rank_rows,
    extract_signal_rows,
)

app = FastAPI(title="bot-api", version="1.0.0")
logger = logging.getLogger("bot-api")

LLM_CLIENT = LlmClient(LlmClientSettings.from_env())
LLM_PROVIDER = LLM_CLIENT.provider_name
LLM_MODEL = LLM_CLIENT.model_name
LLM_BASE_URL = LLM_CLIENT.base_url


SKILL_SERVICE = SkillService()
SKILL_PROVIDER_NAME = SKILL_SERVICE.provider_name
MARKET_RANK_SKILL_ENABLED = SKILL_SERVICE.settings.market_rank_enabled
TRADING_SIGNAL_SKILL_ENABLED = SKILL_SERVICE.settings.trading_signal_enabled
TOKEN_INFO_SKILL_ENABLED = SKILL_SERVICE.settings.token_info_enabled
TOKEN_AUDIT_SKILL_ENABLED = SKILL_SERVICE.settings.token_audit_enabled
ADDRESS_INFO_SKILL_ENABLED = SKILL_SERVICE.settings.address_info_enabled
MARKET_RANK_QUOTE = SKILL_SERVICE.settings.market_rank_quote
TRADING_SIGNAL_QUOTE = SKILL_SERVICE.settings.trading_signal_quote
TRADING_SIGNAL_BUY_BONUS_MULT = float(os.getenv("LLM_TRADING_SIGNAL_BUY_BONUS_MULT", "0.9"))
TRADING_SIGNAL_SELL_PENALTY_MULT = float(os.getenv("LLM_TRADING_SIGNAL_SELL_PENALTY_MULT", "0.0"))

LLM_DEBUG_ENABLED = str(os.getenv("LLM_DEBUG_ENABLED", "true")).strip().lower() in {"1", "true", "yes", "on"}
LLM_DEBUG_MAX_ENTRIES = max(20, min(2000, int(os.getenv("LLM_DEBUG_MAX_ENTRIES", "250") or "250")))
LLM_DEBUG_PROMPT_MAX_CHARS = max(200, min(50000, int(os.getenv("LLM_DEBUG_PROMPT_MAX_CHARS", "8000") or "8000")))
LLM_DEBUG_RESPONSE_MAX_CHARS = max(200, min(50000, int(os.getenv("LLM_DEBUG_RESPONSE_MAX_CHARS", "8000") or "8000")))
LLM_DEBUG_DB_PATH = os.getenv("LLM_DEBUG_DB_PATH", "/app/data/llm-debug.sqlite").strip() or "/app/data/llm-debug.sqlite"
LLM_DEBUG_DB_MAX_ROWS = max(1000, min(2_000_000, int(os.getenv("LLM_DEBUG_DB_MAX_ROWS", "50000") or "50000")))
LLM_CALL_LOG: deque[dict[str, Any]] = deque(maxlen=LLM_DEBUG_MAX_ENTRIES)
LLM_CALL_LOCK = Lock()
LLM_DEBUG_STORE = LlmDebugStore(
    enabled=LLM_DEBUG_ENABLED,
    db_path=LLM_DEBUG_DB_PATH,
    max_rows=LLM_DEBUG_DB_MAX_ROWS,
)


@app.on_event("shutdown")
def _on_shutdown() -> None:
    LLM_DEBUG_STORE.close()


# Compatibility wrappers kept for tests and internal call sites.
def _extract_rank_rows(payload: Any) -> list[dict[str, Any]]:
    return extract_rank_rows(payload)


def _extract_signal_rows(payload: Any) -> list[dict[str, Any]]:
    return extract_signal_rows(payload)


def _build_trading_signal_context(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return build_trading_signal_context(rows, SKILL_SERVICE.settings)


async def _load_market_rank_context(force_refresh: bool = False) -> tuple[Dict[str, dict], Dict[str, Any]]:
    return await SKILL_SERVICE.load_market_rank(force_refresh=force_refresh)


async def _load_trading_signal_context(force_refresh: bool = False) -> tuple[Dict[str, dict], Dict[str, Any]]:
    return await SKILL_SERVICE.load_trading_signal(force_refresh=force_refresh)


async def _load_token_info_items(
    *,
    symbol: str = "",
    chain_id: str = "",
    address: str = "",
    force_refresh: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    return await SKILL_SERVICE.load_token_info(
        symbol=symbol,
        chain_id=chain_id,
        address=address,
        force_refresh=force_refresh,
    )


async def _load_token_audit_items(
    *,
    symbol: str = "",
    chain_id: str = "",
    address: str = "",
    force_refresh: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    return await SKILL_SERVICE.load_token_audit(
        symbol=symbol,
        chain_id=chain_id,
        address=address,
        force_refresh=force_refresh,
    )


async def _load_address_info_items(
    *,
    address: str,
    chain_id: str = "",
    force_refresh: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    return await SKILL_SERVICE.load_address_info(address=address, chain_id=chain_id, force_refresh=force_refresh)


def _clip_text(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return f"{value[:limit]}...[truncated]"


def _record_llm_call(
    *,
    endpoint: str,
    prompt: str,
    raw_response: str,
    parsed_ok: bool,
    error: str | None = None,
) -> None:
    if not LLM_DEBUG_ENABLED:
        return

    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "endpoint": endpoint,
        "model": LLM_MODEL,
        "parsed_ok": bool(parsed_ok),
        "error": (error or "")[:240],
        "prompt": _clip_text(prompt, LLM_DEBUG_PROMPT_MAX_CHARS),
        "response": _clip_text(raw_response, LLM_DEBUG_RESPONSE_MAX_CHARS),
    }
    with LLM_CALL_LOCK:
        LLM_CALL_LOG.append(entry)
    LLM_DEBUG_STORE.insert(entry)


def _dump_llm_calls(limit: int, endpoint: str | None = None) -> tuple[list[dict[str, Any]], int]:
    if LLM_DEBUG_STORE.available:
        items, total = LLM_DEBUG_STORE.fetch(limit=limit, endpoint=endpoint)
        if total > 0:
            return items, total

    with LLM_CALL_LOCK:
        snapshot = list(LLM_CALL_LOG)

    if endpoint:
        endpoint_norm = endpoint.strip().lower()
        filtered = [item for item in snapshot if str(item.get("endpoint", "")).lower() == endpoint_norm]
    else:
        filtered = snapshot
    total = len(filtered)
    return list(reversed(filtered[-limit:])), total


def _fallback(note: str) -> RegimeDecision:
    return RegimeDecision(regime="no_trade", risk_level="high", confidence=0.0, note=note[:220])


def _to_regime_request(candidate: PairCandidate) -> RegimeRequest:
    return RegimeRequest(
        pair=candidate.pair,
        timeframe=candidate.timeframe,
        price=candidate.price,
        ema_20=candidate.ema_20,
        ema_50=candidate.ema_50,
        ema_200=candidate.ema_200,
        rsi_14=candidate.rsi_14,
        adx_14=candidate.adx_14,
        atr_pct=candidate.atr_pct,
        volume_zscore=candidate.volume_zscore,
        trend_4h=candidate.trend_4h,
        market_structure=candidate.market_structure,
    )


def _env_int(key: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(key, str(default)))
    except ValueError:
        value = default
    return max(minimum, min(maximum, value))


def _skill_response(enabled: bool, meta: dict[str, Any], items: list[dict[str, Any]]) -> SkillItemsResponse:
    return SkillItemsResponse(
        enabled=enabled,
        provider=skill_meta_provider(meta) or SKILL_PROVIDER_NAME,
        count=len(items),
        meta=meta,
        items=items,
    )


def _rank_response(
    *,
    req: RankPairsRequest,
    parsed: dict[str, LlmRankDecision],
    source: str,
    reason: str,
    market_rank_meta: dict[str, Any],
    trading_signal_meta: dict[str, Any],
    market_rank_context: dict[str, dict[str, Any]],
    trading_signal_context: dict[str, dict[str, Any]],
) -> RankPairsResponse:
    allowed_risk = set(req.allowed_risk_levels)
    allowed_regimes = set(req.allowed_regimes)
    by_pair = {candidate.pair.upper(): candidate for candidate in req.candidates}
    ranked: list[RankedPair] = []
    selected: list[str] = []

    for candidate in req.candidates:
        key = candidate.pair.upper()
        decision = parsed.get(key)
        rank_info = market_rank_context.get(key, {})
        signal_info = trading_signal_context.get(key, {})
        market_rank_score = max(0.0, min(1.0, float(rank_info.get("rank_score", 0.0) or 0.0)))
        trading_signal_side = str(signal_info.get("side", "neutral")).strip().lower()
        if trading_signal_side not in {"buy", "sell", "neutral"}:
            trading_signal_side = "neutral"
        trading_signal_score = max(0.0, min(1.0, float(signal_info.get("score", 0.0) or 0.0)))

        if decision is None:
            regime: RegimeLiteral = "no_trade"
            risk_level: RiskLiteral = "high"
            confidence = 0.0
            note = "missing_pair_decision"
        else:
            regime = decision.regime
            risk_level = decision.risk_level
            confidence = decision.confidence
            note = decision.note

        deterministic_score = by_pair[key].deterministic_score
        final_score = deterministic_score + (confidence * 3.0) + (market_rank_score * 0.75)
        if trading_signal_side == "buy":
            final_score += trading_signal_score * TRADING_SIGNAL_BUY_BONUS_MULT
        elif trading_signal_side == "sell" and TRADING_SIGNAL_SELL_PENALTY_MULT > 0.0:
            final_score -= trading_signal_score * TRADING_SIGNAL_SELL_PENALTY_MULT
        if regime in allowed_regimes:
            final_score += 0.75
        if risk_level == "low":
            final_score += 0.25

        ranked.append(
            RankedPair(
                pair=candidate.pair,
                regime=regime,
                risk_level=risk_level,
                confidence=confidence,
                note=note,
                deterministic_score=deterministic_score,
                market_rank_score=market_rank_score,
                trading_signal_side=trading_signal_side,
                trading_signal_score=trading_signal_score,
                final_score=final_score,
            )
        )

    ranked.sort(key=lambda item: item.final_score, reverse=True)
    for item in ranked:
        if len(selected) >= req.top_n:
            break
        if item.regime in allowed_regimes and item.risk_level in allowed_risk and item.confidence >= req.min_confidence:
            selected.append(item.pair)

    return RankPairsResponse(
        selected_pairs=selected,
        decisions=ranked,
        source=source,  # type: ignore[arg-type]
        reason=reason,
        market_rank_source=skill_meta_source(market_rank_meta),
        market_rank_errors=skill_meta_errors(market_rank_meta),
        market_rank_provider=skill_meta_provider(market_rank_meta),
        market_rank_upstream_source=skill_meta_upstream_source(market_rank_meta),
        market_rank_upstream_errors=skill_meta_upstream_errors(market_rank_meta),
        trading_signal_source=skill_meta_source(trading_signal_meta),
        trading_signal_errors=skill_meta_errors(trading_signal_meta),
        trading_signal_provider=skill_meta_provider(trading_signal_meta),
        trading_signal_upstream_source=skill_meta_upstream_source(trading_signal_meta),
        trading_signal_upstream_errors=skill_meta_upstream_errors(trading_signal_meta),
    )


def _rank_fallback(
    req: RankPairsRequest,
    reason: str = "deterministic_fallback",
    market_rank_meta: Dict[str, Any] | None = None,
    trading_signal_meta: Dict[str, Any] | None = None,
    market_rank_context: Dict[str, dict] | None = None,
    trading_signal_context: Dict[str, dict] | None = None,
) -> RankPairsResponse:
    market_rank_context = market_rank_context or {}
    trading_signal_context = trading_signal_context or {}
    fallback_smart_buy_min_score = float(os.getenv("LLM_FALLBACK_SMART_BUY_MIN_SCORE", "0.75"))

    parsed: dict[str, LlmRankDecision] = {}
    ordered = sorted(req.candidates, key=lambda item: item.deterministic_score, reverse=True)
    for candidate in ordered:
        key = candidate.pair.upper()
        signal_info = trading_signal_context.get(key, {})
        trading_signal_side = str(signal_info.get("side", "neutral")).strip().lower()
        if trading_signal_side not in {"buy", "sell", "neutral"}:
            trading_signal_side = "neutral"
        trading_signal_score = max(0.0, min(1.0, float(signal_info.get("score", 0.0) or 0.0)))
        trendish = candidate.deterministic_score >= 6.0
        smart_buy = trading_signal_side == "buy" and trading_signal_score >= fallback_smart_buy_min_score
        tradable = trendish or smart_buy

        if trendish:
            regime: RegimeLiteral = "trend_pullback"
        elif smart_buy:
            regime = "mean_reversion"
        else:
            regime = "no_trade"
        risk_level: RiskLiteral = "medium" if tradable else "high"
        confidence = min(0.95, max(0.0, candidate.deterministic_score / 10.0))
        if smart_buy:
            confidence = max(confidence, min(0.95, 0.60 + (trading_signal_score * 0.35)))
        note = "fallback_det_rank_smart_buy" if smart_buy and not trendish else "fallback_det_rank"
        parsed[key] = LlmRankDecision(
            pair=candidate.pair,
            regime=regime,
            risk_level=risk_level,
            confidence=confidence,
            note=note,
        )

    return _rank_response(
        req=req,
        parsed=parsed,
        source="fallback",
        reason=reason,
        market_rank_meta=market_rank_meta or {},
        trading_signal_meta=trading_signal_meta or {},
        market_rank_context=market_rank_context,
        trading_signal_context=trading_signal_context,
    )


def _policy_fallback(req: RuntimePolicyRequest, reason: str) -> RuntimePolicyDecision:
    risk_flags = 0
    if req.closed_trades >= 4 and req.win_rate < 0.45:
        risk_flags += 1
    if req.net_profit_pct <= -1.5:
        risk_flags += 1
    if req.max_drawdown_pct <= -2.0:
        risk_flags += 1
    if req.spike_allowed_rate is not None and req.spike_allowed_rate < 0.25:
        risk_flags += 1
    if req.rotation_candidate_count is not None and req.rotation_candidate_count >= 6 and (req.rotation_selected_count or 0) <= 0:
        risk_flags += 1
    if req.rotation_selected_ratio is not None and req.rotation_candidate_count is not None:
        if req.rotation_candidate_count >= 6 and req.rotation_selected_ratio < 0.15:
            risk_flags += 1
    if req.rotation_avg_selected_confidence is not None and req.rotation_avg_selected_confidence < 0.62:
        risk_flags += 1
    if req.rotation_avg_selected_final_score is not None and req.rotation_avg_selected_final_score < 7.5:
        risk_flags += 1
    if req.rotation_prefilter_rejected_count is not None and req.rotation_candidate_count is not None:
        if req.rotation_candidate_count > 0 and req.rotation_prefilter_rejected_count >= req.rotation_candidate_count:
            risk_flags += 1

    if risk_flags >= 2:
        return RuntimePolicyDecision(
            profile="defensive",
            confidence=0.7,
            note="fallback:defensive due to weak recent stats",
            aggr_entry_strictness="strict",
            risk_stake_multiplier=0.35,
            risk_max_open_trades=1,
            source="fallback",
            reason=reason,
        )

    rotation_strong = (
        req.rotation_selected_count is not None
        and req.rotation_selected_count >= 2
        and req.rotation_selected_ratio is not None
        and req.rotation_selected_ratio >= 0.25
        and req.rotation_avg_selected_confidence is not None
        and req.rotation_avg_selected_confidence >= 0.72
        and req.rotation_avg_selected_final_score is not None
        and req.rotation_avg_selected_final_score >= 8.5
    )
    if req.closed_trades >= 6 and req.win_rate >= 0.58 and req.net_profit_pct >= 1.0 and rotation_strong:
        return RuntimePolicyDecision(
            profile="offensive",
            confidence=0.68,
            note="fallback:offensive due to strong recent stats",
            aggr_entry_strictness="normal",
            risk_stake_multiplier=0.75,
            risk_max_open_trades=2,
            source="fallback",
            reason=reason,
        )

    return RuntimePolicyDecision(
        profile="normal",
        confidence=0.62,
        note="fallback:normal balanced profile",
        aggr_entry_strictness="strict",
        risk_stake_multiplier=0.55,
        risk_max_open_trades=2,
        source="fallback",
        reason=reason,
    )


async def _rank_via_single_classify(req: RankPairsRequest) -> tuple[Dict[str, LlmRankDecision], int]:
    parsed: Dict[str, LlmRankDecision] = {}
    success_count = 0
    ollama_failure_count = 0
    max_ollama_failures = _env_int("LLM_RANK_SINGLE_MAX_OLLAMA_FAILURES", 2, 1, 10)

    for candidate in req.candidates:
        single_req = _to_regime_request(candidate)
        prompt = build_regime_prompt(single_req)
        raw_text = ""
        try:
            raw_text = await _run_ollama(prompt)
            decision = parse_regime_output(raw_text)
        except Exception as exc:
            decision = None
            ollama_failure_count += 1
            _record_llm_call(
                endpoint="rank-pairs-single",
                prompt=prompt,
                raw_response=raw_text,
                parsed_ok=False,
                error=f"ollama_error:{exc}",
            )
            if ollama_failure_count >= max_ollama_failures and success_count == 0:
                return {}, 0

        if decision is None:
            if raw_text:
                _record_llm_call(
                    endpoint="rank-pairs-single",
                    prompt=prompt,
                    raw_response=raw_text,
                    parsed_ok=False,
                    error="invalid_model_output",
                )
            fallback = _fallback("invalid_model_output")
            parsed[candidate.pair.upper()] = LlmRankDecision(
                pair=candidate.pair,
                regime=fallback.regime,
                risk_level=fallback.risk_level,
                confidence=fallback.confidence,
                note="single:invalid_model_output",
            )
            continue

        _record_llm_call(endpoint="rank-pairs-single", prompt=prompt, raw_response=raw_text, parsed_ok=True)
        parsed[candidate.pair.upper()] = LlmRankDecision(
            pair=candidate.pair,
            regime=decision.regime,
            risk_level=decision.risk_level,
            confidence=decision.confidence,
            note=f"single:{decision.note}"[:220],
        )
        success_count += 1

    return parsed, success_count


async def _run_ollama(prompt: str) -> str:
    return await LLM_CLIENT.run(prompt)


@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    return {
        "status": "ok",
        "llm_provider": LLM_PROVIDER,
        "llm_base_url": LLM_BASE_URL,
        "llm_model": LLM_MODEL,
        "ollama_base_url": LLM_BASE_URL if LLM_PROVIDER == "ollama" else "",
        "ollama_model": LLM_MODEL if LLM_PROVIDER == "ollama" else "",
        "skill_provider": SKILL_PROVIDER_NAME,
        "market_rank_skill_enabled": MARKET_RANK_SKILL_ENABLED,
        "trading_signal_skill_enabled": TRADING_SIGNAL_SKILL_ENABLED,
        "token_info_skill_enabled": TOKEN_INFO_SKILL_ENABLED,
        "token_audit_skill_enabled": TOKEN_AUDIT_SKILL_ENABLED,
        "address_info_skill_enabled": ADDRESS_INFO_SKILL_ENABLED,
    }


@app.get("/skills/crypto-market-rank")
async def crypto_market_rank(limit: int = 30, force_refresh: bool = False) -> SkillItemsResponse:
    limit = max(1, min(200, int(limit)))
    pair_map, meta = await _load_market_rank_context(force_refresh=bool(force_refresh))
    ranked = sorted(
        pair_map.values(),
        key=lambda row: (float(row.get("rank_score", 0.0) or 0.0), int(row.get("hits", 0) or 0)),
        reverse=True,
    )
    return _skill_response(MARKET_RANK_SKILL_ENABLED, meta, ranked[:limit])


@app.get("/skills/trading-signal")
async def trading_signal(limit: int = 30, force_refresh: bool = False) -> SkillItemsResponse:
    limit = max(1, min(200, int(limit)))
    pair_map, meta = await _load_trading_signal_context(force_refresh=bool(force_refresh))
    ranked = sorted(pair_map.values(), key=lambda row: float(row.get("score", 0.0) or 0.0), reverse=True)
    return _skill_response(TRADING_SIGNAL_SKILL_ENABLED, meta, ranked[:limit])


@app.get("/skills/query-token-info")
async def query_token_info(
    symbol: str | None = None,
    chain_id: str | None = None,
    address: str | None = None,
    limit: int = 20,
    force_refresh: bool = False,
) -> SkillItemsResponse:
    if not (symbol or address):
        raise HTTPException(status_code=400, detail="symbol or address is required")
    limit = max(1, min(200, int(limit)))
    items, meta = await _load_token_info_items(
        symbol=symbol or "",
        chain_id=chain_id or "",
        address=address or "",
        force_refresh=bool(force_refresh),
    )
    return _skill_response(TOKEN_INFO_SKILL_ENABLED, meta, items[:limit])


@app.get("/skills/query-token-audit")
async def query_token_audit(
    symbol: str | None = None,
    chain_id: str | None = None,
    address: str | None = None,
    limit: int = 20,
    force_refresh: bool = False,
) -> SkillItemsResponse:
    if not (symbol or address):
        raise HTTPException(status_code=400, detail="symbol or address is required")
    limit = max(1, min(200, int(limit)))
    items, meta = await _load_token_audit_items(
        symbol=symbol or "",
        chain_id=chain_id or "",
        address=address or "",
        force_refresh=bool(force_refresh),
    )
    return _skill_response(TOKEN_AUDIT_SKILL_ENABLED, meta, items[:limit])


@app.get("/skills/query-address-info")
async def query_address_info(address: str, chain_id: str | None = None, force_refresh: bool = False) -> SkillItemsResponse:
    if not address.strip():
        raise HTTPException(status_code=400, detail="address is required")
    items, meta = await _load_address_info_items(address=address, chain_id=chain_id or "", force_refresh=bool(force_refresh))
    return _skill_response(ADDRESS_INFO_SKILL_ENABLED, meta, items)


@app.post("/classify", response_model=RegimeDecision)
async def classify(req: RegimeRequest) -> RegimeDecision:
    prompt = build_regime_prompt(req)
    raw_text = ""
    try:
        raw_text = await _run_ollama(prompt)
    except Exception as exc:
        _record_llm_call(
            endpoint="classify",
            prompt=prompt,
            raw_response=raw_text,
            parsed_ok=False,
            error=f"ollama_unavailable:{exc}",
        )
        return _fallback("ollama_unavailable")

    decision = parse_regime_output(raw_text)
    if decision is None:
        _record_llm_call(
            endpoint="classify",
            prompt=prompt,
            raw_response=raw_text,
            parsed_ok=False,
            error="invalid_model_output",
        )
        return _fallback("invalid_model_output")

    _record_llm_call(endpoint="classify", prompt=prompt, raw_response=raw_text, parsed_ok=True)
    return decision


@app.post("/rank-pairs", response_model=RankPairsResponse)
async def rank_pairs(req: RankPairsRequest) -> RankPairsResponse:
    market_rank_context: Dict[str, dict] = {}
    trading_signal_context: Dict[str, dict] = {}
    market_rank_meta: Dict[str, Any] = {"source": "disabled", "errors": []}
    trading_signal_meta: Dict[str, Any] = {"source": "disabled", "errors": []}

    if MARKET_RANK_SKILL_ENABLED:
        market_rank_context, market_rank_meta = await _load_market_rank_context()
        if market_rank_meta.get("source") == "error":
            logger.warning("market-rank skill unavailable: %s", ",".join(skill_meta_errors(market_rank_meta)))
    if TRADING_SIGNAL_SKILL_ENABLED:
        trading_signal_context, trading_signal_meta = await _load_trading_signal_context()
        if trading_signal_meta.get("source") == "error":
            logger.warning("trading-signal skill unavailable: %s", ",".join(skill_meta_errors(trading_signal_meta)))

    prompt = build_rank_prompt(req, market_rank_context=market_rank_context, trading_signal_context=trading_signal_context)
    raw_text = ""
    parse_reason = "batch_ok"

    try:
        raw_text = await _run_ollama(prompt)
        parsed = parse_rank_output(raw_text)
    except Exception as exc:
        parsed = None
        parse_reason = "batch_ollama_error"
        _record_llm_call(
            endpoint="rank-pairs-batch",
            prompt=prompt,
            raw_response=raw_text,
            parsed_ok=False,
            error=f"ollama_error:{exc}",
        )

    if raw_text and parsed is not None:
        _record_llm_call(endpoint="rank-pairs-batch", prompt=prompt, raw_response=raw_text, parsed_ok=True)
    elif raw_text and parsed is None:
        _record_llm_call(
            endpoint="rank-pairs-batch",
            prompt=prompt,
            raw_response=raw_text,
            parsed_ok=False,
            error="invalid_model_output",
        )

    if parsed is None:
        parse_reason = "batch_invalid_model_output"
        parsed, success_count = await _rank_via_single_classify(req)
        if not parsed:
            logger.warning("rank-pairs using deterministic fallback: single_classify_empty")
            return _rank_fallback(
                req,
                reason="single_classify_empty",
                market_rank_meta=market_rank_meta,
                trading_signal_meta=trading_signal_meta,
                market_rank_context=market_rank_context,
                trading_signal_context=trading_signal_context,
            )
        if success_count <= 0:
            logger.warning("rank-pairs using deterministic fallback: single_classify_invalid_all")
            return _rank_fallback(
                req,
                reason="single_classify_invalid_all",
                market_rank_meta=market_rank_meta,
                trading_signal_meta=trading_signal_meta,
                market_rank_context=market_rank_context,
                trading_signal_context=trading_signal_context,
            )
        parse_reason = f"single_classify_ok:{success_count}/{len(req.candidates)}"
        logger.info("rank-pairs recovered via single classify: %s", parse_reason)
    else:
        logger.info("rank-pairs batch parse success: %s decisions", len(parsed))

    return _rank_response(
        req=req,
        parsed=parsed,
        source="llm",
        reason=parse_reason,
        market_rank_meta=market_rank_meta,
        trading_signal_meta=trading_signal_meta,
        market_rank_context=market_rank_context,
        trading_signal_context=trading_signal_context,
    )


@app.post("/policy", response_model=RuntimePolicyDecision)
async def policy(req: RuntimePolicyRequest) -> RuntimePolicyDecision:
    prompt = build_policy_prompt(req)
    raw_text = ""
    try:
        raw_text = await _run_ollama(prompt)
        parsed = parse_policy_output(raw_text)
        if parsed is None:
            logger.warning("policy fallback: invalid_model_output")
            _record_llm_call(
                endpoint="policy",
                prompt=prompt,
                raw_response=raw_text,
                parsed_ok=False,
                error="invalid_model_output",
            )
            return _policy_fallback(req, reason="invalid_model_output")
        _record_llm_call(endpoint="policy", prompt=prompt, raw_response=raw_text, parsed_ok=True)
        parsed.source = "llm"
        parsed.reason = "llm_ok"
        return parsed
    except Exception as exc:
        logger.warning("policy fallback: ollama_unavailable")
        _record_llm_call(
            endpoint="policy",
            prompt=prompt,
            raw_response=raw_text,
            parsed_ok=False,
            error=f"ollama_unavailable:{exc}",
        )
        return _policy_fallback(req, reason="ollama_unavailable")


@app.get("/debug/llm-calls")
async def debug_llm_calls(limit: int = 50, endpoint: str | None = None) -> dict[str, Any]:
    if not LLM_DEBUG_ENABLED:
        return {"items": [], "total": 0, "enabled": False, "storage": "disabled"}
    limit = max(1, min(500, int(limit)))
    items, total = _dump_llm_calls(limit=limit, endpoint=endpoint)
    storage = "sqlite" if LLM_DEBUG_STORE.available else "memory"
    return {"items": items, "total": total, "enabled": True, "storage": storage}
