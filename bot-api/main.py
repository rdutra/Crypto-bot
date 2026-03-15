import logging
import json
import os
import re
from typing import Dict, List, Literal

import httpx
from fastapi import FastAPI
from pydantic import BaseModel, Field

app = FastAPI(title="bot-api", version="1.0.0")
logger = logging.getLogger("bot-api")

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1:8b")
OLLAMA_TIMEOUT = float(os.getenv("OLLAMA_TIMEOUT", "30"))

RegimeLiteral = Literal["trend_pullback", "breakout", "mean_reversion", "chaotic", "no_trade"]
RiskLiteral = Literal["low", "medium", "high"]


class RegimeRequest(BaseModel):
    pair: str
    timeframe: str
    price: float
    ema_20: float
    ema_50: float
    ema_200: float
    rsi_14: float
    adx_14: float
    atr_pct: float
    volume_zscore: float
    trend_4h: str
    market_structure: str


class RegimeDecision(BaseModel):
    regime: RegimeLiteral
    risk_level: RiskLiteral
    confidence: float = Field(ge=0.0, le=1.0)
    note: str = Field(min_length=1, max_length=220)


class PairCandidate(BaseModel):
    pair: str
    timeframe: str
    price: float
    ema_20: float
    ema_50: float
    ema_200: float
    rsi_14: float
    adx_14: float
    atr_pct: float
    volume_zscore: float
    trend_4h: str
    market_structure: str
    deterministic_score: float = Field(default=0.0, ge=0.0, le=100.0)


class RankPairsRequest(BaseModel):
    candidates: List[PairCandidate] = Field(min_length=1, max_length=40)
    top_n: int = Field(default=3, ge=1, le=20)
    min_confidence: float = Field(default=0.6, ge=0.0, le=1.0)
    allowed_risk_levels: List[RiskLiteral] = Field(default_factory=lambda: ["low", "medium"])
    allowed_regimes: List[RegimeLiteral] = Field(default_factory=lambda: ["trend_pullback"])


class LlmRankDecision(BaseModel):
    pair: str
    regime: RegimeLiteral
    risk_level: RiskLiteral
    confidence: float = Field(ge=0.0, le=1.0)
    note: str = Field(min_length=1, max_length=220)


class RankedPair(BaseModel):
    pair: str
    regime: RegimeLiteral
    risk_level: RiskLiteral
    confidence: float = Field(ge=0.0, le=1.0)
    note: str = Field(min_length=1, max_length=220)
    deterministic_score: float = Field(ge=0.0)
    final_score: float


class RankPairsResponse(BaseModel):
    selected_pairs: List[str]
    decisions: List[RankedPair]
    source: Literal["llm", "fallback"]
    reason: str | None = None


def _fallback(note: str) -> RegimeDecision:
    return RegimeDecision(
        regime="no_trade",
        risk_level="high",
        confidence=0.0,
        note=note[:220],
    )


def _extract_json_candidates(raw_text: str) -> List[str]:
    candidates = [raw_text.strip()]

    # Some model outputs wrap JSON with prose; extract the first object block.
    match = re.search(r"\{.*\}", raw_text, re.DOTALL)
    if match:
        candidates.append(match.group(0).strip())

    match_arr = re.search(r"\[.*\]", raw_text, re.DOTALL)
    if match_arr:
        candidates.append(match_arr.group(0).strip())

    return candidates


def _parse_ollama_json(raw_text: str) -> RegimeDecision | None:
    candidates = _extract_json_candidates(raw_text)
    for candidate in candidates:
        if not candidate:
            continue
        try:
            data = json.loads(candidate)
            return RegimeDecision.model_validate(data)
        except Exception:
            continue
    return None


def _parse_rank_output(raw_text: str) -> Dict[str, LlmRankDecision] | None:
    for candidate in _extract_json_candidates(raw_text):
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


def _build_regime_prompt(req: RegimeRequest) -> str:
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


def _build_rank_prompt(req: RankPairsRequest) -> str:
    compact_candidates = []
    for candidate in req.candidates:
        compact_candidates.append(
            {
                "pair": candidate.pair,
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
        "- If uncertain, use no_trade with high risk.\n"
        "- Keep note under 140 characters.\n"
        f"Input JSON:\n{json.dumps({'candidates': compact_candidates}, separators=(',', ':'))}"
    )


def _rank_fallback(req: RankPairsRequest, reason: str = "deterministic_fallback") -> RankPairsResponse:
    ordered = sorted(req.candidates, key=lambda item: item.deterministic_score, reverse=True)
    decisions: List[RankedPair] = []
    selected: List[str] = []

    for candidate in ordered:
        trendish = candidate.deterministic_score >= 6.0
        risk_level: RiskLiteral = "medium" if trendish else "high"
        regime: RegimeLiteral = "trend_pullback" if trendish else "no_trade"
        confidence = min(0.95, max(0.0, candidate.deterministic_score / 10.0))
        final_score = candidate.deterministic_score + (confidence * 3.0)
        note = "fallback_det_rank"

        decisions.append(
            RankedPair(
                pair=candidate.pair,
                regime=regime,
                risk_level=risk_level,
                confidence=confidence,
                note=note,
                deterministic_score=candidate.deterministic_score,
                final_score=final_score,
            )
        )

    allowed_risk = set(req.allowed_risk_levels)
    allowed_regimes = set(req.allowed_regimes)
    for decision in decisions:
        if len(selected) >= req.top_n:
            break
        if (
            decision.regime in allowed_regimes
            and decision.risk_level in allowed_risk
            and decision.confidence >= req.min_confidence
        ):
            selected.append(decision.pair)

    return RankPairsResponse(selected_pairs=selected, decisions=decisions, source="fallback", reason=reason)


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


async def _rank_via_single_classify(req: RankPairsRequest) -> tuple[Dict[str, LlmRankDecision], int]:
    parsed: Dict[str, LlmRankDecision] = {}
    success_count = 0

    for candidate in req.candidates:
        single_req = _to_regime_request(candidate)
        prompt = _build_regime_prompt(single_req)
        try:
            raw_text = await _run_ollama(prompt)
            decision = _parse_ollama_json(raw_text)
        except Exception:
            decision = None

        if decision is None:
            fallback = _fallback("invalid_model_output")
            parsed[candidate.pair.upper()] = LlmRankDecision(
                pair=candidate.pair,
                regime=fallback.regime,
                risk_level=fallback.risk_level,
                confidence=fallback.confidence,
                note="single:invalid_model_output",
            )
            continue

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
    request_body = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.1},
    }

    async with httpx.AsyncClient(timeout=OLLAMA_TIMEOUT) as client:
        response = await client.post(f"{OLLAMA_BASE_URL}/api/generate", json=request_body)
        response.raise_for_status()
        ollama_json = response.json()
        return str(ollama_json.get("response", "")).strip()


@app.get("/healthz")
async def healthz():
    return {
        "status": "ok",
        "ollama_base_url": OLLAMA_BASE_URL,
        "ollama_model": OLLAMA_MODEL,
    }


@app.post("/classify", response_model=RegimeDecision)
async def classify(req: RegimeRequest):
    prompt = _build_regime_prompt(req)
    try:
        raw_text = await _run_ollama(prompt)
    except Exception:
        return _fallback("ollama_unavailable")

    decision = _parse_ollama_json(raw_text)
    if decision is None:
        return _fallback("invalid_model_output")
    return decision


@app.post("/rank-pairs", response_model=RankPairsResponse)
async def rank_pairs(req: RankPairsRequest):
    prompt = _build_rank_prompt(req)
    parse_reason = "batch_ok"

    try:
        raw_text = await _run_ollama(prompt)
        parsed = _parse_rank_output(raw_text)
    except Exception:
        parsed = None
        parse_reason = "batch_ollama_error"

    if parsed is None:
        parse_reason = "batch_invalid_model_output"
        parsed, success_count = await _rank_via_single_classify(req)
        if not parsed:
            logger.warning("rank-pairs using deterministic fallback: single_classify_empty")
            return _rank_fallback(req, reason="single_classify_empty")
        if success_count <= 0:
            parse_reason = "single_classify_invalid_all"
            logger.warning("rank-pairs recovered with no valid single outputs: %s", parse_reason)
        else:
            parse_reason = f"single_classify_ok:{success_count}/{len(req.candidates)}"
            logger.info("rank-pairs recovered via single classify: %s", parse_reason)
    else:
        logger.info("rank-pairs batch parse success: %s decisions", len(parsed))

    allowed_risk = set(req.allowed_risk_levels)
    allowed_regimes = set(req.allowed_regimes)
    by_pair = {candidate.pair.upper(): candidate for candidate in req.candidates}
    ranked: List[RankedPair] = []
    selected: List[str] = []

    for candidate in req.candidates:
        key = candidate.pair.upper()
        decision = parsed.get(key)
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
        final_score = deterministic_score + (confidence * 3.0)
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
                final_score=final_score,
            )
        )

    ranked.sort(key=lambda item: item.final_score, reverse=True)

    for item in ranked:
        if len(selected) >= req.top_n:
            break
        if (
            item.regime in allowed_regimes
            and item.risk_level in allowed_risk
            and item.confidence >= req.min_confidence
        ):
            selected.append(item.pair)

    return RankPairsResponse(selected_pairs=selected, decisions=ranked, source="llm", reason=parse_reason)
