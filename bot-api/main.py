import json
import os
import re
from typing import Literal

import httpx
from fastapi import FastAPI
from pydantic import BaseModel, Field

app = FastAPI(title="bot-api", version="1.0.0")

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1:8b")
OLLAMA_TIMEOUT = float(os.getenv("OLLAMA_TIMEOUT", "30"))


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
    regime: Literal["trend_pullback", "breakout", "mean_reversion", "chaotic", "no_trade"]
    risk_level: Literal["low", "medium", "high"]
    confidence: float = Field(ge=0.0, le=1.0)
    note: str = Field(min_length=1, max_length=220)


def _fallback(note: str) -> RegimeDecision:
    return RegimeDecision(
        regime="no_trade",
        risk_level="high",
        confidence=0.0,
        note=note[:220],
    )


def _parse_ollama_json(raw_text: str) -> RegimeDecision | None:
    candidates = [raw_text.strip()]

    # Some model outputs wrap JSON with prose; extract the first object block.
    match = re.search(r"\{.*\}", raw_text, re.DOTALL)
    if match:
        candidates.append(match.group(0).strip())

    for candidate in candidates:
        if not candidate:
            continue
        try:
            data = json.loads(candidate)
            return RegimeDecision.model_validate(data)
        except Exception:
            continue
    return None


def _build_prompt(req: RegimeRequest) -> str:
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


@app.get("/healthz")
async def healthz():
    return {
        "status": "ok",
        "ollama_base_url": OLLAMA_BASE_URL,
        "ollama_model": OLLAMA_MODEL,
    }


@app.post("/classify", response_model=RegimeDecision)
async def classify(req: RegimeRequest):
    prompt = _build_prompt(req)
    request_body = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.1},
    }

    try:
        async with httpx.AsyncClient(timeout=OLLAMA_TIMEOUT) as client:
            response = await client.post(f"{OLLAMA_BASE_URL}/api/generate", json=request_body)
            response.raise_for_status()
            ollama_json = response.json()
            raw_text = str(ollama_json.get("response", "")).strip()
    except Exception:
        return _fallback("ollama_unavailable")

    decision = _parse_ollama_json(raw_text)
    if decision is None:
        return _fallback("invalid_model_output")
    return decision
