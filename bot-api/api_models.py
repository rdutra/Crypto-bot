from __future__ import annotations

from typing import Any, Dict, List, Literal

from pydantic import BaseModel, Field

RegimeLiteral = Literal["trend_pullback", "breakout", "mean_reversion", "chaotic", "no_trade"]
RiskLiteral = Literal["low", "medium", "high"]
PolicyProfileLiteral = Literal["defensive", "normal", "offensive"]


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
    data_source: str = ""
    candidate_sources: List[str] = Field(default_factory=list)
    recent_closed_trades: int = Field(default=0, ge=0)
    recent_win_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    recent_avg_profit_pct: float = Field(default=0.0, ge=-100.0, le=100.0)
    recent_net_profit_pct: float = Field(default=0.0, ge=-1000.0, le=1000.0)
    historical_penalty: float = Field(default=0.0, ge=0.0, le=10.0)
    coin_news_context: dict[str, Any] = Field(default_factory=dict)


class MarketContext(BaseModel):
    broad_move: Literal["risk_on", "risk_off", "mixed"] = "mixed"
    session_label: str = Field(default="", max_length=32)
    btc_change_pct: float = Field(default=0.0, ge=-100.0, le=100.0)
    eth_change_pct: float = Field(default=0.0, ge=-100.0, le=100.0)
    btc_rsi_1h: float = Field(default=0.0, ge=0.0, le=100.0)
    eth_rsi_1h: float = Field(default=0.0, ge=0.0, le=100.0)
    alt_above_ema20_ratio: float = Field(default=0.0, ge=0.0, le=1.0)
    alt_momentum_ratio: float = Field(default=0.0, ge=0.0, le=1.0)
    overextended: bool = False
    note: str = Field(default="", max_length=160)


class RankPairsRequest(BaseModel):
    candidates: List[PairCandidate] = Field(min_length=1, max_length=40)
    market_context: MarketContext | None = None
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
    market_rank_score: float = Field(default=0.0, ge=0.0, le=1.0)
    trading_signal_side: Literal["buy", "sell", "neutral"] = "neutral"
    trading_signal_score: float = Field(default=0.0, ge=0.0, le=1.0)
    final_score: float


class RankPairsResponse(BaseModel):
    selected_pairs: List[str]
    decisions: List[RankedPair]
    source: Literal["llm", "fallback"]
    reason: str | None = None
    market_rank_source: str | None = None
    market_rank_errors: List[str] = Field(default_factory=list)
    market_rank_provider: str | None = None
    market_rank_upstream_source: str | None = None
    market_rank_upstream_errors: List[str] = Field(default_factory=list)
    trading_signal_source: str | None = None
    trading_signal_errors: List[str] = Field(default_factory=list)
    trading_signal_provider: str | None = None
    trading_signal_upstream_source: str | None = None
    trading_signal_upstream_errors: List[str] = Field(default_factory=list)


class RuntimePolicyRequest(BaseModel):
    lookback_hours: float = Field(default=24.0, ge=1.0, le=336.0)
    closed_trades: int = Field(default=0, ge=0)
    win_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    avg_profit_pct: float = Field(default=0.0, ge=-100.0, le=100.0)
    net_profit_pct: float = Field(default=0.0, ge=-100.0, le=100.0)
    max_drawdown_pct: float = Field(default=0.0, ge=-100.0, le=0.0)
    open_trades: int = Field(default=0, ge=0, le=50)
    spike_allowed_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    rotation_candidate_count: int | None = Field(default=None, ge=0, le=200)
    rotation_selected_count: int | None = Field(default=None, ge=0, le=50)
    rotation_prefilter_rejected_count: int | None = Field(default=None, ge=0, le=200)
    rotation_selected_ratio: float | None = Field(default=None, ge=0.0, le=1.0)
    rotation_avg_selected_confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    rotation_avg_selected_final_score: float | None = Field(default=None, ge=0.0, le=100.0)
    rotation_avg_ranked_confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    rotation_avg_ranked_final_score: float | None = Field(default=None, ge=0.0, le=100.0)
    rotation_source: str = Field(default="", max_length=40)
    rotation_reason: str = Field(default="", max_length=120)
    market_note: str = Field(default="", max_length=160)


class RuntimePolicyDecision(BaseModel):
    profile: PolicyProfileLiteral
    confidence: float = Field(ge=0.0, le=1.0)
    note: str = Field(min_length=1, max_length=220)
    aggr_entry_strictness: Literal["strict", "normal"]
    risk_stake_multiplier: float = Field(ge=0.1, le=1.0)
    risk_max_open_trades: int = Field(ge=1, le=5)
    source: Literal["llm", "fallback"] = "llm"
    reason: str | None = None


class SkillItemsResponse(BaseModel):
    enabled: bool
    provider: str
    count: int
    meta: Dict[str, Any] = Field(default_factory=dict)
    items: List[Dict[str, Any]] = Field(default_factory=list)
