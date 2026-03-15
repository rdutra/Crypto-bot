import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional, Set, Tuple

import numpy as np
import requests
import talib.abstract as ta
from freqtrade.persistence import Trade
from freqtrade.strategy import IStrategy, merge_informative_pair
from pandas import DataFrame

LOGGER = logging.getLogger(__name__)

VALID_STRATEGY_MODES = {"conservative", "aggressive"}
STRATEGY_MODE = os.getenv("STRATEGY_MODE", "conservative").strip().lower()
if STRATEGY_MODE not in VALID_STRATEGY_MODES:
    STRATEGY_MODE = "conservative"


def _env_bool(key: str, default: bool) -> bool:
    value = os.getenv(key)
    if value is None or not value.strip():
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(key: str, default: float, min_value: float, max_value: float) -> float:
    try:
        parsed = float(os.getenv(key, str(default)))
    except ValueError:
        parsed = default
    return max(min_value, min(max_value, parsed))


def _env_roi_table(default: Dict[str, float]) -> Dict[str, float]:
    raw = os.getenv("STRATEGY_MINIMAL_ROI_JSON", "").strip()
    if not raw:
        return default
    try:
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            return default
        normalized: Dict[str, float] = {}
        for key, value in payload.items():
            minute = str(int(key))
            roi = float(value)
            if roi < 0:
                continue
            normalized[minute] = roi
        return normalized or default
    except Exception:
        return default


if STRATEGY_MODE == "aggressive":
    DEFAULT_MINIMAL_ROI = {"0": 0.05, "180": 0.025, "720": 0.0}
    DEFAULT_STOPLOSS = -0.08
    DEFAULT_TRAILING_STOP = False
    DEFAULT_TRAILING_POSITIVE = 0.015
    DEFAULT_TRAILING_OFFSET = 0.04
else:
    DEFAULT_MINIMAL_ROI = {"0": 0.05, "360": 0.02, "1080": 0.0}
    DEFAULT_STOPLOSS = -0.06
    DEFAULT_TRAILING_STOP = False
    DEFAULT_TRAILING_POSITIVE = 0.02
    DEFAULT_TRAILING_OFFSET = 0.04


class LlmTrendPullbackStrategy(IStrategy):
    timeframe = "15m" if STRATEGY_MODE == "aggressive" else "1h"
    informative_timeframe = "1h" if STRATEGY_MODE == "aggressive" else "4h"
    can_short = False

    minimal_roi = _env_roi_table(DEFAULT_MINIMAL_ROI)
    stoploss = _env_float("STRATEGY_STOPLOSS", DEFAULT_STOPLOSS, -0.2, -0.01)
    trailing_stop = _env_bool("STRATEGY_TRAILING_STOP", DEFAULT_TRAILING_STOP)
    trailing_stop_positive = _env_float("STRATEGY_TRAILING_POSITIVE", DEFAULT_TRAILING_POSITIVE, 0.001, 0.1)
    trailing_stop_positive_offset = _env_float("STRATEGY_TRAILING_OFFSET", DEFAULT_TRAILING_OFFSET, 0.002, 0.2)
    trailing_only_offset_is_reached = True
    use_custom_stoploss = True

    startup_candle_count = 250
    process_only_new_candles = True

    _llm_cache: Dict[str, Tuple[bool, str]] = {}
    _entry_rank_log_key: Optional[str] = None

    def _is_aggressive(self) -> bool:
        return STRATEGY_MODE == "aggressive"

    @property
    def protections(self):
        if self._is_aggressive():
            return [
                {"method": "CooldownPeriod", "stop_duration_candles": 2},
                {
                    "method": "StoplossGuard",
                    "lookback_period_candles": 32,
                    "trade_limit": 4,
                    "stop_duration_candles": 8,
                    "only_per_pair": False,
                },
                {
                    "method": "MaxDrawdown",
                    "lookback_period_candles": 64,
                    "trade_limit": 30,
                    "stop_duration_candles": 12,
                    "max_allowed_drawdown": 0.08,
                },
            ]

        return [
            {"method": "CooldownPeriod", "stop_duration_candles": 4},
            {
                "method": "StoplossGuard",
                "lookback_period_candles": 24,
                "trade_limit": 3,
                "stop_duration_candles": 12,
                "only_per_pair": False,
            },
            {
                "method": "MaxDrawdown",
                "lookback_period_candles": 48,
                "trade_limit": 20,
                "stop_duration_candles": 24,
                "max_allowed_drawdown": 0.05,
            },
        ]

    def informative_pairs(self):
        pairs = self.dp.current_whitelist() if self.dp else []
        informative = {(pair, self.informative_timeframe) for pair in pairs}
        benchmark_pair = self._benchmark_pair()
        informative.add((benchmark_pair, self.informative_timeframe))
        informative.add((benchmark_pair, self.timeframe))
        return list(informative)

    def _parse_pairs(self, value: str) -> Set[str]:
        return {part.strip().upper() for part in value.replace(",", " ").split() if part.strip()}

    def _pair_symbol(self, pair: str) -> str:
        # Handles symbols like "BTC/USDT:USDT" by keeping "BTC/USDT".
        return pair.split(":")[0].upper()

    def _core_pairs(self) -> Set[str]:
        return self._parse_pairs(os.getenv("CORE_PAIRS", "BTC/USDT ETH/USDT BNB/USDT"))

    def _risk_pairs(self) -> Set[str]:
        return self._parse_pairs(os.getenv("RISK_PAIRS", "SOL/USDT XRP/USDT AVAX/USDT"))

    def _benchmark_pair(self) -> str:
        return os.getenv("BENCHMARK_PAIR", "BTC/USDT").strip().upper() or "BTC/USDT"

    def _benchmark_filter_for_risk(self) -> bool:
        return _env_bool("BENCHMARK_FILTER_FOR_RISK", True)

    def _benchmark_allow_neutral_for_risk(self) -> bool:
        default = not self._aggr_entry_is_strict()
        return _env_bool("BENCHMARK_ALLOW_NEUTRAL_FOR_RISK", default)

    def _benchmark_chaos_adx(self) -> float:
        default = 16.0 if self._is_aggressive() else 18.0
        return self._float_env("BENCHMARK_CHAOS_ADX", default, 8.0, 40.0)

    def _benchmark_min_spread_pct(self) -> float:
        default = -0.05 if self._is_aggressive() else 0.0
        return self._float_env("BENCHMARK_MIN_SPREAD_PCT", default, -1.0, 2.0)

    def _benchmark_reduce_stake_when_weak(self) -> bool:
        return _env_bool("BENCHMARK_REDUCE_STAKE_WHEN_WEAK", True)

    def _benchmark_risk_stake_mult_when_weak(self) -> float:
        return self._float_env("BENCHMARK_RISK_STAKE_MULT_WHEN_WEAK", 0.6, 0.1, 1.0)

    def _benchmark_core_stake_mult_when_weak(self) -> float:
        return self._float_env("BENCHMARK_CORE_STAKE_MULT_WHEN_WEAK", 0.85, 0.1, 1.0)

    def _is_risk_pair(self, pair: str) -> bool:
        return self._pair_symbol(pair) in self._risk_pairs()

    def _is_core_pair(self, pair: str) -> bool:
        symbol = self._pair_symbol(pair)
        core = self._core_pairs()
        if symbol in core:
            return True
        return symbol not in self._risk_pairs()

    def _float_env(self, key: str, default: float, min_value: float, max_value: float) -> float:
        try:
            value = float(os.getenv(key, str(default)))
        except ValueError:
            value = default
        return max(min_value, min(max_value, value))

    def _int_env(self, key: str, default: int, min_value: int, max_value: int) -> int:
        try:
            value = int(os.getenv(key, str(default)))
        except ValueError:
            value = default
        return max(min_value, min(max_value, value))

    def _risk_stake_multiplier(self) -> float:
        return self._float_env("RISK_STAKE_MULTIPLIER", 0.5, 0.1, 1.0)

    def _risk_max_open_trades(self) -> int:
        return self._int_env("RISK_MAX_OPEN_TRADES", 1, 1, 5)

    def _entry_ranking_enabled(self) -> bool:
        return _env_bool("ENTRY_RANKING_ENABLED", self._is_aggressive())

    def _aggr_entry_strictness(self) -> str:
        value = os.getenv("AGGR_ENTRY_STRICTNESS", "strict").strip().lower()
        if value not in {"normal", "strict"}:
            return "strict"
        return value

    def _aggr_entry_is_strict(self) -> bool:
        return self._aggr_entry_strictness() == "strict"

    def _entry_top_n(self) -> int:
        default = 1 if self._is_aggressive() else 1
        return self._int_env("ENTRY_TOP_N", default, 1, 10)

    def _entry_min_score(self) -> float:
        default = 0.58 if self._is_aggressive() else 0.56
        return self._float_env("ENTRY_MIN_SCORE", default, 0.1, 0.95)

    def _exit_use_rsi_take(self) -> bool:
        return _env_bool("EXIT_USE_RSI_TAKE", False)

    def _stale_trade_hours(self) -> float:
        default = 24.0 if self._is_aggressive() else 40.0
        return self._float_env("STALE_TRADE_HOURS", default, 2.0, 240.0)

    def _stale_min_profit(self) -> float:
        default = 0.01 if self._is_aggressive() else 0.006
        return self._float_env("STALE_MIN_PROFIT", default, -0.02, 0.05)

    def _stale_loss_hours(self) -> float:
        default = 12.0 if self._is_aggressive() else 18.0
        return self._float_env("STALE_LOSS_HOURS", default, 1.0, 120.0)

    def _stale_loss_pct(self) -> float:
        default = -0.02 if self._is_aggressive() else -0.015
        return self._float_env("STALE_LOSS_PCT", default, -0.2, -0.001)

    def _stale_max_hours(self) -> float:
        default = 72.0 if self._is_aggressive() else 120.0
        return self._float_env("STALE_MAX_HOURS", default, 4.0, 720.0)

    def _custom_sl_atr_mult(self) -> float:
        default = 1.6 if self._is_aggressive() else 1.3
        return self._float_env("CUSTOM_SL_ATR_MULT", default, 0.5, 4.0)

    def _custom_sl_min(self) -> float:
        default = self.stoploss
        return self._float_env("CUSTOM_SL_MIN", default, -0.2, -0.01)

    def _custom_sl_max(self) -> float:
        default = -0.015
        return self._float_env("CUSTOM_SL_MAX", default, -0.1, -0.005)

    def _is_live_like(self) -> bool:
        runmode = getattr(getattr(self, "dp", None), "runmode", None)
        runmode_value = str(getattr(runmode, "value", runmode)).lower()
        return runmode_value in {"live", "dry_run"}

    def _entry_ranking_log_enabled(self) -> bool:
        return _env_bool("ENTRY_RANKING_LOG", self._is_live_like())

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return default
        if np.isnan(parsed) or np.isinf(parsed):
            return default
        return parsed

    def _latest_row(self, pair: str) -> Optional[Any]:
        if not self.dp:
            return None
        try:
            analyzed, _ = self.dp.get_analyzed_dataframe(pair=pair, timeframe=self.timeframe)
            if analyzed is None or analyzed.empty:
                return None
            return analyzed.iloc[-1]
        except Exception:
            return None

    def _latest_rows(self, pair: str, count: int = 2) -> Optional[DataFrame]:
        if not self.dp:
            return None
        try:
            analyzed, _ = self.dp.get_analyzed_dataframe(pair=pair, timeframe=self.timeframe)
            if analyzed is None or analyzed.empty:
                return None
            return analyzed.tail(max(1, count))
        except Exception:
            return None

    def _normalize(self, value: float, low: float, high: float) -> float:
        if high <= low:
            return 0.0
        return max(0.0, min(1.0, (value - low) / (high - low)))

    def _entry_score(self, row: Any, pair: str) -> float:
        rsi = self._safe_float(row.get("rsi"), 50.0)
        adx = self._safe_float(row.get("adx"), 0.0)
        spread = self._safe_float(row.get("ema_spread_pct"), 0.0)
        atr_pct = self._safe_float(row.get("atr_pct"), 0.0)
        vol_z = self._safe_float(row.get("volume_z"), 0.0)
        close = self._safe_float(row.get("close"), 0.0)
        ema20 = self._safe_float(row.get("ema20"), close)
        trend_flag = 1.0 if int(self._safe_float(row.get(self._trend_col()), 0.0)) == 1 else 0.0

        rsi_center = 52.0 if self._is_aggressive() else 50.0
        rsi_score = max(0.0, 1.0 - abs(rsi - rsi_center) / 20.0)
        adx_score = self._normalize(adx, 10.0, 35.0)
        spread_score = self._normalize(spread, -0.1, 0.8 if self._is_aggressive() else 0.6)
        atr_score = 1.0 - min(1.0, abs(atr_pct - 2.0) / 3.0)
        vol_score = self._normalize(vol_z, -1.5, 2.5)
        pullback_dist = abs((close / ema20) - 1.0) if ema20 > 0 else 0.02
        pullback_score = max(0.0, 1.0 - min(1.0, pullback_dist / 0.03))

        score = (
            0.22 * trend_flag
            + 0.2 * adx_score
            + 0.18 * spread_score
            + 0.14 * rsi_score
            + 0.12 * pullback_score
            + 0.08 * atr_score
            + 0.06 * vol_score
        )
        if self._is_risk_pair(pair):
            score *= 0.98
        return max(0.0, min(1.0, score))

    def _ranked_entry_allowed(self, pair: str, current_time: datetime) -> bool:
        if not self._entry_ranking_enabled() or not self.dp:
            return True

        whitelist = self.dp.current_whitelist() if self.dp else []
        if not whitelist:
            return True

        candidates = []
        for candidate_pair in whitelist:
            row = self._latest_row(candidate_pair)
            if row is None:
                continue
            if int(self._safe_float(row.get("enter_long"), 0.0)) != 1:
                continue
            score = self._entry_score(row, candidate_pair)
            if score >= self._entry_min_score():
                candidates.append((candidate_pair, score))

        if not candidates:
            return False

        candidates.sort(key=lambda item: item[1], reverse=True)
        selected_pairs = {p for p, _ in candidates[: self._entry_top_n()]}

        top_row = self._latest_row(pair)
        candle_label = ""
        if top_row is not None:
            candle_date = top_row.get("date")
            candle_label = candle_date.isoformat() if hasattr(candle_date, "isoformat") else str(candle_date)
        rank_key = f"{candle_label}:{','.join(sorted(selected_pairs))}"
        if rank_key != self._entry_rank_log_key:
            self._entry_rank_log_key = rank_key
            preview = ", ".join([f"{p}:{s:.2f}" for p, s in candidates[:5]])
            if self._entry_ranking_log_enabled():
                LOGGER.info(
                    "Entry ranking at %s -> selected=%s top_n=%s min_score=%.2f candidates=%s",
                    candle_label or current_time.isoformat(),
                    " ".join(sorted(selected_pairs)) or "none",
                    self._entry_top_n(),
                    self._entry_min_score(),
                    preview or "none",
                )

        return pair in selected_pairs

    def _entry_thresholds(self, pair: str) -> Dict[str, float]:
        if self._is_risk_pair(pair):
            if self._is_aggressive():
                strict = self._aggr_entry_is_strict()
                return {
                    "rsi_min": 38.0,
                    "rsi_max": 66.0,
                    "adx_min": self._float_env("RISK_ADX_MIN", 18.0 if strict else 14.0, 10.0, 35.0),
                    "atr_min": 0.4,
                    "atr_max": self._float_env("RISK_ATR_MAX", 6.0, 1.2, 9.0),
                    "ema_spread_min": self._float_env("RISK_EMA_SPREAD_MIN", 0.0 if strict else -0.05, -0.4, 1.5),
                    "ema20_overext": 1.05,
                    "pullback_floor": 0.94,
                    "vol_mult_min": 0.45 if strict else 0.35,
                    "vol_z_min": -1.8,
                    "rebound_over_prev": 0.995,
                }
            return {
                "rsi_min": 46.0,
                "rsi_max": 56.0,
                "adx_min": self._float_env("RISK_ADX_MIN", 26.0, 18.0, 45.0),
                "atr_min": 1.1,
                "atr_max": self._float_env("RISK_ATR_MAX", 3.4, 1.5, 6.0),
                "ema_spread_min": self._float_env("RISK_EMA_SPREAD_MIN", 0.25, 0.05, 1.0),
                "ema20_overext": 1.01,
                "pullback_floor": 0.98,
                "vol_mult_min": 1.0,
                "vol_z_min": -0.2,
                "rebound_over_prev": 1.002,
            }

        if self._is_aggressive():
            strict = self._aggr_entry_is_strict()
            return {
                "rsi_min": 38.0,
                "rsi_max": 66.0,
                "adx_min": 16.0 if strict else 12.0,
                "atr_min": 0.4,
                "atr_max": 6.0,
                "ema_spread_min": 0.03 if strict else -0.05,
                "ema20_overext": 1.05,
                "pullback_floor": 0.95,
                "vol_mult_min": 0.45 if strict else 0.35,
                "vol_z_min": -1.8,
                "rebound_over_prev": 0.995,
            }

        return {
            "rsi_min": 44.0,
            "rsi_max": 58.0,
            "adx_min": 22.0,
            "atr_min": 0.9,
            "atr_max": 3.8,
            "ema_spread_min": 0.15,
            "ema20_overext": 1.015,
            "pullback_floor": 0.98,
            "vol_mult_min": 0.8,
            "vol_z_min": -0.6,
            "rebound_over_prev": 1.0,
        }

    def _exit_thresholds(self, pair: str) -> Dict[str, float]:
        if self._is_risk_pair(pair):
            if self._is_aggressive():
                return {
                    "rsi_take": 82.0,
                    "ema20_break": 0.992,
                    "adx_weak": 18.0,
                    "ema50_break": 0.978,
                }
            return {
                "rsi_take": 84.0,
                "ema20_break": 0.99,
                "adx_weak": 22.0,
                "ema50_break": 0.985,
            }

        if self._is_aggressive():
            return {
                "rsi_take": 84.0,
                "ema20_break": 0.99,
                "adx_weak": 16.0,
                "ema50_break": 0.975,
            }

        return {
            "rsi_take": 86.0,
            "ema20_break": 0.985,
            "adx_weak": 20.0,
            "ema50_break": 0.98,
        }

    def _llm_enabled(self) -> bool:
        flag = os.getenv("ENABLE_LLM_FILTER", "false").strip().lower()
        if flag not in {"1", "true", "yes", "on"}:
            return False

        runmode = getattr(getattr(self, "dp", None), "runmode", None)
        runmode_value = str(getattr(runmode, "value", runmode)).lower()
        return runmode_value in {"live", "dry_run"}

    def _llm_min_confidence(self) -> float:
        try:
            return float(os.getenv("LLM_MIN_CONFIDENCE", "0.65"))
        except ValueError:
            return 0.65

    def _market_structure(self, row: Any) -> str:
        if row["close"] > row["ema20"] > row["ema50"] > row["ema200"]:
            return "higher_highs"
        return "mixed"

    def _trend_col(self) -> str:
        return f"trend_{self.informative_timeframe}"

    def _ema50_info_col(self) -> str:
        return f"ema50_{self.informative_timeframe}"

    def _ema200_info_col(self) -> str:
        return f"ema200_{self.informative_timeframe}"

    def _llm_allows_trade(self, row: Any, pair: str) -> Tuple[bool, str]:
        candle_time = row["date"].isoformat() if hasattr(row["date"], "isoformat") else str(row["date"])
        cache_key = f"{pair}:{candle_time}"
        cached = self._llm_cache.get(cache_key)
        if cached is not None:
            return cached

        payload = {
            "pair": pair,
            "timeframe": self.timeframe,
            "price": float(row["close"]),
            "ema_20": float(row["ema20"]),
            "ema_50": float(row["ema50"]),
            "ema_200": float(row["ema200"]),
            "rsi_14": float(row["rsi"]),
            "adx_14": float(row["adx"]),
            "atr_pct": float(row["atr_pct"]),
            "volume_zscore": float(row["volume_z"]),
            "trend_4h": "bullish" if bool(row.get(self._trend_col(), 0)) else "bearish",
            "market_structure": self._market_structure(row),
        }
        bot_api = os.getenv("BOT_API_URL", "http://bot-api:8000")
        min_conf = self._llm_min_confidence()

        try:
            response = requests.post(f"{bot_api}/classify", json=payload, timeout=(2, 5))
            response.raise_for_status()
            data = response.json()
            regime = str(data.get("regime", "")).lower()
            risk_level = str(data.get("risk_level", "high")).lower()
            confidence = float(data.get("confidence", 0.0))

            allowed = regime == "trend_pullback" and risk_level in {"low", "medium"} and confidence >= min_conf
            reason = f"{regime}:{risk_level}:{confidence:.2f}"
        except Exception:
            allowed = False
            reason = "llm_error"

        self._llm_cache[cache_key] = (allowed, reason)
        return allowed, reason

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        trend_col = self._trend_col()
        ema50_info_col = self._ema50_info_col()
        ema200_info_col = self._ema200_info_col()
        bench_inf_trend_col = "bench_inf_trend"
        bench_tf_trend_col = "bench_tf_trend"
        bench_tf_adx_col = "bench_tf_adx"
        bench_tf_spread_col = "bench_tf_spread_pct"
        bench_tf_close_col = "bench_tf_close"
        bench_tf_ema20_col = "bench_tf_ema20"
        bench_tf_trend_src = "bench_tf_trend_src"
        bench_tf_adx_src = "bench_tf_adx_src"
        bench_tf_spread_src = "bench_tf_spread_pct_src"
        bench_tf_close_src = "bench_tf_close_src"
        bench_tf_ema20_src = "bench_tf_ema20_src"

        dataframe["ema20"] = ta.EMA(dataframe, timeperiod=20)
        dataframe["ema50"] = ta.EMA(dataframe, timeperiod=50)
        dataframe["ema200"] = ta.EMA(dataframe, timeperiod=200)
        dataframe["rsi"] = ta.RSI(dataframe, timeperiod=14)
        dataframe["adx"] = ta.ADX(dataframe, timeperiod=14)
        dataframe["atr"] = ta.ATR(dataframe, timeperiod=14)
        dataframe["atr_pct"] = dataframe["atr"] / dataframe["close"] * 100.0
        dataframe["atr_pct_sma30"] = dataframe["atr_pct"].rolling(30, min_periods=5).mean()
        dataframe["atr_pct_sma30"] = dataframe["atr_pct_sma30"].fillna(dataframe["atr_pct"])
        dataframe["vol_ma20"] = dataframe["volume"].rolling(20).mean()
        vol_std = dataframe["volume"].rolling(20).std()
        dataframe["volume_z"] = ((dataframe["volume"] - dataframe["vol_ma20"]) / vol_std).replace(
            [np.inf, -np.inf], np.nan
        )
        dataframe["volume_z"] = dataframe["volume_z"].fillna(0.0)
        dataframe["ema_spread_pct"] = ((dataframe["ema20"] - dataframe["ema50"]) / dataframe["close"]) * 100.0
        dataframe[bench_inf_trend_col] = 0
        dataframe[bench_tf_trend_col] = 0
        dataframe[bench_tf_adx_col] = 0.0
        dataframe[bench_tf_spread_col] = 0.0
        dataframe[bench_tf_close_col] = np.nan
        dataframe[bench_tf_ema20_col] = np.nan

        if self.dp:
            informative = self.dp.get_pair_dataframe(pair=metadata["pair"], timeframe=self.informative_timeframe)
            if informative is not None and not informative.empty:
                informative["ema50"] = ta.EMA(informative, timeperiod=50)
                informative["ema200"] = ta.EMA(informative, timeperiod=200)
                informative["trend"] = (informative["ema50"] > informative["ema200"]).astype("int8")
                dataframe = merge_informative_pair(
                    dataframe,
                    informative[["date", "ema50", "ema200", "trend"]],
                    self.timeframe,
                    self.informative_timeframe,
                    ffill=True,
                )
                dataframe[trend_col] = dataframe[trend_col].fillna(0).astype("int8")
            else:
                dataframe[trend_col] = 0
                dataframe[ema50_info_col] = np.nan
                dataframe[ema200_info_col] = np.nan

            benchmark_pair = self._benchmark_pair()
            bench_inf = self.dp.get_pair_dataframe(pair=benchmark_pair, timeframe=self.informative_timeframe)
            if bench_inf is not None and not bench_inf.empty:
                bench_inf["ema50"] = ta.EMA(bench_inf, timeperiod=50)
                bench_inf["ema200"] = ta.EMA(bench_inf, timeperiod=200)
                bench_inf["bench_inf_trend"] = (bench_inf["ema50"] > bench_inf["ema200"]).astype("int8")
                dataframe = merge_informative_pair(
                    dataframe,
                    bench_inf[["date", "bench_inf_trend"]],
                    self.timeframe,
                    self.informative_timeframe,
                    ffill=True,
                )
                bench_inf_merged_col = f"bench_inf_trend_{self.informative_timeframe}"
                if bench_inf_merged_col in dataframe:
                    dataframe[bench_inf_trend_col] = dataframe[bench_inf_merged_col].fillna(0).astype("int8")

            bench_tf = self.dp.get_pair_dataframe(pair=benchmark_pair, timeframe=self.timeframe)
            if bench_tf is not None and not bench_tf.empty:
                bench_tf = bench_tf.copy()
                bench_tf["bench_tf_ema20_tmp"] = ta.EMA(bench_tf, timeperiod=20)
                bench_tf["bench_tf_ema50_tmp"] = ta.EMA(bench_tf, timeperiod=50)
                bench_tf["bench_tf_ema200_tmp"] = ta.EMA(bench_tf, timeperiod=200)
                bench_tf[bench_tf_adx_src] = ta.ADX(bench_tf, timeperiod=14)
                bench_tf[bench_tf_spread_src] = (
                    (bench_tf["bench_tf_ema20_tmp"] - bench_tf["bench_tf_ema50_tmp"]) / bench_tf["close"] * 100.0
                )
                bench_tf[bench_tf_trend_src] = (
                    bench_tf["bench_tf_ema50_tmp"] > bench_tf["bench_tf_ema200_tmp"]
                ).astype("int8")
                bench_tf[bench_tf_close_src] = bench_tf["close"]
                bench_tf[bench_tf_ema20_src] = bench_tf["bench_tf_ema20_tmp"]
                dataframe = dataframe.merge(
                    bench_tf[
                        [
                            "date",
                            bench_tf_trend_src,
                            bench_tf_adx_src,
                            bench_tf_spread_src,
                            bench_tf_close_src,
                            bench_tf_ema20_src,
                        ]
                    ],
                    on="date",
                    how="left",
                )
                dataframe[bench_tf_trend_col] = dataframe[bench_tf_trend_col].combine_first(dataframe[bench_tf_trend_src])
                dataframe[bench_tf_adx_col] = dataframe[bench_tf_adx_col].combine_first(dataframe[bench_tf_adx_src])
                dataframe[bench_tf_spread_col] = dataframe[bench_tf_spread_col].combine_first(dataframe[bench_tf_spread_src])
                dataframe[bench_tf_close_col] = dataframe[bench_tf_close_col].combine_first(dataframe[bench_tf_close_src])
                dataframe[bench_tf_ema20_col] = dataframe[bench_tf_ema20_col].combine_first(dataframe[bench_tf_ema20_src])
                dataframe.drop(
                    columns=[
                        bench_tf_trend_src,
                        bench_tf_adx_src,
                        bench_tf_spread_src,
                        bench_tf_close_src,
                        bench_tf_ema20_src,
                    ],
                    inplace=True,
                    errors="ignore",
                )
        else:
            dataframe[trend_col] = 0
            dataframe[ema50_info_col] = np.nan
            dataframe[ema200_info_col] = np.nan

        dataframe[bench_inf_trend_col] = dataframe[bench_inf_trend_col].fillna(0).astype("int8")
        dataframe[bench_tf_trend_col] = dataframe[bench_tf_trend_col].fillna(0).astype("int8")
        dataframe[bench_tf_adx_col] = dataframe[bench_tf_adx_col].fillna(0.0)
        dataframe[bench_tf_spread_col] = dataframe[bench_tf_spread_col].fillna(0.0)
        dataframe[bench_tf_close_col] = dataframe[bench_tf_close_col].fillna(dataframe["close"])
        dataframe[bench_tf_ema20_col] = dataframe[bench_tf_ema20_col].fillna(dataframe[bench_tf_close_col])

        bench_chaos = (
            (dataframe[bench_tf_adx_col] < self._benchmark_chaos_adx())
            | (dataframe[bench_tf_spread_col] < self._benchmark_min_spread_pct())
            | (
                (dataframe[bench_tf_close_col] < dataframe[bench_tf_ema20_col])
                & (dataframe[bench_tf_trend_col] != 1)
            )
        )
        bench_healthy = (dataframe[bench_inf_trend_col] == 1) & (dataframe[bench_tf_trend_col] == 1) & (~bench_chaos)
        bench_neutral = (dataframe[bench_inf_trend_col] == 1) & (~bench_chaos) & (~bench_healthy)
        if self._benchmark_allow_neutral_for_risk():
            bench_risk_ok = bench_healthy | bench_neutral
        else:
            bench_risk_ok = bench_healthy

        dataframe["bench_chaos"] = bench_chaos.astype("int8")
        dataframe["bench_healthy"] = bench_healthy.astype("int8")
        dataframe["bench_neutral"] = bench_neutral.astype("int8")
        dataframe["bench_risk_ok"] = bench_risk_ok.astype("int8")
        dataframe["bench_weak"] = (~(bench_healthy | bench_neutral)).astype("int8")

        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe["enter_long"] = 0
        dataframe["enter_tag"] = None
        thresholds = self._entry_thresholds(metadata["pair"])
        informative_trend_ratio = 1.0 if self._is_aggressive() else 1.01
        trend_col = self._trend_col()
        ema50_info_col = self._ema50_info_col()
        ema200_info_col = self._ema200_info_col()

        touched_pullback_zone = (
            (dataframe["low"] <= dataframe["ema20"] * 1.002)
            | (dataframe["low"] <= dataframe["ema50"] * 1.01)
            | (dataframe["close"].shift(1) <= dataframe["ema20"].shift(1) * 1.001)
        )
        rebound_confirmed = (
            (dataframe["close"] > dataframe["open"])
            & (dataframe["close"] > dataframe["close"].shift(1) * thresholds["rebound_over_prev"])
            & (dataframe["close"] > dataframe["ema20"])
        )

        if self._is_aggressive():
            strict_aggr = self._aggr_entry_is_strict()
            adx_or_spread_ok = (
                (dataframe["adx"] >= thresholds["adx_min"])
                if strict_aggr
                else ((dataframe["adx"] >= thresholds["adx_min"]) | (dataframe["ema_spread_pct"] > 0))
            )
            trend_ok = (
                ((dataframe[trend_col] == 1) | (dataframe["close"] > dataframe["ema200"] * 1.0))
                if strict_aggr
                else ((dataframe[trend_col] == 1) | (dataframe["close"] > dataframe["ema200"] * 0.98))
            )
            deterministic_entry = (
                (dataframe["close"] > dataframe["ema20"])
                & (dataframe["ema20"] >= dataframe["ema50"] * 0.998)
                & (dataframe["rsi"] >= thresholds["rsi_min"])
                & (dataframe["rsi"] <= thresholds["rsi_max"])
                & adx_or_spread_ok
                & (dataframe["atr_pct"] >= thresholds["atr_min"])
                & (dataframe["atr_pct"] <= thresholds["atr_max"])
                & (dataframe["ema_spread_pct"] >= thresholds["ema_spread_min"])
                & (dataframe["close"] <= dataframe["ema20"] * thresholds["ema20_overext"])
                & (dataframe["close"] >= dataframe["ema50"] * thresholds["pullback_floor"])
                & (dataframe["volume"] > dataframe["vol_ma20"] * thresholds["vol_mult_min"])
                & (dataframe["volume_z"] > thresholds["vol_z_min"])
                & trend_ok
            )
        else:
            deterministic_entry = (
                (dataframe["close"] > dataframe["ema200"])
                & (dataframe["ema20"] > dataframe["ema50"])
                & (dataframe["ema50"] > dataframe["ema200"])
                & (dataframe[ema50_info_col] > dataframe[ema200_info_col] * informative_trend_ratio)
                & (dataframe[trend_col] == 1)
                & (dataframe["rsi"] >= thresholds["rsi_min"])
                & (dataframe["rsi"] <= thresholds["rsi_max"])
                & (dataframe["adx"] >= thresholds["adx_min"])
                & (dataframe["atr_pct"] >= thresholds["atr_min"])
                & (dataframe["atr_pct"] <= thresholds["atr_max"])
                & (dataframe["ema_spread_pct"] >= thresholds["ema_spread_min"])
                & (dataframe["close"] <= dataframe["ema20"] * thresholds["ema20_overext"])
                & (dataframe["close"] >= dataframe["ema50"] * thresholds["pullback_floor"])
                & (dataframe["volume"] > dataframe["vol_ma20"] * thresholds["vol_mult_min"])
                & (dataframe["volume_z"] > thresholds["vol_z_min"])
                & touched_pullback_zone
                & rebound_confirmed
            )

        if self._is_risk_pair(metadata["pair"]) and self._benchmark_filter_for_risk():
            deterministic_entry = deterministic_entry & (dataframe["bench_risk_ok"] == 1)

        dataframe.loc[deterministic_entry, "enter_long"] = 1
        dataframe.loc[deterministic_entry, "enter_tag"] = "base_trend_pullback"

        if self._llm_enabled() and not dataframe.empty:
            idx = dataframe.index[-1]
            if int(dataframe.at[idx, "enter_long"]) == 1:
                allowed, reason = self._llm_allows_trade(dataframe.loc[idx], metadata["pair"])
                if not allowed:
                    dataframe.at[idx, "enter_long"] = 0
                    dataframe.at[idx, "enter_tag"] = f"llm_block:{reason}"[:64]
                else:
                    dataframe.at[idx, "enter_tag"] = f"llm_ok:{reason}"[:64]

        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe["exit_long"] = 0
        dataframe["exit_tag"] = None
        thresholds = self._exit_thresholds(metadata["pair"])
        trend_col = self._trend_col()
        rsi_exit = dataframe["rsi"] > thresholds["rsi_take"] if self._exit_use_rsi_take() else False

        exit_condition = (
            rsi_exit
            | (
                (dataframe["close"] < dataframe["ema20"] * thresholds["ema20_break"])
                & (dataframe["adx"] < thresholds["adx_weak"])
            )
            | (dataframe["close"] < dataframe["ema50"] * thresholds["ema50_break"])
            | (dataframe[trend_col] == 0)
        )
        dataframe.loc[exit_condition, "exit_long"] = 1
        dataframe.loc[exit_condition, "exit_tag"] = "trend_break_or_overbought"
        return dataframe

    def custom_stoploss(
        self,
        pair: str,
        trade: Trade,
        current_time: datetime,
        current_rate: float,
        current_profit: float,
        after_fill: bool,
        **kwargs,
    ) -> Optional[float]:
        _ = (trade, current_time, current_rate, after_fill)

        is_risk = self._is_risk_pair(pair)
        is_core = self._is_core_pair(pair)
        dynamic_sl = float(self.stoploss)
        rows = self._latest_rows(pair, 3)
        if rows is not None and not rows.empty:
            row = rows.iloc[-1]
            prev_row = rows.iloc[-2] if len(rows) >= 2 else row

            atr_pct = max(0.1, self._safe_float(row.get("atr_pct"), 0.0))
            atr_pct_sma = max(0.1, self._safe_float(row.get("atr_pct_sma30"), atr_pct))
            vol_ratio = atr_pct / atr_pct_sma
            vol_spike = vol_ratio >= 1.35
            vol_compression = vol_ratio <= 0.80

            adx = self._safe_float(row.get("adx"), 0.0)
            prev_adx = self._safe_float(prev_row.get("adx"), adx)
            adx_delta = adx - prev_adx
            adx_rising = adx_delta >= 0.4
            adx_rolling_over = adx_delta <= -0.5

            close = self._safe_float(row.get("close"), 0.0)
            ema20 = self._safe_float(row.get("ema20"), close)
            ema50 = self._safe_float(row.get("ema50"), close)
            above_ema20 = close >= ema20 if ema20 > 0 else False
            below_ema20 = close < ema20 if ema20 > 0 else False
            below_ema50 = close < ema50 if ema50 > 0 else False

            atr_mult = self._custom_sl_atr_mult()
            if is_risk:
                atr_mult *= 0.90
            elif is_core:
                atr_mult *= 1.05
            atr_based = -(atr_pct / 100.0) * atr_mult
            dynamic_sl = max(dynamic_sl, atr_based)

            profit_stop: Optional[float] = None
            if current_profit >= 0.08:
                profit_stop = -0.018
            elif current_profit >= 0.05:
                profit_stop = -0.024
            elif current_profit >= 0.03:
                profit_stop = -0.03
            elif current_profit >= 0.015:
                profit_stop = -0.04

            if profit_stop is not None:
                # Risk pairs are tightened earlier; core pairs get slightly more room.
                if is_risk:
                    profit_stop += 0.004
                elif is_core:
                    profit_stop -= 0.002

                # Profit protection based on trend-state transitions.
                if current_profit >= 0.04:
                    if adx_rising and above_ema20 and not below_ema50:
                        # Trend still healthy: keep more room to run.
                        profit_stop -= 0.008
                    elif adx_rolling_over and below_ema20:
                        # Momentum fading and losing EMA20: tighten faster.
                        profit_stop += 0.010

                if below_ema20 and current_profit > 0.02:
                    profit_stop = max(profit_stop, -0.028)
                if below_ema50 and current_profit > 0.0:
                    profit_stop = max(profit_stop, -0.02)

                # Volatility regime after entry proxy via ATR vs ATR mean.
                if vol_spike and below_ema20:
                    profit_stop += 0.006
                elif vol_spike and adx_rising and above_ema20:
                    profit_stop -= 0.004
                elif vol_compression and current_profit > 0.03:
                    profit_stop += 0.003

                dynamic_sl = max(dynamic_sl, profit_stop)

        dynamic_sl = max(dynamic_sl, self._custom_sl_min())
        dynamic_sl = min(dynamic_sl, self._custom_sl_max())
        return max(-0.2, min(-0.005, dynamic_sl))

    def custom_exit(
        self,
        pair: str,
        trade: Trade,
        current_time: datetime,
        current_rate: float,
        current_profit: float,
        **kwargs,
    ) -> Optional[str]:
        _ = (pair, current_rate, kwargs)
        open_dt = getattr(trade, "open_date_utc", None)
        if open_dt is None:
            return None

        age_hours = (current_time - open_dt).total_seconds() / 3600.0
        if age_hours >= self._stale_max_hours() and current_profit < 0.02:
            return "max_age_exit"

        if age_hours >= self._stale_loss_hours() and current_profit <= self._stale_loss_pct():
            return "stale_loss_exit"

        if age_hours >= self._stale_trade_hours() and current_profit < self._stale_min_profit():
            return "stale_trade_exit"

        return None

    def custom_stake_amount(
        self,
        pair: str,
        current_time: datetime,
        current_rate: float,
        proposed_stake: float,
        min_stake: Optional[float],
        max_stake: float,
        leverage: float,
        entry_tag: Optional[str],
        side: str,
        **kwargs,
    ) -> float:
        stake = float(proposed_stake)
        if self._is_risk_pair(pair):
            stake *= self._risk_stake_multiplier()

        if self._benchmark_reduce_stake_when_weak():
            row = self._latest_row(pair)
            if row is not None:
                bench_weak = self._safe_float(row.get("bench_weak"), 0.0) >= 0.5
                bench_chaos = self._safe_float(row.get("bench_chaos"), 0.0) >= 0.5
                if bench_weak or bench_chaos:
                    if self._is_risk_pair(pair):
                        stake *= self._benchmark_risk_stake_mult_when_weak()
                    else:
                        stake *= self._benchmark_core_stake_mult_when_weak()

        if min_stake is not None:
            stake = max(stake, float(min_stake))
        if max_stake is not None:
            stake = min(stake, float(max_stake))
        return stake

    def confirm_trade_entry(
        self,
        pair: str,
        order_type: str,
        amount: float,
        rate: float,
        time_in_force: str,
        current_time: datetime,
        entry_tag: Optional[str],
        side: str,
        **kwargs,
    ) -> bool:
        if self._entry_ranking_enabled() and side == "long":
            if not self._ranked_entry_allowed(pair, current_time):
                return False

        if not self._is_risk_pair(pair):
            return True

        max_risk_open = self._risk_max_open_trades()

        open_trades = []
        try:
            if hasattr(Trade, "get_open_trades"):
                open_trades = Trade.get_open_trades()
            elif hasattr(Trade, "get_trades_proxy"):
                open_trades = Trade.get_trades_proxy(is_open=True)
        except Exception:
            return True

        risk_open_count = sum(1 for trade in open_trades if self._is_risk_pair(getattr(trade, "pair", "")))
        if risk_open_count >= max_risk_open:
            return False

        return True
