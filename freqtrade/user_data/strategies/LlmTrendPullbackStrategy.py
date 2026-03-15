import os
from datetime import datetime
from typing import Any, Dict, Optional, Set, Tuple

import numpy as np
import requests
import talib.abstract as ta
from freqtrade.persistence import Trade
from freqtrade.strategy import IStrategy, merge_informative_pair
from pandas import DataFrame

VALID_STRATEGY_MODES = {"conservative", "aggressive"}
STRATEGY_MODE = os.getenv("STRATEGY_MODE", "conservative").strip().lower()
if STRATEGY_MODE not in VALID_STRATEGY_MODES:
    STRATEGY_MODE = "conservative"


class LlmTrendPullbackStrategy(IStrategy):
    timeframe = "15m" if STRATEGY_MODE == "aggressive" else "1h"
    informative_timeframe = "1h" if STRATEGY_MODE == "aggressive" else "4h"
    can_short = False

    minimal_roi = {"0": 0.03, "120": 0.015, "480": 0.0} if STRATEGY_MODE == "aggressive" else {"0": 0.05, "360": 0.02, "1080": 0.0}
    stoploss = -0.08 if STRATEGY_MODE == "aggressive" else -0.06
    trailing_stop = False
    trailing_stop_positive = 0.02
    trailing_stop_positive_offset = 0.04
    trailing_only_offset_is_reached = True
    use_custom_stoploss = False

    startup_candle_count = 250
    process_only_new_candles = True

    _llm_cache: Dict[str, Tuple[bool, str]] = {}

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
        return [(pair, self.informative_timeframe) for pair in pairs]

    def _parse_pairs(self, value: str) -> Set[str]:
        return {part.strip().upper() for part in value.replace(",", " ").split() if part.strip()}

    def _pair_symbol(self, pair: str) -> str:
        # Handles symbols like "BTC/USDT:USDT" by keeping "BTC/USDT".
        return pair.split(":")[0].upper()

    def _core_pairs(self) -> Set[str]:
        return self._parse_pairs(os.getenv("CORE_PAIRS", "BTC/USDT ETH/USDT BNB/USDT"))

    def _risk_pairs(self) -> Set[str]:
        return self._parse_pairs(os.getenv("RISK_PAIRS", "SOL/USDT XRP/USDT AVAX/USDT"))

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

    def _entry_thresholds(self, pair: str) -> Dict[str, float]:
        if self._is_risk_pair(pair):
            if self._is_aggressive():
                return {
                    "rsi_min": 38.0,
                    "rsi_max": 66.0,
                    "adx_min": self._float_env("RISK_ADX_MIN", 14.0, 10.0, 35.0),
                    "atr_min": 0.4,
                    "atr_max": self._float_env("RISK_ATR_MAX", 6.0, 1.2, 9.0),
                    "ema_spread_min": self._float_env("RISK_EMA_SPREAD_MIN", -0.05, -0.4, 1.5),
                    "ema20_overext": 1.05,
                    "pullback_floor": 0.94,
                    "vol_mult_min": 0.35,
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
            return {
                "rsi_min": 38.0,
                "rsi_max": 66.0,
                "adx_min": 12.0,
                "atr_min": 0.4,
                "atr_max": 6.0,
                "ema_spread_min": -0.05,
                "ema20_overext": 1.05,
                "pullback_floor": 0.95,
                "vol_mult_min": 0.35,
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
                    "rsi_take": 70.0,
                    "ema20_break": 0.992,
                    "adx_weak": 18.0,
                    "ema50_break": 0.978,
                }
            return {
                "rsi_take": 74.0,
                "ema20_break": 0.99,
                "adx_weak": 22.0,
                "ema50_break": 0.985,
            }

        if self._is_aggressive():
            return {
                "rsi_take": 72.0,
                "ema20_break": 0.99,
                "adx_weak": 16.0,
                "ema50_break": 0.975,
            }

        return {
            "rsi_take": 76.0,
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

        dataframe["ema20"] = ta.EMA(dataframe, timeperiod=20)
        dataframe["ema50"] = ta.EMA(dataframe, timeperiod=50)
        dataframe["ema200"] = ta.EMA(dataframe, timeperiod=200)
        dataframe["rsi"] = ta.RSI(dataframe, timeperiod=14)
        dataframe["adx"] = ta.ADX(dataframe, timeperiod=14)
        dataframe["atr"] = ta.ATR(dataframe, timeperiod=14)
        dataframe["atr_pct"] = dataframe["atr"] / dataframe["close"] * 100.0
        dataframe["vol_ma20"] = dataframe["volume"].rolling(20).mean()
        vol_std = dataframe["volume"].rolling(20).std()
        dataframe["volume_z"] = ((dataframe["volume"] - dataframe["vol_ma20"]) / vol_std).replace(
            [np.inf, -np.inf], np.nan
        )
        dataframe["volume_z"] = dataframe["volume_z"].fillna(0.0)
        dataframe["ema_spread_pct"] = ((dataframe["ema20"] - dataframe["ema50"]) / dataframe["close"]) * 100.0

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
        else:
            dataframe[trend_col] = 0
            dataframe[ema50_info_col] = np.nan
            dataframe[ema200_info_col] = np.nan

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
            deterministic_entry = (
                (dataframe["close"] > dataframe["ema20"])
                & (dataframe["ema20"] >= dataframe["ema50"] * 0.998)
                & (dataframe["rsi"] >= thresholds["rsi_min"])
                & (dataframe["rsi"] <= thresholds["rsi_max"])
                & ((dataframe["adx"] >= thresholds["adx_min"]) | (dataframe["ema_spread_pct"] > 0))
                & (dataframe["atr_pct"] >= thresholds["atr_min"])
                & (dataframe["atr_pct"] <= thresholds["atr_max"])
                & (dataframe["ema_spread_pct"] >= thresholds["ema_spread_min"])
                & (dataframe["close"] <= dataframe["ema20"] * thresholds["ema20_overext"])
                & (dataframe["close"] >= dataframe["ema50"] * thresholds["pullback_floor"])
                & (dataframe["volume"] > dataframe["vol_ma20"] * thresholds["vol_mult_min"])
                & (dataframe["volume_z"] > thresholds["vol_z_min"])
                & ((dataframe[trend_col] == 1) | (dataframe["close"] > dataframe["ema200"] * 0.98))
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

        exit_condition = (
            (dataframe["rsi"] > thresholds["rsi_take"])
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
