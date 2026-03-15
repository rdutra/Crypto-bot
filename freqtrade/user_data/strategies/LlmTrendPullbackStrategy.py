import os
from typing import Any, Dict, Tuple

import numpy as np
import requests
import talib.abstract as ta
from freqtrade.strategy import IStrategy, merge_informative_pair
from pandas import DataFrame


class LlmTrendPullbackStrategy(IStrategy):
    timeframe = "1h"
    informative_timeframe = "4h"
    can_short = False

    minimal_roi = {"0": 0.05, "360": 0.02, "1080": 0.0}
    stoploss = -0.06
    trailing_stop = False
    trailing_stop_positive = 0.02
    trailing_stop_positive_offset = 0.04
    trailing_only_offset_is_reached = True
    use_custom_stoploss = False

    startup_candle_count = 250
    process_only_new_candles = True

    _llm_cache: Dict[str, Tuple[bool, str]] = {}

    @property
    def protections(self):
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
            "trend_4h": "bullish" if bool(row.get("trend_4h", 0)) else "bearish",
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
                dataframe["trend_4h"] = dataframe["trend_4h"].fillna(0).astype("int8")
            else:
                dataframe["trend_4h"] = 0
                dataframe["ema50_4h"] = np.nan
                dataframe["ema200_4h"] = np.nan
        else:
            dataframe["trend_4h"] = 0
            dataframe["ema50_4h"] = np.nan
            dataframe["ema200_4h"] = np.nan

        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe["enter_long"] = 0
        dataframe["enter_tag"] = None

        touched_pullback_zone = (
            (dataframe["low"] <= dataframe["ema20"] * 1.002)
            | (dataframe["low"] <= dataframe["ema50"] * 1.01)
            | (dataframe["close"].shift(1) <= dataframe["ema20"].shift(1) * 1.001)
        )
        rebound_confirmed = (
            (dataframe["close"] > dataframe["open"])
            & (dataframe["close"] > dataframe["close"].shift(1))
            & (dataframe["close"] > dataframe["ema20"])
        )

        deterministic_entry = (
            (dataframe["close"] > dataframe["ema200"])
            & (dataframe["ema20"] > dataframe["ema50"])
            & (dataframe["ema50"] > dataframe["ema200"])
            & (dataframe["ema50_4h"] > dataframe["ema200_4h"] * 1.01)
            & (dataframe["trend_4h"] == 1)
            & (dataframe["rsi"] >= 44)
            & (dataframe["rsi"] <= 58)
            & (dataframe["adx"] >= 22)
            & (dataframe["atr_pct"] >= 0.9)
            & (dataframe["atr_pct"] <= 3.8)
            & (dataframe["ema_spread_pct"] >= 0.15)
            & (dataframe["close"] <= dataframe["ema20"] * 1.015)
            & (dataframe["close"] >= dataframe["ema50"] * 0.98)
            & (dataframe["volume"] > dataframe["vol_ma20"] * 0.8)
            & (dataframe["volume_z"] > -0.6)
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

        exit_condition = (
            (dataframe["rsi"] > 76)
            | ((dataframe["close"] < dataframe["ema20"] * 0.985) & (dataframe["adx"] < 20))
            | (dataframe["close"] < dataframe["ema50"] * 0.98)
            | (dataframe["trend_4h"] == 0)
        )
        dataframe.loc[exit_condition, "exit_long"] = 1
        dataframe.loc[exit_condition, "exit_tag"] = "trend_break_or_overbought"
        return dataframe
