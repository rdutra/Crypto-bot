from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, Optional

from pandas import DataFrame

from LlmRotationAlignedStrategy import LlmRotationAlignedStrategy


class LlmHybridStrategy(LlmRotationAlignedStrategy):
    """
    Single-bot strategy with three branches:
    - core pairs: base trend-pullback rules from the parent hierarchy
    - rotated risk pairs: rotation-aligned rules
    - spike-tagged pairs: dedicated spike momentum rules
    """

    def _spike_pairs(self) -> set[str]:
        return set(self._parse_pairs(self._string_env("SPIKE_PAIRS", "")))

    def _is_spike_pair(self, pair: str) -> bool:
        return self._pair_symbol(pair) in self._spike_pairs()

    def _string_env(self, key: str, default: str) -> str:
        value = str(os.getenv(key, default)).strip()
        return value or default

    def _spike_entry_thresholds(self) -> Dict[str, float]:
        strict = self._aggr_entry_is_strict()
        return {
            "rsi_min": 46.0 if strict else 42.0,
            "rsi_max": 82.0 if strict else 86.0,
            "adx_min": 12.0 if strict else 9.0,
            "atr_min": 0.55 if strict else 0.45,
            "atr_max": self._float_env("SPIKE_ATR_MAX", 14.0, 1.0, 25.0),
            "ema_spread_min": self._float_env("SPIKE_EMA_SPREAD_MIN", -0.08 if strict else -0.18, -2.0, 2.0),
            "ema50_proximity": 0.985 if strict else 0.975,
            "ema20_reclaim": 1.0 if strict else 0.995,
            "vol_mult_min": 0.75 if strict else 0.55,
            "vol_z_min": -0.8 if strict else -1.2,
            "breakout_buffer": 0.998 if strict else 0.994,
            "body_pct_min": 0.20 if strict else 0.05,
            "pullback_floor": 0.92 if strict else 0.88,
        }

    def _spike_exit_thresholds(self) -> Dict[str, float]:
        return {
            "rsi_take": 84.0,
            "ema20_break": 0.997,
            "ema50_break": 0.988,
            "adx_weak": 15.0,
            "exhaustion_extension": 1.045,
        }

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe = super().populate_indicators(dataframe, metadata)
        dataframe["breakout_high_12"] = dataframe["high"].rolling(12, min_periods=3).max()
        dataframe["breakout_high_24"] = dataframe["high"].rolling(24, min_periods=6).max()
        dataframe["candle_body_pct"] = ((dataframe["close"] - dataframe["open"]) / dataframe["open"]).fillna(0.0) * 100.0
        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        pair = metadata["pair"]
        if not (self._is_aggressive() and self._is_spike_pair(pair)):
            return super().populate_entry_trend(dataframe, metadata)

        dataframe["enter_long"] = 0
        dataframe["enter_tag"] = None
        thresholds = self._spike_entry_thresholds()
        trend_col = self._trend_col()

        breakout_ready = (
            (dataframe["close"] >= dataframe["breakout_high_12"].shift(1) * thresholds["breakout_buffer"])
            | (dataframe["high"] >= dataframe["breakout_high_24"].shift(1) * thresholds["breakout_buffer"])
        )
        reclaim_ema20 = dataframe["close"] >= dataframe["ema20"] * thresholds["ema20_reclaim"]
        adx_or_spread_ok = (
            (dataframe["adx"] >= thresholds["adx_min"])
            | (dataframe["ema_spread_pct"] >= thresholds["ema_spread_min"])
        )
        volume_ok = (
            (dataframe["volume"] > dataframe["vol_ma20"] * thresholds["vol_mult_min"])
            | (dataframe["volume_z"] >= thresholds["vol_z_min"])
        )
        trend_ok = (
            ((dataframe[trend_col] == 1) & (dataframe["close"] >= dataframe["ema50"] * thresholds["ema50_proximity"]))
            | (dataframe["ema20"] >= dataframe["ema50"] * 0.995)
            | (dataframe["close"] > dataframe["ema200"] * 0.985)
        )

        entry_checks: Dict[str, Any] = {
            "close_gt_ema20": dataframe["close"] >= dataframe["ema20"],
            "reclaim_ema20": reclaim_ema20,
            "breakout_ready": breakout_ready,
            "rsi_min": dataframe["rsi"] >= thresholds["rsi_min"],
            "rsi_max": dataframe["rsi"] <= thresholds["rsi_max"],
            "adx_or_spread": adx_or_spread_ok,
            "atr_min": dataframe["atr_pct"] >= thresholds["atr_min"],
            "atr_max": dataframe["atr_pct"] <= thresholds["atr_max"],
            "pullback_floor": dataframe["close"] >= dataframe["ema50"] * thresholds["pullback_floor"],
            "volume_ok": volume_ok,
            "body_pct_min": dataframe["candle_body_pct"] >= thresholds["body_pct_min"],
            "trend_ok": trend_ok,
        }

        deterministic_entry = dataframe["close"] > 0
        for condition in entry_checks.values():
            deterministic_entry = deterministic_entry & condition

        if self._benchmark_filter_for_risk():
            benchmark_risk_ok = dataframe["bench_risk_ok"] == 1
            entry_checks["benchmark_risk_ok"] = benchmark_risk_ok
            deterministic_entry = deterministic_entry & benchmark_risk_ok

        dataframe.loc[deterministic_entry, "enter_long"] = 1
        dataframe.loc[deterministic_entry, "enter_tag"] = "base_spike_momentum"

        base_allowed = bool(deterministic_entry.iloc[-1]) if not dataframe.empty else False
        if self._llm_enabled() and not dataframe.empty:
            idx = dataframe.index[-1]
            if int(dataframe.at[idx, "enter_long"]) == 1:
                allowed, reason = self._llm_allows_trade(dataframe.loc[idx], pair)
                if not allowed:
                    dataframe.at[idx, "enter_long"] = 0
                    dataframe.at[idx, "enter_tag"] = f"llm_block:{reason}"[:64]
                else:
                    dataframe.at[idx, "enter_tag"] = f"llm_ok:{reason}"[:64]

        if not dataframe.empty:
            idx = dataframe.index[-1]
            final_allowed = int(dataframe.at[idx, "enter_long"]) == 1
            tag = dataframe.at[idx, "enter_tag"]
            self._log_entry_diagnostics(
                dataframe=dataframe,
                pair=pair,
                checks=entry_checks,
                thresholds=thresholds,
                base_allowed=base_allowed,
                final_allowed=final_allowed,
                tag=str(tag) if tag is not None else None,
            )

        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        pair = metadata["pair"]
        if not (self._is_aggressive() and self._is_spike_pair(pair)):
            return super().populate_exit_trend(dataframe, metadata)

        dataframe["exit_long"] = 0
        dataframe["exit_tag"] = None
        thresholds = self._spike_exit_thresholds()
        trend_col = self._trend_col()
        rsi_exit = dataframe["rsi"] > thresholds["rsi_take"] if self._exit_use_rsi_take() else False

        exhaustion = (
            (dataframe["close"] >= dataframe["breakout_high_12"].shift(1) * thresholds["exhaustion_extension"])
            & (dataframe["rsi"] >= thresholds["rsi_take"])
        )
        exit_condition = (
            rsi_exit
            | exhaustion
            | (
                (dataframe["close"] < dataframe["ema20"] * thresholds["ema20_break"])
                & (dataframe["adx"] < thresholds["adx_weak"])
            )
            | (dataframe["close"] < dataframe["ema50"] * thresholds["ema50_break"])
            | ((dataframe[trend_col] == 0) & (dataframe["close"] < dataframe["ema20"]))
        )
        dataframe.loc[exit_condition, "exit_long"] = 1
        dataframe.loc[exit_condition, "exit_tag"] = "spike_breakdown_or_exhaustion"
        return dataframe

    def custom_exit(
        self,
        pair: str,
        trade,
        current_time: datetime,
        current_rate: float,
        current_profit: float,
        **kwargs,
    ) -> Optional[str]:
        if self._is_spike_pair(pair):
            open_dt = getattr(trade, "open_date_utc", None)
            if open_dt is not None:
                age_hours = (current_time - open_dt).total_seconds() / 3600.0
                if age_hours >= 16.0 and current_profit < 0.004:
                    return "spike_stale_exit"
        return super().custom_exit(
            pair=pair,
            trade=trade,
            current_time=current_time,
            current_rate=current_rate,
            current_profit=current_profit,
            **kwargs,
        )
