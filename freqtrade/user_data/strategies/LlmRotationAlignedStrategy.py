from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, Tuple

import requests
from pandas import DataFrame

from LlmTrendPullbackStrategy import LlmTrendPullbackStrategy


class LlmRotationAlignedStrategy(LlmTrendPullbackStrategy):
    """
    A looser risk-pair strategy intended to match the rotator's high-risk /
    mean-reversion-heavy output more closely than the default trend-pullback
    strategy.
    """

    def _entry_thresholds(self, pair: str) -> Dict[str, float]:
        if self._is_risk_pair(pair) and self._is_aggressive():
            strict = self._aggr_entry_is_strict()
            return {
                "rsi_min": 28.0 if strict else 25.0,
                "rsi_max": 74.0 if strict else 78.0,
                "adx_min": 12.0 if strict else 10.0,
                "atr_min": 0.2,
                "atr_max": self._float_env("ROTATION_RISK_ATR_MAX", 8.0, 1.0, 12.0),
                "ema_spread_min": self._float_env(
                    "ROTATION_RISK_EMA_SPREAD_MIN",
                    -0.25 if strict else -0.45,
                    -2.0,
                    1.5,
                ),
                "ema50_proximity": 0.985 if strict else 0.975,
                "ema20_overext": 1.08,
                "pullback_floor": 0.93 if strict else 0.90,
                "vol_mult_min": 0.25 if strict else 0.10,
                "vol_z_min": -2.5,
                "rebound_over_prev": 0.992 if strict else 0.985,
            }

        return super()._entry_thresholds(pair)

    def _exit_thresholds(self, pair: str) -> Dict[str, float]:
        if self._is_risk_pair(pair) and self._is_aggressive():
            return {
                "rsi_take": 72.0,
                "ema20_break": 0.996,
                "adx_weak": 16.0,
                "ema50_break": 0.985,
            }

        return super()._exit_thresholds(pair)


    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        pair = metadata["pair"]
        if not (self._is_aggressive() and self._is_risk_pair(pair)):
            return super().populate_exit_trend(dataframe, metadata)

        dataframe["exit_long"] = 0
        dataframe["exit_tag"] = None
        thresholds = self._exit_thresholds(pair)
        trend_col = self._trend_col()
        rsi_exit = dataframe["rsi"] > thresholds["rsi_take"] if self._exit_use_rsi_take() else False

        trend_break = (
            (dataframe[trend_col] == 0)
            & (dataframe["close"] < dataframe["ema20"] * 0.995)
            & (dataframe["adx"] < (thresholds["adx_weak"] + 6.0))
        )
        exit_condition = (
            rsi_exit
            | (
                (dataframe["close"] < dataframe["ema20"] * thresholds["ema20_break"])
                & (dataframe["adx"] < thresholds["adx_weak"])
            )
            | (dataframe["close"] < dataframe["ema50"] * thresholds["ema50_break"])
            | trend_break
        )
        dataframe.loc[exit_condition, "exit_long"] = 1
        dataframe.loc[exit_condition, "exit_tag"] = "rotation_trend_break_or_overbought"
        return dataframe

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
        bot_api = self._runtime_policy().get("bot_api_url") or os.getenv("BOT_API_URL", "http://bot-api:8000")
        min_conf = self._llm_min_confidence()
        connect_timeout = self._llm_connect_timeout_seconds()
        read_timeout = self._llm_read_timeout_seconds()
        fail_open = self._llm_fail_open()

        try:
            response = requests.post(f"{bot_api}/classify", json=payload, timeout=(connect_timeout, read_timeout))
            response.raise_for_status()
            data = response.json()
            regime = str(data.get("regime", "")).lower()
            risk_level = str(data.get("risk_level", "high")).lower()
            confidence = float(data.get("confidence", 0.0))

            allowed_regimes = {"trend_pullback", "breakout"}
            allowed_risks = {"low", "medium"}
            if self._is_risk_pair(pair):
                allowed_regimes.add("mean_reversion")
                allowed_risks.add("high")

            allowed = regime in allowed_regimes and risk_level in allowed_risks and confidence >= min_conf
            reason = f"{regime}:{risk_level}:{confidence:.2f}"
        except requests.Timeout:
            allowed = fail_open
            reason = "llm_timeout_allow" if fail_open else "llm_timeout"
        except requests.RequestException:
            allowed = fail_open
            reason = "llm_http_error_allow" if fail_open else "llm_http_error"
        except Exception:
            allowed = fail_open
            reason = "llm_error_allow" if fail_open else "llm_error"

        self._llm_cache[cache_key] = (allowed, reason)
        return allowed, reason

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        pair = metadata["pair"]
        if not (self._is_aggressive() and self._is_risk_pair(pair)):
            return super().populate_entry_trend(dataframe, metadata)

        dataframe["enter_long"] = 0
        dataframe["enter_tag"] = None

        thresholds = self._entry_thresholds(pair)
        trend_col = self._trend_col()

        touched_reversion_zone = (
            (dataframe["low"] <= dataframe["ema20"] * 1.01)
            | (dataframe["low"] <= dataframe["ema50"] * 1.02)
            | (dataframe["close"] <= dataframe["ema20"] * 1.01)
        )
        reversal_hint = (
            (dataframe["close"] >= dataframe["close"].shift(1) * thresholds["rebound_over_prev"])
            | (dataframe["close"] > dataframe["open"])
            | (dataframe["rsi"] > dataframe["rsi"].shift(1))
        )
        adx_or_spread_ok = (
            (dataframe["adx"] >= thresholds["adx_min"])
            | (dataframe["ema_spread_pct"] >= thresholds["ema_spread_min"])
        )
        trend_ok = (
            ((dataframe[trend_col] == 1) & (dataframe["close"] > dataframe["ema200"] * 0.985))
            | (dataframe["close"] >= dataframe["ema50"] * thresholds["ema50_proximity"])
            | (dataframe["ema20"] >= dataframe["ema50"] * 0.995)
        )

        entry_checks: Dict[str, Any] = {
            "close_near_ema50": dataframe["close"] >= dataframe["ema50"] * thresholds["ema50_proximity"],
            "rsi_min": dataframe["rsi"] >= thresholds["rsi_min"],
            "rsi_max": dataframe["rsi"] <= thresholds["rsi_max"],
            "adx_or_spread": adx_or_spread_ok,
            "atr_min": dataframe["atr_pct"] >= thresholds["atr_min"],
            "atr_max": dataframe["atr_pct"] <= thresholds["atr_max"],
            "ema20_not_overext": dataframe["close"] <= dataframe["ema20"] * thresholds["ema20_overext"],
            "pullback_floor": dataframe["close"] >= dataframe["ema50"] * thresholds["pullback_floor"],
            "volume_mult": dataframe["volume"] > dataframe["vol_ma20"] * thresholds["vol_mult_min"],
            "volume_z_min": dataframe["volume_z"] > thresholds["vol_z_min"],
            "touched_reversion_zone": touched_reversion_zone,
            "reversal_hint": reversal_hint,
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
        dataframe.loc[deterministic_entry, "enter_tag"] = "base_rotation_aligned"

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
