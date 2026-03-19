import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main as bot_main  # noqa: E402


def _sample_candidate(pair: str = "BTC/USDT", deterministic_score: float = 5.0) -> bot_main.PairCandidate:
    return bot_main.PairCandidate(
        pair=pair,
        timeframe="1h",
        price=100.0,
        ema_20=99.0,
        ema_50=97.0,
        ema_200=90.0,
        rsi_14=55.0,
        adx_14=22.0,
        atr_pct=1.8,
        volume_zscore=0.6,
        trend_4h="bullish",
        market_structure="higher_highs",
        deterministic_score=deterministic_score,
    )


class TradingSignalParsingTests(unittest.TestCase):
    def test_extract_rank_rows_handles_tokens_payload(self) -> None:
        payload = {"code": "000000", "data": {"tokens": [{"symbol": "ABC"}, {"symbol": "XYZ"}]}}
        rows = bot_main._extract_rank_rows(payload)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["symbol"], "ABC")

    def test_extract_signal_rows_handles_nested_payload(self) -> None:
        payload = {"data": {"items": [{"symbol": "BTCUSDT"}, {"symbol": "ETHUSDT"}]}}
        rows = bot_main._extract_signal_rows(payload)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["symbol"], "BTCUSDT")

    def test_trading_signal_context_accepts_token_ticker_without_quote(self) -> None:
        rows = [{"ticker": "Cake", "direction": "buy", "confidence": 0.91, "maxGain": "12.1"}]
        context = bot_main._build_trading_signal_context(rows)
        self.assertIn("CAKE/USDT", context)
        self.assertEqual(context["CAKE/USDT"]["side"], "buy")

    def test_build_trading_signal_context_filters_symbols_and_keeps_best_score(self) -> None:
        rows = [
            {"symbol": "BTCUSDT", "signal": "buy", "score": "0.55", "note": "first"},
            {"symbol": "BTCUSDT", "signal": "buy", "score": "0.75", "note": "better"},
            {"symbol": "ETHDOWNUSDT", "signal": "buy", "score": "0.99", "note": "leveraged_token"},
            {"symbol": "SOLUSDT", "signal": "buy", "score": "0.05", "note": "too_low"},
        ]
        context = bot_main._build_trading_signal_context(rows)
        self.assertIn("BTC/USDT", context)
        self.assertNotIn("ETHDOWN/USDT", context)
        self.assertNotIn("SOL/USDT", context)
        self.assertEqual(context["BTC/USDT"]["note"], "better")
        self.assertEqual(context["BTC/USDT"]["side"], "buy")
        self.assertAlmostEqual(context["BTC/USDT"]["score"], 0.75, places=6)


class RankPairsBehaviorTests(unittest.IsolatedAsyncioTestCase):
    async def test_rank_pairs_returns_skill_meta_without_sell_penalty_by_default(self) -> None:
        req = bot_main.RankPairsRequest(candidates=[_sample_candidate()])

        async def fake_run_ollama(_: str) -> str:
            return json.dumps(
                {
                    "decisions": [
                        {
                            "pair": "BTC/USDT",
                            "regime": "trend_pullback",
                            "risk_level": "low",
                            "confidence": 0.8,
                            "note": "ok",
                        }
                    ]
                }
            )

        async def fake_market_rank_context(force_refresh: bool = False):  # noqa: ARG001
            return {}, {"source": "binance_spot_fallback", "errors": ["using_spot_fallback"]}

        async def fake_trading_signal_context(force_refresh: bool = False):  # noqa: ARG001
            return {"BTC/USDT": {"side": "sell", "score": 1.0}}, {"source": "binance_web3:path", "errors": []}

        original_run_ollama = bot_main._run_ollama
        original_market_loader = bot_main._load_market_rank_context
        original_signal_loader = bot_main._load_trading_signal_context
        original_market_enabled = bot_main.MARKET_RANK_SKILL_ENABLED
        original_signal_enabled = bot_main.TRADING_SIGNAL_SKILL_ENABLED
        original_sell_penalty = bot_main.TRADING_SIGNAL_SELL_PENALTY_MULT

        try:
            bot_main._run_ollama = fake_run_ollama
            bot_main._load_market_rank_context = fake_market_rank_context
            bot_main._load_trading_signal_context = fake_trading_signal_context
            bot_main.MARKET_RANK_SKILL_ENABLED = True
            bot_main.TRADING_SIGNAL_SKILL_ENABLED = True
            bot_main.TRADING_SIGNAL_SELL_PENALTY_MULT = 0.0

            response = await bot_main.rank_pairs(req)
        finally:
            bot_main._run_ollama = original_run_ollama
            bot_main._load_market_rank_context = original_market_loader
            bot_main._load_trading_signal_context = original_signal_loader
            bot_main.MARKET_RANK_SKILL_ENABLED = original_market_enabled
            bot_main.TRADING_SIGNAL_SKILL_ENABLED = original_signal_enabled
            bot_main.TRADING_SIGNAL_SELL_PENALTY_MULT = original_sell_penalty

        self.assertEqual(response.market_rank_source, "binance_spot_fallback")
        self.assertEqual(response.market_rank_errors, ["using_spot_fallback"])
        self.assertEqual(response.trading_signal_source, "binance_web3:path")
        self.assertEqual(response.trading_signal_errors, [])
        self.assertEqual(len(response.decisions), 1)
        self.assertAlmostEqual(response.decisions[0].final_score, 8.4, places=6)


if __name__ == "__main__":
    unittest.main()
