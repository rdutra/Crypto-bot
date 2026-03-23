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
    def test_build_rank_prompt_keeps_candidate_source_tags(self) -> None:
        req = bot_main.RankPairsRequest(
            candidates=[
                bot_main.PairCandidate(
                    pair="WLD/USDT",
                    timeframe="1h",
                    price=1.0,
                    ema_20=0.98,
                    ema_50=0.95,
                    ema_200=0.9,
                    rsi_14=58.0,
                    adx_14=18.0,
                    atr_pct=2.4,
                    volume_zscore=1.1,
                    trend_4h="bearish",
                    market_structure="mixed",
                    deterministic_score=6.5,
                    data_source="exchange",
                    candidate_sources=["spike"],
                )
            ]
        )
        prompt = bot_main.build_rank_prompt(req)
        self.assertIn('"candidate_sources":["spike"]', prompt)
        self.assertIn("scanner-detected momentum candidate", prompt)

    async def test_rank_pairs_returns_extended_skill_meta_without_sell_penalty_by_default(self) -> None:
        req = bot_main.RankPairsRequest(candidates=[_sample_candidate()])

        async def fake_run_llm(_: str) -> str:
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
            return {}, {
                "provider": "official_skill",
                "source": "official_skill:/skills/crypto-market-rank",
                "upstream_source": "/skills/crypto-market-rank",
                "errors": ["using_cached_payload"],
                "upstream_errors": ["using_cached_payload"],
            }

        async def fake_trading_signal_context(force_refresh: bool = False):  # noqa: ARG001
            return {"BTC/USDT": {"side": "sell", "score": 1.0}}, {
                "provider": "direct",
                "source": "binance_web3:path",
                "upstream_source": "binance_web3:path",
                "errors": [],
                "upstream_errors": [],
            }

        original_run_llm = bot_main._run_llm
        original_market_loader = bot_main._load_market_rank_context
        original_signal_loader = bot_main._load_trading_signal_context
        original_market_enabled = bot_main.MARKET_RANK_SKILL_ENABLED
        original_signal_enabled = bot_main.TRADING_SIGNAL_SKILL_ENABLED
        original_sell_penalty = bot_main.TRADING_SIGNAL_SELL_PENALTY_MULT

        try:
            bot_main._run_llm = fake_run_llm
            bot_main._load_market_rank_context = fake_market_rank_context
            bot_main._load_trading_signal_context = fake_trading_signal_context
            bot_main.MARKET_RANK_SKILL_ENABLED = True
            bot_main.TRADING_SIGNAL_SKILL_ENABLED = True
            bot_main.TRADING_SIGNAL_SELL_PENALTY_MULT = 0.0

            response = await bot_main.rank_pairs(req)
        finally:
            bot_main._run_llm = original_run_llm
            bot_main._load_market_rank_context = original_market_loader
            bot_main._load_trading_signal_context = original_signal_loader
            bot_main.MARKET_RANK_SKILL_ENABLED = original_market_enabled
            bot_main.TRADING_SIGNAL_SKILL_ENABLED = original_signal_enabled
            bot_main.TRADING_SIGNAL_SELL_PENALTY_MULT = original_sell_penalty

        self.assertEqual(response.market_rank_source, "official_skill:/skills/crypto-market-rank")
        self.assertEqual(response.market_rank_provider, "official_skill")
        self.assertEqual(response.market_rank_upstream_source, "/skills/crypto-market-rank")
        self.assertEqual(response.market_rank_upstream_errors, ["using_cached_payload"])
        self.assertEqual(response.trading_signal_source, "binance_web3:path")
        self.assertEqual(response.trading_signal_provider, "direct")
        self.assertEqual(response.trading_signal_errors, [])
        self.assertEqual(len(response.decisions), 1)
        self.assertAlmostEqual(response.decisions[0].final_score, 8.4, places=6)

    async def test_rank_pairs_does_not_apply_buy_bonus_to_non_binance_skill_candidates(self) -> None:
        req = bot_main.RankPairsRequest(candidates=[_sample_candidate()])

        async def fake_run_llm(_: str) -> str:
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
            return {}, {"provider": "official_skill", "source": "official_skill", "errors": [], "upstream_errors": []}

        async def fake_trading_signal_context(force_refresh: bool = False):  # noqa: ARG001
            return {"BTC/USDT": {"side": "buy", "score": 1.0}}, {
                "provider": "direct",
                "source": "binance_web3:path",
                "errors": [],
                "upstream_errors": [],
            }

        original_run_llm = bot_main._run_llm
        original_market_loader = bot_main._load_market_rank_context
        original_signal_loader = bot_main._load_trading_signal_context
        original_market_enabled = bot_main.MARKET_RANK_SKILL_ENABLED
        original_signal_enabled = bot_main.TRADING_SIGNAL_SKILL_ENABLED

        try:
            bot_main._run_llm = fake_run_llm
            bot_main._load_market_rank_context = fake_market_rank_context
            bot_main._load_trading_signal_context = fake_trading_signal_context
            bot_main.MARKET_RANK_SKILL_ENABLED = True
            bot_main.TRADING_SIGNAL_SKILL_ENABLED = True
            response = await bot_main.rank_pairs(req)
        finally:
            bot_main._run_llm = original_run_llm
            bot_main._load_market_rank_context = original_market_loader
            bot_main._load_trading_signal_context = original_signal_loader
            bot_main.MARKET_RANK_SKILL_ENABLED = original_market_enabled
            bot_main.TRADING_SIGNAL_SKILL_ENABLED = original_signal_enabled

        self.assertAlmostEqual(response.decisions[0].final_score, 8.4, places=6)

    async def test_query_token_info_returns_normalized_response(self) -> None:
        async def fake_loader(**kwargs):
            self.assertEqual(kwargs["symbol"], "CAKE")
            return [
                {
                    "symbol": "CAKE",
                    "name": "PancakeSwap",
                    "chain": "56",
                    "contract_address": "0xabc",
                    "price": 2.1,
                    "change_24h_pct": 0.12,
                    "volume_24h_usd": 12345.0,
                    "liquidity_usd": 99999.0,
                    "market_cap_usd": 456789.0,
                    "holders": 1234,
                    "top10_holder_share": 0.42,
                    "is_binance_spot_tradable": True,
                }
            ], {"provider": "official_skill", "source": "official_skill", "upstream_source": "/skills/query-token-info", "errors": [], "upstream_errors": []}

        original_loader = bot_main._load_token_info_items
        original_enabled = bot_main.TOKEN_INFO_SKILL_ENABLED
        try:
            bot_main._load_token_info_items = fake_loader
            bot_main.TOKEN_INFO_SKILL_ENABLED = True
            response = await bot_main.query_token_info(symbol="CAKE")
        finally:
            bot_main._load_token_info_items = original_loader
            bot_main.TOKEN_INFO_SKILL_ENABLED = original_enabled

        self.assertTrue(response.enabled)
        self.assertEqual(response.provider, "official_skill")
        self.assertEqual(response.count, 1)
        self.assertEqual(response.items[0]["symbol"], "CAKE")
        self.assertTrue(response.items[0]["is_binance_spot_tradable"])

    async def test_query_address_info_requires_address(self) -> None:
        with self.assertRaises(bot_main.HTTPException) as ctx:
            await bot_main.query_address_info(address="")
        self.assertEqual(ctx.exception.status_code, 400)

    async def test_rank_pairs_fallback_confidence_is_calibrated_lower_than_llm(self) -> None:
        req = bot_main.RankPairsRequest(candidates=[_sample_candidate(pair="CAKE/USDT", deterministic_score=8.1)])

        async def fake_run_llm(_: str) -> str:
            return "not-json"

        async def fake_rank_via_single(_: bot_main.RankPairsRequest) -> tuple[dict[str, bot_main.LlmRankDecision], int]:
            return {}, 0

        original_run_llm = bot_main._run_llm
        original_single = bot_main._rank_via_single_classify
        try:
            bot_main._run_llm = fake_run_llm
            bot_main._rank_via_single_classify = fake_rank_via_single
            response = await bot_main.rank_pairs(req)
        finally:
            bot_main._run_llm = original_run_llm
            bot_main._rank_via_single_classify = original_single

        self.assertEqual(response.source, "fallback")
        self.assertEqual(response.reason, "single_classify_empty")
        self.assertEqual(response.selected_pairs, ["CAKE/USDT"])
        self.assertLess(response.decisions[0].confidence, 0.81)
        self.assertLessEqual(response.decisions[0].confidence, bot_main.FALLBACK_CONFIDENCE_CAP)
        self.assertIn("low_reliability", response.decisions[0].note)

    async def test_rank_pairs_recovers_missing_batch_decisions(self) -> None:
        req = bot_main.RankPairsRequest(
            candidates=[
                _sample_candidate(pair="XRP/USDT", deterministic_score=8.6),
                _sample_candidate(pair="SOL/USDT", deterministic_score=6.6),
            ],
            allowed_risk_levels=["low", "medium", "high"],
            allowed_regimes=["trend_pullback", "breakout", "mean_reversion"],
        )

        async def fake_run_llm(_: str) -> str:
            return json.dumps(
                {
                    "decisions": [
                        {
                            "pair": "XRP/USDT",
                            "regime": "trend_pullback",
                            "risk_level": "low",
                            "confidence": 0.95,
                            "note": "batch ok",
                        }
                    ]
                }
            )

        async def fake_rank_via_single(single_req: bot_main.RankPairsRequest) -> tuple[dict[str, bot_main.LlmRankDecision], int]:
            self.assertEqual([candidate.pair for candidate in single_req.candidates], ["SOL/USDT"])
            return (
                {
                    "SOL/USDT": bot_main.LlmRankDecision(
                        pair="SOL/USDT",
                        regime="trend_pullback",
                        risk_level="low",
                        confidence=0.9,
                        note="single:recovered",
                    )
                },
                1,
            )

        original_run_llm = bot_main._run_llm
        original_single = bot_main._rank_via_single_classify
        try:
            bot_main._run_llm = fake_run_llm
            bot_main._rank_via_single_classify = fake_rank_via_single
            response = await bot_main.rank_pairs(req)
        finally:
            bot_main._run_llm = original_run_llm
            bot_main._rank_via_single_classify = original_single

        self.assertEqual(response.source, "llm")
        self.assertEqual(response.reason, "batch_partial_recovered:2/2")
        self.assertEqual(response.selected_pairs, ["XRP/USDT", "SOL/USDT"])
        by_pair = {decision.pair: decision for decision in response.decisions}
        self.assertEqual(by_pair["SOL/USDT"].note, "single:recovered")
        self.assertNotEqual(by_pair["SOL/USDT"].note, "missing_pair_decision")

    async def test_rank_pairs_penalizes_non_spike_mean_reversion_and_bad_history(self) -> None:
        req = bot_main.RankPairsRequest(
            candidates=[
                bot_main.PairCandidate(
                    pair="CAKE/USDT",
                    timeframe="1h",
                    price=2.0,
                    ema_20=1.98,
                    ema_50=1.95,
                    ema_200=1.8,
                    rsi_14=58.0,
                    adx_14=18.0,
                    atr_pct=2.1,
                    volume_zscore=0.8,
                    trend_4h="mixed",
                    market_structure="mixed",
                    deterministic_score=8.5,
                    recent_closed_trades=5,
                    recent_win_rate=0.2,
                    recent_avg_profit_pct=-0.8,
                    recent_net_profit_pct=-3.0,
                    historical_penalty=2.8,
                ),
                bot_main.PairCandidate(
                    pair="WLD/USDT",
                    timeframe="1h",
                    price=1.0,
                    ema_20=0.98,
                    ema_50=0.97,
                    ema_200=0.9,
                    rsi_14=60.0,
                    adx_14=20.0,
                    atr_pct=3.2,
                    volume_zscore=1.1,
                    trend_4h="mixed",
                    market_structure="mixed",
                    deterministic_score=8.0,
                    candidate_sources=["spike"],
                ),
            ],
            allowed_risk_levels=["low", "medium", "high"],
            allowed_regimes=["trend_pullback", "breakout", "mean_reversion"],
        )

        async def fake_run_llm(_: str) -> str:
            return json.dumps(
                {
                    "decisions": [
                        {
                            "pair": "CAKE/USDT",
                            "regime": "mean_reversion",
                            "risk_level": "high",
                            "confidence": 0.9,
                            "note": "reversion",
                        },
                        {
                            "pair": "WLD/USDT",
                            "regime": "mean_reversion",
                            "risk_level": "high",
                            "confidence": 0.9,
                            "note": "spike reversion",
                        },
                    ]
                }
            )

        original_run_llm = bot_main._run_llm
        try:
            bot_main._run_llm = fake_run_llm
            response = await bot_main.rank_pairs(req)
        finally:
            bot_main._run_llm = original_run_llm

        by_pair = {decision.pair: decision for decision in response.decisions}
        self.assertLess(by_pair["CAKE/USDT"].final_score, by_pair["WLD/USDT"].final_score)

    async def test_rank_pairs_requires_higher_confidence_for_high_risk_mean_reversion(self) -> None:
        req = bot_main.RankPairsRequest(
            candidates=[
                bot_main.PairCandidate(
                    pair="CAKE/USDT",
                    timeframe="1h",
                    price=2.0,
                    ema_20=1.98,
                    ema_50=1.95,
                    ema_200=1.8,
                    rsi_14=58.0,
                    adx_14=18.0,
                    atr_pct=2.1,
                    volume_zscore=0.8,
                    trend_4h="mixed",
                    market_structure="mixed",
                    deterministic_score=9.0,
                ),
                bot_main.PairCandidate(
                    pair="WLD/USDT",
                    timeframe="1h",
                    price=1.0,
                    ema_20=0.98,
                    ema_50=0.97,
                    ema_200=0.9,
                    rsi_14=60.0,
                    adx_14=20.0,
                    atr_pct=3.2,
                    volume_zscore=1.1,
                    trend_4h="mixed",
                    market_structure="mixed",
                    deterministic_score=8.0,
                    candidate_sources=["spike"],
                ),
            ],
            allowed_risk_levels=["low", "medium", "high"],
            allowed_regimes=["trend_pullback", "breakout", "mean_reversion"],
            min_confidence=0.60,
        )

        async def fake_run_llm(_: str) -> str:
            return json.dumps(
                {
                    "decisions": [
                        {
                            "pair": "CAKE/USDT",
                            "regime": "mean_reversion",
                            "risk_level": "high",
                            "confidence": 0.84,
                            "note": "reversion",
                        },
                        {
                            "pair": "WLD/USDT",
                            "regime": "mean_reversion",
                            "risk_level": "high",
                            "confidence": 0.84,
                            "note": "spike reversion",
                        },
                    ]
                }
            )

        original_run_llm = bot_main._run_llm
        try:
            bot_main._run_llm = fake_run_llm
            response = await bot_main.rank_pairs(req)
        finally:
            bot_main._run_llm = original_run_llm

        self.assertEqual(response.selected_pairs, ["WLD/USDT"])


if __name__ == "__main__":
    unittest.main()
