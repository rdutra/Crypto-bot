import asyncio
import logging

import aiohttp
from aiohttp import web

from app.alerts import AlertNotifier
from app.config import Settings
from app.llm_shadow import LlmShadowDecider
from app.scoring import compute_score
from app.state import STATE, now_ts
from app.storage import PredictionStore
from app.streams import stream_symbols
from app.universe import load_universe
from app.web import create_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
LOGGER = logging.getLogger(__name__)


async def scorer_loop(
    settings: Settings,
    store: PredictionStore,
    notifier: AlertNotifier,
    llm_shadow: LlmShadowDecider,
) -> None:
    while True:
        ranked = []
        scored_candidates: list[tuple[str, float, dict, float, bool, bool, bool, bool, bool, bool, bool]] = []
        current_ts = now_ts()

        for symbol, state in STATE.items():
            if not state.trade_events or not state.kline_1m:
                continue

            for t in state.trade_events:
                t["age_s"] = current_ts - t["ts"]

            score, meta = compute_score(state)
            if not meta:
                continue

            spread_ok = meta["spread_pct"] <= settings.max_spread_pct
            cooldown_ok = (current_ts - state.last_alert_ts) > (settings.cooldown_minutes * 60)
            threshold_ok = score >= settings.min_score
            breakout_ok = float(meta.get("breakout", 0.0)) >= settings.min_breakout_pct
            buy_ratio_ok = float(meta.get("buy_ratio", 0.0)) >= settings.min_buy_ratio
            rel_quote_ok = float(meta.get("rel_quote", 0.0)) >= settings.min_rel_quote
            eligible_alert = threshold_ok and spread_ok and cooldown_ok and breakout_ok and buy_ratio_ok and rel_quote_ok
            scored_candidates.append(
                (
                    symbol,
                    score,
                    meta,
                    state.last_price,
                    spread_ok,
                    cooldown_ok,
                    threshold_ok,
                    breakout_ok,
                    buy_ratio_ok,
                    rel_quote_ok,
                    eligible_alert,
                )
            )

            if eligible_alert:
                ranked.append((symbol, score, meta, state.last_price))

        llm_shadow_by_symbol: dict[str, dict] = {}
        if llm_shadow.enabled() and scored_candidates:
            eval_limit = max(settings.top_n_alerts, max(1, settings.llm_shadow_eval_top_n))
            eval_min_score = float(settings.llm_shadow_eval_min_score)
            eval_pool = [
                item for item in sorted(scored_candidates, key=lambda x: x[1], reverse=True) if float(item[1]) >= eval_min_score
            ][:eval_limit]
            for (
                symbol,
                score,
                meta,
                _last_price,
                spread_ok,
                cooldown_ok,
                threshold_ok,
                breakout_ok,
                buy_ratio_ok,
                rel_quote_ok,
                eligible_alert,
            ) in eval_pool:
                llm_result = await llm_shadow.evaluate(symbol=symbol, state=STATE[symbol], current_ts=current_ts)
                llm_shadow_by_symbol[symbol] = llm_result
                if not bool(llm_result.get("cached", False)):
                    store.write_llm_shadow_eval(
                        {
                            "ts": None,
                            "symbol": symbol.upper(),
                            "score": round(float(score), 4),
                            "spread_pct": float(meta.get("spread_pct", 0.0)),
                            "threshold_ok": bool(threshold_ok),
                            "cooldown_ok": bool(cooldown_ok),
                            "eligible_alert": bool(eligible_alert),
                            "filters": {
                                "breakout_ok": bool(breakout_ok),
                                "buy_ratio_ok": bool(buy_ratio_ok),
                                "rel_quote_ok": bool(rel_quote_ok),
                            },
                            "llm_shadow": llm_result,
                        }
                    )

        ranked.sort(key=lambda x: x[1], reverse=True)
        for symbol, score, meta, last_price in ranked[: settings.top_n_alerts]:
            llm_shadow_result = {}
            if llm_shadow.enabled():
                llm_shadow_result = llm_shadow_by_symbol.get(symbol, {})
                if not llm_shadow_result:
                    llm_shadow_result = await llm_shadow.evaluate(symbol=symbol, state=STATE[symbol], current_ts=current_ts)
                    llm_shadow_by_symbol[symbol] = llm_shadow_result

            payload = {
                "symbol": symbol.upper(),
                "score": round(score, 4),
                "price": round(last_price, 8) if last_price > 0 else None,
                "meta": {
                    **meta,
                    "th_score_min": settings.min_score,
                    "th_spread_max": settings.max_spread_pct,
                    "th_breakout_min": settings.min_breakout_pct,
                    "th_buy_ratio_min": settings.min_buy_ratio,
                    "th_rel_quote_min": settings.min_rel_quote,
                },
            }
            if llm_shadow_result:
                payload["llm_shadow"] = llm_shadow_result
                payload["meta"] = dict(meta)
                payload["meta"]["llm_shadow"] = llm_shadow_result

            LOGGER.info("ALERT %s score=%.2f meta=%s", symbol.upper(), score, meta)
            store.write_prediction(payload)
            await notifier.notify_alert(payload)
            STATE[symbol].last_alert_ts = current_ts

        await asyncio.sleep(max(1, settings.loop_seconds))


async def outcomes_loop(settings: Settings, store: PredictionStore) -> None:
    while True:
        current_prices: dict[str, float] = {}
        for symbol, state in STATE.items():
            if state.last_price > 0:
                current_prices[symbol.upper()] = float(state.last_price)

        resolved = store.resolve_due_outcomes(
            current_prices=current_prices,
            limit=max(10, settings.outcome_batch_size),
        )
        if resolved > 0:
            LOGGER.info("Resolved %s pending outcomes.", resolved)

        await asyncio.sleep(max(5, settings.outcome_loop_seconds))


async def web_loop(settings: Settings, store: PredictionStore) -> None:
    app = create_app(store)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=settings.web_host, port=settings.web_port)
    await site.start()
    LOGGER.info("Dashboard listening on http://%s:%s", settings.web_host, settings.web_port)

    # Keep task alive forever.
    await asyncio.Event().wait()


async def main() -> None:
    settings = Settings()
    store = PredictionStore(
        jsonl_path=settings.alert_log_path,
        db_path=settings.db_path,
        horizons_minutes=settings.parsed_outcome_horizons(),
    )
    notifier = AlertNotifier(settings)
    llm_shadow = LlmShadowDecider(settings)

    async with aiohttp.ClientSession() as session:
        symbols = await load_universe(session, settings)

    if not symbols:
        raise RuntimeError("No symbols selected for scanner universe. Adjust SPIKE_* filters.")

    LOGGER.info(
        "Scanner universe loaded: symbols=%s quote=%s min_quote_volume=%.0f",
        len(symbols),
        settings.quote_asset,
        settings.min_quote_volume,
    )
    LOGGER.info("Top symbols sample: %s", ", ".join(s.upper() for s in symbols[:10]))
    LOGGER.info("Prediction DB: %s", settings.db_path)
    if store.orphan_outcomes_pruned > 0:
        LOGGER.info("Scanner DB cleanup: pruned orphan outcomes=%s", store.orphan_outcomes_pruned)
    LOGGER.info("Outcome horizons (min): %s", settings.parsed_outcome_horizons())
    LOGGER.info(
        "Alert gates: min_score=%.3f max_spread=%.3f min_breakout=%.4f min_buy_ratio=%.3f min_rel_quote=%.2f",
        settings.min_score,
        settings.max_spread_pct,
        settings.min_breakout_pct,
        settings.min_buy_ratio,
        settings.min_rel_quote,
    )
    if llm_shadow.enabled():
        LOGGER.info(
            "LLM shadow enabled: bot_api=%s min_conf=%.2f allowed_regimes=%s allowed_risk=%s",
            settings.llm_shadow_bot_api_url,
            settings.llm_shadow_min_confidence,
            sorted(settings.parsed_llm_shadow_allowed_regimes()),
            sorted(settings.parsed_llm_shadow_allowed_risk_levels()),
        )
    if settings.notify_enabled:
        if settings.has_notifier_targets():
            LOGGER.info("Notifications enabled for scanner alerts.")
        else:
            LOGGER.info("Notifications enabled but no targets configured. Set Telegram/Discord env vars.")
    await notifier.notify_startup(len(symbols))

    tasks = [
        stream_symbols(settings.ws_base, symbols, settings.ws_symbols_per_conn),
        scorer_loop(settings, store, notifier, llm_shadow),
        outcomes_loop(settings, store),
    ]
    if settings.web_enabled:
        tasks.append(web_loop(settings, store))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
