import logging

import aiohttp

from app.config import Settings

LOGGER = logging.getLogger(__name__)


class AlertNotifier:
    def __init__(self, settings: Settings):
        self.settings = settings

    async def notify_startup(self, symbols_count: int) -> None:
        if not self._enabled():
            return
        message = (
            f"Spike scanner started. symbols={symbols_count} "
            f"min_score={self.settings.min_score:.2f} "
            f"max_spread_pct={self.settings.max_spread_pct:.2f}"
        )
        await self._send(message)

    async def notify_alert(self, payload: dict) -> None:
        if not self._enabled():
            return
        message = self._format_alert(payload)
        await self._send(message)

    def _enabled(self) -> bool:
        return self.settings.notify_enabled and self.settings.has_notifier_targets()

    def _format_alert(self, payload: dict) -> str:
        symbol = str(payload.get("symbol", "?"))
        score = float(payload.get("score", 0.0))
        price = payload.get("price")
        meta = payload.get("meta", {})
        llm_shadow = payload.get("llm_shadow", {})

        rel_quote = float(meta.get("rel_quote", 0.0))
        rel_trades = float(meta.get("rel_trades", 0.0))
        breakout = float(meta.get("breakout", 0.0)) * 100.0
        spread = float(meta.get("spread_pct", 0.0))
        buy_ratio = float(meta.get("buy_ratio", 0.0))

        price_txt = f"{price}" if price is not None else "n/a"
        llm_text = ""
        if isinstance(llm_shadow, dict) and llm_shadow:
            llm_text = (
                f"\\nllm_allowed={llm_shadow.get('allowed')} regime={llm_shadow.get('regime', '')} "
                f"risk={llm_shadow.get('risk_level', '')} conf={llm_shadow.get('confidence', '')} "
                f"reason={llm_shadow.get('reason', '')}"
            )

        return (
            f"SPIKE ALERT {symbol}\\n"
            f"score={score:.2f} price={price_txt} spread={spread:.3f}%\\n"
            f"rel_quote={rel_quote:.2f} rel_trades={rel_trades:.2f} "
            f"breakout={breakout:.2f}% buy_ratio={buy_ratio:.2f}"
            f"{llm_text}"
        )

    async def _send(self, message: str) -> None:
        timeout = aiohttp.ClientTimeout(total=max(2, self.settings.notify_timeout_seconds))
        async with aiohttp.ClientSession(timeout=timeout) as session:
            if self.settings.telegram_bot_token and self.settings.telegram_chat_id:
                await self._send_telegram(session, message)
            if self.settings.discord_webhook_url:
                await self._send_discord(session, message)

    async def _send_telegram(self, session: aiohttp.ClientSession, message: str) -> None:
        token = self.settings.telegram_bot_token
        chat_id = self.settings.telegram_chat_id
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "disable_web_page_preview": True,
        }
        try:
            async with session.post(url, json=payload) as resp:
                if resp.status >= 300:
                    body = await resp.text()
                    LOGGER.warning("Telegram notify failed: status=%s body=%s", resp.status, body[:300])
        except Exception as exc:
            LOGGER.warning("Telegram notify exception: %s", exc)

    async def _send_discord(self, session: aiohttp.ClientSession, message: str) -> None:
        payload = {"content": message}
        try:
            async with session.post(self.settings.discord_webhook_url, json=payload) as resp:
                if resp.status >= 300:
                    body = await resp.text()
                    LOGGER.warning("Discord notify failed: status=%s body=%s", resp.status, body[:300])
        except Exception as exc:
            LOGGER.warning("Discord notify exception: %s", exc)
