from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import httpx


def _env_bool(key: str, default: bool) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class LlmClientSettings:
    provider: str
    model: str
    timeout_seconds: float
    ollama_base_url: str
    openai_base_url: str
    openai_api_key: str
    openai_path: str
    openai_response_format_json: bool
    openai_max_completion_tokens: int | None
    openai_temperature: float

    @classmethod
    def from_env(cls) -> "LlmClientSettings":
        provider = (os.getenv("LLM_PROVIDER", "ollama").strip().lower() or "ollama")
        model = (
            os.getenv("LLM_MODEL", "").strip()
            or os.getenv("OLLAMA_MODEL", "").strip()
            or "llama3.1:8b"
        )
        timeout_raw = (
            os.getenv("LLM_TIMEOUT", "").strip()
            or os.getenv("OLLAMA_TIMEOUT", "").strip()
            or "30"
        )
        try:
            timeout_seconds = float(timeout_raw)
        except ValueError:
            timeout_seconds = 30.0
        timeout_seconds = max(3.0, min(300.0, timeout_seconds))

        openai_max_completion_tokens_raw = os.getenv("LLM_OPENAI_MAX_COMPLETION_TOKENS", "").strip()
        openai_max_completion_tokens: int | None = None
        if openai_max_completion_tokens_raw:
            try:
                parsed = int(openai_max_completion_tokens_raw)
            except ValueError:
                parsed = 0
            if parsed > 0:
                openai_max_completion_tokens = parsed

        try:
            openai_temperature = float(os.getenv("LLM_OPENAI_TEMPERATURE", "0.1").strip() or "0.1")
        except ValueError:
            openai_temperature = 0.1
        openai_temperature = max(0.0, min(2.0, openai_temperature))

        return cls(
            provider=provider if provider in {"ollama", "openai_compatible"} else "ollama",
            model=model,
            timeout_seconds=timeout_seconds,
            ollama_base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/"),
            openai_base_url=os.getenv("LLM_BASE_URL", "https://api.openai.com/v1").rstrip("/"),
            openai_api_key=os.getenv("LLM_API_KEY", "").strip(),
            openai_path=os.getenv("LLM_OPENAI_CHAT_PATH", "/chat/completions").strip() or "/chat/completions",
            openai_response_format_json=_env_bool("LLM_OPENAI_RESPONSE_FORMAT_JSON", True),
            openai_max_completion_tokens=openai_max_completion_tokens,
            openai_temperature=openai_temperature,
        )


class LlmClient:
    def __init__(self, settings: LlmClientSettings):
        self.settings = settings

    @property
    def provider_name(self) -> str:
        return self.settings.provider

    @property
    def model_name(self) -> str:
        return self.settings.model

    @property
    def base_url(self) -> str:
        if self.settings.provider == "openai_compatible":
            return self.settings.openai_base_url
        return self.settings.ollama_base_url

    async def run(self, prompt: str) -> str:
        if self.settings.provider == "openai_compatible":
            return await self._run_openai_compatible(prompt)
        return await self._run_ollama(prompt)

    async def _run_ollama(self, prompt: str) -> str:
        request_body = {
            "model": self.settings.model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {"temperature": 0.1},
        }
        async with httpx.AsyncClient(timeout=self.settings.timeout_seconds) as client:
            response = await client.post(f"{self.settings.ollama_base_url}/api/generate", json=request_body)
            response.raise_for_status()
            payload = response.json()
        return str(payload.get("response", "")).strip()

    async def _run_openai_compatible(self, prompt: str) -> str:
        if not self.settings.openai_api_key:
            raise RuntimeError("openai_api_key_missing")

        headers = {
            "Authorization": f"Bearer {self.settings.openai_api_key}",
            "Content-Type": "application/json",
        }
        body: dict[str, Any] = {
            "model": self.settings.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": self.settings.openai_temperature,
        }
        if self.settings.openai_response_format_json:
            body["response_format"] = {"type": "json_object"}
        if self.settings.openai_max_completion_tokens is not None:
            body["max_completion_tokens"] = self.settings.openai_max_completion_tokens

        async with httpx.AsyncClient(timeout=self.settings.timeout_seconds) as client:
            response = await client.post(
                f"{self.settings.openai_base_url}{self.settings.openai_path}",
                headers=headers,
                json=body,
            )
            response.raise_for_status()
            payload = response.json()

        try:
            choices = payload.get("choices", [])
            message = choices[0]["message"]
            content = message.get("content", "")
        except Exception as exc:
            raise RuntimeError(f"openai_invalid_response:{exc}") from exc

        if isinstance(content, list):
            text_parts: list[str] = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    text_parts.append(str(item.get("text", "")))
            content = "".join(text_parts)
        if isinstance(content, dict):
            return json.dumps(content, separators=(",", ":"))
        return str(content).strip()
