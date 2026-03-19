import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from llm_client import LlmClientSettings  # noqa: E402


class LlmClientSettingsTests(unittest.TestCase):
    def test_defaults_to_ollama_with_ollama_model(self) -> None:
        original = dict(os.environ)
        try:
            os.environ.pop("LLM_PROVIDER", None)
            os.environ.pop("LLM_MODEL", None)
            os.environ["OLLAMA_MODEL"] = "llama3.1:8b"
            settings = LlmClientSettings.from_env()
        finally:
            os.environ.clear()
            os.environ.update(original)

        self.assertEqual(settings.provider, "ollama")
        self.assertEqual(settings.model, "llama3.1:8b")

    def test_openai_compatible_uses_generic_model_and_key(self) -> None:
        original = dict(os.environ)
        try:
            os.environ["LLM_PROVIDER"] = "openai_compatible"
            os.environ["LLM_MODEL"] = "gpt-4.1-mini"
            os.environ["LLM_BASE_URL"] = "https://example.test/v1"
            os.environ["LLM_API_KEY"] = "secret"
            settings = LlmClientSettings.from_env()
        finally:
            os.environ.clear()
            os.environ.update(original)

        self.assertEqual(settings.provider, "openai_compatible")
        self.assertEqual(settings.model, "gpt-4.1-mini")
        self.assertEqual(settings.openai_base_url, "https://example.test/v1")
        self.assertEqual(settings.openai_api_key, "secret")


if __name__ == "__main__":
    unittest.main()
