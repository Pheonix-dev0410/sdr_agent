import logging
import requests
from config import OPENAI_API_KEY, REQUEST_TIMEOUT
from utils.rate_limiter import rate_limit
from utils.retry import api_call_with_retry

logger = logging.getLogger(__name__)

OPENAI_URL = "https://api.openai.com/v1/responses"
MODEL_SMART = "gpt-4o-mini"          # verification, gap report, company intel
MODEL_FAST  = "gpt-4o-mini"     # role expansion, candidate filtering (cheaper)


def _do_call(prompt: str, use_web_search: bool, temperature: float, model: str = MODEL_SMART) -> str:
    rate_limit("openai")

    body = {
        "model": model,
        "input": prompt,
    }
    if use_web_search:
        body["tools"] = [{"type": "web_search_preview"}]

    response = requests.post(
        OPENAI_URL,
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        json=body,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    data = response.json()

    # Extract text from response structure
    output = data.get("output", [])
    for item in output:
        if item.get("type") == "message":
            for content in item.get("content", []):
                if content.get("type") == "output_text":
                    return content.get("text", "")
    # Fallback for simpler response shapes
    return data.get("output", [{}])[0].get("content", [{}])[0].get("text", "")


def call_gpt5(prompt: str, use_web_search: bool = False, temperature: float = 0.1) -> str:
    """Smart model (gpt-4o) — use for verification, gap reports, company intel."""
    result = api_call_with_retry(_do_call, prompt, use_web_search, temperature, MODEL_SMART)
    if result is None:
        logger.error("call_gpt5 failed after all retries")
        return ""
    return result


def call_gpt_fast(prompt: str, use_web_search: bool = False, temperature: float = 0.1) -> str:
    """Fast/cheap model (gpt-4o-mini) — use for role expansion, candidate filtering."""
    result = api_call_with_retry(_do_call, prompt, use_web_search, temperature, MODEL_FAST)
    if result is None:
        logger.error("call_gpt_fast failed after all retries")
        return ""
    return result
