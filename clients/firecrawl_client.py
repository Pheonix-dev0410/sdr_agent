import logging
import requests
from config import FIRECRAWL_API_KEY, REQUEST_TIMEOUT
from utils.rate_limiter import rate_limit
from utils.retry import api_call_with_retry

logger = logging.getLogger(__name__)

FIRECRAWL_URL = "https://api.firecrawl.dev/v1/scrape"


def _do_scrape(url: str) -> str:
    rate_limit("firecrawl")
    response = requests.post(
        FIRECRAWL_URL,
        headers={
            "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
            "Content-Type": "application/json",
        },
        json={"url": url, "formats": ["markdown"], "onlyMainContent": True},
        timeout=REQUEST_TIMEOUT,
    )
    if response.status_code in (404, 403, 410):
        return ""
    response.raise_for_status()
    data = response.json()
    return data.get("data", {}).get("markdown", "")


def scrape_url(url: str) -> str:
    result = api_call_with_retry(_do_scrape, url)
    if result is None:
        logger.warning(f"Firecrawl scrape failed for {url}")
        return ""
    return result
