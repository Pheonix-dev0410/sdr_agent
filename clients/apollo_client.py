import logging
import requests
from config import APOLLO_API_KEY, REQUEST_TIMEOUT
from utils.rate_limiter import rate_limit
from utils.retry import api_call_with_retry

logger = logging.getLogger(__name__)

# Apollo people search requires a paid plan — mixed_people/search and people/match
# are not available on free tier. This client returns empty immediately.
# org/enrich is free but doesn't return contacts.

APOLLO_ORG_URL = "https://api.apollo.io/v1/organizations/enrich"


def search_people(domain: str, titles: list[str], page: int = 1, per_page: int = 5) -> list[dict]:
    """People search is paywalled on Apollo free plan — returns empty list."""
    logger.debug(f"Apollo people search skipped (free plan) for {domain}")
    return []


def enrich_org(domain: str) -> dict:
    """Org enrichment is available on free plan — returns company metadata."""
    rate_limit("apollo")
    response = requests.get(
        APOLLO_ORG_URL,
        headers={"X-Api-Key": APOLLO_API_KEY},
        params={"domain": domain},
        timeout=REQUEST_TIMEOUT,
    )
    if response.status_code != 200:
        return {}
    return response.json().get("organization", {})
