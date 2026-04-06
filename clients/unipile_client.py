import itertools
import logging
import re
import requests
from config import UNIPILE_API_KEY, UNIPILE_BASE_URL, UNIPILE_ACCOUNT_ID, UNIPILE_SEARCH_ACCOUNT_IDS, REQUEST_TIMEOUT
from utils.rate_limiter import rate_limit
from utils.retry import api_call_with_retry

logger = logging.getLogger(__name__)

# Round-robin iterator over search-capable accounts
_search_account_cycle = itertools.cycle(UNIPILE_SEARCH_ACCOUNT_IDS) if UNIPILE_SEARCH_ACCOUNT_IDS else None


def _next_search_account() -> str:
    if _search_account_cycle:
        return next(_search_account_cycle)
    return UNIPILE_ACCOUNT_ID  # fallback


def _headers() -> dict:
    return {
        "X-API-KEY": UNIPILE_API_KEY,
        "accept": "application/json",
        "content-type": "application/json",
    }


def extract_username(url: str) -> str | None:
    """Extract LinkedIn username/slug from any LinkedIn URL format."""
    if not url:
        return None
    url = url.strip().rstrip("/").split("?")[0]

    # Sales Nav lead: /sales/lead/ABC123,NAME_SEARCH
    m = re.search(r"/sales/lead/([^,/]+)", url)
    if m:
        return m.group(1)

    # Standard /in/ format
    m = re.search(r"/in/([^/]+)$", url)
    if m:
        return m.group(1)

    return None


def _do_fetch_profile(username: str) -> dict:
    rate_limit("unipile")
    response = requests.get(
        f"{UNIPILE_BASE_URL}/api/v1/users/{username}",
        headers=_headers(),
        params={"account_id": UNIPILE_ACCOUNT_ID},
        timeout=REQUEST_TIMEOUT,
    )
    if response.status_code in (404, 403, 401):
        return {"_not_found": True}
    response.raise_for_status()
    return response.json()


def fetch_linkedin_profile(username: str) -> dict:
    """Fetch a LinkedIn profile by username via Unipile."""
    result = api_call_with_retry(_do_fetch_profile, username)
    if result is None:
        logger.warning(f"Unipile profile fetch failed for {username}")
        return {"_not_found": True}
    return result


def _do_search_salesnav(sales_nav_url: str, keywords: str, limit: int) -> list[dict]:
    """Search via Sales Nav URL passed directly to Unipile."""
    rate_limit("unipile")
    account_id = _next_search_account()

    # Append keyword filter to the Sales Nav URL if provided
    search_url = sales_nav_url
    if keywords and "keywords=" not in sales_nav_url:
        encoded = keywords.replace(" ", "%20")
        if "query=(" in search_url:
            search_url = search_url.replace("query=(", f"query=(keywords%3A{encoded}%2C", 1)

    response = requests.post(
        f"{UNIPILE_BASE_URL}/api/v1/linkedin/search",
        headers=_headers(),
        params={"account_id": account_id},
        json={"url": search_url, "limit": limit},
        timeout=REQUEST_TIMEOUT,
    )
    if response.status_code in (401, 403):
        logger.warning(f"Account {account_id} not usable for search ({response.status_code}), trying next")
        # Try next account once
        account_id = _next_search_account()
        response = requests.post(
            f"{UNIPILE_BASE_URL}/api/v1/linkedin/search",
            headers=_headers(),
            params={"account_id": account_id},
            json={"url": search_url, "limit": limit},
            timeout=REQUEST_TIMEOUT,
        )
    response.raise_for_status()
    return response.json().get("items", [])


def search_salesnav(sales_nav_url: str, keywords: str = "", limit: int = 10) -> list[dict]:
    """
    Search LinkedIn via Sales Navigator URL.
    Pass the company's sales_nav_url from the webhook — already filtered to that company.
    Optionally append keywords to narrow by role.
    """
    if not UNIPILE_SEARCH_ACCOUNT_IDS:
        logger.warning("No UNIPILE_SEARCH_ACCOUNT_IDS configured — skipping Sales Nav search")
        return []
    result = api_call_with_retry(_do_search_salesnav, sales_nav_url, keywords, limit)
    if result is None:
        logger.warning(f"Sales Nav search failed for keywords='{keywords}'")
        return []
    return result


def _do_search_classic(keywords: str, company_id: str, limit: int) -> list[dict]:
    """Classic LinkedIn search filtered by company org ID."""
    rate_limit("unipile")
    body = {
        "api": "classic",
        "category": "people",
        "keywords": keywords,
        "limit": limit,
    }
    if company_id:
        body["company"] = [str(company_id)]

    response = requests.post(
        f"{UNIPILE_BASE_URL}/api/v1/linkedin/search",
        headers=_headers(),
        params={"account_id": UNIPILE_ACCOUNT_ID},
        json=body,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json().get("items", [])


def search_classic(keywords: str, company_id: str = "", limit: int = 10) -> list[dict]:
    """Fallback: Classic LinkedIn search by keywords + company org ID."""
    result = api_call_with_retry(_do_search_classic, keywords, company_id, limit)
    if result is None:
        logger.warning(f"Classic search failed for keywords='{keywords}'")
        return []
    return result


def normalize_salesnav_item(item: dict) -> dict:
    """Normalize a Sales Nav search result into a standard candidate dict."""
    pos = item.get("current_positions", [{}])[0] if item.get("current_positions") else {}
    return {
        "first_name": item.get("first_name", item.get("name", "").split(" ")[0]),
        "last_name": item.get("last_name", " ".join(item.get("name", "").split(" ")[1:])),
        "title": pos.get("role", item.get("headline", "")),
        "current_company": pos.get("company", ""),
        "linkedin_url": item.get("public_profile_url", item.get("profile_url", "")),
        "public_identifier": item.get("public_identifier", ""),
        "tenure_years": pos.get("tenure_at_role", {}).get("years"),
    }


def normalize_classic_item(item: dict) -> dict:
    """Normalize a Classic search result into a standard candidate dict."""
    return {
        "first_name": item.get("name", "").split(" ")[0],
        "last_name": " ".join(item.get("name", "").split(" ")[1:]),
        "title": item.get("headline", ""),
        "current_company": "",
        "linkedin_url": item.get("public_profile_url", item.get("profile_url", "")),
        "public_identifier": item.get("public_identifier", ""),
    }


def extract_profile_fields(profile: dict) -> dict:
    """Normalize a raw Unipile /users/{username} response."""
    if not profile or profile.get("_not_found"):
        return {}

    positions = profile.get("positions", profile.get("experience", []))
    current = next(
        (p for p in positions if not p.get("end_date") and not p.get("endDate")),
        positions[0] if positions else {},
    )

    # Fall back to parsing headline when positions are empty
    # e.g. "Chief Commercial Officer at Britannia Industries Limited"
    headline = profile.get("headline", "")
    headline_title = ""
    headline_company = ""
    if headline and not current:
        parts = headline.split(" at ", 1)
        if len(parts) == 2:
            headline_title = parts[0].strip()
            headline_company = parts[1].strip()

    return {
        "current_company": current.get("company", current.get("companyName", "")) or headline_company,
        "current_title": current.get("title", current.get("jobTitle", "")) or headline_title,
        "headline": headline,
        "location": profile.get("location", profile.get("geoLocation", "")),
        "first_name": profile.get("first_name", ""),
        "last_name": profile.get("last_name", ""),
        "public_identifier": profile.get("public_identifier", profile.get("publicIdentifier", "")),
        "all_positions": positions,
    }
