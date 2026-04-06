import logging
import requests
from config import ZEROBOUNCE_API_KEY, REQUEST_TIMEOUT
from utils.rate_limiter import rate_limit
from utils.retry import api_call_with_retry

logger = logging.getLogger(__name__)

ZEROBOUNCE_URL = "https://api.zerobounce.net/v2/validate"


def _do_verify_email(email: str) -> dict:
    rate_limit("zerobounce")
    response = requests.get(
        ZEROBOUNCE_URL,
        params={"api_key": ZEROBOUNCE_API_KEY, "email": email},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def verify_email(email: str) -> dict:
    result = api_call_with_retry(_do_verify_email, email)
    if result is None:
        logger.warning(f"ZeroBounce verification failed for {email}")
        return {"status": "unknown", "error": "request_failed"}
    return result


def is_valid(zb_result: dict) -> bool:
    return zb_result.get("status", "").lower() == "valid"
