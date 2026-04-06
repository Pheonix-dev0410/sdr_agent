import time
from config import RATE_LIMITS

_last_call: dict[str, float] = {}


def rate_limit(service: str) -> None:
    now = time.time()
    if service in _last_call:
        wait = RATE_LIMITS.get(service, 0) - (now - _last_call[service])
        if wait > 0:
            time.sleep(wait)
    _last_call[service] = time.time()
