import time
import logging
from config import MAX_RETRIES

logger = logging.getLogger(__name__)


def api_call_with_retry(func, *args, max_retries: int = MAX_RETRIES, **kwargs):
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                return None
