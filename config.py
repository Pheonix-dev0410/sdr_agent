import os
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
APOLLO_API_KEY = os.getenv("APOLLO_API_KEY")
UNIPILE_API_KEY = os.getenv("UNIPILE_API_KEY")
UNIPILE_BASE_URL = os.getenv("UNIPILE_BASE_URL", "https://api21.unipile.com:15157")
UNIPILE_ACCOUNT_ID = os.getenv("UNIPILE_ACCOUNT_ID", "")
UNIPILE_SEARCH_ACCOUNT_IDS = [
    x.strip() for x in os.getenv("UNIPILE_SEARCH_ACCOUNT_IDS", "").split(",") if x.strip()
]
ZEROBOUNCE_API_KEY = os.getenv("ZEROBOUNCE_API_KEY")
FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY")
CLAY_API_KEY = os.getenv("CLAY_API_KEY")
CLAY_WEBHOOK_URL = os.getenv("CLAY_WEBHOOK_URL")
CLAY_TABLE_ID = os.getenv("CLAY_TABLE_ID")
GOOGLE_SHEETS_CREDS_PATH = os.getenv("GOOGLE_SHEETS_CREDS_PATH")
OUTPUT_SHEET_ID = os.getenv("OUTPUT_SHEET_ID")

RATE_LIMITS = {
    "unipile": 3.0,
    "apollo": 1.0,
    "openai": 0.5,
    "zerobounce": 0.1,
    "firecrawl": 0.5,
    "clay": 1.0,
}

MAX_RETRIES = 3
REQUEST_TIMEOUT = 120
MATCH_CONFIDENCE_THRESHOLD = 0.6
