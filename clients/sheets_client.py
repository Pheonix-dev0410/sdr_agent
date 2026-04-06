import logging
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from config import GOOGLE_SHEETS_CREDS_PATH

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

_service = None


def _get_service():
    global _service
    if _service is None:
        creds = Credentials.from_service_account_file(GOOGLE_SHEETS_CREDS_PATH, scopes=SCOPES)
        _service = build("sheets", "v4", credentials=creds)
    return _service


def read_sheet(sheet_id: str, range_name: str) -> list[list]:
    try:
        service = _get_service()
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=range_name,
        ).execute()
        return result.get("values", [])
    except Exception as e:
        logger.error(f"Sheets read failed: {e}")
        return []


def write_rows(sheet_id: str, rows: list[list], range_name: str = "Sheet1!A1") -> bool:
    try:
        service = _get_service()
        body = {"values": rows}
        service.spreadsheets().values().append(
            spreadsheetId=sheet_id,
            range=range_name,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body=body,
        ).execute()
        logger.info(f"Wrote {len(rows)} rows to sheet {sheet_id}")
        return True
    except Exception as e:
        logger.error(f"Sheets write failed: {e}")
        return False


SHEET_HEADERS = [
    "Company Name", "Country", "First Name", "Last Name", "Job Title",
    "Matched Role", "Role Tier", "LinkedIn URL", "Email", "Email Status",
    "Phone 1", "Phone 2", "Verification Status", "Confidence", "Source", "Issues",
]


def contact_to_row(contact: dict, company_name: str, country: str) -> list:
    return [
        company_name,
        country,
        contact.get("first_name", ""),
        contact.get("last_name", ""),
        contact.get("job_title", ""),
        contact.get("matched_role", ""),
        contact.get("role_tier", ""),
        contact.get("linkedin_url", ""),
        contact.get("email", ""),
        contact.get("email_status", ""),
        contact.get("phone_1", ""),
        contact.get("phone_2", ""),
        contact.get("verification_status", ""),
        str(contact.get("confidence", "")),
        contact.get("source", ""),
        "; ".join(contact.get("issues", [])),
    ]
