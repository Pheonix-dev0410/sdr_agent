import logging
from datetime import datetime, timezone
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


# ── Tab names ─────────────────────────────────────────────────────────────────
TAB_TARGET_ACCOUNTS  = "Target Accounts"
TAB_FIRST_CLEAN_LIST = "First Clean List"
TAB_ACCEPTED         = "Accepted"
TAB_UNDER_REVIEW     = "UnderReview"
TAB_REJECTED         = "Rejected"

# ── Headers ───────────────────────────────────────────────────────────────────

TARGET_ACCOUNTS_HEADERS = [
    "Company Name",                                         # A
    "Parent Company Name",                                  # B
    "Sales Navigator Link",                                 # C
    "Company Domain",                                       # D
    "SDR Name",                                             # E
    "Email Format( Firstname-amy , Lastname- williams)",    # F
    "Account type",                                         # G
    "Account Size",                                         # H
    "",                                                     # I  (blank column in spec)
    "Send to N8N",                                          # J
]

# First Clean List — what n8n writes (cols A-P); we read from here
FIRST_CLEAN_LIST_HEADERS = [
    "Company Name",                            # A
    "Normalized Company Name (Parent Group)",  # B
    "Company Domain Name",                     # C
    "Account type",                            # D
    "Account Size",                            # E
    "Country",                                 # F
    "First Name",                              # G
    "Last Name",                               # H
    "Job titles (English)",                    # I
    "Buying Role",                             # J
    "Linekdin Url",                            # K  ← typo preserved to match n8n
    "Email",                                   # L
    "Phone-1",                                 # M
    "Phone-2",                                 # N
    "Source",                                  # O
    "Pipeline Status",                         # P
]

# Verification sheets — Accepted, Under Review, Rejected (all same schema)
VERIFICATION_HEADERS = [
    "Company Name",                            # A
    "Normalized Company Name (Parent Group)",  # B
    "Company Domain Name",                     # C
    "Account type",                            # D
    "Account Size",                            # E
    "Country",                                 # F
    "First Name",                              # G
    "Last Name",                               # H
    "Job titles (English)",                    # I
    "Buying Role",                             # J
    "Linekdin Url",                            # K  ← typo preserved to match spec
    "Email",                                   # L
    "Phone-1",                                 # M
    "Phone-2",                                 # N
    "Source",                                  # O
    "LinkedIn Status",                         # P
    "Employment Verified",                     # Q
    "Title Match",                             # R
    "Actual Title Found",                      # S
    "Overall Status",                          # T
    "Verification Notes",                      # U
]

# Legacy — kept for backward compat
SHEET_HEADERS = VERIFICATION_HEADERS


# ── Low-level helpers ─────────────────────────────────────────────────────────

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


def _append_rows(sheet_id: str, tab: str, rows: list[list]) -> bool:
    """Append rows to a tab (creates header row if tab is empty)."""
    if not rows:
        return True
    try:
        service = _get_service()
        service.spreadsheets().values().append(
            spreadsheetId=sheet_id,
            range=f"'{tab}'!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows},
        ).execute()
        logger.info(f"Appended {len(rows)} rows to '{tab}'")
        return True
    except Exception as e:
        logger.error(f"Sheets append failed on '{tab}': {e}")
        return False


def _create_tab(sheet_id: str, tab_name: str) -> bool:
    """Create a new sheet tab. Returns True on success."""
    try:
        service = _get_service()
        service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
        ).execute()
        logger.info(f"Created tab '{tab_name}'")
        return True
    except Exception as e:
        logger.warning(f"Could not create tab '{tab_name}': {e}")
        return False


def _ensure_headers(sheet_id: str, tab: str, headers: list[str]) -> None:
    """
    Ensure the tab exists and has a header row.
    Strategy: always attempt to create the tab first.
      - If creation succeeds  → new tab, write headers immediately.
      - If creation fails     → tab already exists, check if A1 is empty.
    This avoids the problem where read_sheet swallows the 400 error and
    returns [] which is indistinguishable from an empty-but-existing tab.
    """
    try:
        service = _get_service()
        service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab}}}]},
        ).execute()
        # Tab was just created — write headers straight away
        logger.info(f"Created tab '{tab}'")
        _append_rows(sheet_id, tab, [headers])
    except Exception:
        # Tab already exists — check if it needs headers
        existing = read_sheet(sheet_id, f"'{tab}'!A1:A1")
        if not existing:
            _append_rows(sheet_id, tab, [headers])


# ── Target Accounts ───────────────────────────────────────────────────────────

def write_target_account(sheet_id: str, meta: dict) -> bool:
    """
    Write one row to the Target Accounts sheet when a company is triggered.
    Called immediately on POST /api/trigger.
    Header row is managed manually in the sheet — never written by code.
    """
    row = [
        meta.get("company_name", ""),                               # A Company Name
        meta.get("parent_company_name", ""),                        # B Parent Company Name
        meta.get("sales_nav_url", ""),                              # C Sales Navigator Link
        meta.get("domain", ""),                                     # D Company Domain
        meta.get("sdr_assigned", ""),                               # E SDR Name
        meta.get("email_format", ""),                               # F Email Format
        meta.get("account_type", ""),                               # G Account type
        meta.get("account_size", ""),                               # H Account Size
        "",                                                          # I (blank)
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"), # J Send to N8N (timestamp)
    ]
    return _append_rows(sheet_id, TAB_TARGET_ACCOUNTS, [row])


# ── First Clean List ──────────────────────────────────────────────────────────

def count_pending_contacts(sheet_id: str, company_name: str) -> int:
    """
    Count unverified contacts for company_name WITHOUT marking them.
    Used by the sheet poller to detect when n8n has finished writing.
    """
    rows = read_sheet(sheet_id, f"'{TAB_FIRST_CLEAN_LIST}'!A:P")
    if not rows or len(rows) < 2:
        return 0
    header = rows[0]
    count = 0
    for row in rows[1:]:
        while len(row) < 16:
            row.append("")
        d = dict(zip(header, row))
        company_match = company_name.lower() in d.get("Company Name", "").lower()
        pipeline_status = d.get("Pipeline Status", "").strip()
        has_name = bool(d.get("First Name") or d.get("Last Name"))
        if company_match and pipeline_status == "" and has_name:
            count += 1
    return count


def read_first_clean_list_for_company(sheet_id: str, company_name: str) -> list[dict]:
    """
    Read ONLY unverified rows (Pipeline Status col P = blank) for company_name.
    This ensures we never re-verify a contact we already processed.
    After reading, marks those rows as "verified" in col P so they won't be picked up again.
    """
    # Read A:P (16 cols — everything n8n writes)
    rows = read_sheet(sheet_id, f"'{TAB_FIRST_CLEAN_LIST}'!A:P")
    if not rows or len(rows) < 2:
        return []

    header = rows[0]
    # Col P index (0-based) = 15
    status_col_idx = 15

    contacts = []
    rows_to_mark = []  # 1-based sheet row numbers to mark as "verified"

    for sheet_row_idx, row in enumerate(rows[1:], start=2):  # start=2 because row 1 is header
        while len(row) < 16:
            row.append("")

        d = dict(zip(header, row))

        # Only process rows for this company that haven't been verified yet
        company_match = company_name.lower() in d.get("Company Name", "").lower()
        pipeline_status = d.get("Pipeline Status", "").strip()
        already_processed = pipeline_status != ""

        if not company_match or already_processed:
            continue

        if not d.get("First Name") and not d.get("Last Name"):
            continue

        contacts.append({
            "company_name": d.get("Company Name", ""),
            "normalized_name": d.get("Normalized Company Name (Parent Group)", ""),
            "domain": d.get("Company Domain Name", ""),
            "account_type": d.get("Account type", ""),
            "account_size": d.get("Account Size", ""),
            "country": d.get("Country", ""),
            "first_name": d.get("First Name", ""),
            "last_name": d.get("Last Name", ""),
            "job_title": d.get("Job titles (English)", ""),
            "buying_role": d.get("Buying Role", ""),
            "linkedin_url": d.get("Linekdin Url", ""),
            "email": d.get("Email", ""),
            "phone_1": d.get("Phone-1", ""),
            "phone_2": d.get("Phone-2", ""),
            "source": d.get("Source", "n8n"),
        })
        rows_to_mark.append(sheet_row_idx)

    logger.info(
        f"Read {len(contacts)} unverified contacts from First Clean List for '{company_name}'"
    )

    # Mark those rows as "verified" in col P so they won't be picked up again
    if rows_to_mark:
        try:
            service = _get_service()
            write_data = [
                {
                    "range": f"'{TAB_FIRST_CLEAN_LIST}'!P{r}",
                    "values": [["verified"]],
                }
                for r in rows_to_mark
            ]
            service.spreadsheets().values().batchUpdate(
                spreadsheetId=sheet_id,
                body={"valueInputOption": "RAW", "data": write_data},
            ).execute()
            logger.info(f"Marked {len(rows_to_mark)} rows as 'verified' in First Clean List")
        except Exception as e:
            logger.warning(f"Could not mark rows as verified: {e}")

    return contacts


def write_contacts_to_first_clean_list(
    sheet_id: str,
    contacts: list[dict],
    meta: dict,
    pipeline_status: str = "searcher",
) -> bool:
    """
    Write contacts to the First Clean List tab with the given pipeline_status.
    Used to record searcher/company-intel contacts before they are verified,
    so the First Clean List contains every contact found from any source.

    pipeline_status must be non-blank so the sheet poller does not re-pick
    these rows as unverified n8n output.
    """
    if not contacts:
        return True
    rows = []
    for c in contacts:
        rows.append([
            c.get("company_name", ""),                                              # A
            c.get("normalized_name") or meta.get("parent_company_name", ""),       # B
            c.get("domain") or meta.get("domain", ""),                              # C
            c.get("account_type") or meta.get("account_type", ""),                 # D
            c.get("account_size") or meta.get("account_size", ""),                 # E
            c.get("country") or meta.get("country", ""),                           # F
            c.get("first_name", ""),                                                # G
            c.get("last_name", ""),                                                 # H
            c.get("job_title", ""),                                                 # I
            c.get("buying_role", ""),                                               # J
            c.get("linkedin_url", ""),                                              # K
            c.get("email", ""),                                                     # L
            c.get("phone_1", ""),                                                   # M
            c.get("phone_2", ""),                                                   # N
            c.get("source", "searcher"),                                            # O
            pipeline_status,                                                        # P
        ])
    ok = _append_rows(sheet_id, TAB_FIRST_CLEAN_LIST, rows)
    if ok:
        logger.info(
            f"Wrote {len(rows)} contact(s) to First Clean List (status='{pipeline_status}')"
        )
    return ok


# ── Verification sheets ───────────────────────────────────────────────────────

def _contact_to_verification_row(
    contact: dict, company_name: str, country: str, meta: dict
) -> list:
    """Map a verified contact dict to the 21-column verification sheet row."""
    return [
        company_name,                                           # A Company Name
        contact.get("normalized_name") or meta.get("parent_company_name") or company_name,  # B
        contact.get("domain") or meta.get("domain", ""),       # C Company Domain Name
        contact.get("account_type") or meta.get("account_type", ""),  # D Account type
        contact.get("account_size") or meta.get("account_size", ""),  # E Account Size
        country,                                                # F Country
        contact.get("first_name", ""),                         # G First Name
        contact.get("last_name", ""),                          # H Last Name
        contact.get("job_title", ""),                          # I Job titles (English)
        contact.get("buying_role") or contact.get("matched_role", ""),  # J Buying Role
        contact.get("linkedin_url", ""),                       # K Linekdin Url
        contact.get("email", ""),                              # L Email
        contact.get("phone_1", ""),                            # M Phone-1
        contact.get("phone_2", ""),                            # N Phone-2
        contact.get("source", "pipeline"),                     # O Source
        contact.get("unipile_status", ""),                     # P LinkedIn Status
        contact.get("company_confirmed", ""),                  # Q Employment Verified
        contact.get("title_match", ""),                        # R Title Match  (from verifier issues)
        contact.get("matched_role") or contact.get("job_title", ""),  # S Actual Title Found
        contact.get("verification_status", ""),                # T Overall Status
        "; ".join(contact.get("issues", [])) or contact.get("reason", ""),  # U Verification Notes
    ]


def write_verified_contacts(
    sheet_id: str,
    contacts: list[dict],
    company_name: str,
    country: str,
    meta: dict,
) -> dict:
    """
    Route verified contacts to Accepted / Under Review / Rejected tabs.
    Returns counts per tab.
    """
    accepted = []
    under_review = []
    rejected = []

    for c in contacts:
        status = c.get("verification_status", "needs_review")
        row = _contact_to_verification_row(c, company_name, country, meta)
        if status == "valid":
            accepted.append(row)
        elif status == "invalid":
            rejected.append(row)
        else:
            under_review.append(row)

    counts = {"accepted": 0, "under_review": 0, "rejected": 0}

    for tab, rows, key in [
        (TAB_ACCEPTED,    accepted,    "accepted"),
        (TAB_UNDER_REVIEW, under_review, "under_review"),
        (TAB_REJECTED,    rejected,    "rejected"),
    ]:
        if rows:
            _ensure_headers(sheet_id, tab, VERIFICATION_HEADERS)
            if _append_rows(sheet_id, tab, rows):
                counts[key] = len(rows)

    logger.info(
        f"Wrote to sheets: accepted={counts['accepted']} "
        f"under_review={counts['under_review']} rejected={counts['rejected']}"
    )
    return counts


# ── Legacy helper (used by old scripts) ──────────────────────────────────────

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


def contact_to_row(contact: dict, company_name: str, country: str) -> list:
    """Legacy row builder — kept for old run_britannia_full.py etc."""
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
