"""Quick test: write one row to Under Review sheet and verify it works."""
from config import OUTPUT_SHEET_ID
from clients.sheets_client import (
    _ensure_headers, _append_rows,
    TAB_UNDER_REVIEW, VERIFICATION_HEADERS,
)

if not OUTPUT_SHEET_ID:
    print("ERROR: OUTPUT_SHEET_ID not set in .env")
    exit(1)

print(f"Sheet ID: {OUTPUT_SHEET_ID}")
print(f"Tab: {TAB_UNDER_REVIEW}")
print("Ensuring headers...")
_ensure_headers(OUTPUT_SHEET_ID, TAB_UNDER_REVIEW, VERIFICATION_HEADERS)

test_row = [""] * len(VERIFICATION_HEADERS)
test_row[0]  = "TEST COMPANY"        # A Company Name
test_row[6]  = "Test"                # G First Name
test_row[7]  = "Contact"             # H Last Name
test_row[8]  = "Test Title"          # I Job Title
test_row[19] = "needs_review"        # T Overall Status
test_row[20] = "Manual test row — delete me"  # U Notes

print("Writing test row...")
ok = _append_rows(OUTPUT_SHEET_ID, TAB_UNDER_REVIEW, [test_row])
print(f"Result: {'SUCCESS' if ok else 'FAILED'}")
