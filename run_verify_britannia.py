"""
Standalone script: verify Britannia contacts in rows 294-335 of 'First Clean List'
and write results back to columns O-U in-place.
"""
import sys
import json
import logging
import time

sys.path.insert(0, ".")

from clients.sheets_client import _get_service
from clients.unipile_client import fetch_linkedin_profile, extract_username, extract_profile_fields
from clients.openai_client import call_gpt5
from utils.json_parser import parse_gpt_json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("logs/verify_britannia.log")],
)
logger = logging.getLogger(__name__)

SHEET_ID = "1UpH1O2EtIFPM1F_Z52LRs1ov7fmQF_eSx4ukDHUXoDo"
TAB = "First Clean List"
START_ROW = 294
END_ROW = 335
COMPANY_NAME = "Britannia Industries"
COMPANY_DOMAIN = "britannia.co.in"

# Column indices (0-based)
COL_FIRST_NAME = 6   # G
COL_LAST_NAME  = 7   # H
COL_JOB_TITLE  = 8   # I
COL_LINKEDIN   = 10  # K
COL_EMAIL      = 11  # L
COL_SOURCE     = 14  # O
COL_LI_STATUS  = 15  # P
COL_EMP_VER    = 16  # Q
COL_TITLE_MATCH = 17 # R
COL_ACTUAL_TITLE = 18 # S
COL_OVERALL    = 19  # T
COL_NOTES      = 20  # U

TARGET_ROLES = [
    "CEO / MD / President / Managing Director",
    "Sales Director / VP Sales / SVP Sales",
    "Head of IT / IT Director / CIO / GM IT",
    "COO / Operations Director / GM Operations",
    "RTM Head / GTM Head / RTM Director / GTM Director",
    "Sales Operations Head / Sales Operations Manager",
    "Customer Development Head / Customer Development Manager",
    "Head of General Trade / Head of Independents / Head of Fragmented Trade",
    "Sales Excellence Director / Commercial Excellence Director",
    "Field Sales Director",
    "Digital Transformation Head / Chief Digital Officer",
    "Head of Digital Commerce / eB2B Head / eB2B Director",
    "Business Intelligence Head / Analytics Director / Business Intelligence Director",
    "Head of GenAI / Head of AI / AI Director",
    "Head of Telesales",
    "Trade Marketing Head",
    "Sales Automation Head / SFA Manager / Sales IT Manager",
]

ROLE_TIERS = {
    "final_decision_makers": ["CEO", "MD", "President", "VP", "Executive Director", "COO", "EVP", "Vice President", "Managing Director"],
    "key_decision_makers": ["Sales Director", "VP Sales", "CIO", "SVP Sales", "IT Head", "Head of IT"],
    "key_influencers": ["Sales Excellence Director", "Commercial Excellence Director", "Field Sales Director", "Chief Digital Officer", "Digital Transformation Head", "RTM Head", "GTM Head", "Sales Operations Head", "Sales Operations Manager", "Customer Development Head", "Head of General Trade", "GM IT", "IT Director", "Business Intelligence Head", "Analytics Director", "Head of Digital Commerce", "eB2B Head", "GTM Director", "RTM Director", "Head of GenAI", "Head of AI", "AI Director", "Head of Telesales"],
    "gate_keepers": ["Sales Automation Head", "Sales Effectiveness Manager", "Sales Capability Manager", "Sales IT Manager", "SFA Manager", "Trade Marketing Head", "RTM Manager", "GTM Manager", "Customer Development Manager", "Analytics Manager", "eB2B Manager", "GenAI Manager"],
}


def safe_get(row, idx, default=""):
    try:
        return row[idx] if idx < len(row) else default
    except IndexError:
        return default


def verify_contact_gpt(first, last, job_title, linkedin_url, unipile_profile):
    roles_list = "\n".join(f"- {r}" for r in TARGET_ROLES)
    tiers_text = (
        f"Final Decision Makers: {', '.join(ROLE_TIERS['final_decision_makers'])}\n"
        f"Key Decision Makers: {', '.join(ROLE_TIERS['key_decision_makers'])}\n"
        f"Key Influencers (sample): {', '.join(ROLE_TIERS['key_influencers'][:8])}...\n"
        f"Gate Keepers (sample): {', '.join(ROLE_TIERS['gate_keepers'][:6])}..."
    )

    prompt = f"""You are a B2B data verification agent for SalesCode.ai.

VERIFY THIS CONTACT at Britannia Industries (India, global manufacturer):

Name: {first} {last}
Title from our mapping: {job_title}
LinkedIn URL: {linkedin_url}
LinkedIn profile data (from Unipile): {json.dumps(unipile_profile)}

TARGET ROLES:
{roles_list}

ROLE TIERS:
{tiers_text}

CHECK:
1. LINKEDIN STATUS: Is the LinkedIn profile reachable / does it exist?
   - "found" = Unipile returned data
   - "not_found" = Unipile returned nothing

2. EMPLOYMENT VERIFIED: Does their current LinkedIn role confirm they are at Britannia Industries (or a Britannia subsidiary like Britannia Bel Foods, Britannia Dairy)?
   - "yes" = confirmed currently at Britannia
   - "no" = currently at a different company (departed)
   - "uncertain" = no LinkedIn data to confirm

3. TITLE MATCH: Does their title match any target role?
   - "yes" = matches a target role
   - "no" = doesn't match any target role
   - "adjacent" = related but not exact

4. ACTUAL TITLE FOUND: What is their actual current title from LinkedIn? (use job_title from mapping if no LinkedIn data)

5. OVERALL STATUS:
   - "valid" = at Britannia + title matches target role
   - "invalid" = departed (at another company)
   - "no_role_match" = at Britannia but title doesn't match any target role
   - "needs_review" = uncertain

6. VERIFICATION NOTES: One concise sentence explaining the decision.

Return ONLY this JSON:
{{"linkedin_status": "found|not_found", "employment_verified": "yes|no|uncertain", "title_match": "yes|no|adjacent", "actual_title_found": "their current title", "overall_status": "valid|invalid|no_role_match|needs_review", "matched_role": "matched target role or null", "role_tier": "final_decision_maker|key_decision_maker|key_influencer|gate_keeper|none", "verification_notes": "one sentence"}}"""

    raw = call_gpt5(prompt, use_web_search=False, temperature=0.1)
    result = parse_gpt_json(raw)
    if not result:
        logger.warning(f"GPT parse failed for {first} {last}")
        return {
            "linkedin_status": "not_found",
            "employment_verified": "uncertain",
            "title_match": "no",
            "actual_title_found": job_title,
            "overall_status": "needs_review",
            "matched_role": None,
            "role_tier": "none",
            "verification_notes": "GPT response parse failed",
        }
    return result


def run():
    import os
    os.makedirs("logs", exist_ok=True)

    service = _get_service()

    # Read rows 294-335
    range_name = f"'{TAB}'!A{START_ROW}:U{END_ROW}"
    result = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=range_name
    ).execute()
    rows = result.get("values", [])
    logger.info(f"Read {len(rows)} rows from sheet")

    updates = []  # list of (sheet_row_number, result_dict)

    for i, row in enumerate(rows):
        sheet_row = START_ROW + i
        first = safe_get(row, COL_FIRST_NAME)
        last = safe_get(row, COL_LAST_NAME)
        job_title = safe_get(row, COL_JOB_TITLE)
        linkedin_url = safe_get(row, COL_LINKEDIN)

        logger.info(f"[{sheet_row}] Verifying {first} {last} | {job_title}")

        # Step 1: Unipile profile fetch
        username = extract_username(linkedin_url)
        unipile_profile = {}
        if username:
            raw = fetch_linkedin_profile(username)
            if not raw.get("_not_found"):
                unipile_profile = extract_profile_fields(raw)
                logger.info(f"  Unipile: found → {unipile_profile.get('current_title')} @ {unipile_profile.get('current_company')}")
            else:
                logger.info(f"  Unipile: not found for {username}")

        # Step 2: GPT verification
        gpt = verify_contact_gpt(first, last, job_title, linkedin_url, unipile_profile)
        logger.info(f"  GPT: {gpt.get('overall_status')} | {gpt.get('verification_notes')}")

        updates.append((sheet_row, gpt))

    # Batch write back columns O-U for each row
    logger.info(f"Writing {len(updates)} rows back to sheet...")

    data = []
    for sheet_row, gpt in updates:
        range_str = f"'{TAB}'!O{sheet_row}:U{sheet_row}"
        values = [[
            "First Clean List",                        # O: Source
            gpt.get("linkedin_status", ""),           # P: LinkedIn Status
            gpt.get("employment_verified", ""),       # Q: Employment Verified
            gpt.get("title_match", ""),               # R: Title Match
            gpt.get("actual_title_found", ""),        # S: Actual Title Found
            gpt.get("overall_status", ""),            # T: Overall Status
            gpt.get("verification_notes", ""),        # U: Verification Notes
        ]]
        data.append({"range": range_str, "values": values})

    service.spreadsheets().values().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()

    logger.info("Done. Sheet updated.")

    # Summary
    statuses = [u[1].get("overall_status") for u in updates]
    print(f"\n{'='*50}")
    print(f"BRITANNIA VERIFICATION SUMMARY ({START_ROW}-{END_ROW})")
    print(f"{'='*50}")
    print(f"  valid:          {statuses.count('valid')}")
    print(f"  invalid:        {statuses.count('invalid')}")
    print(f"  no_role_match:  {statuses.count('no_role_match')}")
    print(f"  needs_review:   {statuses.count('needs_review')}")
    print(f"  total:          {len(updates)}")


if __name__ == "__main__":
    run()
