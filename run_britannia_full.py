"""
Full Britannia Industries pipeline:
1. Verify all 42 contacts (rows 294-335 of 'First Clean List') → write O-U in-place
2. Generate gap report → which target roles are missing
3. Searcher waterfall for each missing role (Unipile SalesNav → Apollo → Clay → GPT web)
4. Verify searcher-found contacts
5. Write all searcher+verified results to Sheet12
"""
import sys
import json
import logging
import os
import time

sys.path.insert(0, ".")
os.makedirs("logs", exist_ok=True)

from clients.sheets_client import _get_service
from clients.unipile_client import (
    fetch_linkedin_profile, extract_username, extract_profile_fields,
    search_salesnav, search_classic,
    normalize_salesnav_item, normalize_classic_item,
)
from clients.openai_client import call_gpt5
from clients.apollo_client import search_people as apollo_search
from clients.clay_client import enrich as clay_enrich
from utils.json_parser import parse_gpt_json
from config import MATCH_CONFIDENCE_THRESHOLD

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/britannia_full.log"),
    ],
)
logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
SHEET_ID    = "1UpH1O2EtIFPM1F_Z52LRs1ov7fmQF_eSx4ukDHUXoDo"
TAB_SOURCE  = "First Clean List"
TAB_OUTPUT  = "Sheet12"
START_ROW   = 294
END_ROW     = 335

COMPANY_NAME   = "Britannia Industries"
COMPANY_DOMAIN = "britannia.co.in"
COUNTRY        = "India"
ACCOUNT_TYPE   = "manufacturer"

# Britannia Sales Nav URL (from previous session)
SALES_NAV_URL = (
    "https://www.linkedin.com/sales/search/people"
    "?query=(recentSearchParam%3A(id%3A3566099044%2CdoLogHistory%3Atrue)"
    "%2Cfilters%3AList((type%3ACURRENT_COMPANY%2Cvalues%3AList((id%3Aurn%253Ali%253Aorganization%253A162479"
    "%2Ctext%3ABritannia%2520Industries%2CselectionType%3AINCLUDED)))"
    "%2C(type%3ASENIORITY_LEVEL%2Cvalues%3AList("
    "(id%3A4%2Ctext%3AManager%2CselectionType%3AINCLUDED)"
    "%2C(id%3A5%2Ctext%3ADirector%2CselectionType%3AINCLUDED)"
    "%2C(id%3A6%2Ctext%3AVP%2CselectionType%3AINCLUDED)"
    "%2C(id%3A7%2Ctext%3ACXO%2CselectionType%3AINCLUDED)"
    "%2C(id%3A8%2Ctext%3APartner%2CselectionType%3AINCLUDED)"
    "%2C(id%3A9%2Ctext%3AOwner%2CselectionType%3AINCLUDED)))))"
    "&sessionId=hFPXLHHTTIKdlhsFWE0VgA%3D%3D"
)
LINKEDIN_ORG_ID = "162479"

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
    "Chief Commercial Officer / CCO",
]

ROLE_TIERS = {
    "final_decision_makers": ["CEO", "MD", "President", "VP", "Executive Director", "COO", "EVP",
                               "Vice President", "Managing Director", "Chief Commercial Officer", "CCO"],
    "key_decision_makers": ["Sales Director", "VP Sales", "CIO", "SVP Sales", "IT Head", "Head of IT"],
    "key_influencers": ["Sales Excellence Director", "Commercial Excellence Director", "Field Sales Director",
                        "Chief Digital Officer", "Digital Transformation Head", "RTM Head", "GTM Head",
                        "Sales Operations Head", "Sales Operations Manager", "Customer Development Head",
                        "Head of General Trade", "GM IT", "IT Director", "Business Intelligence Head",
                        "Analytics Director", "Head of Digital Commerce", "eB2B Head", "GTM Director",
                        "RTM Director", "Head of GenAI", "Head of AI", "AI Director", "Head of Telesales"],
    "gate_keepers": ["Sales Automation Head", "Sales Effectiveness Manager", "Sales Capability Manager",
                     "Sales IT Manager", "SFA Manager", "Trade Marketing Head", "RTM Manager", "GTM Manager",
                     "Customer Development Manager", "Analytics Manager", "eB2B Manager", "GenAI Manager"],
}

# ── Helpers ───────────────────────────────────────────────────────────────────
def safe_get(row, idx, default=""):
    try:
        return row[idx] if idx < len(row) else default
    except IndexError:
        return default


def _roles_list_text():
    return "\n".join(f"- {r}" for r in TARGET_ROLES)


def _tiers_text():
    return (
        f"Final Decision Makers: {', '.join(ROLE_TIERS['final_decision_makers'])}\n"
        f"Key Decision Makers: {', '.join(ROLE_TIERS['key_decision_makers'])}\n"
        f"Key Influencers: {', '.join(ROLE_TIERS['key_influencers'][:10])}...\n"
        f"Gate Keepers: {', '.join(ROLE_TIERS['gate_keepers'][:6])}..."
    )


# ── Step 1: Verify a single contact ──────────────────────────────────────────
def verify_contact(first, last, job_title, linkedin_url, unipile_profile, email=""):
    # Determine email domain signal
    email_domain = email.split("@")[1].lower() if "@" in email else ""
    BRITANNIA_DOMAINS = {"britannia.co.in", "britindia.com", "britanniaind.com"}
    email_is_britannia = email_domain in BRITANNIA_DOMAINS

    email_signal = ""
    if email_is_britannia:
        email_signal = f"\nEMAIL DOMAIN SIGNAL: Contact has a corporate Britannia email ({email}) — this is STRONG evidence they are currently employed at Britannia. Treat email domain as confirmation of employment unless LinkedIn explicitly shows they left."
    elif email:
        email_signal = f"\nEmail on file: {email} (non-Britannia domain — do not use as employment proof)"

    prompt = f"""You are a B2B data verification agent for SalesCode.ai.

VERIFY THIS CONTACT at Britannia Industries (India, FMCG/CPG manufacturer):

Name: {first} {last}
Title from mapping: {job_title}
LinkedIn URL: {linkedin_url}
LinkedIn profile data (Unipile): {json.dumps(unipile_profile)}{email_signal}

IMPORTANT — Unipile only returns basic profile for 3rd-degree connections (no positions list, just headline).
Use ALL available signals: headline text, email domain, mapped title.
Britannia corporate email domains: britannia.co.in, britindia.com
If headline mentions Britannia AND email is @britindia.com or @britannia.co.in → employment_verified = "yes"
If headline mentions ONLY other companies as current (e.g., "at IFFCO", "at Tata") → "no"
If headline is a career summary listing multiple past companies with no clear current indicator → use email domain as the deciding factor

TARGET ROLES:
{_roles_list_text()}

ROLE TIERS:
{_tiers_text()}

CHECK:
1. LINKEDIN STATUS: "found" = Unipile returned data, "not_found" = nothing returned
2. EMPLOYMENT VERIFIED: "yes" = confirmed at Britannia/subsidiary (Britannia Bel Foods, Britannia Dairy, Britindia), "no" = currently at a different company, "uncertain" = genuinely can't tell even with email signal
3. TITLE MATCH: "yes" = matches a target role, "no" = no match, "adjacent" = related but not exact
4. ACTUAL TITLE FOUND: their actual current title from LinkedIn (or job_title from mapping if no LinkedIn data)
5. OVERALL STATUS: "valid" = at Britannia + title matches, "invalid" = departed, "no_role_match" = at Britannia but wrong role, "needs_review" = genuinely uncertain
6. VERIFICATION NOTES: one concise sentence

Return ONLY this JSON:
{{"linkedin_status":"found|not_found","employment_verified":"yes|no|uncertain","title_match":"yes|no|adjacent","actual_title_found":"title","overall_status":"valid|invalid|no_role_match|needs_review","matched_role":"matched target role or null","role_tier":"final_decision_maker|key_decision_maker|key_influencer|gate_keeper|none","verification_notes":"one sentence"}}"""

    raw = call_gpt5(prompt, use_web_search=False)
    result = parse_gpt_json(raw)
    if not result:
        logger.warning(f"  GPT parse failed for {first} {last} | raw: {raw[:150]}")
        return {
            "linkedin_status": "not_found",
            "employment_verified": "uncertain",
            "title_match": "no",
            "actual_title_found": job_title,
            "overall_status": "needs_review",
            "matched_role": None,
            "role_tier": "none",
            "verification_notes": "GPT parse failed",
        }
    return result


# ── Step 2: Gap report ────────────────────────────────────────────────────────
BRITANNIA_DOMAINS = {"britannia.co.in", "britindia.com", "britanniaind.com"}
BRITANNIA_KEYWORDS = {"britannia", "britindia"}


def _is_likely_at_britannia(contact: dict) -> bool:
    """
    Return True if we have enough signal to treat this person as currently at Britannia,
    even if GPT said needs_review / uncertain.
    Signals (any one is sufficient):
      - overall_status == "valid"
      - employment_verified == "yes"
      - email domain is a known Britannia domain
      - LinkedIn headline contains "britannia" or "britindia" (current indicator)
        AND title_match == "yes" or "adjacent"
    """
    if contact.get("overall_status") == "valid":
        return True
    if contact.get("employment_verified") == "yes":
        return True
    email = contact.get("email", "")
    if "@" in email and email.split("@")[1].lower() in BRITANNIA_DOMAINS:
        return True
    headline = contact.get("headline", "").lower()
    title_match = contact.get("title_match", "no")
    if any(kw in headline for kw in BRITANNIA_KEYWORDS) and title_match in ("yes", "adjacent"):
        return True
    return False


def generate_gap_report(verified_contacts):
    """
    Smart gap report:
    - A role is COVERED if any contact is likely at Britannia AND their matched_role
      overlaps with the target role bucket (fuzzy substring match).
    - Uses _is_likely_at_britannia() instead of just overall_status == valid.
    """
    covered_roles = set()
    for c in verified_contacts:
        if not _is_likely_at_britannia(c):
            continue
        matched = c.get("matched_role") or c.get("actual_title_found") or c.get("job_title", "")
        if matched:
            covered_roles.add(matched)
            logger.info(f"  COVERED by '{c.get('first_name')} {c.get('last_name')}': {matched} (status={c.get('overall_status')}, emp={c.get('employment_verified')})")

    # A target role is missing only if no covered role overlaps with it (fuzzy)
    missing = []
    for target in TARGET_ROLES:
        target_lower = target.lower()
        # Split compound role bucket (e.g. "CIO / Head of IT / IT Director") into parts
        target_parts = [p.strip().lower() for p in target_lower.replace(" / ", "/").split("/")]
        is_covered = False
        for cr in covered_roles:
            cr_lower = cr.lower()
            # Check if any part of the target bucket matches the covered role
            if any(part in cr_lower or cr_lower in part for part in target_parts):
                is_covered = True
                break
            # Also check reverse: covered role parts against target string
            if cr_lower in target_lower or target_lower in cr_lower:
                is_covered = True
                break
        if not is_covered:
            missing.append(target)

    logger.info(f"Gap report: {len(covered_roles)} signals covering roles, {len(missing)} missing")
    for r in missing:
        logger.info(f"  MISSING: {r}")
    return missing, list(covered_roles)


# ── Step 3: Searcher helpers ──────────────────────────────────────────────────
def _expand_role_terms(role, country):
    prompt = f"""Generate 6-8 LinkedIn search keywords for finding someone with this role at a company in {country}:
Role: {role}

Include English title variations, abbreviations, and common Indian variants.
Examples for "Head of IT / CIO": ["CIO", "Chief Information Officer", "IT Head", "Head of IT", "VP Technology", "IT Director", "GM IT"]

Return ONLY this JSON:
{{"terms": ["term1","term2","term3","term4","term5","term6"]}}"""
    raw = call_gpt5(prompt, use_web_search=False)
    result = parse_gpt_json(raw)
    if not result:
        return [role]
    return result.get("terms", [role])


def _filter_candidates_gpt(candidates, role, company_name):
    if not candidates:
        return {"match_found": False}
    prompt = f"""I need someone filling the role "{role}" at {company_name}.

Search results:
{json.dumps(candidates[:8])}

Which person best matches? Must be at {company_name} or subsidiary. Senior level only.

Return ONLY this JSON:
{{"match_found":true,"person_index":0,"first_name":"...","last_name":"...","title":"...","linkedin_url":"...","public_identifier":"...","confidence":0.85,"reason":"..."}}"""
    raw = call_gpt5(prompt, use_web_search=False)
    result = parse_gpt_json(raw)
    if not result:
        return {"match_found": False}
    return result


def search_role_unipile(role, search_terms):
    """Search via Sales Nav → fallback to Classic."""
    keywords = " OR ".join(search_terms[:4])
    logger.info(f"    Unipile SalesNav search: keywords='{keywords}'")
    items = search_salesnav(SALES_NAV_URL, keywords=keywords, limit=10)
    if items:
        candidates = [normalize_salesnav_item(i) for i in items]
        logger.info(f"    SalesNav returned {len(candidates)} candidates")
        match = _filter_candidates_gpt(candidates, role, COMPANY_NAME)
        if match.get("match_found") and match.get("confidence", 0) >= MATCH_CONFIDENCE_THRESHOLD:
            # Fetch full profile
            idx = match.get("person_index", 0)
            identifier = candidates[idx].get("public_identifier", "") if idx < len(candidates) else ""
            if identifier:
                prof = fetch_linkedin_profile(identifier)
                fields = extract_profile_fields(prof)
                if fields.get("current_title"):
                    match["title"] = fields["current_title"]
                if fields.get("first_name"):
                    match["first_name"] = fields["first_name"]
                if fields.get("last_name"):
                    match["last_name"] = fields["last_name"]
            match["source"] = "unipile_salesnav"
            return match

    # Fallback: classic search
    logger.info(f"    SalesNav miss — trying Classic search")
    items2 = search_classic(keywords=f"{search_terms[0]} {COMPANY_NAME}", company_id=LINKEDIN_ORG_ID, limit=10)
    if items2:
        candidates2 = [normalize_classic_item(i) for i in items2]
        logger.info(f"    Classic returned {len(candidates2)} candidates")
        match2 = _filter_candidates_gpt(candidates2, role, COMPANY_NAME)
        if match2.get("match_found") and match2.get("confidence", 0) >= MATCH_CONFIDENCE_THRESHOLD:
            match2["source"] = "unipile_classic"
            return match2

    return None


def search_role_apollo(role, search_terms):
    # Apollo people search requires paid plan — skip immediately
    logger.debug(f"    Apollo: skipped (free plan only supports org enrichment)")
    return None


def search_role_clay(role, search_terms):
    # Clay REST API deprecated — skip immediately
    logger.debug(f"    Clay: skipped (REST API deprecated, MCP not callable from script)")
    return None


def search_role_gpt_web(role, search_terms):
    logger.info(f"    GPT web search for: {role}")
    prompt = f"""Find the person who currently holds the role "{role}" at Britannia Industries (India, FMCG).
Domain: britannia.co.in

Search LinkedIn, news, press releases, Glassdoor, Business Standard, Economic Times, LiveMint, and any relevant source.

Return ONLY this JSON:
{{"found":true,"first_name":"...","last_name":"...","title":"...","linkedin_url":"url or null","source":"where found","confidence":0.7}}"""
    raw = call_gpt5(prompt, use_web_search=True)
    result = parse_gpt_json(raw)
    if not result or not result.get("found"):
        return None
    if result.get("confidence", 0) >= 0.5:
        result["match_found"] = True
        result["source"] = "gpt_web"
        return result
    return None


def search_for_role(role):
    """Full waterfall for one missing role."""
    logger.info(f"\n  >> Searching for: {role}")

    # Expand role to search terms
    terms = _expand_role_terms(role, COUNTRY)
    logger.info(f"    Terms: {terms}")

    # Layer 1: Unipile
    match = search_role_unipile(role, terms)
    if match:
        logger.info(f"    FOUND via {match.get('source')}: {match.get('first_name')} {match.get('last_name')} | {match.get('title')}")
        return match

    # Layer 2: Apollo
    match = search_role_apollo(role, terms)
    if match:
        logger.info(f"    FOUND via apollo: {match.get('first_name')} {match.get('last_name')}")
        return match

    # Layer 3: Clay
    match = search_role_clay(role, terms)
    if match:
        logger.info(f"    FOUND via clay: {match.get('first_name')} {match.get('last_name')}")
        return match

    # Layer 4: GPT web search
    match = search_role_gpt_web(role, terms)
    if match:
        logger.info(f"    FOUND via gpt_web: {match.get('first_name')} {match.get('last_name')}")
        return match

    logger.info(f"    NOT FOUND — manual flag")
    return None


# ── Sheet12 writer ────────────────────────────────────────────────────────────
def contact_to_sheet12_row(contact, verified):
    """Map contact + verification result to Sheet12 columns A-U."""
    return [
        COMPANY_NAME,                                   # A: Company Name
        "Britannia Industries Limited",                 # B: Normalized Company Name
        COMPANY_DOMAIN,                                 # C: Company Domain
        ACCOUNT_TYPE,                                   # D: Account type
        "Large",                                        # E: Account Size
        COUNTRY,                                        # F: Country
        contact.get("first_name", ""),                  # G: First Name
        contact.get("last_name", ""),                   # H: Last Name
        contact.get("job_title") or contact.get("title", ""),  # I: Job Title (English)
        contact.get("matched_role") or verified.get("matched_role", ""),  # J: Buying Role
        contact.get("linkedin_url", ""),                # K: LinkedIn URL
        contact.get("email", ""),                       # L: Email
        contact.get("phone_1", ""),                     # M: Phone-1
        contact.get("phone_2", ""),                     # N: Phone-2
        contact.get("source", "searcher"),              # O: Source
        verified.get("linkedin_status", ""),            # P: LinkedIn Status
        verified.get("employment_verified", ""),        # Q: Employment Verified
        verified.get("title_match", ""),                # R: Title Match
        verified.get("actual_title_found", ""),         # S: Actual Title Found
        verified.get("overall_status", ""),             # T: Overall Status
        verified.get("verification_notes", ""),         # U: Verification Notes
    ]


# ── MAIN ──────────────────────────────────────────────────────────────────────
def run():
    service = _get_service()

    # ── PHASE 1: Verify all 42 Britannia rows ─────────────────────────────────
    logger.info(f"\n{'='*70}")
    logger.info(f"PHASE 1: Verifying rows {START_ROW}-{END_ROW} from '{TAB_SOURCE}'")
    logger.info(f"{'='*70}")

    range_name = f"'{TAB_SOURCE}'!A{START_ROW}:U{END_ROW}"
    result = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=range_name
    ).execute()
    rows = result.get("values", [])
    logger.info(f"Read {len(rows)} rows")

    verify_updates = []  # (sheet_row, gpt_result, contact_dict)
    status_counts = {"valid": 0, "invalid": 0, "no_role_match": 0, "needs_review": 0}

    for i, row in enumerate(rows):
        sheet_row = START_ROW + i
        first       = safe_get(row, 6)
        last        = safe_get(row, 7)
        job_title   = safe_get(row, 8)
        linkedin_url = safe_get(row, 10)
        email       = safe_get(row, 11)

        logger.info(f"\n[Row {sheet_row}] {first} {last} | {job_title}")

        # Unipile fetch
        username = extract_username(linkedin_url)
        unipile_profile = {}
        if username:
            raw_prof = fetch_linkedin_profile(username)
            if not raw_prof.get("_not_found"):
                unipile_profile = extract_profile_fields(raw_prof)
                logger.info(f"  Unipile: title='{unipile_profile.get('current_title')}' headline='{unipile_profile.get('headline','')[:80]}'")
            else:
                logger.info(f"  Unipile: NOT FOUND for {username}")
        else:
            logger.info(f"  Unipile: no LinkedIn URL")

        # GPT verify
        gpt = verify_contact(first, last, job_title, linkedin_url, unipile_profile, email)
        status = gpt.get("overall_status", "needs_review")
        status_counts[status] = status_counts.get(status, 0) + 1
        logger.info(f"  → {status} | emp_verified={gpt.get('employment_verified')} | title_match={gpt.get('title_match')} | {gpt.get('verification_notes','')[:100]}")

        verify_updates.append((sheet_row, gpt, {
            "first_name": first, "last_name": last,
            "job_title": job_title, "linkedin_url": linkedin_url,
            "email": email, "matched_role": gpt.get("matched_role"),
            "overall_status": status,
            "employment_verified": gpt.get("employment_verified"),
            "title_match": gpt.get("title_match"),
            "actual_title_found": gpt.get("actual_title_found", job_title),
            "headline": unipile_profile.get("headline", ""),
        }))

    # Write O-U back to First Clean List
    logger.info(f"\nWriting verification results to '{TAB_SOURCE}' columns O-U...")
    write_data = []
    for sheet_row, gpt, _ in verify_updates:
        write_data.append({
            "range": f"'{TAB_SOURCE}'!O{sheet_row}:U{sheet_row}",
            "values": [[
                "First Clean List",
                gpt.get("linkedin_status", ""),
                gpt.get("employment_verified", ""),
                gpt.get("title_match", ""),
                gpt.get("actual_title_found", ""),
                gpt.get("overall_status", ""),
                gpt.get("verification_notes", ""),
            ]],
        })
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"valueInputOption": "RAW", "data": write_data},
    ).execute()
    logger.info("Phase 1 write complete.")

    logger.info(f"\nPHASE 1 SUMMARY:")
    for k, v in status_counts.items():
        logger.info(f"  {k}: {v}")

    # ── PHASE 2: Gap report ────────────────────────────────────────────────────
    logger.info(f"\n{'='*70}")
    logger.info("PHASE 2: Gap report — finding missing roles")
    logger.info(f"{'='*70}")

    verified_contacts = [c for _, _, c in verify_updates]
    missing_roles, covered_roles = generate_gap_report(verified_contacts)
    logger.info(f"Covered roles: {covered_roles}")
    logger.info(f"Missing roles ({len(missing_roles)}): {missing_roles}")

    # ── PHASE 3: Searcher waterfall ────────────────────────────────────────────
    logger.info(f"\n{'='*70}")
    logger.info(f"PHASE 3: Searcher — finding {len(missing_roles)} missing roles")
    logger.info(f"{'='*70}")

    found_contacts = []
    manual_roles = []

    for role in missing_roles:
        match = search_for_role(role)
        if match:
            found_contacts.append({
                "first_name": match.get("first_name", ""),
                "last_name": match.get("last_name", ""),
                "title": match.get("title", ""),
                "job_title": match.get("title", ""),
                "linkedin_url": match.get("linkedin_url", ""),
                "email": match.get("email", ""),
                "phone_1": "",
                "phone_2": "",
                "source": match.get("source", "searcher"),
                "matched_role": role,
                "confidence": match.get("confidence", 0),
            })
        else:
            manual_roles.append(role)

    logger.info(f"\nPHASE 3 SUMMARY: found={len(found_contacts)} manual={len(manual_roles)}")
    for c in found_contacts:
        logger.info(f"  FOUND: {c['first_name']} {c['last_name']} | {c['title']} | via {c['source']}")
    for r in manual_roles:
        logger.info(f"  MANUAL: {r}")

    # ── PHASE 4: Verify searcher outputs ─────────────────────────────────────
    logger.info(f"\n{'='*70}")
    logger.info(f"PHASE 4: Verifying {len(found_contacts)} searcher-found contacts")
    logger.info(f"{'='*70}")

    sheet12_rows = []

    for contact in found_contacts:
        first       = contact["first_name"]
        last        = contact["last_name"]
        job_title   = contact["title"]
        linkedin_url = contact["linkedin_url"]

        logger.info(f"\n  Verifying searcher contact: {first} {last} | {job_title}")

        # Unipile fetch (if we have a LinkedIn URL)
        username = extract_username(linkedin_url)
        unipile_profile = {}
        if username:
            raw_prof = fetch_linkedin_profile(username)
            if not raw_prof.get("_not_found"):
                unipile_profile = extract_profile_fields(raw_prof)
                logger.info(f"    Unipile: headline='{unipile_profile.get('headline','')[:80]}'")

        # GPT verify
        gpt = verify_contact(first, last, job_title, linkedin_url, unipile_profile)
        logger.info(f"    → {gpt.get('overall_status')} | {gpt.get('verification_notes','')[:100]}")

        sheet12_rows.append(contact_to_sheet12_row(contact, gpt))

    # Also add manual-flag rows to Sheet12
    for role in manual_roles:
        sheet12_rows.append([
            COMPANY_NAME, "Britannia Industries Limited", COMPANY_DOMAIN,
            ACCOUNT_TYPE, "Large", COUNTRY,
            "", "", role, role,
            "", "", "", "",
            "manual_needed",
            "", "", "", "", "needs_review",
            f"No contact found for '{role}' — manual research required",
        ])

    # ── PHASE 5: Write to Sheet12 ─────────────────────────────────────────────
    logger.info(f"\n{'='*70}")
    logger.info(f"PHASE 5: Writing {len(sheet12_rows)} rows to '{TAB_OUTPUT}'")
    logger.info(f"{'='*70}")

    if sheet12_rows:
        service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=f"'{TAB_OUTPUT}'!A2",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": sheet12_rows},
        ).execute()
        logger.info(f"Wrote {len(sheet12_rows)} rows to Sheet12.")

    # ── FINAL SUMMARY ─────────────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"BRITANNIA FULL PIPELINE COMPLETE")
    print(f"{'='*70}")
    print(f"PHASE 1 — Verification of existing contacts ({START_ROW}-{END_ROW}):")
    for k, v in status_counts.items():
        print(f"  {k:15}: {v}")
    print(f"\nPHASE 2 — Gap report:")
    print(f"  Covered roles  : {len(covered_roles)}")
    print(f"  Missing roles  : {len(missing_roles)}")
    print(f"\nPHASE 3 — Searcher results:")
    print(f"  Found          : {len(found_contacts)}")
    print(f"  Manual needed  : {len(manual_roles)}")
    for r in manual_roles:
        print(f"    - {r}")
    print(f"\nSheet12 rows written: {len(sheet12_rows)}")
    print(f"{'='*70}")


if __name__ == "__main__":
    run()
