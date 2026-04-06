"""
Verifier only — rows 294-335 of 'First Clean List'.
Writes O-U back in-place. Stops after gap report. No searcher.
"""
import sys, json, logging, os
sys.path.insert(0, ".")
os.makedirs("logs", exist_ok=True)

from clients.sheets_client import _get_service
from clients.unipile_client import fetch_linkedin_profile, extract_username, extract_profile_fields
from clients.openai_client import call_gpt5
from utils.json_parser import parse_gpt_json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("logs/verify_only.log")],
)
logger = logging.getLogger(__name__)

SHEET_ID   = "1UpH1O2EtIFPM1F_Z52LRs1ov7fmQF_eSx4ukDHUXoDo"
TAB        = "First Clean List"
START_ROW  = 294
END_ROW    = 335

BRITANNIA_DOMAINS = {"britannia.co.in", "britindia.com", "britanniaind.com"}
BRITANNIA_KEYWORDS = {"britannia", "britindia"}

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
    "final_decision_makers": ["CEO","MD","President","VP","Executive Director","COO","EVP",
                               "Vice President","Managing Director","Chief Commercial Officer","CCO"],
    "key_decision_makers":   ["Sales Director","VP Sales","CIO","SVP Sales","IT Head","Head of IT"],
    "key_influencers":       ["Sales Excellence Director","Commercial Excellence Director","Field Sales Director",
                              "Chief Digital Officer","Digital Transformation Head","RTM Head","GTM Head",
                              "Sales Operations Head","Sales Operations Manager","Customer Development Head",
                              "Head of General Trade","GM IT","IT Director","Business Intelligence Head",
                              "Analytics Director","Head of Digital Commerce","eB2B Head","GTM Director",
                              "RTM Director","Head of GenAI","Head of AI","AI Director","Head of Telesales"],
    "gate_keepers":          ["Sales Automation Head","Sales Effectiveness Manager","Sales Capability Manager",
                              "Sales IT Manager","SFA Manager","Trade Marketing Head","RTM Manager","GTM Manager",
                              "Customer Development Manager","Analytics Manager","eB2B Manager","GenAI Manager"],
}


def safe_get(row, idx, default=""):
    try: return row[idx] if idx < len(row) else default
    except IndexError: return default


def verify_contact(first, last, job_title, linkedin_url, unipile_profile, email=""):
    email_domain = email.split("@")[1].lower() if "@" in email else ""
    email_is_britannia = email_domain in BRITANNIA_DOMAINS

    email_signal = ""
    if email_is_britannia:
        email_signal = f"\nEMAIL DOMAIN SIGNAL: Contact has a Britannia corporate email ({email}). This is STRONG proof of current employment. Treat as confirmed unless LinkedIn explicitly shows they left."
    elif email:
        email_signal = f"\nEmail on file: {email} (non-Britannia domain)"

    roles_list = "\n".join(f"- {r}" for r in TARGET_ROLES)
    tiers_text = (
        f"Final Decision Makers: {', '.join(ROLE_TIERS['final_decision_makers'])}\n"
        f"Key Decision Makers: {', '.join(ROLE_TIERS['key_decision_makers'])}\n"
        f"Key Influencers: {', '.join(ROLE_TIERS['key_influencers'][:10])}...\n"
        f"Gate Keepers: {', '.join(ROLE_TIERS['gate_keepers'][:6])}..."
    )

    prompt = f"""You are a B2B data verification agent for SalesCode.ai.

VERIFY THIS CONTACT at Britannia Industries (India, FMCG/CPG manufacturer):

Name: {first} {last}
Title from mapping: {job_title}
LinkedIn URL: {linkedin_url}
LinkedIn profile data (Unipile): {json.dumps(unipile_profile)}{email_signal}

NOTE: Unipile only returns basic data for 3rd-degree connections (no positions list, just headline).
Use ALL signals: headline text, email domain, mapped title.
Britannia domains: britannia.co.in, britindia.com
- If headline mentions Britannia AND email is Britannia domain → employment_verified = "yes"
- If headline clearly shows a DIFFERENT current company (e.g. "at IFFCO", "at Tata Consumer") → "no"
- Career summary headlines listing multiple past employers (e.g. "Britannia | IFFCO | Tata") → use email domain as deciding factor
- If headline contains "Britannia" in any form and title_match = yes → overall_status = "valid"

TARGET ROLES:
{roles_list}

ROLE TIERS:
{tiers_text}

CHECK:
1. LINKEDIN STATUS: "found" / "not_found"
2. EMPLOYMENT VERIFIED: "yes" / "no" / "uncertain"
3. TITLE MATCH: "yes" / "no" / "adjacent"
4. ACTUAL TITLE FOUND: current title from LinkedIn, or job_title if no data
5. OVERALL STATUS: "valid" / "invalid" / "no_role_match" / "needs_review"
6. VERIFICATION NOTES: one sentence

Return ONLY this JSON:
{{"linkedin_status":"found|not_found","employment_verified":"yes|no|uncertain","title_match":"yes|no|adjacent","actual_title_found":"title","overall_status":"valid|invalid|no_role_match|needs_review","matched_role":"matched target role or null","role_tier":"final_decision_maker|key_decision_maker|key_influencer|gate_keeper|none","verification_notes":"one sentence"}}"""

    raw = call_gpt5(prompt, use_web_search=False)
    result = parse_gpt_json(raw)
    if not result:
        logger.warning(f"  GPT parse failed | raw: {raw[:150]}")
        return {"linkedin_status":"not_found","employment_verified":"uncertain","title_match":"no",
                "actual_title_found":job_title,"overall_status":"needs_review",
                "matched_role":None,"role_tier":"none","verification_notes":"GPT parse failed"}
    return result


def _is_likely_at_britannia(c):
    if c.get("overall_status") == "valid": return True
    if c.get("employment_verified") == "yes": return True
    email = c.get("email", "")
    if "@" in email and email.split("@")[1].lower() in BRITANNIA_DOMAINS: return True
    headline = c.get("headline", "").lower()
    if any(kw in headline for kw in BRITANNIA_KEYWORDS) and c.get("title_match") in ("yes","adjacent"):
        return True
    return False


def generate_gap_report(verified_contacts):
    covered_roles = set()
    for c in verified_contacts:
        if not _is_likely_at_britannia(c): continue
        matched = c.get("matched_role") or c.get("actual_title_found") or c.get("job_title","")
        if matched:
            covered_roles.add(matched)
            logger.info(f"  COVERED: {c['first_name']} {c['last_name']} → {matched} (status={c.get('overall_status')}, emp={c.get('employment_verified')})")

    missing = []
    for target in TARGET_ROLES:
        target_lower = target.lower()
        parts = [p.strip().lower() for p in target_lower.replace(" / ", "/").split("/")]
        is_covered = any(
            any(part in cr.lower() or cr.lower() in part for part in parts)
            or cr.lower() in target_lower or target_lower in cr.lower()
            for cr in covered_roles
        )
        if not is_covered:
            missing.append(target)

    logger.info(f"\nGap report: {len(covered_roles)} covered signals, {len(missing)} missing roles")
    for r in missing:
        logger.info(f"  MISSING: {r}")
    return missing, list(covered_roles)


def run():
    service = _get_service()
    rows = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"'{TAB}'!A{START_ROW}:U{END_ROW}"
    ).execute().get("values", [])
    logger.info(f"Read {len(rows)} rows\n")

    verify_updates = []
    counts = {"valid":0,"invalid":0,"no_role_match":0,"needs_review":0}

    for i, row in enumerate(rows):
        sheet_row = START_ROW + i
        first        = safe_get(row, 6)
        last         = safe_get(row, 7)
        job_title    = safe_get(row, 8)
        linkedin_url = safe_get(row, 10)
        email        = safe_get(row, 11)

        logger.info(f"[Row {sheet_row}] {first} {last} | {job_title} | email={email}")

        username = extract_username(linkedin_url)
        unipile_profile = {}
        if username:
            raw_prof = fetch_linkedin_profile(username)
            if not raw_prof.get("_not_found"):
                unipile_profile = extract_profile_fields(raw_prof)
                logger.info(f"  Unipile: title='{unipile_profile.get('current_title')}' | headline='{unipile_profile.get('headline','')[:90]}'")
            else:
                logger.info(f"  Unipile: NOT FOUND ({username})")
        else:
            logger.info("  Unipile: no LinkedIn URL")

        gpt = verify_contact(first, last, job_title, linkedin_url, unipile_profile, email)
        status = gpt.get("overall_status","needs_review")
        counts[status] = counts.get(status, 0) + 1
        logger.info(f"  → {status} | emp={gpt.get('employment_verified')} | title={gpt.get('title_match')} | {gpt.get('verification_notes','')[:100]}\n")

        verify_updates.append((sheet_row, gpt, {
            "first_name": first, "last_name": last, "job_title": job_title,
            "linkedin_url": linkedin_url, "email": email,
            "matched_role": gpt.get("matched_role"),
            "actual_title_found": gpt.get("actual_title_found", job_title),
            "overall_status": status,
            "employment_verified": gpt.get("employment_verified"),
            "title_match": gpt.get("title_match"),
            "headline": unipile_profile.get("headline",""),
        }))

    # Write O-U back
    logger.info(f"Writing {len(verify_updates)} rows to sheet columns O-U...")
    write_data = []
    for sheet_row, gpt, _ in verify_updates:
        write_data.append({
            "range": f"'{TAB}'!O{sheet_row}:U{sheet_row}",
            "values": [[
                "First Clean List",
                gpt.get("linkedin_status",""),
                gpt.get("employment_verified",""),
                gpt.get("title_match",""),
                gpt.get("actual_title_found",""),
                gpt.get("overall_status",""),
                gpt.get("verification_notes",""),
            ]],
        })
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"valueInputOption":"RAW","data":write_data},
    ).execute()
    logger.info("Sheet updated.\n")

    # Gap report
    verified_contacts = [c for _, _, c in verify_updates]
    missing_roles, covered_roles = generate_gap_report(verified_contacts)

    # Summary
    print(f"\n{'='*65}")
    print(f"VERIFIER RESULTS — Britannia rows {START_ROW}-{END_ROW}")
    print(f"{'='*65}")
    print(f"  valid:         {counts['valid']}")
    print(f"  invalid:       {counts['invalid']}")
    print(f"  no_role_match: {counts['no_role_match']}")
    print(f"  needs_review:  {counts['needs_review']}")
    print(f"\nGAP REPORT:")
    print(f"  Covered roles ({len(covered_roles)}): {', '.join(sorted(covered_roles))}")
    print(f"\n  Missing roles ({len(missing_roles)}):")
    for r in missing_roles:
        print(f"    - {r}")
    print(f"{'='*65}")


if __name__ == "__main__":
    run()
