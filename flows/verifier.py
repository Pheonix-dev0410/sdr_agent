import json
import logging
from pathlib import Path

from clients.openai_client import call_gpt5
from clients.unipile_client import fetch_linkedin_profile, extract_username, extract_profile_fields
from utils.json_parser import parse_gpt_json

logger = logging.getLogger(__name__)

# Load target roles
_roles_path = Path(__file__).parent.parent / "data" / "target_roles.json"
with open(_roles_path) as f:
    _roles_data: dict = json.load(f)

TARGET_ROLES: dict[str, list[str]] = {k: v for k, v in _roles_data.items() if not k.startswith("_")}
ROLE_TIERS: dict[str, list[str]] = _roles_data.get("_role_tiers", {})




def _verify_contact_with_gpt(
    contact: dict,
    company_name: str,
    country: str,
    account_type: str,
    unipile_profile: dict,
    company_intel: dict,
) -> dict:
    target_roles = TARGET_ROLES.get(account_type.lower(), TARGET_ROLES.get("distributor", []))
    roles_list = "\n".join(f"- {r}" for r in target_roles)

    tiers_text = f"""
ROLE PRIORITY TIERS (highest to lowest value):
1. Final Decision Makers: {", ".join(ROLE_TIERS.get("final_decision_makers", []))}
2. Key Decision Makers: {", ".join(ROLE_TIERS.get("key_decision_makers", []))}
3. Key Influencers: {", ".join(ROLE_TIERS.get("key_influencers", [])[:10])}... (see full list in target roles)
4. Gate Keepers: {", ".join(ROLE_TIERS.get("gate_keepers", [])[:8])}...
"""

    prompt = f"""You are a B2B data verification agent for a CPG/FMCG sales technology company (SalesCode.ai).

VERIFY THIS CONTACT:

Company we mapped: {company_name} ({country})
Contact from our mapping: {contact.get('first_name', '')} {contact.get('last_name', '')}, Title: {contact.get('job_title', '')}
LinkedIn profile data (from Unipile): {json.dumps(unipile_profile)}

ADDITIONAL COMPANY INTEL (scraped from web sources):
People found at this company from various web sources:
{json.dumps(company_intel.get('people_found', []))}

Relevant scraped content snippets:
{company_intel.get('combined_text', '')[:3000]}

OUR TARGET ROLE LIST for {account_type}:
{roles_list}

{tiers_text}

CHECK:

1. COMPANY MATCH: Does their CURRENT role match {company_name}? Account for subsidiaries, brand names, local entity names:
   - "FEMSA Comercio" = "OXXO"
   - "Hindustan Unilever" = "Unilever India"
   - "PT Coca-Cola Amatil Indonesia" = "CCAI"
   - "Arca Continental" includes "AC Bebidas", "Tonicorp", "Lindley"
   - "GEPP" = "PepsiCo Mexico bottler"
   - Bepensa operates "Bepensa Bebidas", "Bepensa Industrial", etc.
   If most recent role is at a DIFFERENT company, they likely left.

2. ROLE RELEVANCE: Does their title match any target role? Consider titles in ANY language:
   - Spanish: "Director de Ventas" = "Sales Director", "Gerente de TI" = "IT Manager", "Jefe de Operaciones" = "Head of Operations"
   - Indonesian: "Kepala IT" = "IT Head", "Direktur Penjualan" = "Sales Director"
   - Arabic: "مدير المبيعات" = "Sales Director", "مدير تقنية المعلومات" = "IT Director"
   - Hindi: "बिक्री निदेशक" = "Sales Director"
   - Thai: "ผู้อำนวยการฝ่ายขาย" = "Sales Director"
   Return the matched ENGLISH role AND the tier it belongs to.

3. CROSS-REFERENCE: Is this person mentioned in the scraped company intel? Do web sources confirm or contradict?

4. STALENESS: If they recently started at a DIFFERENT company, flag "possibly_departed".

5. SENIORITY: Entry-level/intern/junior? Flag if yes.

6. MULTIPLE ROLES: Concurrent roles at multiple companies? Note primary.

Return ONLY this JSON:
{{"status": "valid|invalid|needs_review", "current_company_confirmed": true, "matched_role": "English role name or null", "role_tier": "final_decision_maker|key_decision_maker|key_influencer|gate_keeper|none", "confidence": 0.85, "issues": [], "reason": "one sentence"}}"""

    raw = call_gpt5(prompt, use_web_search=False, temperature=0.1)
    result = parse_gpt_json(raw)
    if not result:
        logger.warning(f"GPT verification failed to parse for {contact.get('first_name')} {contact.get('last_name')}")
        return {"status": "needs_review", "current_company_confirmed": False, "matched_role": None, "confidence": 0.0, "issues": ["gpt_parse_failed"], "reason": "Could not parse GPT response"}
    return result


def _web_verify_contact(contact: dict, company_name: str, country: str) -> dict:
    prompt = f"""Does {contact.get('first_name', '')} {contact.get('last_name', '')} currently work at {company_name} in {country}? What is their current role?

Search everywhere: LinkedIn, Facebook, press releases, news, industry events, business directories, government filings, social media, Google Maps, job boards, any source in any language.

Return ONLY this JSON:
{{"still_at_company": true, "current_title": "title or unknown", "source": "where you found this", "confidence": 0.85}}"""

    raw = call_gpt5(prompt, use_web_search=True, temperature=0.1)
    result = parse_gpt_json(raw)
    if not result:
        return {"still_at_company": None, "current_title": "unknown", "source": "none", "confidence": 0.0}
    return result


def _generate_gap_report(
    verified_contacts: list[dict],
    company_name: str,
    country: str,
    account_type: str,
    company_intel: dict,
) -> dict:
    target_roles = TARGET_ROLES.get(account_type.lower(), TARGET_ROLES.get("distributor", []))
    roles_list = "\n".join(f"- {r}" for r in target_roles)

    contact_list = [
        {"name": f"{c.get('first_name', '')} {c.get('last_name', '')}", "role": c.get("matched_role", c.get("job_title", ""))}
        for c in verified_contacts
        if c.get("verification_status") == "valid"
    ]

    prompt = f"""Given these VERIFIED contacts at {company_name} ({country}):
{json.dumps(contact_list)}

And the TARGET ROLE LIST for a {account_type}:
{roles_list}

Also, the following people were found on external web sources during company research but were NOT in the n8n contact list:
{json.dumps(company_intel.get('people_found', []))}

Which target roles are NOT covered? Consider both the verified contacts AND the people found from web sources. If a person from web sources fills a gap, note them as a potential lead.

Return ONLY this JSON:
{{"missing_roles": ["role1"], "covered_roles": ["role2"], "coverage_percentage": 75, "potential_leads_from_web": [{{"name": "...", "title": "...", "source": "...", "likely_role": "..."}}]}}"""

    raw = call_gpt5(prompt, use_web_search=False, temperature=0.1)
    result = parse_gpt_json(raw)
    if not result:
        logger.warning("Gap report GPT parse failed")
        return {"missing_roles": [], "covered_roles": [], "coverage_percentage": 0, "potential_leads_from_web": []}
    return result


def verify_contacts(
    company_context: dict,
    contacts: list[dict],
    company_intel: dict,
) -> dict:
    company_name = company_context["company_name"]
    country = company_context["country"]
    account_type = company_context.get("account_type", "distributor")

    verified_contacts = []
    valid_count = 0
    invalid_count = 0
    needs_review_count = 0

    for contact in contacts:
        logger.info(f"Verifying {contact.get('first_name')} {contact.get('last_name')} at {company_name}")
        result = dict(contact)  # copy

        # Step 1: Unipile profile fetch
        linkedin_url = contact.get("linkedin_url", "")
        username = extract_username(linkedin_url)
        unipile_profile = {}
        unipile_status = "not_found"

        if username:
            raw_profile = fetch_linkedin_profile(username)
            if not raw_profile.get("_not_found"):
                unipile_profile = extract_profile_fields(raw_profile)
                unipile_status = "found"
                logger.info(f"Unipile profile found for {username}")
            else:
                logger.info(f"Unipile profile not found for {username}")

        result["unipile_status"] = unipile_status

        # Step 2: GPT verification using Unipile data + company intel
        gpt_result = _verify_contact_with_gpt(
            contact, company_name, country, account_type, unipile_profile, company_intel
        )

        status = gpt_result.get("status", "needs_review")
        confidence = gpt_result.get("confidence", 0.0)
        company_intel_resolved = any(
            contact.get("first_name", "").lower() in p.get("name", "").lower()
            and contact.get("last_name", "").lower() in p.get("name", "").lower()
            for p in company_intel.get("people_found", [])
        )

        # Step 3: Conditional deep web verification
        needs_web_check = (
            (unipile_status == "not_found" or confidence < 0.5 or status == "needs_review")
            and not company_intel_resolved
        )

        if needs_web_check:
            logger.info(f"Running web verification for {contact.get('first_name')} {contact.get('last_name')}")
            web_result = _web_verify_contact(contact, company_name, country)
            web_confidence = web_result.get("confidence", 0.0)

            if web_result.get("still_at_company") is True and web_confidence > 0.6:
                status = "valid"
                confidence = max(confidence, web_confidence)
            elif web_result.get("still_at_company") is False and web_confidence > 0.7:
                status = "invalid"
                confidence = max(confidence, web_confidence)

            gpt_result["web_verification"] = web_result

        # Step 4: Compile (emails already verified upstream — skip ZeroBounce)
        result.update({
            "matched_role": gpt_result.get("matched_role"),
            "role_tier": gpt_result.get("role_tier", "none"),
            "verification_status": status,
            "confidence": confidence,
            "company_confirmed": gpt_result.get("current_company_confirmed", False),
            "email_status": "pre-verified",
            "issues": gpt_result.get("issues", []),
            "source": "n8n",
        })

        if status == "valid":
            valid_count += 1
        elif status == "invalid":
            invalid_count += 1
        else:
            needs_review_count += 1

        verified_contacts.append(result)

    # Step 6: Gap report
    gap_report = _generate_gap_report(
        verified_contacts, company_name, country, account_type, company_intel
    )

    return {
        "verified_contacts": verified_contacts,
        "valid_count": valid_count,
        "invalid_count": invalid_count,
        "needs_review_count": needs_review_count,
        "gap_report": gap_report,
    }
