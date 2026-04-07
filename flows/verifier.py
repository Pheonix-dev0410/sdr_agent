import json
import logging
from pathlib import Path

from clients.openai_client import call_gpt5
from clients.unipile_client import fetch_linkedin_profile, extract_username, extract_profile_fields
from clients.zerobounce_client import verify_email as zb_verify_email, is_valid as zb_is_valid
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

1. COMPANY MATCH: Does their CURRENT role match {company_name}?

   CONFIRMED = active LinkedIn role clearly shows {company_name} (or a known subsidiary/brand).
   UNCONFIRMED ≠ DEPARTED — if LinkedIn data is empty, sparse, or outdated, that is NOT evidence
   the person left. Small companies, startups, and founders often have incomplete LinkedIn profiles.
   Set current_company_confirmed: true unless LinkedIn ACTIVELY shows a DIFFERENT company as
   their current primary role.

   Account for subsidiaries and brand names:
   - "FEMSA Comercio" = "OXXO", "Hindustan Unilever" = "Unilever India"
   - "PT Coca-Cola Amatil Indonesia" = "CCAI", "Arca Continental" includes "AC Bebidas"
   - "GEPP" = "PepsiCo Mexico bottler"

   Only set current_company_confirmed: false if LinkedIn CLEARLY shows a different company
   as their primary current employer — not just because the data is missing or ambiguous.

2. TITLE PARSING — do this BEFORE role matching:

   Titles often contain noise. Extract the core role first:

   a) COMPOUND TITLES — take the highest-seniority component:
      - "CEO & Co-Founder" → evaluate as "CEO"
      - "Co-Founder and Chief Technology Officer" → evaluate as "CTO"
      - "Co-Founder and Technology Head" → evaluate as "Head of Technology / CTO"
      - "Managing Director & Founder" → evaluate as "Managing Director"
      - Rule: "Co-Founder", "Founder", "Partner" combined with a C-suite/Director title →
        the C-suite/Director title is the primary role for matching purposes.

   b) ROLE-AT-COMPANY FORMAT — strip the company part:
      - "COO- Peter England" → evaluate as "COO" (ignore everything after "- ")
      - "COO, Sunlight Resources Ltd" → evaluate as "COO" (ignore everything after ", ")
      - "Head of Sales @ Pladis" → evaluate as "Head of Sales"
      - Pattern: if title contains " - ", " @ ", " at " followed by a company name → strip it.

   c) SCOPE/GEOGRAPHIC MODIFIERS — do not change the role type:
      - "Global Sales Director" = "Sales Director"
      - "Regional COO" = "COO"
      - "National Sales Head" = "Head of Sales"
      - "Senior Sales Director" = "Sales Director"
      - "Group CFO" = "CFO"
      - The scope prefix (Global/Regional/National/Senior/Group/Corporate) describes coverage,
        not a different role. Match against the base role.

3. ROLE MATCH — apply after title parsing above:

   ONLY set matched_role if the extracted core role is genuinely the same function AND seniority
   as something in the target list.

   SENIORITY IS NOT INTERCHANGEABLE:
   - "Manager" ≠ "Director" ≠ "Head" ≠ "VP" ≠ "Chief"
   - "Sales Manager" does NOT match "Sales Director / VP Sales / SVP Sales"
   - "IT Manager" does NOT match "Head of IT / IT Director / CIO"
   - A Manager-level role reporting to the Director we want is NOT a match

   WHAT COUNTS AS A MATCH:
   - Same title in another language (e.g. "Director de Ventas" = "Sales Director")
   - Common abbreviations (e.g. "CIO" = "Chief Information Officer" = "Head of IT / IT Director / CIO")
   - Equivalent seniority + function (e.g. "Chief Sales Officer" = "Sales Director / VP Sales")
   - Compound title after extraction (e.g. "CEO & Co-Founder" → extracted "CEO" matches "CEO / MD / President")

   WHAT IS NOT A MATCH — return matched_role: null and role_tier: "none":
   - Lower seniority (Manager when we want Director/Head/VP/Chief)
   - Adjacent function (e.g. "HR Director" does not match any sales/IT/ops target role)
   - When in doubt — return null. Do NOT force a match.

4. CROSS-REFERENCE: Is this person in the scraped company intel? Do web sources confirm or contradict?

5. STALENESS: Only flag "possibly_departed" if LinkedIn ACTIVELY shows a different company
   as the current primary role — not if data is simply missing.

6. MULTIPLE ROLES / SIDE VENTURES:
   - Senior professionals commonly hold a corporate role AND run a startup/advisory/board seat.
   - If the person holds a senior role at {company_name} AND has a side venture → still employed.
   - Signals of side venture (not primary job): "Founder", "Co-Founder", "Advisor", "Board Member",
     "Independent Consultant", "Angel Investor" — these almost never replace a corporate role.
   - When in doubt and {company_name} role appears active → keep current_company_confirmed: true.

Return ONLY this JSON:
{{"status": "valid|invalid|needs_review", "current_company_confirmed": true, "matched_role": "exact target role string from the list above, or null if no genuine match", "role_tier": "final_decision_maker|key_decision_maker|key_influencer|gate_keeper|none", "confidence": 0.85, "issues": [], "reason": "one sentence explaining the role match decision"}}"""

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

        role_tier = gpt_result.get("role_tier", "none")

        # Hard rule: if role does not match ANY target role tier → Rejected immediately.
        # No exceptions — junior/lower-level roles are never Accepted or Under Review.
        if role_tier == "none" or not gpt_result.get("matched_role"):
            status = "invalid"
            issues = gpt_result.get("issues", [])
            if "no_role_match" not in issues:
                issues = issues + ["no_role_match"]
            gpt_result["issues"] = issues
            logger.info(
                f"  Rejected (no role match): "
                f"{contact.get('first_name')} {contact.get('last_name')} | "
                f"title='{contact.get('job_title')}'"
            )

        # Step 4: ZeroBounce email verification (only if contact has an email)
        email = contact.get("email", "")
        email_status = "no_email"
        if email and "@" in email:
            try:
                zb_result = zb_verify_email(email)
                if zb_is_valid(zb_result):
                    email_status = "valid"
                else:
                    zb_stat = zb_result.get("status", "unknown").lower()
                    email_status = zb_stat  # e.g. "invalid", "catch-all", "unknown", "spamtrap"
                    # Downgrade to needs_review if valid contact but bad email
                    if status == "valid" and zb_stat in ("invalid", "spamtrap", "abuse"):
                        status = "needs_review"
                        issues = gpt_result.get("issues", []) + ["email_invalid"]
                        gpt_result["issues"] = issues
                        logger.info(
                            f"  Email invalid ({zb_stat}): {email} — downgraded to needs_review"
                        )
                logger.info(f"  ZeroBounce {email}: {email_status}")
            except Exception as e:
                logger.warning(f"ZeroBounce failed for {email}: {e}")
                email_status = "unverified"

        # Step 5: Compile
        result.update({
            "matched_role": gpt_result.get("matched_role"),
            "role_tier": role_tier,
            "title_match": "yes" if gpt_result.get("matched_role") else "no",
            "verification_status": status,
            "confidence": confidence,
            "company_confirmed": gpt_result.get("current_company_confirmed", False),
            "email_status": email_status,
            "issues": gpt_result.get("issues", []),
            "reason": gpt_result.get("reason", ""),
            "source": contact.get("source", "n8n"),
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
