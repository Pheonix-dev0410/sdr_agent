import json
import logging
from pathlib import Path

from clients.openai_client import call_gpt5, call_gpt_fast
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

Search LinkedIn, Facebook, press releases, news, business directories, in any language.

Return ONLY this JSON:
{{"still_at_company": true, "current_title": "title or unknown", "source": "where you found this", "confidence": 0.85}}"""

    raw = call_gpt_fast(prompt, use_web_search=True, temperature=0.1)
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
    """
    Programmatic gap report — no LLM call needed.
    Compares matched_role of valid contacts against the target roles list.
    People found via company intel are surfaced as potential leads for uncovered roles.
    """
    target_roles = TARGET_ROLES.get(account_type.lower(), TARGET_ROLES.get("distributor", []))

    covered_roles = list({
        c["matched_role"]
        for c in verified_contacts
        if c.get("verification_status") == "valid" and c.get("matched_role")
    })

    # A target role is "covered" if any verified valid contact matched it
    missing_roles = [r for r in target_roles if r not in covered_roles]

    coverage_pct = round(100 * len(covered_roles) / len(target_roles)) if target_roles else 0

    # Surface company-intel people as potential leads for missing roles
    potential_leads = []
    for person in company_intel.get("people_found", []):
        title = person.get("title", "").lower()
        for role in missing_roles:
            # Simple keyword overlap between intel person's title and missing role
            role_words = set(role.lower().split())
            if any(w in title for w in role_words if len(w) > 3):
                potential_leads.append({
                    "name": person.get("name", ""),
                    "title": person.get("title", ""),
                    "source": person.get("source", "company_intel"),
                    "likely_role": role,
                })
                break  # one role per person

    logger.info(
        f"Gap report: covered={len(covered_roles)} missing={len(missing_roles)} "
        f"coverage={coverage_pct}% leads_from_intel={len(potential_leads)}"
    )
    return {
        "missing_roles": missing_roles,
        "covered_roles": covered_roles,
        "coverage_percentage": coverage_pct,
        "potential_leads_from_web": potential_leads,
    }


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
    assigned_emails: set[str] = set()  # track emails already assigned in this batch

    logger.info(f"━━━ VERIFIER START: {company_name} | {len(contacts)} contacts ━━━")

    for idx, contact in enumerate(contacts, 1):
        name = f"{contact.get('first_name','')} {contact.get('last_name','')}".strip()
        logger.info(f"── [{idx}/{len(contacts)}] {name} | title='{contact.get('job_title','')}' | email='{contact.get('email','')}' | li='{contact.get('linkedin_url','')}'")
        result = dict(contact)  # copy

        # Step 1: Unipile profile fetch
        linkedin_url = contact.get("linkedin_url", "")
        username = extract_username(linkedin_url)
        unipile_profile = {}
        unipile_status = "not_found"

        if username:
            logger.info(f"   [Unipile] Fetching LinkedIn profile for '{username}'")
            raw_profile = fetch_linkedin_profile(username)
            if not raw_profile.get("_not_found"):
                unipile_profile = extract_profile_fields(raw_profile)
                unipile_status = "found"
                logger.info(f"   [Unipile] ✓ Profile found — current_title='{unipile_profile.get('current_title','')}' company='{unipile_profile.get('current_company','')}'")
            else:
                logger.info(f"   [Unipile] ✗ Profile not found for '{username}'")
        else:
            logger.info(f"   [Unipile] Skipped — no LinkedIn URL")

        # Detect empty profile: Unipile returned a response but no title/company data
        unipile_empty = (
            unipile_status == "found"
            and not unipile_profile.get("current_title")
            and not unipile_profile.get("current_company")
        )
        if unipile_empty:
            logger.info(f"   [Unipile] ⚠ Profile returned but empty (no title/company) — will trigger web verify")

        result["unipile_status"] = unipile_status

        # Step 2: GPT verification using Unipile data + company intel
        logger.info(f"   [GPT-4o] Verifying contact (unipile_status={unipile_status}, empty={unipile_empty})")
        gpt_result = _verify_contact_with_gpt(
            contact, company_name, country, account_type, unipile_profile, company_intel
        )
        logger.info(
            f"   [GPT-4o] → status={gpt_result.get('status')} "
            f"matched_role='{gpt_result.get('matched_role')}' "
            f"tier={gpt_result.get('role_tier')} "
            f"confidence={gpt_result.get('confidence')} "
            f"company_confirmed={gpt_result.get('current_company_confirmed')} "
            f"reason='{gpt_result.get('reason','')}'"
        )

        status = gpt_result.get("status", "needs_review")
        confidence = gpt_result.get("confidence", 0.0)

        # ── Fallback role match ────────────────────────────────────────────────
        # GPT sometimes returns matched_role=None for valid compound/abbreviated titles
        # (e.g. "Co-Founder & Technology Head" → should match "Head of IT / IT Director / CIO / GM IT").
        # If company is confirmed but GPT gave no match, try Python keyword matching as a rescue.
        if not gpt_result.get("matched_role") and gpt_result.get("current_company_confirmed"):
            target_roles_list = TARGET_ROLES.get(account_type.lower(), TARGET_ROLES.get("distributor", []))
            # Use the LinkedIn title if available (more reliable than input title), else input title
            title_for_match = (
                unipile_profile.get("current_title") or contact.get("job_title", "")
            ).lower()
            # Strip compound noise: take the part after "& " or "and " for co-founder combos
            for sep in [" & ", " and ", ", "]:
                if sep in title_for_match:
                    parts = [p.strip() for p in title_for_match.split(sep)]
                    # Prefer the non-founder part
                    non_founder = [p for p in parts if "founder" not in p and "partner" not in p]
                    if non_founder:
                        title_for_match = non_founder[-1]  # last non-founder segment
            for target_role in target_roles_list:
                role_keywords = [w for w in target_role.lower().replace("/", " ").split() if len(w) > 3]
                matches = sum(1 for kw in role_keywords if kw in title_for_match)
                if matches >= 2 or (len(role_keywords) == 1 and role_keywords[0] in title_for_match):
                    gpt_result["matched_role"] = target_role
                    logger.info(
                        f"   [Role fallback] ✓ Keyword match rescued: '{title_for_match}' → '{target_role}'"
                    )
                    break

        company_intel_resolved = any(
            contact.get("first_name", "").lower() in p.get("name", "").lower()
            and contact.get("last_name", "").lower() in p.get("name", "").lower()
            for p in company_intel.get("people_found", [])
        )

        # Don't trust intel if the job title itself mentions a DIFFERENT company
        # e.g. "President FMCG, RP Sanjiv Goenka Group" or "Head of AI, Accenture"
        job_title_lower = contact.get("job_title", "").lower()
        title_names_other_company = (
            company_intel_resolved
            and company_name.lower() not in job_title_lower
            and any(c in job_title_lower for c in [",", " at ", " @ ", "- "])
        )
        if title_names_other_company:
            company_intel_resolved = False
            logger.info(f"   [Intel] ⚠ Title appears to name a different company — not trusting intel, will web verify")
        elif company_intel_resolved:
            logger.info(f"   [Intel] ✓ Person found in company intel — skipping web verify")

        # Step 3: Web verify
        # Fires when:
        #   - Unipile found nothing at all (not_found)
        #   - Unipile returned an empty profile (found but blank) ← new
        #   - GPT confidence is low
        # Does NOT fire if already hard-rejected by role (waste of quota)
        needs_web_check = (
            (unipile_status == "not_found" or unipile_empty or confidence < 0.35)
            and not company_intel_resolved
            and status != "invalid"
        )

        if needs_web_check:
            reason_parts = []
            if unipile_status == "not_found": reason_parts.append("unipile=not_found")
            if unipile_empty: reason_parts.append("unipile=empty_profile")
            if confidence < 0.35: reason_parts.append(f"confidence={confidence:.2f}")
            logger.info(f"   [GPT-mini+web] Running web verification ({', '.join(reason_parts)})")
            web_result = _web_verify_contact(contact, company_name, country)
            web_confidence = web_result.get("confidence", 0.0)
            logger.info(
                f"   [GPT-mini+web] → still_at_company={web_result.get('still_at_company')} "
                f"confidence={web_confidence:.2f} source='{web_result.get('source','')}'"
            )

            if web_result.get("still_at_company") is True and web_confidence > 0.6:
                # Web confirmed — promote back to valid regardless of empty Unipile
                status = "valid"
                confidence = max(confidence, web_confidence)
                logger.info(f"   [GPT-mini+web] ✓ Web confirmed employment — promoted to valid")
            elif web_result.get("still_at_company") is False and web_confidence > 0.7:
                status = "invalid"
                confidence = max(confidence, web_confidence)
                logger.info(f"   [GPT-mini+web] ✗ Web confirmed NOT at company — invalid")
            else:
                # Web inconclusive + empty Unipile → needs_review for manual check
                if unipile_empty and status != "invalid":
                    status = "needs_review"
                    logger.info(f"   [GPT-mini+web] ~ Inconclusive + empty LinkedIn → needs_review (manual check)")

            gpt_result["web_verification"] = web_result
        else:
            logger.info(f"   [Web verify] Skipped (unipile={unipile_status}, empty={unipile_empty}, confidence={confidence:.2f}, intel={company_intel_resolved}, status={status})")

        role_tier = gpt_result.get("role_tier", "none")
        matched_role = gpt_result.get("matched_role")

        # If GPT gave a matched_role but tier=none, that's a contradiction —
        # infer the tier by checking which tier bucket the matched_role falls in.
        if matched_role and role_tier == "none":
            for tier_name, tier_roles in ROLE_TIERS.items():
                if any(matched_role.lower() in r.lower() or r.lower() in matched_role.lower() for r in tier_roles):
                    role_tier = tier_name
                    gpt_result["role_tier"] = role_tier
                    logger.info(f"   [Tier fix] Inferred tier={role_tier} for matched_role='{matched_role}'")
                    break

        # Hard rule: if role does not match ANY target role tier → Rejected immediately.
        if role_tier == "none" or not matched_role:
            status = "invalid"
            issues = gpt_result.get("issues", [])
            if "no_role_match" not in issues:
                issues = issues + ["no_role_match"]
            gpt_result["issues"] = issues
            logger.info(
                f"   [REJECTED] No role match — title='{contact.get('job_title')}' "
                f"issues={gpt_result['issues']}"
            )

        # Step 4: ZeroBounce email verification
        # C-suite tiers (final/key decision makers) are NEVER downgraded by email alone —
        # their email is tried plus all permutations before giving up, and even then
        # they stay valid with a note.
        email = contact.get("email", "")
        email_status = "no_email"
        C_SUITE_TIERS = {"final_decision_maker", "key_decision_maker"}
        is_c_suite = role_tier in C_SUITE_TIERS

        # Skip ZeroBounce entirely for non-c-suite contacts already rejected —
        # no point spending API quota on someone we're rejecting anyway
        if status == "invalid" and not is_c_suite:
            logger.info(f"   [ZeroBounce] Skipped — contact is already invalid (non-c-suite)")

        elif email and "@" in email:
            domain_part = email.split("@")[1] if "@" in email else ""
            first = contact.get("first_name", "")
            last = contact.get("last_name", "")

            def _try_zb(addr: str) -> tuple[bool, str]:
                """Returns (is_valid, status_str)."""
                try:
                    r = zb_verify_email(addr)
                    if zb_is_valid(r):
                        return True, "valid"
                    return False, r.get("status", "unknown").lower()
                except Exception as e:
                    logger.warning(f"   [ZeroBounce] ERROR for {addr}: {e}")
                    return False, "unverified"

            logger.info(f"   [ZeroBounce] Checking email: {email}")
            ok, zb_stat = _try_zb(email)

            if ok:
                if email in assigned_emails:
                    # Another contact in this batch already owns this email
                    logger.warning(f"   [ZeroBounce] ⚠ {email} already assigned to another contact — treating as invalid")
                    ok = False
                    zb_stat = "collision"
                else:
                    email_status = "valid"
                    assigned_emails.add(email)
                    logger.info(f"   [ZeroBounce] ✓ {email} → valid")

            if not ok:
                logger.info(f"   [ZeroBounce] ~ {email} → {zb_stat} — trying permutations")

                # Build all permutations using email_patterns
                from utils.email_patterns import construct_email, FALLBACK_FORMATS
                all_formats = [
                    'firstname.lastname', 'flastname', 'f.lastname', 'firstname_lastname',
                    'firstnamelastname', 'firstname', 'lastname.firstname', 'firstname-lastname',
                    'f_lastname', 'firstnamel', 'firstname.l', 'fl',
                ]
                tried = {email}
                found_email = None
                for fmt in all_formats:
                    candidate = construct_email(first, last, fmt, domain_part)
                    if candidate in tried or candidate in assigned_emails:
                        continue
                    tried.add(candidate)
                    c_ok, c_stat = _try_zb(candidate)
                    logger.info(f"   [ZeroBounce] ~ {candidate} ({fmt}) → {c_stat}")
                    if c_ok:
                        found_email = candidate
                        email_status = "valid"
                        logger.info(f"   [ZeroBounce] ✓ Found valid email via permutation: {found_email}")
                        break

                if found_email:
                    # Update the contact email to the working one
                    assigned_emails.add(found_email)
                    contact["email"] = found_email
                    result["email"] = found_email
                elif is_c_suite:
                    # C-suite: keep valid, just note the email issue
                    email_status = zb_stat
                    logger.warning(
                        f"   [ZeroBounce] All permutations failed for {name} "
                        f"— C-suite tier, keeping status={status} with email_note"
                    )
                    issues = gpt_result.get("issues", []) + ["email_unverified"]
                    gpt_result["issues"] = issues
                else:
                    # Non-c-suite: downgrade if email is definitively bad
                    email_status = zb_stat
                    if status == "valid" and zb_stat in ("invalid", "spamtrap", "abuse"):
                        status = "needs_review"
                        issues = gpt_result.get("issues", []) + ["email_invalid"]
                        gpt_result["issues"] = issues
                        logger.warning(f"   [ZeroBounce] ✗ All permutations failed → downgraded to needs_review")

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

        STATUS_ICON = {"valid": "✅", "invalid": "❌", "needs_review": "🔶"}
        logger.info(
            f"   {STATUS_ICON.get(status,'?')} FINAL: {name} → {status.upper()} "
            f"| role='{gpt_result.get('matched_role') or 'none'}' tier={role_tier} "
            f"| email={email_status} confidence={confidence:.2f}"
        )

        if status == "valid":
            valid_count += 1
        elif status == "invalid":
            invalid_count += 1
        else:
            needs_review_count += 1

        verified_contacts.append(result)

    logger.info(
        f"━━━ VERIFIER DONE: {company_name} | "
        f"✅ valid={valid_count} ❌ invalid={invalid_count} 🔶 review={needs_review_count} ━━━"
    )

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
