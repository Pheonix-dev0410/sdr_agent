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



def resolve_role(raw_title: str, account_type: str) -> str | None:
    """
    LLM-only role resolver. Always passes the full target roles list so the LLM
    has full context to make the best semantic match regardless of language or
    title convention. Hallucination is prevented by output validation — the LLM
    answer must be an exact string match in the list, otherwise None is returned.
    """
    target_roles_list = TARGET_ROLES.get(account_type.lower(), TARGET_ROLES.get("distributor", []))
    return _llm_confirm_role(raw_title, target_roles_list)


def _llm_confirm_role(raw_title: str, shortlist: list[str]) -> str | None:
    """
    Ask the LLM to pick one role from the given shortlist for this title.
    The LLM output is validated against the shortlist — if it returns anything
    not in the list, we return None. Zero hallucination risk.
    """
    options = "\n".join(f"  {i+1}. {r}" for i, r in enumerate(shortlist))
    prompt = f"""You are a global B2B role classifier. A professional has the job title:

"{raw_title}"

Which ONE of the following roles best describes what this person does?
If none genuinely match, answer "none".

OPTIONS:
{options}

CLASSIFICATION RULES:
1. Match on FUNCTION and SENIORITY — both must align.
2. Strip geographic/scope qualifiers (Global, Regional, National, Senior, Deputy, Jr, Sr) before judging.
3. Strip company suffixes — "COO at Unilever" → evaluate "COO" only.
4. For compound titles like "Co-Founder & CTO", evaluate the non-founder part.
5. Treat acronyms and their spelled-out equivalents as identical (CTO = Chief Technology Officer, MD = Managing Director, etc.).

SENIORITY REQUIREMENT — the all-caps options are HEAD/DIRECTOR/VP/C-SUITE level roles.
Answer "none" if the title is clearly a junior or mid-level position, even if the function matches:
- Junior signals: Executive, Associate, Analyst, Coordinator, Specialist, Representative, Agent, Officer, Consultant, Intern, Trainee, Assistant
- Examples that must return "none": "Trade Marketing Executive", "Telesales Agent", "Sales Operations Analyst", "IT Coordinator", "Digital Commerce Specialist"
- A "Manager" title is borderline — only accept if the option explicitly includes "Manager" (e.g. "Sales Operations Manager") AND the title is a functional head with no reports above them in that function.

MULTILINGUAL — titles in any language are valid; reason by function and seniority regardless of language:
- CEO/MD equivalents: Director General, Gerente General (ES), Directeur Général (FR), Geschäftsführer (DE), Direktur Utama (ID)
- Sales Director equivalents: Director Comercial/de Ventas (ES), Directeur Commercial (FR), Vertriebsleiter (DE), Direktur Penjualan (ID)
- Head of IT equivalents: Director de TI/Tecnología (ES), Directeur Informatique (FR), Kepala IT (ID)
- COO equivalents: Director de Operaciones (ES), Directeur des Opérations (FR), Direktur Operasional (ID)

WHEN TO ANSWER "none":
- Wrong department (HR, Finance, Legal, Procurement, Brand Marketing, PR, R&D, Supply Chain) with no overlap to any listed option
- Title too vague to assign confidently (bare "Manager" or "Director" with no function)
- Seniority clearly below Head/Director/VP level (see above)

Reply with ONLY the exact text of your chosen option, or the word "none"."""

    raw = call_gpt_fast(prompt, use_web_search=False, temperature=0.0)
    answer = raw.strip().strip('"').strip("'")

    # Validate: must be an exact match from the shortlist
    for role in shortlist:
        if answer.lower() == role.lower():
            return role

    # LLM said "none" or returned something not in the list
    return None




def _verify_contact_with_gpt(
    contact: dict,
    company_name: str,
    country: str,
    unipile_profile: dict,
    company_intel: dict,
) -> dict:
    """
    GPT's ONLY job here: confirm whether this person currently works at {company_name}.
    Role matching is handled entirely by resolve_role() — we do NOT ask GPT about roles.
    """
    prompt = f"""You are verifying whether a person currently works at a specific company.

PERSON: {contact.get('first_name', '')} {contact.get('last_name', '')}
INPUT TITLE: {contact.get('job_title', '')}
TARGET COMPANY: {company_name} ({country})

LINKEDIN DATA (from Unipile — may be incomplete for small companies/startups):
{json.dumps(unipile_profile)}

ADDITIONAL EVIDENCE (web-scraped intel about {company_name}):
People confirmed at this company from web sources:
{json.dumps(company_intel.get('people_found', [])[:15])}

Relevant content:
{company_intel.get('combined_text', '')[:2000]}

YOUR ONLY TASK — answer: does this person currently work at {company_name}?

RULES:
1. current_company_confirmed = true if:
   - LinkedIn shows {company_name} (or a known subsidiary/parent/brand) as current employer, OR
   - This person appears in the company intel above with a matching name, OR
   - LinkedIn data is empty/sparse but nothing contradicts {company_name} employment

2. current_company_confirmed = false ONLY if:
   - LinkedIn CLEARLY shows a DIFFERENT company as their primary current role
   - "Ex-" prefix on the {company_name} role
   - Web sources explicitly say they left {company_name}

3. Treat empty LinkedIn as inconclusive (NOT as evidence they left).
   Startups and small companies often have incomplete profiles.

4. Side ventures (Founder/Advisor/Board) do NOT override a corporate role — someone can
   hold both. Only mark false if the corporate role is clearly at a different company.

5. status should be:
   - "valid" if company confirmed and no red flags
   - "needs_review" if inconclusive (empty LinkedIn, ambiguous intel)
   - "invalid" if clearly at a different company

Return ONLY this JSON:
{{"status": "valid|needs_review|invalid", "current_company_confirmed": true, "confidence": 0.85, "issues": [], "reason": "one sentence"}}"""

    raw = call_gpt5(prompt, use_web_search=False, temperature=0.1)
    result = parse_gpt_json(raw)
    if not result:
        logger.warning(f"GPT verification failed to parse for {contact.get('first_name')} {contact.get('last_name')}")
        return {"status": "needs_review", "current_company_confirmed": False, "confidence": 0.0, "issues": ["gpt_parse_failed"], "reason": "Could not parse GPT response"}
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

        # ── Step 2a: Deterministic role resolution (no LLM) ──────────────────
        # Try both the LinkedIn title (ground truth) and the input title.
        # resolve_role uses regex patterns — never hallucinates.
        li_title   = unipile_profile.get("current_title", "")
        input_title = contact.get("job_title", "")
        python_role = resolve_role(li_title, account_type) or resolve_role(input_title, account_type)
        if python_role:
            logger.info(f"   [Role] ✓ Resolved deterministically: '{li_title or input_title}' → '{python_role}'")
        else:
            logger.info(f"   [Role] ~ No deterministic match for '{li_title or input_title}' — GPT will decide")

        # ── Step 2b: GPT — company confirmation only ──────────────────────────
        # GPT's job is ONLY to confirm whether this person currently works at {company_name}.
        # Role matching is handled by Python above; we pass python_role to GPT for context
        # but do NOT ask GPT to match roles.
        logger.info(f"   [GPT-4o] Verifying contact (unipile_status={unipile_status}, empty={unipile_empty})")
        gpt_result = _verify_contact_with_gpt(
            contact, company_name, country, unipile_profile, company_intel
        )
        logger.info(
            f"   [GPT-4o] → status={gpt_result.get('status')} "
            f"company_confirmed={gpt_result.get('current_company_confirmed')} "
            f"confidence={gpt_result.get('confidence')} "
            f"reason='{gpt_result.get('reason','')}'"
        )

        # Role always comes from Python resolver — ignore whatever GPT returned for matched_role
        gpt_result["matched_role"] = python_role

        status = gpt_result.get("status", "needs_review")
        confidence = gpt_result.get("confidence", 0.0)

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

        # Skip ZeroBounce if web verify confirmed the person is NOT at the company —
        # no point verifying an email for someone who doesn't work there
        web_confirmed_departed = (
            gpt_result.get("web_verification", {}).get("still_at_company") is False
            and gpt_result.get("web_verification", {}).get("confidence", 0) > 0.7
        )
        # Also skip for non-c-suite contacts already rejected for any reason
        if web_confirmed_departed:
            logger.info(f"   [ZeroBounce] Skipped — web confirmed not at company")
        elif status == "invalid" and not is_c_suite:
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
