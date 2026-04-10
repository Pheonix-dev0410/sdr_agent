import json
import logging
import re

from clients.openai_client import call_gpt5, call_gpt_fast
from clients.unipile_client import (
    search_salesnav, search_classic,
    fetch_linkedin_profile, extract_username,
    normalize_salesnav_item, normalize_classic_item, extract_profile_fields,
)
from clients.apollo_client import search_people as apollo_search
from clients.clay_client import enrich as clay_enrich
from clients.firecrawl_client import scrape_url
from utils.json_parser import parse_gpt_json
from utils.email_patterns import construct_email, get_fallback_emails
from utils.dedup import deduplicate
from config import MATCH_CONFIDENCE_THRESHOLD

# ZeroBounce removed — emails are verified upstream
def verify_email(email: str) -> dict:
    return {"status": "unknown"}

logger = logging.getLogger(__name__)


def _expand_roles(missing_roles: list[str], country: str) -> dict[str, list[str]]:
    prompt = f"""I need to search LinkedIn for people filling these roles at a company in {country}:
{json.dumps(missing_roles)}

For EACH role, generate 8-10 search terms including:
- English title variations (formal, informal, abbreviated)
- Spanish variations (if LATAM/Spain)
- Local language variations for {country}
- Adjacent/related titles
- Common abbreviations (CTO, CFO, VP, GM, etc.)

Examples:
- "Head of IT" => ["IT Director", "CIO", "Director de TI", "Gerente de Sistemas", "Jefe de Informática", "VP Technology", "Head of Technology", "IT Manager", "Director de Tecnología", "Chief Information Officer"]
- "Head of Distribution" => ["Distribution Director", "VP Distribution", "Logistics Director", "Gerente de Distribución", "Director de Logística", "Jefe de Distribución", "Head of Logistics", "Supply Chain Director"]

Return ONLY this JSON:
{{"role_clusters": {{"exact role name": ["term1", "term2"]}}}}"""

    raw = call_gpt_fast(prompt, use_web_search=False, temperature=0.1)
    result = parse_gpt_json(raw)
    if not result:
        logger.warning("Role expansion GPT parse failed, using role names as-is")
        return {role: [role] for role in missing_roles}
    return result.get("role_clusters", {role: [role] for role in missing_roles})


def _strip_title_noise(title: str) -> str:
    """
    Remove company-name suffixes and noise from job titles before matching.
    Examples:
      "COO- Peter England"              → "COO"
      "COO, Sunlight Resources Ltd"     → "COO"
      "Head of Sales @ Pladis"          → "Head of Sales"
      "Sales Director at Coca-Cola"     → "Sales Director"
      "Global Sales Director"           → "Global Sales Director"  (scope prefix kept, handled by GPT)
    """
    import re
    # Strip " - Company", " @ Company", " at Company" patterns
    # Only strip if what follows looks like a proper noun / company name (starts with uppercase)
    title = re.sub(r'\s*[-–]\s+[A-Z][^,]*$', '', title).strip()
    title = re.sub(r'\s+[@]\s+[A-Z][^,]*$', '', title).strip()
    title = re.sub(r'\s+at\s+[A-Z][^,]*$', '', title, flags=re.IGNORECASE).strip()
    # Strip trailing ", Company Name" (comma followed by capitalised words)
    title = re.sub(r',\s+[A-Z][A-Za-z\s&.]+(?:Ltd|Inc|Corp|LLC|GmbH|Pvt|Limited|Co\.|Group)?\.?\s*$', '', title).strip()
    return title


def _filter_candidates(candidates: list[dict], current_role: str, company_name: str, country: str) -> dict:
    if not candidates:
        return {"match_found": False}

    # Pre-clean titles to remove company-name noise before sending to GPT
    cleaned = []
    for c in candidates[:10]:
        c2 = dict(c)
        c2["title"] = _strip_title_noise(c2.get("title", ""))
        cleaned.append(c2)

    prompt = f"""I'm looking for someone who CURRENTLY works at {company_name} in {country} and fills the role "{current_role}".

Here are search results:
{json.dumps(cleaned)}

Rules:
- The person MUST currently work at {company_name} (or a known subsidiary/brand of it). Reject anyone whose title shows they work at a DIFFERENT company.
- Titles in any language are fine.
- Seniority must match: want Director/Head/VP/C-suite level — not junior or Manager-level.
- Scope prefixes (Global, Regional, National, Senior) don't disqualify: "Global Sales Director" = "Sales Director".
- Compound titles: "CEO & Co-Founder" → counts as CEO. "Co-Founder and CTO" → counts as CTO.
- If no candidate clearly works at {company_name}, return match_found: false.

Return ONLY this JSON:
{{"match_found": true, "person_index": 0, "first_name": "...", "last_name": "...", "title": "...", "linkedin_url": "...", "confidence": 0.85, "reason": "..."}}"""

    raw = call_gpt_fast(prompt, use_web_search=False, temperature=0.1)
    result = parse_gpt_json(raw)
    if not result:
        return {"match_found": False}
    return result


def _layer1_unipile(
    sales_nav_url: str, company_id: str, search_terms: list[str], current_role: str, company_name: str, country: str
) -> dict | None:
    """
    Layer 1: Search LinkedIn via Unipile.
    Primary: Sales Nav URL search (already scoped to the company, highest quality).
    Fallback: Classic search filtered by company org ID.
    """
    keywords = " ".join(search_terms[:5])
    raw_items = []

    if sales_nav_url:
        raw_items = search_salesnav(sales_nav_url, keywords=keywords, limit=10)
        candidates = [normalize_salesnav_item(i) for i in raw_items]
    elif company_id:
        raw_items = search_classic(keywords=keywords, company_id=company_id, limit=10)
        candidates = [normalize_classic_item(i) for i in raw_items]

    if not candidates:
        return None

    result = _filter_candidates(candidates, current_role, company_name, country)
    if not result.get("match_found") or result.get("confidence", 0) < MATCH_CONFIDENCE_THRESHOLD:
        return None

    # Fetch full profile to confirm current title/company
    idx = result.get("person_index", 0)
    identifier = candidates[idx].get("public_identifier", "") if idx < len(candidates) else ""
    if identifier:
        profile = fetch_linkedin_profile(identifier)
        fields = extract_profile_fields(profile)
        if fields:
            result["title"] = fields.get("current_title", result.get("title", ""))
            result["first_name"] = fields.get("first_name", result.get("first_name", ""))
            result["last_name"] = fields.get("last_name", result.get("last_name", ""))

    result["source"] = "unipile"
    return result


def _layer2_apollo(
    domain: str, search_terms: list[str], current_role: str, company_name: str, country: str
) -> dict | None:
    if not domain:
        return None
    candidates = apollo_search(domain, search_terms[:5], per_page=5)
    if not candidates:
        return None
    normalized = [
        {
            "first_name": c.get("first_name", ""),
            "last_name": c.get("last_name", ""),
            "title": c.get("title", ""),
            "linkedin_url": c.get("linkedin_url", ""),
        }
        for c in candidates
    ]
    result = _filter_candidates(normalized, current_role, company_name, country)
    if result.get("match_found") and result.get("confidence", 0) >= MATCH_CONFIDENCE_THRESHOLD:
        result["source"] = "apollo"
        # Preserve extra fields from Apollo result
        idx = result.get("person_index", 0)
        if idx < len(candidates):
            result["_raw"] = candidates[idx]
        return result
    return None


def _layer3_clay(
    domain: str, search_terms: list[str], current_role: str, company_name: str, country: str
) -> dict | None:
    clay_result = clay_enrich(domain or "", company_name, current_role, search_terms[:5], country)
    if not clay_result:
        return None
    # Clay may return a list of people or a single person
    candidates = clay_result if isinstance(clay_result, list) else clay_result.get("people", [clay_result])
    if not candidates:
        return None
    normalized = [
        {
            "first_name": c.get("first_name", ""),
            "last_name": c.get("last_name", ""),
            "title": c.get("title", ""),
            "linkedin_url": c.get("linkedin_url", ""),
        }
        for c in candidates
    ]
    result = _filter_candidates(normalized, current_role, company_name, country)
    if result.get("match_found") and result.get("confidence", 0) >= MATCH_CONFIDENCE_THRESHOLD:
        result["source"] = "clay"
        return result
    return None


def _layer4_firecrawl_gpt(
    domain: str,
    search_terms: list[str],
    current_role: str,
    company_name: str,
    country: str,
    account_type: str,
    already_scraped_urls: list[str],
) -> dict | None:
    domain_hint = f"Website: {domain}" if domain else "Website unknown."
    local_terms = ", ".join(search_terms[3:6]) if len(search_terms) > 3 else ""

    prompt = f"""Find the person who handles "{current_role}" at "{company_name}" in {country}. They are a {account_type} in CPG/FMCG. {domain_hint}

This may be a small/regional company. Search EVERYWHERE:
1. LinkedIn: site:linkedin.com/in "{company_name}" "{search_terms[0] if search_terms else current_role}"
2. Facebook business page for {company_name}
3. Google Maps listing for {company_name}
4. Local job boards (Computrabajo, Naukri, JobStreet, Bayt, Indeed) - postings reveal who holds roles
5. Government/business registries in {country}
6. Local news mentioning {company_name}
7. Parent brand's distributor page
8. Industry directories, chamber of commerce listings
9. WhatsApp business directory
10. Conference/event attendee lists
11. Try in local language: {local_terms}

Return ONLY this JSON:
{{"found": true, "first_name": "...", "last_name": "...", "title": "...", "linkedin_url": "url or null", "source": "where you found them", "source_url": "URL of the page", "confidence": 0.75}}"""

    raw = call_gpt5(prompt, use_web_search=True, temperature=0.1)
    result = parse_gpt_json(raw)
    if not result or not result.get("found"):
        return None

    # Firecrawl the source URL if GPT returned one and we haven't scraped it already
    source_url = result.get("source_url", "")
    if source_url and source_url not in already_scraped_urls:
        content = scrape_url(source_url)
        if content and len(content) > 100:
            result["scraped_context"] = content[:2000]

    if result.get("confidence", 0) >= 0.5:
        result["source"] = "firecrawl"
        result["match_found"] = True
        return result
    return None


def _layer5_deep_search(
    current_role: str, company_name: str, country: str, account_type: str, search_terms: list[str]
) -> dict | None:
    prompt = f"""I MUST find someone at "{company_name}" in {country} who handles "{current_role}" or anything related.

Previous searches found nothing. Try unconventional sources:
1. Search for the company owner/founder - in small companies they often handle multiple roles
2. Search for ANY employee and look for org chart clues
3. Check if the company has posted jobs for this role (reveals the role exists)
4. Search local trade publications and industry newsletters
5. Check if any industry award lists or certification registries mention this company
6. Try variations of the company name (with/without "S.A. de C.V.", "S.A.S.", "PT", "Pvt Ltd")
7. Search for the company phone number - sometimes directories list a contact person
8. Check government procurement databases - if they've bid on contracts, the representative is listed

If you truly cannot find anyone for this exact role, look for the closest adjacent role that exists at this company. A "General Manager" at a 30-person company likely handles distribution too.

Return ONLY this JSON:
{{"found": true, "first_name": "...", "last_name": "...", "title": "...", "linkedin_url": "url or null", "source": "...", "source_url": "...", "confidence": 0.6, "is_adjacent_role": false, "adjacent_note": ""}}"""

    raw = call_gpt5(prompt, use_web_search=True, temperature=0.2)
    result = parse_gpt_json(raw)
    if not result or not result.get("found"):
        return None
    result["source"] = "gpt5_web"
    result["match_found"] = True
    return result


def _lookup_linkedin(first: str, last: str, company_name: str) -> str:
    """
    Try to find a LinkedIn URL for a person via Unipile classic search.
    Returns the URL string or empty string if not found.
    Used as a fallback when firecrawl/GPT finds a name but no LinkedIn profile.
    """
    try:
        query = f"{first} {last} {company_name}"
        results = search_classic(keywords=query, limit=5)
        if not results:
            return ""
        # Pick the first result that mentions the company name
        company_lower = company_name.lower()
        for r in results:
            item = normalize_classic_item(r)
            title = item.get("title", "").lower()
            headline = item.get("headline", "").lower()
            if company_lower in title or company_lower in headline:
                return item.get("linkedin_url", "") or item.get("profile_url", "")
        # Fallback: just return the first result's URL
        first_item = normalize_classic_item(results[0])
        return first_item.get("linkedin_url", "") or first_item.get("profile_url", "")
    except Exception as e:
        logger.debug(f"LinkedIn lookup failed for {first} {last}: {e}")
        return ""


def _is_likely_hallucinated_linkedin(url: str) -> bool:
    """
    Return True if the LinkedIn URL looks GPT-fabricated.
    Hallucinations often append a long numeric suffix: /in/name-123456789
    Real slugs are alphanumeric, may have short numeric parts (≤ 6 chars).
    """
    if not url:
        return False
    slug = url.rstrip("/").split("/in/")[-1] if "/in/" in url else ""
    if not slug:
        return True  # Not a /in/ URL at all
    # If the last hyphen-segment is a long pure-digit string (7+ digits), it's fake
    parts = slug.split("-")
    if parts and re.fullmatch(r"\d{7,}", parts[-1]):
        return True
    return False


def _build_contact_from_match(
    match: dict,
    current_role: str,
    company_context: dict,
    email_format: str,
) -> dict:
    first = match.get("first_name", "")
    last = match.get("last_name", "")
    domain = company_context.get("domain", "")
    company_name = company_context.get("company_name", "")

    # Get extra fields from raw Apollo/Clay result if available
    raw = match.get("_raw", {})

    # LinkedIn URL: use what the layer found, validate it, fall back to Unipile name search
    linkedin_url = match.get("linkedin_url") or raw.get("linkedin_url", "")

    # Reject hallucinated URLs (e.g. ankur-arora-123456789 from GPT deep search)
    if linkedin_url and _is_likely_hallucinated_linkedin(linkedin_url):
        logger.warning(f"  [LinkedIn] ⚠ Likely hallucinated URL discarded: {linkedin_url}")
        linkedin_url = ""

    # If GPT-sourced URL passes heuristic, verify it actually resolves via Unipile
    if linkedin_url and match.get("source") == "gpt5_web":
        username = extract_username(linkedin_url)
        if username:
            probe = fetch_linkedin_profile(username)
            if probe.get("_not_found"):
                logger.warning(f"  [LinkedIn] ⚠ GPT URL {linkedin_url} not found on Unipile — discarding")
                linkedin_url = ""

    if not linkedin_url and first and last and company_name:
        linkedin_url = _lookup_linkedin(first, last, company_name)
        if linkedin_url:
            logger.info(f"  LinkedIn found via name search for {first} {last}: {linkedin_url}")

    email = ""
    email_status = "unknown"
    if domain and first and last:
        primary_email = construct_email(first, last, email_format, domain)
        zb = verify_email(primary_email)
        if zb.get("status", "").lower() == "valid":
            email = primary_email
            email_status = "valid"
        else:
            for fallback in get_fallback_emails(first, last, domain):
                if fallback == primary_email:
                    continue
                zb = verify_email(fallback)
                if zb.get("status", "").lower() == "valid":
                    email = fallback
                    email_status = "valid"
                    break
            if not email:
                email = primary_email
                email_status = zb.get("status", "unknown")

    return {
        "first_name": first,
        "last_name": last,
        "job_title": match.get("title", ""),
        "matched_role": current_role,
        "linkedin_url": linkedin_url,
        "email": email,
        "email_status": email_status,
        "phone_1": raw.get("phone_numbers", [{}])[0].get("sanitized_number", "") if raw.get("phone_numbers") else "",
        "phone_2": "",
        "verification_status": "valid",
        "confidence": match.get("confidence", 0.0),
        "source": match.get("source", "unknown"),
        "issues": [],
    }


def search_gaps(
    company_context: dict,
    missing_roles: list[str],
    existing_contacts: list[dict],
    company_intel: dict,
    potential_leads: list[dict],
) -> dict:
    company_name = company_context["company_name"]
    country = company_context["country"]
    account_type = company_context.get("account_type", "distributor")
    domain = company_context.get("domain", "")
    linkedin_numeric_id = company_context.get("linkedin_numeric_id", "")
    sales_nav_url = company_context.get("sales_nav_url", "")
    email_format = company_context.get("email_format", "firstname.lastname")

    already_scraped_urls = company_intel.get("scraped_urls", [])
    new_contacts: list[dict] = []
    manual_tasks: list[dict] = []
    remaining_roles = list(missing_roles)

    # Step 0: Check potential leads from company intel
    leads_by_role: dict[str, dict] = {}
    for lead in potential_leads:
        likely_role = lead.get("likely_role", "")
        if likely_role and likely_role in remaining_roles:
            leads_by_role[likely_role] = lead

    for role, lead in leads_by_role.items():
        logger.info(f"Using company intel lead for role {role}: {lead.get('name')}")
        name_parts = lead.get("name", "").split(" ", 1)
        first = name_parts[0] if name_parts else ""
        last = name_parts[1] if len(name_parts) > 1 else ""

        # Try to get LinkedIn via Unipile name search
        li_results = search_classic(keywords=f"{lead.get('name', '')} {company_name}", limit=3)
        linkedin_url = ""
        if li_results:
            match = _filter_candidates(li_results, role, company_name, country)
            if match.get("match_found"):
                linkedin_url = match.get("linkedin_url", "")
                first = match.get("first_name", first)
                last = match.get("last_name", last)

        contact = _build_contact_from_match(
            {
                "first_name": first,
                "last_name": last,
                "title": lead.get("title", ""),
                "linkedin_url": linkedin_url,
                "confidence": 0.7,
                "source": "company_intel",
            },
            role,
            company_context,
            email_format,
        )
        new_contacts.append(contact)
        remaining_roles.remove(role)

    if not remaining_roles:
        logger.info("All gaps filled from company intel leads")
        return {
            "new_contacts": deduplicate(new_contacts, existing_contacts),
            "manual_tasks": manual_tasks,
            "total_found": len(new_contacts),
            "total_manual": 0,
        }

    # Step 1: Role expansion
    logger.info(f"━━━ SEARCHER START: {company_name} | {len(remaining_roles)} missing roles ━━━")
    logger.info(f"   [GPT-mini] Expanding roles → search terms")
    role_clusters = _expand_roles(remaining_roles, country)

    # Step 2: Waterfall for each role
    for ridx, role in enumerate(remaining_roles, 1):
        search_terms = role_clusters.get(role, [role])
        logger.info(f"── [{ridx}/{len(remaining_roles)}] Role: '{role}' | terms: {search_terms[:4]}")
        match = None

        # Layer 1: Unipile
        if linkedin_numeric_id or sales_nav_url:
            logger.info(f"   [L1 Unipile] Searching Sales Nav / classic LinkedIn")
            match = _layer1_unipile(sales_nav_url, linkedin_numeric_id, search_terms, role, company_name, country)
            if match:
                logger.info(f"   [L1 Unipile] ✓ Found: {match.get('first_name')} {match.get('last_name')} title='{match.get('title')}' confidence={match.get('confidence')}")
            else:
                logger.info(f"   [L1 Unipile] ✗ No match")
        else:
            logger.info(f"   [L1 Unipile] Skipped — no linkedin_numeric_id or sales_nav_url")

        # Layer 2: Apollo
        if not match and domain:
            logger.info(f"   [L2 Apollo] Searching domain={domain}")
            match = _layer2_apollo(domain, search_terms, role, company_name, country)
            if match:
                logger.info(f"   [L2 Apollo] ✓ Found: {match.get('first_name')} {match.get('last_name')} title='{match.get('title')}'")
            else:
                logger.info(f"   [L2 Apollo] ✗ No match")
        elif not match:
            logger.info(f"   [L2 Apollo] Skipped — no domain")

        # Layer 3: Clay
        if not match:
            logger.info(f"   [L3 Clay] Searching")
            match = _layer3_clay(domain, search_terms, role, company_name, country)
            if match:
                logger.info(f"   [L3 Clay] ✓ Found: {match.get('first_name')} {match.get('last_name')}")
            else:
                logger.info(f"   [L3 Clay] ✗ No match")

        # Layer 4: Firecrawl + GPT web search
        if not match:
            logger.info(f"   [L4 GPT-4o+web] Firecrawl web search")
            match = _layer4_firecrawl_gpt(
                domain, search_terms, role, company_name, country, account_type, already_scraped_urls
            )
            if match:
                logger.info(f"   [L4 GPT-4o+web] ✓ Found: {match.get('first_name')} {match.get('last_name')} source='{match.get('source')}' confidence={match.get('confidence')}")
            else:
                logger.info(f"   [L4 GPT-4o+web] ✗ No match")

        # Layer 5: Deep GPT web search
        if not match:
            logger.info(f"   [L5 GPT-4o+web] Deep search")
            match = _layer5_deep_search(role, company_name, country, account_type, search_terms)
            if match:
                logger.info(f"   [L5 GPT-4o+web] ✓ Found: {match.get('first_name')} {match.get('last_name')} source='{match.get('source')}' adjacent={match.get('is_adjacent_role')}")
            else:
                logger.info(f"   [L5 GPT-4o+web] ✗ No match — flagging manual")

        # Layer 6: Manual flag
        if not match:
            logger.info(f"   [L6 Manual] ⚑ Flagged for manual follow-up: '{role}'")
            manual_tasks.append({
                "role": role,
                "company": company_name,
                "country": country,
                "task": (
                    f"Find {role} at {company_name} ({country}). "
                    f"Exhausted: LinkedIn, Apollo, Clay, company website, Facebook, "
                    f"Google Maps, job boards, government registries, news, directories. "
                    f"Try: (1) Ask existing contacts for referrals. "
                    f"(2) Call company directly. "
                    f"(3) Check WhatsApp/social media DMs."
                ),
            })
            continue

        # Build contact from match
        contact = _build_contact_from_match(match, role, company_context, email_format)
        logger.info(f"   → Contact built: {contact.get('first_name')} {contact.get('last_name')} email='{contact.get('email')}' li='{contact.get('linkedin_url')}'")
        new_contacts.append(contact)

    deduped = deduplicate(new_contacts, existing_contacts)
    logger.info(f"━━━ SEARCHER DONE: found={len(deduped)} manual={len(manual_tasks)} ━━━")

    return {
        "new_contacts": deduped,
        "manual_tasks": manual_tasks,
        "total_found": len(deduped),
        "total_manual": len(manual_tasks),
    }
