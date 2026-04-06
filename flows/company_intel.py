import json
import logging
from clients.firecrawl_client import scrape_url
from clients.openai_client import call_gpt5
from utils.json_parser import parse_gpt_json

logger = logging.getLogger(__name__)

# Module-level cache keyed by company_name
_cache: dict[str, dict] = {}

WEBSITE_PATHS = [
    '/', '/about', '/about-us', '/team', '/leadership', '/our-team',
    '/nosotros', '/quienes-somos', '/equipo', '/nuestro-equipo',
    '/sobre-nosotros', '/directivos', '/gobierno-corporativo',
    '/contact', '/contacto', '/investors', '/investor-relations',
    '/governance', '/corporate-governance',
    '/tentang-kami', '/tim-kami',
]

NEWS_PATHS = ['/news', '/press', '/noticias', '/press-releases', '/media', '/blog']


def _scrape_website(domain: str) -> dict[str, str]:
    scraped = {}
    base = domain.rstrip('/')
    if not base.startswith('http'):
        base = f'https://{base}'

    for path in WEBSITE_PATHS:
        url = base + path
        content = scrape_url(url)
        if content and len(content) > 100:
            scraped[f'website:{path}'] = content
            logger.info(f"Scraped {url} ({len(content)} chars)")

    # Try news paths — stop after first hit
    for path in NEWS_PATHS:
        url = base + path
        content = scrape_url(url)
        if content and len(content) > 100:
            scraped[f'website:{path}'] = content
            logger.info(f"Scraped news page {url} ({len(content)} chars)")
            break

    return scraped


def _gpt_find_external_sources(
    company_name: str, domain: str, country: str, account_type: str
) -> dict:
    domain_hint = f"Their website is {domain}" if domain else "Their website is unknown."
    prompt = f"""Search the web extensively for information about the company "{company_name}" in {country}. They are a {account_type} in the CPG/FMCG industry. {domain_hint}

Find ALL available pages that mention this company's people, leadership, team members, directors, owners, or employees. This may be a small/regional company, so search broadly. Search in both English and the local language of {country}.

Search strategies (try ALL of these):
1. "{company_name}" + "director" OR "gerente" OR "manager" OR "jefe" OR "owner"
2. "{company_name}" + "{country}" + "equipo" OR "team" OR "leadership"
3. "{company_name}" on Facebook (business pages often list team and roles)
4. "{company_name}" on Google Maps (sometimes shows owner/manager name)
5. "{company_name}" in government/tax/business registries for {country}
6. "{company_name}" in local news, trade publications, or industry events
7. "{company_name}" on local job boards (Computrabajo, Naukri, JobStreet, Bayt) - job postings reveal org structure and who holds which role
8. "{company_name}" on their parent brand's distributor/partner page
9. "{company_name}" in trade association or chamber of commerce directories
10. "{company_name}" in any industry directory
11. "{company_name}" annual report or investor page

Return ONLY this JSON:
{{
  "pages_found": [
    {{"url": "https://...", "description": "what this page contains", "likely_has_people": true}}
  ],
  "people_mentioned_directly": [
    {{"name": "Full Name", "title": "Their Title/Role", "source": "where you saw this"}}
  ]
}}"""

    raw = call_gpt5(prompt, use_web_search=True, temperature=0.2)
    result = parse_gpt_json(raw)
    if not result:
        logger.warning("GPT external sources search returned unparseable response")
        return {"pages_found": [], "people_mentioned_directly": []}
    return result


def _firecrawl_external_pages(pages_found: list[dict]) -> dict[str, str]:
    scraped = {}
    for page in pages_found:
        if not page.get("likely_has_people"):
            continue
        url = page.get("url", "")
        if not url:
            continue
        content = scrape_url(url)
        if content and len(content) > 100:
            scraped[f'external:{url}'] = content
            logger.info(f"Scraped external page {url} ({len(content)} chars)")
    return scraped


def _extract_people(company_name: str, country: str, combined_content: str) -> list[dict]:
    prompt = f"""I have scraped multiple web pages about the company "{company_name}" in {country}.

Here is all the content:

{combined_content}

Extract EVERY person mentioned with their title/role at this company. Include:
- People from team/about/leadership pages
- People from news articles (especially appointments/promotions)
- People from Facebook pages
- People from government/business registries (listed as directors, partners, representatives)
- People from job postings (e.g., "report to the Distribution Manager" tells us that role exists)
- People from event/conference attendee lists
- People from any other source

Return ONLY this JSON:
{{"people": [{{"name": "Full Name", "title": "Their Title/Role", "source": "which URL or source"}}]}}"""

    raw = call_gpt5(prompt, use_web_search=False, temperature=0.1)
    result = parse_gpt_json(raw)
    if not result:
        logger.warning("GPT people extraction returned unparseable response")
        return []
    return result.get("people", [])


def scrape_company_intel(
    company_name: str,
    domain: str,
    country: str,
    account_type: str,
) -> dict:
    if company_name in _cache:
        logger.info(f"Using cached company intel for {company_name}")
        return _cache[company_name]

    logger.info(f"Starting company intel scrape for {company_name} ({country})")
    scraped_content: dict[str, str] = {}

    # Step 1: scrape company website
    if domain:
        website_content = _scrape_website(domain)
        scraped_content.update(website_content)
        logger.info(f"Website scrape: {len(website_content)} pages with content")
    else:
        logger.info("No domain provided, skipping website scrape")

    # Step 2: GPT web search for external sources
    gpt_result = _gpt_find_external_sources(company_name, domain, country, account_type)
    pages_found = gpt_result.get("pages_found", [])
    people_mentioned_directly = gpt_result.get("people_mentioned_directly", [])
    logger.info(f"GPT found {len(pages_found)} external pages, {len(people_mentioned_directly)} direct people mentions")

    # Step 3: Firecrawl external pages
    external_content = _firecrawl_external_pages(pages_found)
    scraped_content.update(external_content)

    # Step 4: Extract all people from all scraped content
    combined_text = "\n\n".join(
        f"=== {source} ===\n{content}"
        for source, content in scraped_content.items()
    )

    people_found = _extract_people(company_name, country, combined_text[:15000])
    # Merge with directly mentioned people (dedup by name)
    existing_names = {p["name"].lower() for p in people_found}
    for p in people_mentioned_directly:
        if p.get("name", "").lower() not in existing_names:
            people_found.append(p)

    result = {
        "scraped_content": scraped_content,
        "people_found": people_found,
        "combined_text": combined_text,
        "scraped_urls": list(scraped_content.keys()),
    }

    _cache[company_name] = result
    logger.info(f"Company intel complete for {company_name}: {len(scraped_content)} pages, {len(people_found)} people found")
    return result


def get_cached_intel(company_name: str) -> dict | None:
    return _cache.get(company_name)
