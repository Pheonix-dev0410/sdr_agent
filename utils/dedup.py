import re


def normalize_linkedin_url(url: str) -> str:
    if not url:
        return ''
    return url.lower().rstrip('/').split('?')[0]


def normalize_name(first: str, last: str) -> str:
    """Lowercase, strip whitespace, collapse internal spaces."""
    f = re.sub(r'\s+', ' ', first.strip().lower())
    l = re.sub(r'\s+', ' ', last.strip().lower())
    return f"{f} {l}".strip()


def deduplicate(new_contacts: list[dict], existing_contacts: list[dict]) -> list[dict]:
    """
    Remove contacts from new_contacts that already exist in existing_contacts,
    and also remove duplicates within new_contacts itself.

    Primary key:   LinkedIn URL (normalized)
    Secondary key: full name (first + last, lowercased) — used when no LinkedIn URL

    This prevents the same person appearing multiple times when the searcher
    finds them for different roles or different layers (firecrawl, gpt5_web, etc.)
    return the same person with different email formats.
    """
    # Keys from already-verified contacts
    existing_urls: set[str] = set()
    existing_names: set[str] = set()

    for c in existing_contacts:
        url = normalize_linkedin_url(c.get('linkedin_url', ''))
        if url:
            existing_urls.add(url)
        name = normalize_name(c.get('first_name', ''), c.get('last_name', ''))
        if name.strip():
            existing_names.add(name)

    deduped: list[dict] = []
    seen_urls: set[str] = set()
    seen_names: set[str] = set()

    for c in new_contacts:
        url = normalize_linkedin_url(c.get('linkedin_url', ''))
        name = normalize_name(c.get('first_name', ''), c.get('last_name', ''))

        # Skip if already in existing verified contacts
        if url and url in existing_urls:
            continue
        if name and name in existing_names:
            continue

        # Skip duplicates within this batch
        if url and url in seen_urls:
            continue
        if name and name in seen_names:
            continue

        if url:
            seen_urls.add(url)
        if name:
            seen_names.add(name)

        deduped.append(c)

    return deduped
