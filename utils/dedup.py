def normalize_linkedin_url(url: str) -> str:
    if not url:
        return ''
    return url.lower().rstrip('/').split('?')[0]


def deduplicate(new_contacts: list[dict], existing_contacts: list[dict]) -> list[dict]:
    existing_urls = {
        normalize_linkedin_url(c.get('linkedin_url', ''))
        for c in existing_contacts
        if c.get('linkedin_url')
    }

    deduped = []
    seen = set()

    for c in new_contacts:
        url = normalize_linkedin_url(c.get('linkedin_url', ''))
        if url and (url in existing_urls or url in seen):
            continue
        if url:
            seen.add(url)
        deduped.append(c)

    return deduped
