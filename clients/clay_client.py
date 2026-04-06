import logging

logger = logging.getLogger(__name__)

# Clay's v1 REST API (find-contacts endpoint) is deprecated.
# Clay search is available via the Clay MCP server in Claude's context,
# but not callable from Python scripts directly.
# The searcher waterfall skips this layer and falls through to GPT web search.


def enrich(
    domain: str,
    company_name: str,
    target_title: str,
    search_terms: list[str],
    country: str,
) -> dict:
    logger.debug(f"Clay REST API deprecated — skipping for {company_name} / {target_title}")
    return {}
