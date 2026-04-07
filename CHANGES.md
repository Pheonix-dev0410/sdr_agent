# Workflow Changes

## n8n Integration (main.py rewrite)

- Added `POST /api/trigger` — sends company metadata to n8n webhook, stores context locally
- Added `POST /api/n8n/contacts` — receives employee contacts back from n8n (accepts any JSON shape)
- Contacts are buffered per company; 180-second silence timer resets on every new batch
- After silence, auto-flush runs the full pipeline per company sequentially
- Added `_normalize_contact()` — maps any n8n field names to standard internal fields
- Added `_infer_meta_from_contacts()` — extracts company context from contacts if trigger was never called
- Added `submit_to_n8n()` — async HTTP POST to n8n with 3-retry / 30s backoff
- Added `_company_metadata` store — holds trigger context so it's available when contacts arrive
- Pipeline now runs in `run_in_executor` (thread pool) so it never blocks the async event loop

## Pipeline Steps (per company, inside flush)

1. Company intel scrape — Firecrawl + GPT web search for leadership/team pages
2. Verify n8n contacts — Unipile LinkedIn fetch + GPT cross-check → gap report
3. Searcher waterfall for missing roles — Unipile SalesNav → Apollo → Clay → GPT web → manual flag
4. Deduplicate — searcher-found contacts vs already-verified n8n contacts
5. Write all results to Google Sheets (verified + new + manual tasks)

## Management Endpoints Added

- `POST /api/n8n/flush` — immediately flush buffer without waiting 180s
- `GET /api/n8n/buffer` — see what contacts are waiting
- `GET /api/n8n/pipeline` — live step-by-step progress per company (poll from frontend)
- `GET /api/n8n/debug` — shows last received batch and field mapping (debug n8n mismatches)
- `GET /api/n8n/companies` — list triggered companies and their stored metadata

## Config / Dependencies

- `config.py` — added `N8N_WEBHOOK_URL` and `N8N_SUBMISSION_DELAY` from `.env`
- `requirements.txt` — added `httpx` for async HTTP calls to n8n

## Bug Fixes

- All `logging.StreamHandler()` calls replaced with UTF-8 wrapped stream — fixes `UnicodeEncodeError` on Windows for arrow characters (`→`) in log messages
- Applies to: `main.py`, `run_britannia_full.py`, `run_verify_only.py`, `run_verify_britannia.py`, `run_verify_britannia_test3.py`

## Backward Compatibility

- `POST /webhook/verify-and-search` kept as legacy endpoint (contacts in payload, no n8n buffering)
