import json
import logging
import re
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel

from config import OUTPUT_SHEET_ID
from flows.company_intel import scrape_company_intel
from flows.verifier import verify_contacts
from flows.searcher import search_gaps
from clients.sheets_client import write_rows, contact_to_row, SHEET_HEADERS
from utils.dedup import deduplicate

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline.log"),
    ],
)
logger = logging.getLogger(__name__)


# --- Pydantic models ---
class Contact(BaseModel):
    first_name: str = ""
    last_name: str = ""
    job_title: str = ""
    linkedin_url: str = ""
    email: str = ""
    phone_1: str = ""
    phone_2: str = ""


class WebhookPayload(BaseModel):
    company_name: str
    country: str
    domain: str = ""
    account_type: str = "distributor"
    linkedin_numeric_id: str = ""
    email_format: str = "firstname.lastname"
    sales_nav_url: str = ""
    contacts: list[Contact] = []


# --- LinkedIn numeric ID extraction from Sales Nav URL ---
def _parse_linkedin_id_from_url(url: str) -> str:
    if not url:
        return ""
    m = re.search(r'/sales/company/(\d+)', url)
    return m.group(1) if m else ""


# --- Main pipeline ---
def run_pipeline(payload: dict) -> None:
    company_name = payload["company_name"]
    country = payload["country"]
    domain = payload.get("domain", "")
    account_type = payload.get("account_type", "distributor")
    email_format = payload.get("email_format", "firstname.lastname") or "firstname.lastname"
    contacts = payload.get("contacts", [])

    # Resolve linkedin_numeric_id
    linkedin_numeric_id = payload.get("linkedin_numeric_id", "")
    if not linkedin_numeric_id:
        linkedin_numeric_id = _parse_linkedin_id_from_url(payload.get("sales_nav_url", ""))

    company_context = {
        "company_name": company_name,
        "country": country,
        "domain": domain,
        "account_type": account_type,
        "linkedin_numeric_id": linkedin_numeric_id,
        "email_format": email_format,
    }

    start = time.time()
    logger.info(f"Pipeline start: {company_name} ({country}) | {len(contacts)} contacts | account_type={account_type}")

    # Step a: Company intel (cached per company_name)
    company_intel = scrape_company_intel(company_name, domain, country, account_type)

    # Step b + c: Verify contacts + gap report
    if contacts:
        verify_result = verify_contacts(company_context, contacts, company_intel)
        verified_contacts = verify_result["verified_contacts"]
        gap_report = verify_result["gap_report"]
        logger.info(
            f"Verification complete: valid={verify_result['valid_count']} "
            f"invalid={verify_result['invalid_count']} "
            f"needs_review={verify_result['needs_review_count']}"
        )
    else:
        # No contacts from n8n — search all target roles
        logger.info("No contacts provided, searching all target roles directly")
        verified_contacts = []
        import json as _json
        from pathlib import Path
        roles_path = Path(__file__).parent / "data" / "target_roles.json"
        with open(roles_path) as f:
            all_roles = _json.load(f)
        missing_roles = all_roles.get(account_type.lower(), all_roles.get("distributor", []))
        gap_report = {
            "missing_roles": missing_roles,
            "covered_roles": [],
            "coverage_percentage": 0,
            "potential_leads_from_web": [],
        }

    # Step d: Search gaps
    missing_roles = gap_report.get("missing_roles", [])
    potential_leads = gap_report.get("potential_leads_from_web", [])

    if missing_roles:
        search_result = search_gaps(
            company_context,
            missing_roles,
            verified_contacts,
            company_intel,
            potential_leads,
        )
        new_contacts = search_result["new_contacts"]
        manual_tasks = search_result["manual_tasks"]
        logger.info(
            f"Search complete: found={search_result['total_found']} manual={search_result['total_manual']}"
        )
    else:
        new_contacts = []
        manual_tasks = []
        logger.info("No missing roles — skipping waterfall search")

    # Steps e+f: Dedup new contacts against verified list (already done inside searcher, but double-check)
    all_contacts = deduplicate(new_contacts, verified_contacts)

    # Step g: Write to Google Sheets
    rows = [SHEET_HEADERS]
    for contact in verified_contacts:
        rows.append(contact_to_row(contact, company_name, country))
    for contact in all_contacts:
        rows.append(contact_to_row(contact, company_name, country))

    if manual_tasks:
        rows.append(["--- MANUAL TASKS ---"] + [""] * 14)
        for task in manual_tasks:
            rows.append([
                task["company"], task["country"], "", "", task["role"],
                "", "", "", "", "", "", "manual_needed", "", "manual_needed",
                task["task"],
            ])

    if OUTPUT_SHEET_ID:
        write_rows(OUTPUT_SHEET_ID, rows)
    else:
        logger.warning("OUTPUT_SHEET_ID not set — skipping Google Sheets write")

    elapsed = time.time() - start
    logger.info(
        f"Pipeline complete: {company_name} | "
        f"verified={len(verified_contacts)} new={len(all_contacts)} manual={len(manual_tasks)} | "
        f"elapsed={elapsed:.1f}s"
    )


# --- FastAPI app ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("B2B Contact Mapping Pipeline started")
    yield
    logger.info("B2B Contact Mapping Pipeline shutting down")


app = FastAPI(title="B2B Contact Mapping Pipeline", lifespan=lifespan)


@app.post("/webhook/verify-and-search")
async def webhook(payload: WebhookPayload, background_tasks: BackgroundTasks):
    payload_dict = payload.model_dump()
    # Convert Contact objects in contacts list to plain dicts
    payload_dict["contacts"] = [c for c in payload_dict["contacts"]]
    background_tasks.add_task(run_pipeline, payload_dict)
    return {"status": "processing", "company": payload.company_name}


@app.get("/health")
def health():
    return {"status": "ok"}
