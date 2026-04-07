"""
B2B Contact Mapping Pipeline — Full n8n Integration

Complete flow:
  1. POST /api/trigger       — send company metadata to n8n webhook
  2. n8n enriches company, finds employees
  3. POST /api/n8n/contacts  — n8n drip-feeds contacts back (accepts any JSON shape)
  4. Contacts buffered per company; 180-second silence timer resets on each new POST
  5. After silence, auto-flush runs the full pipeline for each company:
       a. Company intel scrape  (Firecrawl + GPT web search)
       b. Verify n8n contacts   (Unipile LinkedIn + GPT)
       c. Gap report            — which target roles are missing?
       d. Searcher waterfall    — Unipile SalesNav -> Apollo -> Clay -> GPT web
       e. Deduplicate           — new contacts vs verified contacts
       f. Write all results to Google Sheets
  6. Management: /api/n8n/flush, /api/n8n/buffer, /api/n8n/pipeline, /api/n8n/debug
"""

import asyncio
import io
import json
import logging
import re
import sys
import time
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, BackgroundTasks, Request, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from config import OUTPUT_SHEET_ID, N8N_WEBHOOK_URL, N8N_SUBMISSION_DELAY
from flows.company_intel import scrape_company_intel
from flows.verifier import verify_contacts, TARGET_ROLES
from flows.searcher import search_gaps
from clients.sheets_client import write_rows, contact_to_row, SHEET_HEADERS
from utils.dedup import deduplicate

# ── Logging ───────────────────────────────────────────────────────────────────
_stdout_utf8 = io.TextIOWrapper(
    sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(_stdout_utf8),
        logging.FileHandler("pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ── Buffer / Chain State ──────────────────────────────────────────────────────
_N8N_BUFFER_TIMEOUT = 180  # seconds of silence before auto-flush

# company_name -> list of (raw_dict, normalized_dict)
_n8n_buffer_contacts: dict[str, list[tuple[dict, dict]]] = {}
_n8n_buffer_timer: asyncio.Task | None = None
_n8n_buffer_lock: asyncio.Lock = None   # initialized in lifespan
_n8n_chain_lock: asyncio.Lock = None    # initialized in lifespan

# Pipeline progress (for status polling)
_n8n_chain_running: bool = False
_n8n_chain_current_company: str = ""
_n8n_chain_current_step: str = ""
_n8n_pipeline_results: list[dict] = []
_n8n_pipeline_companies: list[str] = []
_n8n_last_received: dict = {}

# Company metadata store: populated on /api/trigger so we have full context
# when contacts arrive later from n8n
_company_metadata: dict[str, dict] = {}


# ── Field Normalization ───────────────────────────────────────────────────────

def _extract_field(d: dict, *keys: str) -> str:
    """Return the first non-empty string found among the given keys."""
    for k in keys:
        v = d.get(k)
        if v and str(v).strip():
            return str(v).strip()
    return ""


def _normalize_contact(raw: dict) -> dict:
    """
    Accept ANY JSON shape from n8n and map it to our standard contact fields.
    Falls back gracefully: extracts name from email local-part or LinkedIn slug.
    """
    email = _extract_field(
        raw, "email", "address", "email_address", "work_email", "e_mail", "mail"
    )
    domain = _extract_field(raw, "domain", "company_domain", "company_domain_name")
    company = _extract_field(
        raw, "company_name", "company", "account", "organization", "org", "account_name"
    )

    # Derive domain from email if not explicit
    if not domain and email and "@" in email:
        domain = email.split("@")[1]
    # Derive company from domain if not explicit
    if not company and domain:
        company = domain.split(".")[0].capitalize()

    first_name = _extract_field(
        raw, "first_name", "firstname", "first", "given_name", "fname"
    )
    last_name = _extract_field(
        raw, "last_name", "lastname", "last", "family_name", "surname", "lname"
    )

    # Fall back: parse name from email (e.g. rajneet.kohli@x.com -> Rajneet Kohli)
    if not first_name and not last_name and email and "@" in email:
        local = email.split("@")[0]
        parts = re.split(r"[._\-]", local)
        if len(parts) >= 2:
            first_name = parts[0].capitalize()
            last_name = " ".join(p.capitalize() for p in parts[1:])
        elif len(parts) == 1 and len(local) > 2:
            first_name = local.capitalize()

    linkedin_url = _extract_field(
        raw,
        "linkedin_url", "linkedin", "linekdin_url", "linkedin_profile",
        "li_url", "linkedin_link", "profile_url",
    )

    # Fall back: parse name from LinkedIn slug
    if not first_name and not last_name and linkedin_url:
        m = re.search(r"linkedin\.com/in/([^/?&#]+)", linkedin_url)
        if m:
            slug = m.group(1).rstrip("/")
            slug = re.sub(r"-[a-f0-9]{6,}$", "", slug)  # strip trailing hex IDs
            parts = slug.split("-")
            if len(parts) >= 2:
                first_name = parts[0].capitalize()
                last_name = " ".join(p.capitalize() for p in parts[1:])

    return {
        "company_name": company,
        "domain": domain,
        "account_type": _extract_field(raw, "account_type", "type"),
        "account_size": _extract_field(raw, "account_size", "size", "company_size"),
        "country": _extract_field(raw, "country", "location", "geo", "region"),
        "first_name": first_name,
        "last_name": last_name,
        "job_title": _extract_field(
            raw,
            "job_title", "title", "job_titles", "job_titles_(english)",
            "job_title_(english)", "role", "position", "designation",
        ),
        "buying_role": _extract_field(
            raw, "buying_role", "role_type", "buyer_role", "contact_type"
        ),
        "linkedin_url": linkedin_url,
        "email": email,
        "phone_1": _extract_field(
            raw, "phone_1", "phone", "phone1", "mobile", "telephone"
        ),
        "phone_2": _extract_field(raw, "phone_2", "phone2", "secondary_phone"),
    }


def _infer_meta_from_contacts(contacts: list[dict]) -> dict:
    """
    When a company was NOT triggered via /api/trigger (n8n sent contacts
    unsolicited), infer company context from the contact data itself.
    """
    for c in contacts:
        if c.get("domain") or c.get("country") or c.get("account_type"):
            return {
                "country": c.get("country", ""),
                "domain": c.get("domain", ""),
                "account_type": c.get("account_type") or "distributor",
                "email_format": "firstname.lastname",
                "sales_nav_url": "",
                "linkedin_numeric_id": "",
                "account_size": c.get("account_size", ""),
            }
    return {
        "country": "",
        "domain": "",
        "account_type": "distributor",
        "email_format": "firstname.lastname",
        "sales_nav_url": "",
        "linkedin_numeric_id": "",
        "account_size": "",
    }


# ── n8n Submission ────────────────────────────────────────────────────────────

async def submit_to_n8n(payload: dict[str, Any]) -> bool:
    """
    POST a company row payload to the n8n webhook.
    Retries up to 3 times with 30-second backoff.
    Returns True on success, False on permanent failure.
    """
    if not N8N_WEBHOOK_URL:
        logger.warning("N8N_WEBHOOK_URL not configured — skipping n8n submission")
        return False

    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    N8N_WEBHOOK_URL,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                )
                resp.raise_for_status()
            logger.info(
                f"n8n submitted: company={payload.get('Company_Name', '')} "
                f"status={resp.status_code}"
            )
            return True
        except httpx.HTTPError as e:
            if attempt == 2:
                logger.error(f"n8n submission failed after 3 attempts: {e}")
                return False
            backoff = 30 * (attempt + 1)
            logger.warning(f"n8n retry {attempt + 1}/3 in {backoff}s — {e}")
            await asyncio.sleep(backoff)

    return False


# ── Core Pipeline (synchronous — runs in thread executor) ─────────────────────

def _run_company_pipeline(
    company_name: str,
    contacts: list[dict],
    meta: dict,
    status_record: dict,
) -> None:
    """
    Full pipeline for one company.
    This function is synchronous (all IO calls are blocking) so it MUST be
    invoked via asyncio.get_running_loop().run_in_executor().

    contacts      — list of normalized contact dicts received from n8n
    meta          — company context dict {country, domain, account_type, ...}
    status_record — mutable dict updated in-place; the async polling endpoint
                    reads it live to show progress
    """
    global _n8n_chain_current_step

    company_context = {
        "company_name": company_name,
        "country": meta.get("country", ""),
        "domain": meta.get("domain", ""),
        "account_type": meta.get("account_type", "distributor"),
        "email_format": meta.get("email_format", "firstname.lastname"),
        "sales_nav_url": meta.get("sales_nav_url", ""),
        "linkedin_numeric_id": meta.get("linkedin_numeric_id", ""),
    }
    country = company_context["country"]
    account_type = company_context["account_type"]
    start = time.time()

    logger.info(
        f"Pipeline start: {company_name} ({country}) | "
        f"{len(contacts)} n8n contacts | account_type={account_type}"
    )

    def _step(name: str, state: str = "running") -> None:
        global _n8n_chain_current_step
        _n8n_chain_current_step = name
        status_record["steps"][name] = state

    # ── a. Company intel (Firecrawl + GPT web search) ─────────────────────────
    _step("company_intel")
    try:
        company_intel = scrape_company_intel(
            company_name,
            company_context["domain"],
            country,
            account_type,
        )
        _step("company_intel", "done")
    except Exception as e:
        logger.error(f"Company intel failed for {company_name}: {e}")
        _step("company_intel", f"failed: {e}")
        company_intel = {
            "scraped_content": {},
            "people_found": [],
            "combined_text": "",
            "scraped_urls": [],
        }

    # ── b. Verify n8n contacts + produce gap report ───────────────────────────
    _step("verify_n8n")
    verified_contacts: list[dict] = []
    gap_report: dict = {}
    try:
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
            # n8n sent nothing — treat all target roles as missing
            logger.info("No n8n contacts received — will search all target roles")
            all_roles = TARGET_ROLES
            missing_all = all_roles.get(
                account_type.lower(), all_roles.get("distributor", [])
            )
            gap_report = {
                "missing_roles": missing_all,
                "covered_roles": [],
                "coverage_percentage": 0,
                "potential_leads_from_web": [],
            }
        _step("verify_n8n", "done")
    except Exception as e:
        logger.error(f"Verification failed for {company_name}: {e}")
        _step("verify_n8n", f"failed: {e}")
        gap_report = {
            "missing_roles": [],
            "covered_roles": [],
            "coverage_percentage": 0,
            "potential_leads_from_web": [],
        }

    # ── c+d. Searcher waterfall for missing roles ─────────────────────────────
    missing_roles: list[str] = gap_report.get("missing_roles", [])
    potential_leads: list[dict] = gap_report.get("potential_leads_from_web", [])

    _step("searcher")
    new_contacts: list[dict] = []
    manual_tasks: list[dict] = []
    try:
        if missing_roles:
            logger.info(
                f"Searching {len(missing_roles)} missing roles for {company_name}"
            )
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
                f"Searcher complete: found={search_result['total_found']} "
                f"manual={search_result['total_manual']}"
            )
        else:
            logger.info("All roles covered — skipping searcher waterfall")
        _step("searcher", "done")
    except Exception as e:
        logger.error(f"Searcher failed for {company_name}: {e}")
        _step("searcher", f"failed: {e}")

    # ── e. Deduplicate (new searcher contacts vs already-verified n8n contacts) ─
    all_new_contacts = deduplicate(new_contacts, verified_contacts)

    # ── f. Write to Google Sheets ─────────────────────────────────────────────
    _step("sheet_write")
    try:
        rows: list[list] = [SHEET_HEADERS]

        for contact in verified_contacts:
            rows.append(contact_to_row(contact, company_name, country))

        for contact in all_new_contacts:
            rows.append(contact_to_row(contact, company_name, country))

        if manual_tasks:
            pad = [""] * (len(SHEET_HEADERS) - 1)
            rows.append(["--- MANUAL TASKS ---"] + pad)
            for task in manual_tasks:
                row = [""] * len(SHEET_HEADERS)
                row[0] = task["company"]
                row[1] = task["country"]
                row[4] = task["role"]
                row[5] = task["role"]
                row[6] = "manual"
                row[12] = "manual_needed"
                row[14] = "manual_needed"
                row[15] = task["task"]
                rows.append(row)

        if OUTPUT_SHEET_ID:
            write_rows(OUTPUT_SHEET_ID, rows)
            logger.info(f"Wrote {len(rows)} rows to Google Sheets for {company_name}")
        else:
            logger.warning("OUTPUT_SHEET_ID not set — skipping Google Sheets write")
        _step("sheet_write", "done")
    except Exception as e:
        logger.error(f"Sheet write failed for {company_name}: {e}")
        _step("sheet_write", f"failed: {e}")

    elapsed = time.time() - start
    logger.info(
        f"Pipeline complete: {company_name} | "
        f"n8n_verified={len(verified_contacts)} searcher_found={len(all_new_contacts)} "
        f"manual={len(manual_tasks)} | elapsed={elapsed:.1f}s"
    )

    status_record["summary"] = {
        "n8n_contacts_received": len(contacts),
        "verified": len(verified_contacts),
        "searcher_found": len(all_new_contacts),
        "manual_needed": len(manual_tasks),
        "missing_roles": missing_roles,
        "covered_roles": gap_report.get("covered_roles", []),
        "coverage_pct": gap_report.get("coverage_percentage", 0),
        "elapsed_s": round(elapsed, 1),
    }


# ── Buffer Timer ──────────────────────────────────────────────────────────────

async def _n8n_buffer_reset_timer() -> None:
    """Cancel any running countdown and start a fresh 180-second timer."""
    global _n8n_buffer_timer

    if _n8n_buffer_timer and not _n8n_buffer_timer.done():
        _n8n_buffer_timer.cancel()

    async def _countdown() -> None:
        await asyncio.sleep(_N8N_BUFFER_TIMEOUT)
        async with _n8n_buffer_lock:
            await _n8n_buffer_flush()

    _n8n_buffer_timer = asyncio.create_task(_countdown())


# ── Buffer Flush ──────────────────────────────────────────────────────────────

async def _n8n_buffer_flush() -> None:
    """
    Snapshot and clear the buffer, then run the full pipeline for each
    buffered company sequentially (companies are independent, so order
    doesn't matter; sequential keeps resource usage predictable).
    """
    global _n8n_buffer_contacts, _n8n_buffer_timer
    global _n8n_chain_running, _n8n_chain_current_company, _n8n_chain_current_step
    global _n8n_pipeline_results, _n8n_pipeline_companies

    # Atomically snapshot + clear so new contacts can buffer while we process
    buffered = {co: list(entries) for co, entries in _n8n_buffer_contacts.items()}
    _n8n_buffer_contacts.clear()
    _n8n_buffer_timer = None

    if not buffered:
        return

    company_list = sorted(buffered.keys())
    logger.info(
        f"Buffer flush: {len(company_list)} companies, "
        f"{sum(len(v) for v in buffered.values())} total contacts"
    )

    async with _n8n_chain_lock:
        _n8n_chain_running = True
        _n8n_pipeline_results.clear()
        _n8n_pipeline_companies.clear()
        _n8n_pipeline_companies.extend(company_list)

        for co in company_list:
            _n8n_pipeline_results.append({
                "company": co,
                "contacts_received": len(buffered[co]),
                "status": "pending",
                "steps": {
                    "company_intel": "pending",
                    "verify_n8n": "pending",
                    "searcher": "pending",
                    "sheet_write": "pending",
                },
                "summary": {},
            })

        try:
            loop = asyncio.get_running_loop()

            for i, company in enumerate(company_list):
                entries = buffered[company]
                # Extract just the normalized contact dicts
                contacts = [norm for _raw, norm in entries]
                # Use stored trigger metadata; fall back to inferring from contacts
                meta = _company_metadata.get(company) or _infer_meta_from_contacts(contacts)

                _n8n_chain_current_company = company
                company_result = _n8n_pipeline_results[i]
                company_result["status"] = "running"

                try:
                    await loop.run_in_executor(
                        None,
                        _run_company_pipeline,
                        company, contacts, meta, company_result,
                    )
                    company_result["status"] = "done"
                except Exception as e:
                    logger.error(f"Pipeline executor crashed for {company}: {e}")
                    company_result["status"] = f"crashed: {e}"

                # Brief pause between companies to avoid hammering APIs
                if i < len(company_list) - 1:
                    await asyncio.sleep(5)

        finally:
            _n8n_chain_running = False
            _n8n_chain_current_company = ""
            _n8n_chain_current_step = ""


# ── FastAPI App ───────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _n8n_buffer_lock, _n8n_chain_lock
    _n8n_buffer_lock = asyncio.Lock()
    _n8n_chain_lock = asyncio.Lock()
    logger.info("B2B Contact Mapping Pipeline started")
    yield
    logger.info("B2B Contact Mapping Pipeline shutting down")


app = FastAPI(title="B2B Contact Mapping Pipeline", lifespan=lifespan)

# ── UI Page ───────────────────────────────────────────────────────────────────

_UI_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>SDR Pipeline</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f7fa;color:#1f2937}
.wrap{max-width:700px;margin:40px auto;padding:0 20px}
h1{font-size:22px;font-weight:700;margin-bottom:4px}
.sub{color:#6b7280;font-size:13px;margin-bottom:28px}
.card{background:#fff;border-radius:10px;padding:24px;box-shadow:0 1px 4px rgba(0,0,0,.08);margin-bottom:18px}
.card h2{font-size:15px;font-weight:600;margin-bottom:16px;color:#374151}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px}
.full{grid-template-columns:1fr}
label{display:block;font-size:11px;font-weight:600;color:#6b7280;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px}
input,select{width:100%;padding:9px 12px;border:1.5px solid #e5e7eb;border-radius:7px;font-size:14px;color:#111;background:#fff}
input:focus,select:focus{outline:none;border-color:#6366f1}
.btn-run{width:100%;margin-top:10px;padding:11px;background:#6366f1;color:#fff;border:none;border-radius:8px;font-size:15px;font-weight:600;cursor:pointer}
.btn-run:hover{background:#4f46e5}
.btn-run:disabled{background:#a5b4fc;cursor:not-allowed}
.btn-sm{padding:6px 14px;font-size:12px;font-weight:500;border:1.5px solid #e5e7eb;border-radius:6px;background:#f9fafb;cursor:pointer;margin-right:8px}
.btn-sm:hover{background:#f3f4f6}
.alert{padding:10px 14px;border-radius:7px;font-size:13px;margin-bottom:14px}
.blue{background:#eff6ff;color:#1d4ed8;border:1px solid #bfdbfe}
.green{background:#f0fdf4;color:#15803d;border:1px solid #bbf7d0}
.red{background:#fef2f2;color:#b91c1c;border:1px solid #fecaca}
.step-row{display:flex;align-items:center;padding:11px 0;border-bottom:1px solid #f3f4f6}
.step-row:last-child{border:none}
.step-info{flex:1}
.step-name{font-size:14px;font-weight:500}
.step-desc{font-size:12px;color:#9ca3af;margin-top:2px}
.badge{padding:4px 12px;border-radius:20px;font-size:12px;font-weight:500;white-space:nowrap}
.b-pending{background:#f3f4f6;color:#9ca3af}
.b-running{background:#eff6ff;color:#3b82f6}
.b-done{background:#f0fdf4;color:#16a34a}
.b-failed{background:#fef2f2;color:#dc2626}
.stats{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-top:14px}
.stat{background:#f9fafb;border-radius:8px;padding:14px;text-align:center}
.stat-n{font-size:26px;font-weight:700;color:#6366f1}
.stat-l{font-size:11px;color:#6b7280;margin-top:3px}
@keyframes spin{to{transform:rotate(360deg)}}
.spin{display:inline-block;width:10px;height:10px;border:2px solid #bfdbfe;border-top-color:#3b82f6;border-radius:50%;animation:spin .7s linear infinite;margin-right:5px;vertical-align:middle}
#panel{display:none}
#summCard{display:none}
</style>
</head>
<body>
<div class="wrap">
  <h1>SDR Contact Pipeline</h1>
  <p class="sub">Enter a company — pipeline finds, verifies and writes all contacts to Google Sheets.</p>

  <div class="card">
    <h2>Company Details</h2>
    <form id="frm">
      <div class="grid2 full">
        <div><label>Company Name *</label>
          <input name="company_name" placeholder="e.g. Britannia Industries" required autofocus></div>
      </div>
      <div class="grid2">
        <div><label>Country</label><input name="country" value="India"></div>
        <div><label>Domain</label><input name="domain" placeholder="britannia.co.in"></div>
      </div>
      <div class="grid2">
        <div><label>Account Type</label>
          <select name="account_type">
            <option value="distributor">Distributor</option>
            <option value="manufacturer">Manufacturer</option>
            <option value="bottler">Bottler</option>
            <option value="retailer">Retailer</option>
            <option value="wholesaler">Wholesaler</option>
          </select></div>
        <div><label>Email Format</label><input name="email_format" value="firstname.lastname"></div>
      </div>
      <div class="grid2 full">
        <div><label>Sales Navigator URL (optional)</label>
          <input name="sales_nav_url" placeholder="https://www.linkedin.com/sales/..."></div>
      </div>
      <button class="btn-run" type="submit" id="runBtn">▶ Run Pipeline</button>
    </form>
  </div>

  <div id="panel">
    <div class="card">
      <div id="alert"></div>
      <div id="helpers" style="display:none;margin-bottom:14px">
        <p style="font-size:13px;color:#6b7280;margin-bottom:10px">
          Waiting for n8n to send contacts. You can also test manually:
        </p>
        <button class="btn-sm" onclick="simContacts()">Inject test contact</button>
        <button class="btn-sm" onclick="flushNow()">Flush now (skip wait)</button>
      </div>
      <div id="compName" style="font-size:16px;font-weight:600;margin-bottom:3px"></div>
      <div id="compStep" style="font-size:13px;color:#6b7280;margin-bottom:14px"></div>
      <div id="steps"></div>
    </div>
    <div class="card" id="summCard">
      <div class="alert green" style="margin-bottom:0">
        &#10003; Pipeline complete — results written to Google Sheets.
      </div>
      <div class="stats" id="stats"></div>
    </div>
  </div>
</div>

<script>
const STEPS = {
  company_intel: ['Company Intel',    'Firecrawl + GPT web search for leadership'],
  verify_n8n:    ['Verify n8n Contacts','Unipile LinkedIn fetch + GPT check'],
  searcher:      ['Searcher Waterfall','Unipile → Apollo → Clay → GPT web'],
  sheet_write:   ['Write to Sheets',  'Appending results to Google Sheets'],
};

let company = '', pollId = null;

function badge(s){
  if(!s||s==='pending') return '<span class="badge b-pending">pending</span>';
  if(s==='running')     return '<span class="badge b-running"><span class="spin"></span>running</span>';
  if(s==='done')        return '<span class="badge b-done">&#10003; done</span>';
  return                       '<span class="badge b-failed">failed</span>';
}

function renderSteps(steps){
  return Object.entries(STEPS).map(([k,[name,desc]])=>`
    <div class="step-row">
      <div class="step-info">
        <div class="step-name">${name}</div>
        <div class="step-desc">${desc}</div>
      </div>
      ${badge(steps[k])}
    </div>`).join('');
}

function setAlert(msg,type='blue'){
  document.getElementById('alert').innerHTML=`<div class="alert ${type}">${msg}</div>`;
}

async function poll(){
  try{
    const d = await fetch('/api/n8n/pipeline').then(r=>r.json());
    const res = (d.results||[]).find(r=>r.company===company)||(d.results||[])[0];

    if(d.running && res){
      document.getElementById('compStep').textContent = 'Step: '+(d.current_step||'...');
      document.getElementById('steps').innerHTML = renderSteps(res.steps||{});
      document.getElementById('helpers').style.display='none';
      setAlert('<span class="spin"></span>Pipeline running…');
    } else if(res && res.status==='done'){
      clearInterval(pollId);
      document.getElementById('steps').innerHTML = renderSteps(res.steps||{});
      document.getElementById('compStep').textContent='Complete';
      document.getElementById('helpers').style.display='none';
      setAlert('Done!','green');
      showSummary(res.summary||{});
      document.getElementById('runBtn').disabled=false;
      document.getElementById('runBtn').textContent='▶ Run Pipeline';
    } else if(res && res.status && (res.status.startsWith('crashed')||res.status.startsWith('failed'))){
      clearInterval(pollId);
      setAlert('Pipeline error: '+res.status,'red');
      document.getElementById('runBtn').disabled=false;
      document.getElementById('runBtn').textContent='▶ Run Pipeline';
    }

    if(!d.running && d.buffer && d.buffer.timer_active){
      document.getElementById('helpers').style.display='block';
      setAlert('Waiting for n8n contacts… ('+d.buffer.total_contacts+' buffered so far)');
    }
  }catch(e){console.error(e)}
}

function showSummary(s){
  const stats=[
    [s.n8n_contacts_received??'-','n8n Contacts'],
    [s.verified??'-','Verified'],
    [s.searcher_found??'-','Searcher Found'],
    [s.manual_needed??'-','Manual Needed'],
    [(s.coverage_pct!=null?s.coverage_pct+'%':'-'),'Coverage'],
    [(s.elapsed_s!=null?s.elapsed_s+'s':'-'),'Time'],
  ];
  document.getElementById('stats').innerHTML=stats.map(
    ([n,l])=>`<div class="stat"><div class="stat-n">${n}</div><div class="stat-l">${l}</div></div>`
  ).join('');
  document.getElementById('summCard').style.display='block';
}

async function simContacts(){
  const domain=document.querySelector('[name=domain]').value||'example.com';
  await fetch('/api/n8n/contacts',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify([{first_name:'Test',last_name:'Contact',
      job_title:'Sales Director',email:'test@'+domain,company_name:company}])});
  setAlert('<span class="spin"></span>Test contact injected — flushing…');
  document.getElementById('helpers').style.display='none';
  await fetch('/api/n8n/flush',{method:'POST'});
}

async function flushNow(){
  await fetch('/api/n8n/flush',{method:'POST'});
  document.getElementById('helpers').style.display='none';
  setAlert('<span class="spin"></span>Flushing pipeline…');
}

document.getElementById('frm').addEventListener('submit',async e=>{
  e.preventDefault();
  const fd=new FormData(e.target);
  const payload=Object.fromEntries(fd.entries());
  company=payload.company_name;

  document.getElementById('runBtn').disabled=true;
  document.getElementById('runBtn').textContent='Sending to n8n…';
  document.getElementById('panel').style.display='block';
  document.getElementById('summCard').style.display='none';
  document.getElementById('compName').textContent=company;
  document.getElementById('compStep').textContent='Sending to n8n…';
  document.getElementById('steps').innerHTML=renderSteps({});

  try{
    await fetch('/api/trigger',{method:'POST',
      headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
    setAlert('Triggered. Waiting for n8n to send contacts back…');
    document.getElementById('helpers').style.display='block';
    document.getElementById('runBtn').textContent='Running…';
  }catch(err){
    setAlert('Trigger failed: '+err.message,'red');
    document.getElementById('runBtn').disabled=false;
    document.getElementById('runBtn').textContent='▶ Run Pipeline';
    return;
  }

  if(pollId) clearInterval(pollId);
  pollId=setInterval(poll,2000);
});
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def ui():
    """Simple web UI — open http://localhost:8000 in your browser."""
    return _UI_HTML


# ── Pydantic Models ───────────────────────────────────────────────────────────

class TriggerPayload(BaseModel):
    """
    Payload for POST /api/trigger.
    Sends company metadata to n8n and stores context for the incoming contacts.
    """
    company_name: str
    country: str = ""
    domain: str = ""
    parent_company_name: str = ""
    sales_nav_url: str = ""
    linkedin_numeric_id: str = ""
    sdr_assigned: str = ""
    email_format: str = "firstname.lastname"
    account_type: str = "distributor"
    account_size: str = ""
    row: int = 0


class RunResponse(BaseModel):
    status: str
    company: str = ""
    message: str


# ── Trigger Endpoint ──────────────────────────────────────────────────────────

@app.post("/api/trigger", response_model=RunResponse)
async def trigger_company(payload: TriggerPayload):
    """
    Step 1 of the pipeline.

    1. Stores company metadata locally (so we can reconstruct company_context
       when n8n sends contacts back)
    2. POSTs the company row to the n8n webhook (fire-and-forget)

    n8n will respond asynchronously by POSTing contacts to /api/n8n/contacts.
    """
    _company_metadata[payload.company_name] = {
        "country": payload.country,
        "domain": payload.domain,
        "account_type": payload.account_type,
        "email_format": payload.email_format,
        "sales_nav_url": payload.sales_nav_url,
        "linkedin_numeric_id": payload.linkedin_numeric_id,
        "account_size": payload.account_size,
        "parent_company_name": payload.parent_company_name,
        "sdr_assigned": payload.sdr_assigned,
        "row": payload.row,
    }

    n8n_payload = {
        "sheetName": "Target Accounts",
        "row": payload.row,
        "Company_Name": payload.company_name,
        "Parent_Company_Name": payload.parent_company_name,
        "Sales_Navigator_Link": payload.sales_nav_url,
        "Company_Domain": payload.domain,
        "SDR_Name": payload.sdr_assigned,
        "Email_Format(_Firstname-amy_,_Lastname-_williams)": payload.email_format,
        "Account_type": payload.account_type,
        "Account_Size": payload.account_size,
        "country": payload.country,
    }

    # Fire-and-forget — do not block the HTTP response
    asyncio.create_task(submit_to_n8n(n8n_payload))

    logger.info(
        f"Triggered n8n for: {payload.company_name} ({payload.country}) "
        f"domain={payload.domain} account_type={payload.account_type}"
    )
    return RunResponse(
        status="triggered",
        company=payload.company_name,
        message=(
            f"Sent {payload.company_name} to n8n. "
            f"Contacts expected on POST /api/n8n/contacts."
        ),
    )


# ── Receive Contacts from n8n ─────────────────────────────────────────────────

@app.post("/api/n8n/contacts", response_model=RunResponse)
async def n8n_contacts(request: Request):
    """
    Step 2 — n8n sends enriched employee contacts here.

    Accepted JSON shapes:
      - bare array:          [{"first_name": ...}, ...]
      - "contacts" wrapper:  {"contacts": [{...}]}
      - "contact" wrapper:   {"contact": {...}}
      - single flat object:  {"first_name": ...}

    Contacts are buffered per company_name.
    A 180-second silence timer is reset on every POST.
    After 180s with no new contacts, the pipeline fires automatically.
    """
    from datetime import datetime, timezone

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    # Normalize to a flat list of raw dicts
    raw_contacts: list[dict] = []
    if isinstance(body, dict):
        if "contacts" in body:
            v = body["contacts"]
            raw_contacts = v if isinstance(v, list) else [v]
        elif "contact" in body:
            v = body["contact"]
            raw_contacts = v if isinstance(v, list) else [v]
        else:
            raw_contacts = [body]
    elif isinstance(body, list):
        raw_contacts = body
    else:
        raise HTTPException(
            status_code=400, detail="Cannot parse contacts from request body"
        )

    if not raw_contacts:
        raise HTTPException(status_code=400, detail="No contacts in request body")

    buffered_count = 0
    skipped = 0
    skip_reasons: list[str] = []
    companies_seen: set[str] = set()

    for raw in raw_contacts:
        if not isinstance(raw, dict):
            skipped += 1
            skip_reasons.append(f"not a dict: {str(raw)[:60]}")
            continue

        c = _normalize_contact(raw)

        # Must have at least a name to be useful
        if not c["first_name"] and not c["last_name"]:
            skipped += 1
            skip_reasons.append(
                f"{c['company_name'] or '(no company)'}: no name — "
                f"keys present: {list(raw.keys())[:6]}"
            )
            continue

        company_key = c["company_name"] or "(unknown)"
        async with _n8n_buffer_lock:
            _n8n_buffer_contacts.setdefault(company_key, []).append((raw, c))

        buffered_count += 1
        if c["company_name"]:
            companies_seen.add(c["company_name"])

    if buffered_count > 0:
        async with _n8n_buffer_lock:
            await _n8n_buffer_reset_timer()
            total_buffered = sum(len(v) for v in _n8n_buffer_contacts.values())
            buffer_companies = sorted(_n8n_buffer_contacts.keys())

        _n8n_last_received.clear()
        _n8n_last_received.update({
            "received_at": timestamp,
            "buffered_this_batch": buffered_count,
            "skipped": skipped,
            "skip_reasons": skip_reasons,
            "companies_this_batch": sorted(companies_seen),
            "buffer_companies": buffer_companies,
            "buffer_total_contacts": total_buffered,
            "buffer_timeout_secs": _N8N_BUFFER_TIMEOUT,
        })

        skip_msg = (
            f" Skipped {skipped}: {'; '.join(skip_reasons[:3])}" if skipped else ""
        )
        return RunResponse(
            status="buffered",
            company=", ".join(sorted(companies_seen)),
            message=(
                f"Buffered {buffered_count} contacts for "
                f"{len(companies_seen)} companies. "
                f"Pipeline fires after {_N8N_BUFFER_TIMEOUT}s silence.{skip_msg}"
            ),
        )
    else:
        return RunResponse(
            status="empty",
            company="",
            message=(
                f"No valid contacts found. "
                f"Skipped {skipped}: {'; '.join(skip_reasons[:3])}"
            ),
        )


# ── Management Endpoints ──────────────────────────────────────────────────────

@app.post("/api/n8n/flush")
async def n8n_flush():
    """
    Immediately flush the contact buffer without waiting for the 180s timeout.
    Useful for testing or when you know all contacts have arrived.
    """
    async with _n8n_buffer_lock:
        companies = sorted(_n8n_buffer_contacts.keys())
        total = sum(len(v) for v in _n8n_buffer_contacts.values())
        if not companies:
            return {"status": "empty", "message": "Buffer is empty — nothing to flush"}
        if _n8n_buffer_timer and not _n8n_buffer_timer.done():
            _n8n_buffer_timer.cancel()

    async def _manual_flush():
        async with _n8n_buffer_lock:
            await _n8n_buffer_flush()

    asyncio.create_task(_manual_flush())
    return {
        "status": "flushing",
        "companies": companies,
        "message": (
            f"Flushing {total} contacts for {len(companies)} companies: "
            f"{', '.join(companies)}"
        ),
    }


@app.get("/api/n8n/buffer")
async def n8n_buffer_status():
    """Show what is currently in the buffer, waiting to be processed."""
    return {
        "companies": sorted(_n8n_buffer_contacts.keys()),
        "total_contacts": sum(len(v) for v in _n8n_buffer_contacts.values()),
        "per_company": {k: len(v) for k, v in _n8n_buffer_contacts.items()},
        "timeout_secs": _N8N_BUFFER_TIMEOUT,
        "timer_active": _n8n_buffer_timer is not None and not _n8n_buffer_timer.done(),
        "chain_running": _n8n_chain_running,
        "chain_current_company": _n8n_chain_current_company,
    }


@app.get("/api/n8n/pipeline")
async def n8n_pipeline_status():
    """
    Live per-company pipeline progress.
    Poll this endpoint from a frontend to show real-time status.
    """
    return {
        "running": _n8n_chain_running,
        "current_company": _n8n_chain_current_company,
        "current_step": _n8n_chain_current_step,
        "companies": _n8n_pipeline_companies,
        "results": _n8n_pipeline_results,
        "buffer": {
            "companies": sorted(_n8n_buffer_contacts.keys()),
            "total_contacts": sum(len(v) for v in _n8n_buffer_contacts.values()),
            "timer_active": (
                _n8n_buffer_timer is not None and not _n8n_buffer_timer.done()
            ),
        },
    }


@app.get("/api/n8n/debug")
async def n8n_debug():
    """
    Shows the last received contact batch and how fields were mapped.
    Use this to diagnose n8n field-name mismatches.
    """
    return (
        dict(_n8n_last_received)
        if _n8n_last_received
        else {"message": "No data received yet"}
    )


@app.get("/api/n8n/companies")
async def n8n_companies():
    """List companies that have been triggered and their stored metadata."""
    return {
        "count": len(_company_metadata),
        "companies": {
            name: {
                "country": m.get("country"),
                "domain": m.get("domain"),
                "account_type": m.get("account_type"),
            }
            for name, m in _company_metadata.items()
        },
    }


# ── Legacy Endpoint (backward compatibility) ──────────────────────────────────

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


def _run_pipeline_legacy(payload: dict) -> None:
    """
    Legacy synchronous pipeline — called from /webhook/verify-and-search.
    Contacts are already included in the payload (no n8n buffering).
    """
    company_name = payload["company_name"]
    country = payload["country"]
    domain = payload.get("domain", "")
    account_type = payload.get("account_type", "distributor")
    email_format = payload.get("email_format", "firstname.lastname") or "firstname.lastname"
    contacts = payload.get("contacts", [])

    # Resolve linkedin_numeric_id from sales_nav_url if not explicit
    linkedin_numeric_id = payload.get("linkedin_numeric_id", "")
    if not linkedin_numeric_id:
        m = re.search(r"/sales/company/(\d+)", payload.get("sales_nav_url", ""))
        if m:
            linkedin_numeric_id = m.group(1)

    meta = {
        "country": country,
        "domain": domain,
        "account_type": account_type,
        "email_format": email_format,
        "sales_nav_url": payload.get("sales_nav_url", ""),
        "linkedin_numeric_id": linkedin_numeric_id,
    }
    status_record = {
        "steps": {
            "company_intel": "pending",
            "verify_n8n": "pending",
            "searcher": "pending",
            "sheet_write": "pending",
        },
        "summary": {},
    }
    _run_company_pipeline(company_name, contacts, meta, status_record)


@app.post("/webhook/verify-and-search")
async def webhook(payload: WebhookPayload, background_tasks: BackgroundTasks):
    """
    Legacy endpoint — kept for backward compatibility.
    For new integrations use POST /api/trigger instead.
    """
    background_tasks.add_task(_run_pipeline_legacy, payload.model_dump())
    return {"status": "processing", "company": payload.company_name}


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}
