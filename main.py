import os
import json
import time
import re
import unicodedata
import threading
import traceback
from typing import Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv

# Load local .env first (Railway Variables still override)
load_dotenv()

print("BOOTING APP ✅", flush=True)

# ----------------------
# ENV CONFIG
# ----------------------
GOOGLE_SHEETS_ID = os.getenv("GOOGLE_SHEETS_ID")                 # required
GOOGLE_SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "Sheet1")
GOOGLE_SHEET_RANGE = os.getenv("GOOGLE_SHEET_RANGE", "A:B")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")  # required

# Accept either name (people often use AIRTABLE_API_KEY)
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN") or os.getenv("AIRTABLE_API_KEY")  # required
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")               # required
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")         # required

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))

# Airtable field names (override via env if needed)
FIELD_STATUS = os.getenv("FIELD_STATUS", "Status")
FIELD_CREATOR_NAME = os.getenv("FIELD_CREATOR_NAME", "Creator / Contact Name")
FIELD_TIER = os.getenv("FIELD_TIER", "Tier")
FIELD_EMAIL = os.getenv("FIELD_EMAIL", "Email")
FIELD_PAYOUT_MONTHLY = os.getenv("FIELD_PAYOUT_MONTHLY", "payout_monthly")
FIELD_PAYOUT_ANNUAL = os.getenv("FIELD_PAYOUT_ANNUAL", "payout_annual")
FIELD_LINK_NAME = os.getenv("FIELD_LINK_NAME", "link_name")

DEFAULT_STATUS = os.getenv("DEFAULT_STATUS", "Signed - Onboarding")
DEFAULT_TIER = os.getenv("DEFAULT_TIER", "Affiliate")
DEFAULT_PAYOUT_MONTHLY = float(os.getenv("DEFAULT_PAYOUT_MONTHLY", "3.6"))
DEFAULT_PAYOUT_ANNUAL = float(os.getenv("DEFAULT_PAYOUT_ANNUAL", "30.0"))

AIRTABLE_API_BASE = "https://api.airtable.com/v0"

# Validate required env vars (but print clearly first)
REQUIRED = [
    ("GOOGLE_SHEETS_ID", GOOGLE_SHEETS_ID),
    ("GOOGLE_SERVICE_ACCOUNT_JSON", GOOGLE_SERVICE_ACCOUNT_JSON),
    ("AIRTABLE_TOKEN (or AIRTABLE_API_KEY)", AIRTABLE_TOKEN),
    ("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID),
    ("AIRTABLE_TABLE_NAME", AIRTABLE_TABLE_NAME),
]
missing = [name for name, val in REQUIRED if not val]
if missing:
    print(f"❌ Missing env vars: {missing}", flush=True)
    raise RuntimeError(f"Missing env vars: {missing}")

print("✅ Env vars look present.", flush=True)

app = FastAPI()

# ----------------------
# HELPERS
# ----------------------
def _normalize_letters_only(value: str) -> str:
    """
    link_name rules:
    - lowercase
    - no spaces
    - no numbers
    - letters only a-z
    """
    if not value:
        return ""

    value = unicodedata.normalize("NFKD", value)
    value = "".join([c for c in value if not unicodedata.combining(c)])
    value = value.lower()
    value = re.sub(r"\d+", "", value)
    value = re.sub(r"[^a-z]", "", value)
    return value


def make_link_name(creator_name: str, email: str) -> str:
    base = _normalize_letters_only(creator_name)
    if not base:
        local = email.split("@")[0] if "@" in email else email
        base = _normalize_letters_only(local)
    if not base:
        base = "creator"
    return base[:24]


def sheets_client():
    try:
        sa_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    except Exception as e:
        print("❌ GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON.", flush=True)
        raise

    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def read_sheet_rows() -> List[Tuple[str, str]]:
    """
    Returns list of (email, creator_name) from columns A and B.
    Skips header row if it looks like a header.
    """
    service = sheets_client()
    range_ = f"{GOOGLE_SHEET_TAB}!{GOOGLE_SHEET_RANGE}"
    resp = service.spreadsheets().values().get(
        spreadsheetId=GOOGLE_SHEETS_ID,
        range=range_,
        majorDimension="ROWS",
    ).execute()

    values = resp.get("values", [])
    rows: List[Tuple[str, str]] = []

    for i, row in enumerate(values):
        email = (row[0] if len(row) > 0 else "").strip()
        name = (row[1] if len(row) > 1 else "").strip()

        if not email and not name:
            continue

        if i == 0 and ("email" in email.lower() or "confirm" in email.lower()):
            continue

        if "@" not in email:
            continue

        rows.append((email, name))

    return rows


def airtable_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
    }


def _airtable_url() -> str:
    return f"{AIRTABLE_API_BASE}/{AIRTABLE_BASE_ID}/{requests.utils.quote(AIRTABLE_TABLE_NAME)}"


def airtable_find_by_email(email: str) -> Optional[Dict]:
    params = {"filterByFormula": f'{{{FIELD_EMAIL}}}="{email}"', "maxRecords": 1}
    r = requests.get(_airtable_url(), headers=airtable_headers(), params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    records = data.get("records", [])
    return records[0] if records else None


def airtable_link_name_exists(link_name: str) -> bool:
    params = {"filterByFormula": f'{{{FIELD_LINK_NAME}}}="{link_name}"', "maxRecords": 1}
    r = requests.get(_airtable_url(), headers=airtable_headers(), params=params, timeout=30)
    r.raise_for_status()
    return len(r.json().get("records", [])) > 0


def make_unique_link_name(creator_name: str, email: str) -> str:
    base = make_link_name(creator_name, email)
    if not airtable_link_name_exists(base):
        return base

    local = email.split("@")[0]
    suffix = _normalize_letters_only(local)

    candidates = []
    if suffix:
        candidates.append((base[:18] + suffix[:6])[:24])
        candidates.append((base[:16] + suffix[:8])[:24])
        candidates.append((base[:14] + suffix[:10])[:24])

    candidates.append((base + "x")[:24])
    candidates.append((base + "xx")[:24])

    for c in candidates:
        if c and not airtable_link_name_exists(c):
            return c

    return (base + "xxx")[:24]


def airtable_create_record(email: str, creator_name: str) -> Dict:
    link_name = make_unique_link_name(creator_name, email)

    fields = {
        FIELD_STATUS: DEFAULT_STATUS,
        FIELD_CREATOR_NAME: creator_name or "",
        FIELD_TIER: DEFAULT_TIER,
        FIELD_EMAIL: email,
        FIELD_PAYOUT_MONTHLY: DEFAULT_PAYOUT_MONTHLY,
        FIELD_PAYOUT_ANNUAL: DEFAULT_PAYOUT_ANNUAL,
        FIELD_LINK_NAME: link_name,
    }

    payload = {"fields": fields}
    r = requests.post(_airtable_url(), headers=airtable_headers(), json=payload, timeout=30)
    r.raise_for_status()
    return r.json()


# ----------------------
# WORKER LOOP
# ----------------------
_last_run = {"ts": None, "added": 0, "skipped": 0, "errors": 0, "last_error": None}


def sync_once():
    added = 0
    skipped = 0
    errors = 0
    last_error = None

    try:
        rows = read_sheet_rows()
    except Exception as e:
        last_error = f"read_sheet_rows failed: {e}"
        print("❌ " + last_error, flush=True)
        print(traceback.format_exc(), flush=True)
        _last_run.update({"ts": int(time.time()), "added": 0, "skipped": 0, "errors": 1, "last_error": last_error})
        return

    for email, name in rows:
        try:
            existing = airtable_find_by_email(email)
            if existing:
                skipped += 1
                continue

            airtable_create_record(email=email, creator_name=name)
            added += 1
        except Exception as e:
            errors += 1
            last_error = f"{email}: {e}"
            print("❌ Airtable op failed for " + email, flush=True)
            print(traceback.format_exc(), flush=True)

    _last_run.update(
        {"ts": int(time.time()), "added": added, "skipped": skipped, "errors": errors, "last_error": last_error}
    )


def worker_loop():
    print(f"✅ Worker started. Poll every {POLL_SECONDS}s", flush=True)
    while True:
        sync_once()
        time.sleep(POLL_SECONDS)


@app.on_event("startup")
def on_startup():
    # Start background sync worker
    t = threading.Thread(target=worker_loop, daemon=True)
    t.start()


# ----------------------
# ROUTES
# ----------------------
@app.get("/health")
def health():
    return {"ok": True}


@app.get("/")
def root():
    return {
        "ok": True,
        "service": "triptrack-sheets-airtable",
        "airtable_table": AIRTABLE_TABLE_NAME,
        "poll_seconds": POLL_SECONDS,
        "last_run": _last_run,
    }


@app.post("/sync-now")
def sync_now():
    sync_once()
    return {"ok": True, "last_run": _last_run}