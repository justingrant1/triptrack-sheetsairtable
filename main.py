import os

print("BOOTING APP ✅", flush=True)

REQUIRED = [
    "GOOGLE_SHEETS_ID",
    "GOOGLE_SERVICE_ACCOUNT_JSON",
    "AIRTABLE_TOKEN",
    "AIRTABLE_BASE_ID",
    "AIRTABLE_TABLE_NAME",
]
missing = [k for k in REQUIRED if not os.environ.get(k)]
if missing:
    raise RuntimeError(f"Missing env vars: {missing}")
    
import os
import json
import time
import re
import unicodedata
import threading
from typing import Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI
from google.oauth2 import service_account
from googleapiclient.discovery import build

from dotenv import load_dotenv
import os

load_dotenv()

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")

# ======================
# ENV CONFIG
# ======================
GOOGLE_SHEETS_ID = os.environ["GOOGLE_SHEETS_ID"]                 # e.g. 1ksvosUrXVUhMGiQMvBwTNdZokleURKCpg_Gtx9EeYg
GOOGLE_SHEET_TAB = os.environ.get("GOOGLE_SHEET_TAB", "Sheet1")  # tab name
GOOGLE_SHEET_RANGE = os.environ.get("GOOGLE_SHEET_RANGE", "A:B") # we only need A+B
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]  # entire JSON as string

AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]                     # PAT token
AIRTABLE_BASE_ID = os.environ["AIRTABLE_BASE_ID"]
AIRTABLE_TABLE_NAME = os.environ["AIRTABLE_TABLE_NAME"]

POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "60"))

# Airtable field names (edit if your base uses different names)
FIELD_STATUS = os.environ.get("FIELD_STATUS", "Status")
FIELD_CREATOR_NAME = os.environ.get("FIELD_CREATOR_NAME", "Creator / Contact Name")
FIELD_TIER = os.environ.get("FIELD_TIER", "Tier")
FIELD_EMAIL = os.environ.get("FIELD_EMAIL", "Email")
FIELD_PAYOUT_MONTHLY = os.environ.get("FIELD_PAYOUT_MONTHLY", "payout_monthly")
FIELD_PAYOUT_ANNUAL = os.environ.get("FIELD_PAYOUT_ANNUAL", "payout_annual")
FIELD_LINK_NAME = os.environ.get("FIELD_LINK_NAME", "link_name")

DEFAULT_STATUS = "Signed - Onboarding"
DEFAULT_TIER = "Affiliate"
DEFAULT_PAYOUT_MONTHLY = 3.6
DEFAULT_PAYOUT_ANNUAL = 30.0

AIRTABLE_API_BASE = "https://api.airtable.com/v0"

app = FastAPI()


# ======================
# HELPERS
# ======================
def _normalize_letters_only(value: str) -> str:
    """
    Make link_name:
    - lowercase
    - no spaces
    - no numbers
    - letters only a-z
    """
    if not value:
        return ""

    # normalize accents -> ascii
    value = unicodedata.normalize("NFKD", value)
    value = "".join([c for c in value if not unicodedata.combining(c)])

    value = value.lower()
    # remove digits
    value = re.sub(r"\d+", "", value)
    # keep letters only
    value = re.sub(r"[^a-z]", "", value)

    return value


def make_link_name(creator_name: str, email: str) -> str:
    base = _normalize_letters_only(creator_name)

    if not base:
        # fallback from email local part (letters only)
        local = email.split("@")[0] if "@" in email else email
        base = _normalize_letters_only(local)

    if not base:
        base = "creator"

    # Keep it a reasonable length
    return base[:24]


def sheets_client():
    sa_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def read_sheet_rows() -> List[Tuple[str, str]]:
    """
    Returns list of (email, creator_name) from columns A and B.
    Skips header row automatically if it looks like a header.
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
        # pad to 2 cols
        email = (row[0] if len(row) > 0 else "").strip()
        name = (row[1] if len(row) > 1 else "").strip()

        # skip empty
        if not email and not name:
            continue

        # naive header skip
        if i == 0 and ("email" in email.lower() or "confirm" in email.lower()):
            continue

        if "@" not in email:
            # skip junk rows
            continue

        rows.append((email, name))

    return rows


def airtable_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
    }


def airtable_find_by_email(email: str) -> Optional[Dict]:
    """
    Returns the Airtable record if email exists, else None.
    """
    url = f"{AIRTABLE_API_BASE}/{AIRTABLE_BASE_ID}/{requests.utils.quote(AIRTABLE_TABLE_NAME)}"
    # Airtable formula: {Email}="a@b.com"
    params = {"filterByFormula": f'{{{FIELD_EMAIL}}}="{email}"', "maxRecords": 1}
    r = requests.get(url, headers=airtable_headers(), params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    records = data.get("records", [])
    return records[0] if records else None


def airtable_link_name_exists(link_name: str) -> bool:
    url = f"{AIRTABLE_API_BASE}/{AIRTABLE_BASE_ID}/{requests.utils.quote(AIRTABLE_TABLE_NAME)}"
    params = {"filterByFormula": f'{{{FIELD_LINK_NAME}}}="{link_name}"', "maxRecords": 1}
    r = requests.get(url, headers=airtable_headers(), params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return len(data.get("records", [])) > 0


def make_unique_link_name(creator_name: str, email: str) -> str:
    """
    Ensure link_name is unique without numbers by adding a letters-only suffix from email if needed.
    """
    base = make_link_name(creator_name, email)

    if not airtable_link_name_exists(base):
        return base

    # letters-only suffix from email local-part
    local = email.split("@")[0]
    suffix = _normalize_letters_only(local)

    # try a few variants (still letters only, no numbers)
    candidates = []
    if suffix:
        candidates.append((base[:18] + suffix[:6])[:24])
        candidates.append((base[:16] + suffix[:8])[:24])
        candidates.append((base[:14] + suffix[:10])[:24])

    # as a last resort: add "x" padding (still letters)
    candidates.append((base + "x")[:24])
    candidates.append((base + "xx")[:24])

    for c in candidates:
        if c and not airtable_link_name_exists(c):
            return c

    # final fallback
    return (base + "xxx")[:24]


def airtable_create_record(email: str, creator_name: str) -> Dict:
    url = f"{AIRTABLE_API_BASE}/{AIRTABLE_BASE_ID}/{requests.utils.quote(AIRTABLE_TABLE_NAME)}"

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
    r = requests.post(url, headers=airtable_headers(), json=payload, timeout=30)
    r.raise_for_status()
    return r.json()


# ======================
# WORKER LOOP
# ======================
_last_run = {"ts": None, "added": 0, "skipped": 0, "errors": 0}


def sync_once():
    added = 0
    skipped = 0
    errors = 0

    try:
        rows = read_sheet_rows()
    except Exception:
        _last_run["ts"] = int(time.time())
        _last_run["added"] = 0
        _last_run["skipped"] = 0
        _last_run["errors"] = 1
        return

    for email, name in rows:
        try:
            existing = airtable_find_by_email(email)
            if existing:
                skipped += 1
                continue

            airtable_create_record(email=email, creator_name=name)
            added += 1
        except Exception:
            errors += 1

    _last_run["ts"] = int(time.time())
    _last_run["added"] = added
    _last_run["skipped"] = skipped
    _last_run["errors"] = errors


def worker_loop():
    while True:
        sync_once()
        time.sleep(POLL_SECONDS)


@app.on_event("startup")
def on_startup():
    t = threading.Thread(target=worker_loop, daemon=True)
    t.start()


@app.get("/")
def root():
    return {
        "ok": True,
        "airtable_table": AIRTABLE_TABLE_NAME,
        "poll_seconds": POLL_SECONDS,
        "last_run": _last_run,
    }


@app.post("/sync-now")
def sync_now():
    sync_once()
    return {"ok": True, "last_run": _last_run}