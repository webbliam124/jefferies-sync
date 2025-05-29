# api/airtable_upsert.py
"""
Serverless function for Vercel that *upserts* a contact into Airtable.

Key behaviour
─────────────
• Primary match key  : `User ID`
• Secondary match key: `Phone Number` (used only if no User-ID record found)
• One record per phone number – avoids duplicates across platforms
• New optional fields:
    – User Mood
    – Interested Property
    – Viewing?
    – Available Date

Request formats accepted
────────────────────────
1. Vapi tool-call payloads   (payload["message"]["type"] == "tool-calls")
2. Legacy JSON              {"inputVars": {...}}

Runtime & packages
──────────────────
Python ≥3.12
Requires `requests` and `python-dotenv`
"""

from __future__ import annotations

import json
import os
import urllib.parse
from http.server import BaseHTTPRequestHandler

import requests
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────
#  Configuration helpers
# ─────────────────────────────────────────────────────────────

load_dotenv()                                      # only used locally
AIRTABLE_BASE = os.getenv("AIRTABLE_BASE")
AIRTABLE_TABLE = os.getenv("AIRTABLE_TABLE")
AIRTABLE_PAT = os.getenv("AIRTABLE_PAT")
VAPI_API_KEY = os.getenv("VAPI_API_KEY") or "super-secret-21C"

HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_PAT}" if AIRTABLE_PAT else "",
    "Content-Type":  "application/json",
}

# ─────────────────────────────────────────────────────────────
#  Business logic
# ─────────────────────────────────────────────────────────────


def _find_record(base: str, table: str, field: str, value: str) -> dict | None:
    """Return the first Airtable record whose {field} equals value (or None)."""
    formula = urllib.parse.quote_plus(f"{{{field}}}='{value}'")
    url = f"https://api.airtable.com/v0/{base}/{table}?filterByFormula={formula}&maxRecords=1"
    response = requests.get(url, headers=HEADERS, timeout=10)
    records = response.json().get("records") if response.ok else None
    return records[0] if records else None


def upsert_to_airtable(payload: dict) -> dict:
    """Upsert one contact, returning a status dict suitable for the caller."""
    iv = payload.get("inputVars", {})

    # ───── required & optional vars ─────────────────────────
    # raises KeyError if absent
    user_id: str = iv["userId"]
    phone: str | None = iv.get("phone")             # may be missing / empty
    name = iv.get("name")
    email = iv.get("email")
    intent = iv.get("intent")
    mood = iv.get("userMood")
    property_ = iv.get("interestedProperty")
    viewing = iv.get("viewing")
    date_av = iv.get("availableDate")

    # allow overrides per-call (handy for tests)
    base_id = iv.get("baseId") or AIRTABLE_BASE
    table_id = iv.get("tableId") or AIRTABLE_TABLE
    pat = iv.get("pat") or AIRTABLE_PAT
    if not all((base_id, table_id, pat)):
        raise RuntimeError("Missing Airtable credentials (base, table or PAT)")

    HEADERS["Authorization"] = f"Bearer {pat}"

    # ───── 1. look-up: by User ID, else by Phone Number ────
    record = _find_record(base_id, table_id, "User ID", user_id)
    if record is None and phone:
        record = _find_record(base_id, table_id, "Phone Number", phone)

    # ───── 2. build fields payload (exclude Nones) ─────────
    fields = {k: v for k, v in {
        "User ID":            user_id,
        "Name":               name,
        "Phone Number":       phone,
        "Email Address":      email,
        "User Intent":        intent,
        "User Mood":          mood,
        "Interested Property": property_,
        "Viewing?":           viewing,
        "Available Date":     date_av,
    }.items() if v not in (None, "")}

    # ───── 3. choose method & URL ──────────────────────────
    if record:
        url = f"https://api.airtable.com/v0/{base_id}/{table_id}/{record['id']}"
        method = requests.patch
    else:
        url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
        method = requests.post

    # ───── 4. send (retry once) ────────────────────────────
    ok = (
        method(url, headers=HEADERS, json={"fields": fields}, timeout=10).ok
        or method(url, headers=HEADERS, json={"fields": fields}, timeout=10).ok
    )

    return {
        "status": "success" if ok else "upsert-failed",
        "debug":  f"Airtable {'upsert ✓' if ok else 'upsert ✗'} for {user_id}",
    }


# ─────────────────────────────────────────────────────────────
#  Vercel HTTP entry-point
# ─────────────────────────────────────────────────────────────


class handler(BaseHTTPRequestHandler):
    """
    Vercel’s Python runtime instantiates this class.  
    Implement OPTIONS for CORS preflight and POST for main traffic.
    """

    # helper: write JSON + CORS headers
    def _json(self, code: int, obj: dict) -> None:
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(obj).encode())

    # ───── CORS pre-flight ─────────────────────────────────
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers",
                         "Content-Type, x-api-key")
        self.end_headers()

    # ───── main POST endpoint ──────────────────────────────
    def do_POST(self):
        # 0. API-key verification  (case-insensitive header)
        got_key = next((v for k, v in self.headers.items()
                       if k.lower() == "x-api-key"), None)
        if got_key != VAPI_API_KEY:
            return self._json(403, {"error": "Forbidden"})

        # 1. read body (tolerate empty)
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b"{}"

        try:
            payload = json.loads(body.decode() or "{}")

            # 2A. Vapi tool-call
            if payload.get("message", {}).get("type") == "tool-calls":
                tool_call = payload["message"]["toolCallList"][0]
                arg_source = tool_call.get("function", {}).get(
                    "arguments") or tool_call.get("arguments")
                result = upsert_to_airtable({"inputVars": arg_source})
                return self._json(200, {
                    "results": [{
                        "toolCallId": tool_call["id"],
                        "result":     result["status"],
                    }]
                })

            # 2B. Legacy / Voiceflow
            if "inputVars" in payload:
                return self._json(200, upsert_to_airtable(payload))

            # 3. otherwise…
            raise ValueError("Unrecognised request schema")

        except Exception as exc:                # pylint: disable=broad-except
            print("ERROR:", exc, flush=True)    # appears in Vercel logs
            self._json(500, {"error": str(exc)})
