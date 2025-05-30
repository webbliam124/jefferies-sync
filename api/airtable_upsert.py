#!/usr/bin/env python3
"""
Upsert a contact into Airtable.

Behaviour
─────────
• Primary match key  : User ID
• Secondary match key: Phone Number
• Optional/extra fields:
      – User Mood
      – Interested Property
      – Viewing?
      – Available Date
• Retries once if Airtable returns non-OK.

Accepted payload shape (always):
    {"inputVars": { … }}

Environment variables (set in Vercel):
    AIRTABLE_BASE   = appXXXXXXXXXXXXXX
    AIRTABLE_TABLE  = tblXXXXXXXXXXXXXX
    AIRTABLE_PAT    = patXXXXXXXXXXXXXX
"""

from __future__ import annotations

import os
import urllib.parse
from typing import Dict, Any

import requests
from dotenv import load_dotenv

load_dotenv()               # works for `vercel dev`

# ─────────────────────────────────────────────────────────────


def _find_one(
    base: str, table: str, field: str, value: str, headers: Dict[str, str]
) -> Dict[str, Any] | None:
    """Return first Airtable record where {field} == value, else None."""
    formula = urllib.parse.quote_plus(f"{{{field}}}='{value}'")
    url = f"https://api.airtable.com/v0/{base}/{table}?filterByFormula={formula}&maxRecords=1"
    resp = requests.get(url, headers=headers, timeout=10)
    if not resp.ok:
        return None
    recs = resp.json().get("records") or []
    return recs[0] if recs else None


# ─────────────────────────────────────────────────────────────
def upsert_to_airtable(payload: Dict[str, Any]) -> Dict[str, str]:
    """
    Upsert one contact; returns:
        {"status": "success" | "upsert-failed", "debug": "..."}
    """
    iv = payload.get("inputVars", {})
    user_id: str = iv["userId"]                 # KeyError if absent
    phone = iv.get("phone")
    name = iv.get("name")
    email = iv.get("email")
    intent = iv.get("intent")
    mood = iv.get("userMood")
    prop_int = iv.get("interestedProperty")
    viewing = iv.get("viewing")
    avail_date = iv.get("availableDate")

    base = iv.get("baseId") or os.getenv("AIRTABLE_BASE")
    table = iv.get("tableId") or os.getenv("AIRTABLE_TABLE")
    pat = iv.get("pat") or os.getenv("AIRTABLE_PAT")
    if not all((base, table, pat)):
        raise RuntimeError("Missing Airtable credentials")

    headers = {
        "Authorization": f"Bearer {pat}",
        "Content-Type":  "application/json",
    }

    # 1 lookup: by User ID, else Phone
    record = _find_one(base, table, "User ID", user_id, headers)
    if record is None and phone:
        record = _find_one(base, table, "Phone Number", phone, headers)

    # 2 build Airtable fields payload (omit null/blank)
    fields = {k: v for k, v in {
        "User ID":             user_id,
        "Name":                name,
        "Phone Number":        phone,
        "Email Address":       email,
        "User Intent":         intent,
        "User Mood":           mood,
        "Interested Property": prop_int,
        "Viewing?":            viewing,
        "Available Date":      avail_date,
    }.items() if v not in (None, "")}

    # 3 PATCH vs POST
    if record:
        url = f"https://api.airtable.com/v0/{base}/{table}/{record['id']}"
        method = requests.patch
    else:
        url = f"https://api.airtable.com/v0/{base}/{table}"
        method = requests.post

    # ─── DEBUG — log full request URL and Airtable's raw response ───
    resp = method(url, headers=headers, json={"fields": fields}, timeout=10)
    print("Airtable⇢", resp.status_code, url, resp.text, flush=True)
    ok = resp.ok or method(url, headers=headers, json={
                           "fields": fields}, timeout=10).ok

    ok = (
        method(url, headers=headers, json={"fields": fields}, timeout=10).ok or
        method(url, headers=headers, json={"fields": fields}, timeout=10).ok
    )

    return {
        "status": "success" if ok else "upsert-failed",
        "debug":  f"Airtable {'upsert ✓' if ok else 'upsert ✗'} for {user_id}",
    }


# Optional CLI smoke-test
if __name__ == "__main__":
    print(
        upsert_to_airtable(
            {"inputVars": {"userId": "cli-test-001", "phone": "+27 11 555 1234"}}
        )
    )
