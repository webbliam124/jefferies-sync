#!/usr/bin/env python3
# File: api/vapi_handler.py
"""
VAPI → Property search proxy for Vercel
───────────────────────────────────────
• POST /api/vapi_handler  – JSON body (see schema below)
• OPTIONS                 – CORS pre-flight

Body schema (simplified):

{
  "name": "Alice",                 # optional – voice greeting only
  "location": "London",            # required – text search / keyword
  "status": "current|sold",        # optional – default "current"
  "beds_min": 2,                   # optional
  "baths_min": 1,                  # optional
  "price_min": 500000,             # optional
  "price_max": 1000000,            # optional
  "phone_number": "2776…",         # optional – send brochure via WhatsApp
  "dry": true                      # optional – skip WhatsApp when true
}

Success → 200 JSON summary (same as property_search CLI).  
Errors  → 4xx / 5xx JSON `{"error": "…"}`
"""

from __future__ import annotations

import json
import os
import traceback
from typing import Any, Dict, Tuple

from dotenv import load_dotenv

# the property_search helper functions live one directory up
from lib.property_search import (  # pylint: disable=import-error
    Settings,
    PropertyRepository,
    summarise,
    send_whatsapp,
)

# ──────────────── CORS helpers ──────────────────────────────────
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


def _response(
    status: int,
    payload: Dict[str, Any] | str,
    extra_headers: Dict[str, str] | None = None,
) -> Tuple[int, Dict[str, str], str]:
    """Return Vercel-style response tuple."""
    headers = {
        "Content-Type": "application/json",
        **CORS_HEADERS,
        **(extra_headers or {}),
    }
    body = payload if isinstance(payload, str) else json.dumps(
        payload, ensure_ascii=False)
    return status, headers, body


def _bad(status: int, msg: str) -> Tuple[int, Dict[str, str], str]:
    return _response(status, {"error": msg})


# ──────────────── Vercel entrypoint ─────────────────────────────
def handler(request):  # Vercel python runtime passes a werkzeug Request-like obj
    # ---- CORS pre-flight --------------------------------------------------
    if request.method == "OPTIONS":
        return _response(204, "")

    if request.method != "POST":
        return _bad(405, "only POST supported")

    # ---- parse JSON body --------------------------------------------------
    try:
        body = request.get_json(force=True, silent=False)
    except Exception:
        return _bad(400, "Invalid JSON")

    if not isinstance(body, dict):
        return _bad(400, "Body must be an object")

    keyword = (body.get("location") or "").strip()
    if not keyword:
        return _bad(400, "location is required")

    # ---- build search query ----------------------------------------------
    query = {
        "keyword": keyword,
        "status": body.get("status", "current"),
    }
    for field in ("beds_min", "baths_min", "price_min", "price_max", "purpose"):
        if field in body:
            query[field] = body[field]

    # ---- init repository --------------------------------------------------
    load_dotenv()
    try:
        cfg = Settings.from_env()
        repo = PropertyRepository(cfg)
        if not repo.ping():
            return _bad(503, "database unavailable")
    except Exception as exc:  # pylint: disable=broad-except
        return _bad(500, f"config error: {exc}")

    # ---- search -----------------------------------------------------------
    try:
        doc, _tier = repo.find_one(query)
    except ValueError as exc:
        return _bad(400, str(exc))
    except Exception as exc:  # pylint: disable=broad-except
        traceback.print_exc()
        return _bad(500, f"search failed: {exc}")

    if not doc:
        return _bad(404, "no matching property")

    summary = summarise(doc)

    # ---- optional WhatsApp ------------------------------------------------
    phone = (body.get("phone_number") or "").strip()
    if phone and not body.get("dry"):
        try:
            send_whatsapp(cfg, phone, summary)
            summary["whatsapp"] = "sent"
        except Exception as exc:  # pylint: disable=broad-except
            summary["whatsapp"] = f"error: {exc}"

    return _response(200, summary)
