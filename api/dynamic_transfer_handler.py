#!/usr/bin/env python3
"""
/api/dynamic_transfer_handler.py
────────────────────────────────
Webhook that services Vapi’s `transfer-destination-request` event.

Key features
• Looks up the agent’s phone for a given listing_id in MongoDB.
• Normalises the phone into E.164 (UK default; adapt as needed).
• Returns a **warm-transfer-experimental** destination so the caller
  stays on the line while Vapi dials the agent.
• If the agent does not answer (voicemail / timeout) Vapi delivers a
  polite fallback message and returns control to the assistant, so the
  AI can continue the conversation.
• If the listing is unknown or lacks a phone, we send an `error`
  response – the assistant will apologise and carry on.

Environment variables (set in Vercel → Settings → Environment):
    MONGODB_URI        Mongo Atlas SRV string
    DB_NAME            (default: JefferiesJames)
    COLLECTION_NAME    (default: properties)
    DEFAULT_CALLER_ID  CLI to present to the agent (optional)
    COUNTRY_DIAL_CODE  e.g. "+44"  (used when numbers lack + prefix)
"""

from __future__ import annotations

# stdlib
import json
import os
import re
from http.server import BaseHTTPRequestHandler
from typing import Any, Dict, Tuple

# third-party
from dotenv import load_dotenv
from pymongo import MongoClient

# ────────────────────────────────────── helpers

CORS = {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


def _json(code: int, payload: Dict[str, Any] | str) -> Tuple[int, list[tuple[str, str]], bytes]:
    headers = [("Content-Type", "application/json"), *CORS.items()]
    body = payload if isinstance(payload, str) else json.dumps(
        payload, ensure_ascii=False)
    return code, headers, body.encode()


def _normalise(number: str | None) -> str | None:
    """
    Return number in E.164.  Crude UK-centric normaliser:
    • strip non-digits
    • if starts with 0  → replace leading 0 with default dial code
    • if already starts with + → leave as is
    """
    if not number:
        return None

    number = re.sub(r"[^\d+]", "", number)          # keep digits / +
    if number.startswith("+"):
        return number
    if number.startswith("0"):
        return os.getenv("COUNTRY_DIAL_CODE", "+44") + number.lstrip("0")
    # already international but missing + (e.g. 4475…)
    if number[0].isdigit() and len(number) > 10:
        return "+" + number
    return None  # invalid


# ───────────────────────────────── entry-point
class handler(BaseHTTPRequestHandler):                                   # noqa: N801
    """Vercel looks for an object literally called `handler`."""

    # silence access log
    def log_message(self, *_):
        return

    # ---------- OPTIONS (CORS pre-flight) ----------
    def do_OPTIONS(self):                                                # pylint: disable=invalid-name
        self._send(*_json(204, ""))

    # ---------------- POST -------------------------
    def do_POST(self):                                                   # pylint: disable=invalid-name
        try:
            length = int(self.headers.get("Content-Length", "0"))
            event = json.loads(self.rfile.read(length) or "{}")
        except Exception:
            return self._send(*_json(400, {"error": "invalid JSON"}))

        if event.get("type") != "transfer-destination-request":
            return self._send(*_json(400, {"error": "wrong webhook type"}))

        self._send(*self._handle(event))

    # ================ core logic ===================
    def _handle(self, event: Dict[str, Any]) -> Tuple[int, list[tuple[str, str]], bytes]:
        artefact = event.get("artifact") or {}
        args = (artefact.get("toolCall") or {}).get("arguments") or {}

        listing_id = args.get("listing_id")
        if not listing_id:
            return _json(200, {"error": "missing listing_id"})

        # -- 1. Mongo look-up --------------------------------------------------
        try:
            load_dotenv()                                                # allows local .env
            client = MongoClient(os.environ["MONGODB_URI"], tz_aware=True)
            db = client[os.getenv("DB_NAME", "JefferiesJames")]
            col = db[os.getenv("COLLECTION_NAME", "properties")]

            rec = col.find_one({"_id": listing_id}) or col.find_one(
                {"id": listing_id})
        except Exception as exc:
            return _json(500, {"error": f"DB error: {exc}"})

        if not rec:
            return _json(200, {"error": "listing not found"})

        # -- 2. Pick the first agent ------------------------------------------
        agent = (rec.get("agents") or [{}])[0]
        raw_num = agent.get("phone_mobile") or agent.get("phone_direct")
        number = _normalise(raw_num)
        name = agent.get("name") or "our negotiator"

        if not number:
            return _json(200, {"error": "agent has no valid phone number"})

        # -- 3. Build warm-transfer-experimental destination -------------------
        transcript = artefact.get("transcript", "")

        destination = {
            "type": "number",
            "number": number,
            "numberE164CheckEnabled": True,
            "callerId": os.getenv("DEFAULT_CALLER_ID", ""),
            "message": f"Connecting you to {name}.",
            "transferPlan": {
                "mode": "warm-transfer-experimental",
                "message": f"Transferring a caller who wishes to discuss listing {listing_id}.",
                "summaryPlan": {
                    "enabled": True,
                    "messages": [
                        {
                            "role": "system",
                            "content": "Please provide a concise summary of the call."
                        },
                        {
                            "role": "user",
                            "content": f"Here is the transcript:\n\n{transcript}\n"
                        }
                    ]
                },
                "fallbackPlan": {
                    "message": (
                        "The agent did not answer. I'll stay on the line so we can "
                        "continue our conversation."
                    ),
                    "endCallEnabled": False          # hand control back to the AI
                }
            }
        }

        return _json(200, {"destination": destination})

    # ------------ helper to write the response ------------
    def _send(self, code: int, headers: list[tuple[str, str]], body: bytes):
        self.send_response(code)
        for k, v in headers:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)
