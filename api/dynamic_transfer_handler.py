#!/usr/bin/env python3
"""
/api/dynamic_transfer_handler.py
────────────────────────────────
Dynamic warm-transfer webhook for Vapi.

• Looks up an agent’s phone from MongoDB using `listing_id`
• Normalises into E.164 (+44 default)
• Returns warm-transfer-experimental JSON with a summary + voicemail fallback
• Never lets the call drop: unknown events and internal errors are handled
  with HTTP 200 and an {error: "..."} payload so control stays with the AI.

ENV VARS  (Vercel → Settings → Environment)
────────
MONGODB_URI        Mongo Atlas SRV
DB_NAME            default JefferiesJames
COLLECTION_NAME    default properties
DEFAULT_CALLER_ID  CLI shown to the agent (optional)
COUNTRY_DIAL_CODE  default “+44”
FALLBACK_NUMBER    office line if agent has no number (optional)
"""

from __future__ import annotations

# ── stdlib ───────────────────────────────────────────────────────────
import json
import os
import re
from http.server import BaseHTTPRequestHandler
from typing import Any, Dict, Tuple

# ── third-party ──────────────────────────────────────────────────────
from dotenv import load_dotenv
from pymongo import MongoClient

# ─────────────────────────────────────────────────────────────────────
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


# --- phone normaliser ------------------------------------------------
def _normalise(num: str | None) -> str | None:
    """Return E.164 number (UK default)."""
    if not num:
        return None
    num = re.sub(r"[^\d+]", "", num)
    if num.startswith("+"):
        return num
    if num.startswith("0"):
        return os.getenv("COUNTRY_DIAL_CODE", "+44") + num.lstrip("0")
    if len(num) > 10 and num[0].isdigit():
        return "+" + num
    return None


# ────────────────────────── HTTP entry-point recognised by Vercel ────
class handler(BaseHTTPRequestHandler):                                    # noqa: N801
    """Vercel expects a symbol literally called `handler`."""

    # silence default log
    def log_message(self, *_):
        return

    # CORS pre-flight
    def do_OPTIONS(self):                                                 # pylint: disable=invalid-name
        self._respond(*_json(204, ""))

    # POST
    def do_POST(self):                                                    # pylint: disable=invalid-name
        try:
            length = int(self.headers.get("Content-Length", "0"))
            event = json.loads(self.rfile.read(length) or "{}")
        except Exception:
            return self._respond(*_json(200, {"error": "invalid JSON"}))

        # Ignore any webhook that is NOT a transfer-destination request
        if event.get("type") != "transfer-destination-request":
            # noop → AI continues
            return self._respond(*_json(200, {}))

        self._respond(*self._handle(event))

    # ─────────────── core logic ───────────────────────────────────────
    def _handle(self, event: Dict[str, Any]) -> Tuple[int, list[tuple[str, str]], bytes]:
        artefact = event.get("artifact") or {}
        args = (artefact.get("toolCall") or {}).get("arguments") or {}
        listing_id = args.get("listing_id")

        if not listing_id:
            return _json(200, {"error": "missing listing_id"})

        # 1. Mongo fetch ------------------------------------------------
        try:
            load_dotenv()  # allow local .env for dev
            client = MongoClient(os.environ["MONGODB_URI"], tz_aware=True)
            col = client[
                os.getenv("DB_NAME", "JefferiesJames")
            ][os.getenv("COLLECTION_NAME", "properties")]

            rec = col.find_one({"_id": listing_id}) or col.find_one(
                {"id": listing_id})
        except Exception as exc:
            return _json(200, {"error": f"DB error: {exc}"})

        if not rec:
            return _json(200, {"error": "listing not found"})

        # 2. Agent phone selection -------------------------------------
        agent = (rec.get("agents") or [{}])[0]

        phones = [
            agent.get("phone_mobile"),
            agent.get("phone_direct"),
            os.getenv("FALLBACK_NUMBER"),
        ]
        number = next((n for n in (_normalise(p) for p in phones) if n), None)

        if not number:
            return _json(200, {"error": "no valid phone number available"})

        name = agent.get("name") or "our negotiator"
        transcript = artefact.get("transcript", "")

        # 3. Build destination -----------------------------------------
        destination = {
            "type": "number",
            "number": number,
            "callerId": os.getenv("DEFAULT_CALLER_ID", ""),
            "message": f"Connecting you to {name}.",
            "transferPlan": {
                "mode": "warm-transfer-experimental",
                "message": f"Transferring a caller about listing {listing_id}.",
                "summaryPlan": {
                    "enabled": True,
                    "messages": [
                        {
                            "role": "system",
                            "content": "Please provide a concise summary of the call."
                        },
                        {
                            "role": "user",
                            "content": (
                                f"Listing ID: {listing_id}\n"
                                f"Caller transcript:\n\n{transcript}\n"
                            )
                        }
                    ]
                },
                "fallbackPlan": {
                    "message": (
                        "The agent did not answer. I'll stay on the line so we can "
                        "continue our conversation."
                    ),
                    "endCallEnabled": False
                }
            }
        }

        return _json(200, {"destination": destination})

    # ───────────── helper to send the HTTP response ───────────────────
    def _respond(self, code: int, headers: list[tuple[str, str]], body: bytes):
        self.send_response(code)
        for k, v in headers:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)
