#!/usr/bin/env python3
"""
/api/dynamic_transfer_handler.py
────────────────────────────────
Webhook for Vapi’s `transfer-destination-request` event.

• Receives POST JSON from Vapi
• Extracts any arguments you passed in the tool call   (eg. listing_id)
• Looks up the right agent in MongoDB
• Responds with either {"destination": …} or {"error": …}

Environment variables (add them in Vercel → Settings → Environment):
    MONGODB_URI        Mongo Atlas SRV string
    DB_NAME            Defaults to JefferiesJames
    COLLECTION_NAME    Defaults to properties
    DEFAULT_CALLER_ID  Number you want the agent to see (optional)
    FALLBACK_NUMBER    Where to send calls if the listing has no agent
"""

from __future__ import annotations

# stdlib
import json
import os
from http.server import BaseHTTPRequestHandler
from typing import Any, Dict, Tuple

# third-party
from dotenv import load_dotenv
from pymongo import MongoClient

# ───────────────────────────────────────── helpers
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


# ───────────────────────────────────────── entry-point recognised by Vercel
class handler(BaseHTTPRequestHandler):                        # noqa: N801
    """Vercel looks for a symbol literally called `handler`."""

    def log_message(self, *_):  # silence default access log
        return

    # ---- CORS pre-flight ----
    def do_OPTIONS(self):                                      # pylint: disable=invalid-name
        self._send(*_json(204, ""))

    # ---- POST --------------
    def do_POST(self):                                         # pylint: disable=invalid-name
        try:
            length = int(self.headers.get("Content-Length", "0"))
            event = json.loads(self.rfile.read(length) or "{}")
        except Exception:
            return self._send(*_json(400, {"error": "invalid JSON"}))

        if event.get("type") != "transfer-destination-request":
            return self._send(*_json(400, {"error": "wrong webhook type"}))

        self._send(*self._handle(event))

    # ───────────────────────────────────── core logic
    def _handle(self, event: Dict[str, Any]) -> Tuple[int, list[tuple[str, str]], bytes]:
        artefact = event.get("artifact") or {}
        args = (artefact.get("toolCall") or {}).get("arguments") or {}

        listing_id = args.get("listing_id")
        if not listing_id:
            return _json(200, {"error": "missing listing_id"})

        # 1. Look up the listing → agent
        try:
            # allows local `.env` during testing
            load_dotenv()
            client = MongoClient(os.environ["MONGODB_URI"], tz_aware=True)
            rec = client[os.getenv("DB_NAME", "JefferiesJames")][os.getenv("COLLECTION_NAME", "properties")] \
                .find_one({"_id": listing_id}) \
                or client[os.getenv("DB_NAME", "JefferiesJames")][os.getenv("COLLECTION_NAME", "properties")] \
                .find_one({"id": listing_id})
        except Exception as exc:                               # DB connection or query failed
            return _json(500, {"error": f"DB error: {exc}"})

        if not rec:
            return _json(200, {"error": "listing not found"})

        agent = (rec.get("agents") or [{}])[0]
        number = agent.get("phone_mobile") or agent.get("phone_direct")
        name = agent.get("name") or "our negotiator"

        if not number:
            number = os.getenv("FALLBACK_NUMBER")
            if not number:
                return _json(200, {"error": "agent has no number"})

        # 2. Craft **warm-transfer with summary** destination
        transcript = artefact.get("transcript", "")
        destination = {
            "type":   "number",
            "number": number,
            "message": f"Connecting you to {name}.",
            "callerId": os.getenv("DEFAULT_CALLER_ID", ""),
            "transferPlan": {
                "mode": "warm-transfer-with-summary",
                "summaryPlan": {
                    "enabled": True,
                    "messages": [
                        {"role": "system",
                            "content": "Please provide a concise summary of the call."},
                        {"role": "user",
                            "content": f"Here is the transcript:\n\n{transcript}\n"}
                    ],
                },
            },
        }

        return _json(200, {"destination": destination})

    # ---- internal helper ----
    def _send(self, code: int, headers: list[tuple[str, str]], body: bytes):
        self.send_response(code)
        for k, v in headers:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)
