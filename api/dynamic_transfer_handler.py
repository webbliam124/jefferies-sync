#!/usr/bin/env python3
"""
Dynamic transfer-destination endpoint for Vapi.

• Path: /api/dynamic_transfer_handler
• Triggered by the `transfer-destination-request` webhook.
• Looks at toolCall.arguments.listing_id ⇒ loads that listing from MongoDB
  ⇒ extracts the agent details ⇒ returns a warm-transfer destination.

Environment
───────────
MONGODB_URI       – same Atlas URI you already use
DB_NAME           – defaults to JefferiesJames
COLLECTION_NAME   – defaults to properties
DEFAULT_CALLER_ID – caller-ID to present to the agent
FALLBACK_NUMBER   – (optional) where to send calls if no agent found
"""

from __future__ import annotations

# stdlib
import io
import json
import os
from http.server import BaseHTTPRequestHandler
from typing import Any, Dict, List, Tuple

# third-party
from dotenv import load_dotenv
from pymongo import MongoClient

# ───────────────────────────────────────────────────────── helpers ──
CORS = {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


def _json_response(code: int,
                   payload: Dict[str, Any] | List[Any] | str
                   ) -> Tuple[int, list[tuple[str, str]], bytes]:
    headers = [("Content-Type", "application/json"), *CORS.items()]
    body = payload if isinstance(payload, str) else json.dumps(
        payload, ensure_ascii=False)
    return code, headers, body.encode()


# ───────────────────────────────────────────────── entry-point class
class handler(BaseHTTPRequestHandler):                      # noqa: N801
    """Vercel looks for a symbol literally called `handler`."""

    def log_message(self, *_):  # silence default log
        return

    # ---- CORS pre-flight -------------------------------------------
    def do_OPTIONS(self):  # pylint: disable=invalid-name
        self._send(*_json_response(204, ""))

    # ---- POST -------------------------------------------------------
    def do_POST(self):  # pylint: disable=invalid-name
        try:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            event = json.loads(body or "{}")
        except Exception:
            self._send(*_json_response(400, {"error": "invalid JSON"}))
            return

        if event.get("type") != "transfer-destination-request":
            self._send(*_json_response(400, {"error": "wrong webhook type"}))
            return

        self._send(*self._handle_transfer(event))

    # =================================================================
    # core business logic
    # =================================================================
    def _handle_transfer(
        self, event: Dict[str, Any]
    ) -> Tuple[int, list[tuple[str, str]], bytes]:

        artefact = event.get("artifact") or {}
        tool_call = artefact.get("toolCall") or {}
        arguments = tool_call.get("arguments") or {}

        listing_id = arguments.get("listing_id")
        if not listing_id:
            return _json_response(200, {"error": "missing listing_id"})

        # 1. fetch the listing straight from MongoDB
        try:
            load_dotenv()                                         # .env in Vercel
            mongo_uri = os.environ["MONGODB_URI"]
            db_name = os.getenv("DB_NAME", "JefferiesJames")
            coll_name = os.getenv("COLLECTION_NAME", "properties")

            client = MongoClient(mongo_uri, tz_aware=True)
            rec = client[db_name][coll_name].find_one(
                {"_id": listing_id}) or client[db_name][coll_name].find_one(
                {"id": listing_id})
        except Exception as exc:
            return _json_response(500, {"error": f"DB error {exc}"})

        if not rec:
            return _json_response(200, {"error": "listing not found"})

        # 2. pull the agent
        agent = (rec.get("agents") or [{}])[0]
        number = agent.get("phone_mobile") or agent.get("phone_direct")
        name = agent.get("name") or "our negotiator"

        if not number:
            number = os.getenv("FALLBACK_NUMBER")
            if not number:
                return _json_response(200, {"error": "agent has no number"})

        # 3. craft warm-transfer destination with summary
        transcript = artefact.get("transcript", "")
        caller_id = os.getenv("DEFAULT_CALLER_ID", "")

        dest = {
            "type": "number",
            "number": number,
            "message": f"Putting you through to {name}. Please hold.",
            "callerId": caller_id,
            "numberE164CheckEnabled": True,
            "transferPlan": {
                "mode": "warm-transfer-with-summary",
                "summaryPlan": {
                    "enabled": True,
                    "messages": [
                        {
                            "role": "system",
                            "content": "Provide a concise summary of the call."
                        },
                        {
                            "role": "user",
                            "content": f"Here is the transcript:\n\n{transcript}\n"
                        }
                    ]
                }
            }
        }

        return _json_response(200, {"destination": dest})

    # helper ----------------------------------------------------------
    def _send(self, code: int,
              headers: list[tuple[str, str]],
              body: bytes):
        self.send_response(code)
        for k, v in headers:
            self.send_header(k, v)
        self.end_headers()
        if body:
            buf = io.BytesIO(body)
            self.wfile.write(buf.read())
