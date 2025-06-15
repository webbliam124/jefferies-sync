#!/usr/bin/env python3
"""
Dynamic warm-transfer webhook for Vapi.ai

• Accepts a `transfer-destination-request`
• Looks up the negotiator for <listing_id> in MongoDB
• Replies with a JSON “destination” object Vapi can dial
"""

from __future__ import annotations

import json
import os
import re
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Tuple

from dotenv import load_dotenv
from pymongo import MongoClient

# ── environment ------------------------------------------------------

load_dotenv()  # read .env once at start-up

# Re-use the client across invocations – faster and avoids connection storms
_COLLECTION = (
    MongoClient(os.environ["MONGODB_URI"], tz_aware=True)
    [os.getenv("DB_NAME", "JefferiesJames")]
    [os.getenv("COLLECTION_NAME", "properties")]
)

# ── small helpers ----------------------------------------------------


def _json(code: int, payload: Dict[str, Any] | str) -> Tuple[int, list, bytes]:
    """Return (status, headers, body) as the handler expects."""
    headers = [("Content-Type", "application/json")]
    return code, headers, (
        payload.encode() if isinstance(payload, str) else json.dumps(payload).encode()
    )


def _log(*msg):  # print to stderr to surface in Vercel logs
    print(*msg, file=sys.stderr, flush=True)


def _norm(number: str | None) -> str | None:
    """Normalise a phone number to E.164 (+27…).  Return None if invalid."""
    if not number:
        return None

    number = re.sub(r"[^\d+]", "", number)  # strip spaces, dashes, braces

    # already in E.164
    if number.startswith("+"):
        return number

    # local 0… number → prefix with country dial code
    if number.startswith("0"):
        return os.getenv("COUNTRY_DIAL_CODE", "+44") + number.lstrip("0")

    # bare international digits, guess it is already full
    if len(number) > 10:
        return "+" + number

    return None


# ── HTTP handler -----------------------------------------------------


class handler(BaseHTTPRequestHandler):  # noqa: N801 (Vercel’s naming convention)
    """Single-file HTTP webhook for Vercel / AWS Lambda / Google Cloud Functions."""

    # Silence the default “code 200, blah-blah” access log
    def log_message(self, *_):  # noqa: D401
        return

    def do_OPTIONS(self):
        self._send(*_json(204, ""))  # pre-flight CORS

    def do_POST(self):  # noqa: D401
        try:
            raw = self.rfile.read(
                int(self.headers.get("Content-Length", "0")) or 0)
            event = json.loads(raw or "{}")
        except Exception:
            return self._send(*_json(200, {"error": "invalid JSON"}))

        if event.get("type") != "transfer-destination-request":
            # ignore everything else (assistant-message, ping, etc.)
            return self._send(*_json(200, {}))

        self._send(*self._handle_transfer(event))

    # -----------------------------------------------------------------

    def _handle_transfer(self, event: Dict[str, Any]) -> Tuple[int, list, bytes]:
        """Build the destination JSON or return an error for the assistant."""
        artefact = event.get("artifact") or {}
        tool_call = artefact.get("toolCall") or {}

        # Vapi sometimes serialises complex args as a JSON string
        raw_args = tool_call.get("arguments", {})
        if isinstance(raw_args, str):
            try:
                raw_args = json.loads(raw_args)
            except json.JSONDecodeError:
                raw_args = {}

        listing_id: str | None = raw_args.get("listing_id")
        _log("listing_id:", listing_id)

        if not listing_id:
            return _json(200, {"error": "missing listing_id"})

        # ── Mongo lookup ──────────────────────────────────────────────
        try:
            rec = _COLLECTION.find_one({"_id": listing_id}) or _COLLECTION.find_one(
                {"id": listing_id}
            )
        except Exception as exc:
            return _json(200, {"error": f"DB error: {exc}"})

        if not rec:
            return _json(200, {"error": "listing not found"})

        agent = (rec.get("agents") or [{}])[0]
        phones = [
            agent.get("phone_mobile"),
            agent.get("phone_direct"),
            os.getenv("FALLBACK_NUMBER"),
        ]

        number = next((n for n in (_norm(p) for p in phones) if n), None)
        _log("dial:", number or "—")

        if not number:
            return _json(200, {"error": "no valid phone"})

        destination: Dict[str, Any] = {
            "type": "number",
            "number": number,
            "message": f"Connecting you to {agent.get('name', 'our negotiator')}.",
            # preserve original caller CLI; some PBXs reject anonymous
            "callerId": event.get("phoneNumber", os.getenv("DEFAULT_CALLER_ID", "")),
            "numberE164CheckEnabled": True,
            "transferPlan": {  # warm transfer with fallback
                "mode": "warm-transfer-experimental",
                "fallbackPlan": {
                    "message": "The agent did not answer.",
                    "endCallEnabled": False,
                },
            },
        }
        return _json(200, {"destination": destination})

    # -----------------------------------------------------------------

    def _send(self, status: int, headers: list, body: bytes):
        self.send_response(status)
        for k, v in headers:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)


# ── Local smoke-test harness ----------------------------------------

if __name__ == "__main__":
    """
    Run `python webhook.py 8000` and then

      curl -X POST http://localhost:8000 \
           -H "Content-Type: application/json" \
           -d '{"type":"transfer-destination-request",
                "phoneNumber":"+27831234567",
                "artifact":{"toolCall":{
                  "name":"dynamicDestinationTransferCall",
                  "arguments":"{\"listing_id\":\"ABC123\"}"}}}'

    to see exactly what Vapi will receive.
    """
    port = int(os.getenv("PORT", 8000))
    _log(f"★ listening on http://0.0.0.0:{port}")
    HTTPServer(("", port), handler).serve_forever()
