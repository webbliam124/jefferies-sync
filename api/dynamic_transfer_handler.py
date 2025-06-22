#!/usr/bin/env python3
"""
Dynamic warm-transfer webhook – standalone flavour.
Useful only if Vapi hits this path directly.
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

# ── env & logging ────────────────────────────────────────────────────
load_dotenv()
DEBUG = os.getenv("DEBUG") == "1"


def _log(*msg):
    if DEBUG:
        print(*msg, file=sys.stderr, flush=True)


COLL = (
    MongoClient(os.environ["MONGODB_URI"], tz_aware=True)
    [os.getenv("DB_NAME", "JefferiesJames")]
    [os.getenv("COLLECTION_NAME", "properties")]
)

DIAL_CODE = os.getenv("COUNTRY_DIAL_CODE", "+44")
FALLBACK_NUMBER = os.getenv("FALLBACK_NUMBER")
CLI_DEFAULT = os.getenv("DEFAULT_CALLER_ID", "")

# ── helpers ──────────────────────────────────────────────────────────


def _json(code: int, payload: Dict[str, Any] | str) -> Tuple[int, list, bytes]:
    return code, [("Content-Type", "application/json")], (
        payload.encode() if isinstance(payload, str) else json.dumps(payload).encode()
    )


def _norm(num: str | None) -> str | None:
    if not num:
        return None
    num = re.sub(r"[^\d+]", "", num)
    if num.startswith("+"):
        return num
    if num.startswith("0"):
        return DIAL_CODE + num.lstrip("0")
    if len(num) > 10:
        return "+" + num
    return None

# ── HTTP handler ─────────────────────────────────────────────────────


class handler(BaseHTTPRequestHandler):  # noqa: N801
    def log_message(self, *_):  # silence default
        return

    def do_POST(self):
        try:
            raw = self.rfile.read(
                int(self.headers.get("Content-Length", "0")) or 0)
            evt = json.loads(raw or "{}")
        except Exception:
            return self._send(*_json(200, {"error": "invalid JSON"}))

        if evt.get("type") != "transfer-destination-request":
            return self._send(*_json(200, {}))  # ignore everything else

        self._send(*self._handle_transfer(evt))

    # -----------------------------------------------------------------
    def _handle_transfer(self, evt: Dict[str, Any]):
        args = (evt.get("artifact") or {}).get(
            "toolCall", {}).get("arguments", {})
        if isinstance(args, str):
            try:
                args = json.loads(args)
            except json.JSONDecodeError:
                args = {}

        listing_id = args.get("listing_id")
        _log("listing_id:", listing_id)

        if not listing_id:
            return _json(200, {"error": "missing listing_id"})

        rec = COLL.find_one({"_id": listing_id}) or COLL.find_one(
            {"id": listing_id})
        agent = (rec.get("agents") or [{}])[0] if rec else {}

        phones = [agent.get("phone_mobile"), agent.get(
            "phone_direct"), FALLBACK_NUMBER]
        number = next((n for n in (_norm(p) for p in phones) if n), None)
        _log("dial:", number or "—")

        if not number:
            return _json(200, {"error": "no valid phone"})

        dest = {
            "type": "number",
            "number": number,
            "message": f"Connecting you to {agent.get('name', 'our negotiator')}.",
            "callerId": evt.get("phoneNumber", CLI_DEFAULT),
            "numberE164CheckEnabled": True,
            "transferPlan": {
                "mode": "warm-transfer-experimental",
                "fallbackPlan": {"message": "The agent did not answer.", "endCallEnabled": False},
            },
        }
        return _json(200, {"destination": dest})

    # -----------------------------------------------------------------
    def _send(self, code: int, hdrs: list, body: bytes):
        self.send_response(code)
        for k, v in hdrs:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)


# ── local smoke-test ─────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    _log(f"★ listening on http://0.0.0.0:{port}")
    HTTPServer(("", port), handler).serve_forever()
