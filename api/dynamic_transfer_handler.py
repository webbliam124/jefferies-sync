#!/usr/bin/env python3
"""
Dynamic warm-transfer webhook for Vapi.
• Expects a transfer-destination-request
• Finds the agent for <listing_id> in Mongo
• Replies with a destination JSON that Vapi dials
"""

from __future__ import annotations
import json
import os
import re
import sys
from http.server import BaseHTTPRequestHandler
from typing import Any, Dict, Tuple
from dotenv import load_dotenv
from pymongo import MongoClient

# ── helpers ──────────────────────────────────────────────────────────


def _json(code: int, payload: Dict[str, Any] | str):
    hdr = [("Content-Type", "application/json")]
    return code, hdr, (payload if isinstance(payload, str)
                       else json.dumps(payload)).encode()


def _log(*m): print(*m, file=sys.stderr, flush=True)


def _norm(n: str | None) -> str | None:
    if not n:
        return None
    n = re.sub(r"[^\d+]", "", n)
    if n.startswith("+"):
        return n
    if n.startswith("0"):
        return os.getenv("COUNTRY_DIAL_CODE", "+44")+n.lstrip("0")
    if len(n) > 10:
        return "+"+n
    return None

# ── HTTP entry-point ─────────────────────────────────────────────────


class handler(BaseHTTPRequestHandler):                                   # noqa: N801
    def log_message(self, *_): return

    def do_OPTIONS(self): self._send(*_json(204, ""))

    def do_POST(self):
        try:
            body = self.rfile.read(
                int(self.headers.get("Content-Length", "0")))
            evt = json.loads(body or "{}")
        except Exception:
            return self._send(*_json(200, {"error": "invalid JSON"}))

        if evt.get("type") != "transfer-destination-request":
            return self._send(*_json(200, {}))        # ignore other events

        self._send(*self._handle(evt))

    # -----------------------------------------------------------------
    def _handle(self, evt: Dict[str, Any]):
        art = evt.get("artifact") or {}
        args = (art.get("toolCall") or {}).get("arguments") or {}
        lid = args.get("listing_id")
        _log("listing_id:", lid)

        if not lid:
            return _json(200, {"error": "missing listing_id"})

        try:
            load_dotenv()
            col = MongoClient(os.environ["MONGODB_URI"], tz_aware=True)[
                os.getenv("DB_NAME", "JefferiesJames")][
                os.getenv("COLLECTION_NAME", "properties")]
            rec = col.find_one({"_id": lid}) or col.find_one({"id": lid})
        except Exception as exc:
            return _json(200, {"error": f"DB error:{exc}"})

        if not rec:
            return _json(200, {"error": "listing not found"})

        ag = (rec.get("agents") or [{}])[0]
        phones = [ag.get("phone_mobile"), ag.get("phone_direct"),
                  os.getenv("FALLBACK_NUMBER")]
        number = next((n for n in (_norm(p) for p in phones) if n), None)
        _log("dial:", number)

        if not number:
            return _json(200, {"error": "no valid phone"})

        dest = {
            "type": "number",
            "number": number,
            "message": f"Connecting you to {ag.get('name', 'our negotiator')}.",
            "callerId": os.getenv("DEFAULT_CALLER_ID", ""),
            "transferPlan": {
                "mode": "warm-transfer-experimental",
                "fallbackPlan": {"message": "The agent did not answer.",
                                 "endCallEnabled": False}
            }
        }
        return _json(200, {"destination": dest})

    def _send(self, code: int, hdr: list, body: bytes):
        self.send_response(code)
        [self.send_header(k, v) for k, v in hdr]
        self.end_headers()
        self.wfile.write(body)
