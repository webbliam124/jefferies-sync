#!/usr/bin/env python3
"""
api/vapi_proxy.py  —  Vapi → TIXAE proxy with dynamic transfers (UK numbers default)

Behaviour
─────────
• If Vapi POSTs a **transfer-destination-request** →
    → look up the listing_id in MongoDB
    → reply with {"destination": …} so Vapi bridges the call.

• If the LLM mistakenly does a **phone-call-control → forward** and the
  “number” field is a 5- or 6-digit listing ID →
    → convert it on the fly into a transfer-destination-request
      and handle as above.

• Every other event (assistant-message, call.end, etc.) is forwarded,
  with all original headers, to the existing TIXAE webhook
  https://na-gcp-api.vg-stuff.com/… .

Dependencies: only `pymongo` and `python-dotenv`.
"""

from __future__ import annotations

import json
import os
import re
import ssl
import sys
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Tuple

from dotenv import load_dotenv
from pymongo import MongoClient

# ── environment ------------------------------------------------------

load_dotenv()  # read variables set in Vercel dashboard

TIXAE_URL = (
    "https://na-gcp-api.vg-stuff.com/v2/agents/"
    "gFMPPgOtlMcxzrwbUbEl/vapi-event"
)

VAPI_SECRET = os.getenv("VAPI_SECRET")          # optional shared secret
FALLBACK = os.getenv("FALLBACK_NUMBER")      # duty negotiator phone
DIAL_CODE = os.getenv("COUNTRY_DIAL_CODE", "+44")
CLI_DEFAULT = os.getenv("DEFAULT_CALLER_ID", "")

COLLECTION = (
    MongoClient(os.environ["MONGODB_URI"], tz_aware=True)
    [os.environ["DB_NAME"]]
    [os.environ["COLLECTION_NAME"]]
)

# ── small helpers ----------------------------------------------------


def _json(code: int, payload: Dict[str, Any] | str) -> Tuple[int, list, bytes]:
    """Return (status, headers, body) for _send()."""
    hdr = [("Content-Type", "application/json")]
    body = payload.encode() if isinstance(
        payload, str) else json.dumps(payload).encode()
    return code, hdr, body


def _norm(num: str | None) -> str | None:
    """Normalise a dial string to E.164 (“+44…”) or return None."""
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


def _pipe_to_tixae(payload: bytes, hdrs: dict[str, str]):
    """Fire-and-forget forward to the original TIXAE endpoint."""
    headers = {"Content-Type": "application/json", **hdrs}
    req = urllib.request.Request(
        TIXAE_URL, data=payload, headers=headers, method="POST")
    try:
        urllib.request.urlopen(
            req, context=ssl.create_default_context(), timeout=5)
    except Exception as exc:
        print("⚠︎ forward failed:", exc, file=sys.stderr, flush=True)


# ── HTTP handler -----------------------------------------------------


class handler(BaseHTTPRequestHandler):  # noqa: N801 (Vercel naming)
    def log_message(self, *_):
        return  # silence default access log

    # -----------------------------------------------------------------
    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))

        # Optional signature check (accept legacy "secret" header too)
        hdr_secret = self.headers.get
        if VAPI_SECRET and hdr_secret("x-vapi-secret") != VAPI_SECRET and hdr_secret("secret") != VAPI_SECRET:
            print("⚠︎ header secret mismatch",
                  self.headers.get("x-vapi-secret"), file=sys.stderr, flush=True)
            return self._send(*_json(401, {"error": "unauthenticated"}))

        try:
            evt = json.loads(raw or "{}")
        except json.JSONDecodeError:
            return self._send(*_json(200, {"error": "invalid JSON"}))

        etype = evt.get("type")

        # 1️⃣  Normal dynamic-transfer request
        if etype == "transfer-destination-request":
            return self._send(*self._handle_transfer(evt))

        # 2️⃣  Rogue forward with a listing-id masquerading as a phone number
        if etype == "phone-call-control" and evt.get("request") == "forward":
            num = evt.get("forwardingPhoneNumber", "")
            if re.fullmatch(r"\d{5,6}", num):
                synthetic = {
                    "type": "transfer-destination-request",
                    "phoneNumber": evt.get("callerId", ""),
                    "artifact": {
                        "toolCall": {"arguments": json.dumps({"listing_id": num})}
                    },
                }
                return self._send(*self._handle_transfer(synthetic))

        # 3️⃣  Everything else → straight to TIXAE
        _pipe_to_tixae(raw, dict(self.headers))
        return self._send(*_json(200, {"success": True}))

    # -----------------------------------------------------------------
    def _handle_transfer(self, evt: Dict[str, Any]):
        args_raw = (
            evt.get("artifact") or {}
        ).get("toolCall", {}).get("arguments", {})
        if isinstance(args_raw, str):
            try:
                args_raw = json.loads(args_raw)
            except json.JSONDecodeError:
                args_raw = {}

        listing_id = args_raw.get("listing_id")
        if not listing_id:
            return _json(200, {"error": "missing listing_id"})

        rec = COLLECTION.find_one({"_id": listing_id}) or COLLECTION.find_one(
            {"id": listing_id}
        )
        agent = (rec.get("agents") or [{}])[0] if rec else {}

        phones = [agent.get("phone_mobile"), agent.get(
            "phone_direct"), FALLBACK]
        number = next((n for n in (_norm(p) for p in phones) if n), None)
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
                "fallbackPlan": {
                    "message": "The agent did not answer.",
                    "endCallEnabled": False,
                },
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


# ── local smoke-test -------------------------------------------------
if __name__ == "__main__":
    print("★ proxy listening on http://0.0.0.0:8000", file=sys.stderr)
    HTTPServer(("", 8000), handler).serve_forever()
