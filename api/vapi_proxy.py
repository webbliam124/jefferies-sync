#!/usr/bin/env python3
"""
api/vapi_proxy.py  –  robust Vapi → TIXAE proxy (2025-06 version)

───────────────────────────────────────────────────────────────────────────────
Env-vars you MUST set in Vercel (or locally)
───────────────────────────────────────────────────────────────────────────────
VAPI_SECRET           shared secret Vapi sends in x-vapi-secret
MONGODB_URI           connection string
DB_NAME               Mongo database name
COLLECTION_NAME       collection that stores listing docs
FALLBACK_NUMBER       duty negotiator’s E.164 mobile  (“+44…”, “+27…”, …)
TIXAE_AGENT_ID        e.g. y77c1kx9fboojeu5  (see TIXAE URL in dashboard)

Optional tweaks
───────────────
COUNTRY_DIAL_CODE     default +CC when normalising (“+44” if unset)
DEFAULT_CALLER_ID     CLI if the Vapi event lacks one
OUTBOUND_CLI          fixed, Twilio-verified CLI for the transfer leg
DEBUG=1               verbose logs (headers + payloads)
RETRY_TO_TIXAE=1      retry TIXAE forward once on error
───────────────────────────────────────────────────────────────────────────────
This proxy:

• Accepts both “proper” `transfer-destination-request` and rogue
  `phone-call-control/forward` events (with a 5- or 6-digit listing ID).

• Looks up the listing → picks phone_mobile → phone_direct → FALLBACK_NUMBER.

• Answers **only those two event types** with a destination object
  (new and legacy schema in one response).  All other events are forwarded
  unchanged to the original TIXAE webhook.

• Emits one INFO line per event + deep DEBUG when DEBUG=1.
"""

from __future__ import annotations

import json
import logging
import os
import re
import ssl
import sys
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Tuple
from urllib.error import HTTPError, URLError

from dotenv import load_dotenv
from pymongo import MongoClient

# ── env & logging ────────────────────────────────────────────────────
load_dotenv()

DEBUG = os.getenv("DEBUG") == "1"
RETRY_TIXAE = os.getenv("RETRY_TO_TIXAE") == "1"

logging.basicConfig(
    stream=sys.stderr,
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(levelname)s %(message)s",
)
LOG = logging.getLogger(__name__)
if DEBUG:
    logging.getLogger("pymongo").setLevel(logging.WARNING)

# ── constants ────────────────────────────────────────────────────────
VAPI_SECRET = os.getenv("VAPI_SECRET")
FALLBACK_NUM = os.getenv("FALLBACK_NUMBER")
DIAL_CODE = os.getenv("COUNTRY_DIAL_CODE", "+44")
CLI_DEFAULT = os.getenv("DEFAULT_CALLER_ID", "")
OUTBOUND_CLI = os.getenv("OUTBOUND_CLI", CLI_DEFAULT)

TIXAE_URL = (
    "https://na-gcp-api.vg-stuff.com/v2/agents/"
    f"{os.getenv('TIXAE_AGENT_ID')}/vapi-event"
)

COLL = (
    MongoClient(os.environ["MONGODB_URI"], tz_aware=True)
    [os.environ["DB_NAME"]][os.environ["COLLECTION_NAME"]]
)

# ── helpers ──────────────────────────────────────────────────────────


def _json(code: int, payload: Dict[str, Any] | str) -> Tuple[int, list, bytes]:
    body = payload.encode() if isinstance(
        payload, str) else json.dumps(payload).encode()
    return code, [("Content-Type", "application/json")], body


def _norm(num: str | None) -> str | None:
    """Return E.164 version of num or None."""
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


def _post_to_tixae(blob: bytes, hdrs: dict[str, str]) -> None:
    """Fire-and-forget (with optional retry) to the legacy TIXAE webhook."""
    def _once() -> None:
        req = urllib.request.Request(
            TIXAE_URL,
            data=blob,
            headers={
                "Content-Type": "application/json",
                "x-request-id": hdrs.get("x-call-id", ""),
            },
            method="POST",
        )
        start = time.perf_counter()
        with urllib.request.urlopen(req, context=ssl.create_default_context(), timeout=8) as r:
            LOG.info("→ TIXAE %s (%.0f ms)", r.status,
                     (time.perf_counter() - start) * 1000)

    try:
        _once()
    except (HTTPError, URLError, ssl.SSLError) as exc:
        if not RETRY_TIXAE:
            LOG.warning("TIXAE forward failed: %s", exc)
            return
        LOG.warning("TIXAE error (%s) – retrying once …", exc)
        try:
            _once()
        except Exception as exc2:
            LOG.error("TIXAE retry failed: %s", exc2)

# ── HTTP handler ─────────────────────────────────────────────────────


class handler(BaseHTTPRequestHandler):  # noqa: N801
    def log_message(self, *_: Any) -> None:  # silence default access log
        return

    # -----------------------------------------------------------------
    def do_POST(self) -> None:  # noqa: N802
        raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if DEBUG:
            LOG.debug("HDR %s", dict(self.headers) | {"bodyLen": len(raw)})

        # Shared-secret check
        incoming_secret = (
            self.headers.get("x-vapi-secret")
            or self.headers.get("x-vapi-signature")
            or self.headers.get("secret")
        )
        if VAPI_SECRET and incoming_secret != VAPI_SECRET:
            return self._send(*_json(401, {"error": "unauthenticated"}))

        # Decode JSON
        try:
            data = json.loads(raw or "{}")
        except json.JSONDecodeError:
            return self._send(*_json(400, {"error": "invalid JSON"}))

        evt = data["message"] if isinstance(
            data.get("message"), dict) else data
        etype = evt.get("type")
        LOG.info("★ %s", etype)

        # 1️⃣ normal transfer-destination-request
        if etype == "transfer-destination-request":
            return self._send(*self._handle_transfer(evt))

        # 2️⃣ LLM issued phone-call-control/forward with a 5/6-digit listing ID
        if etype == "phone-call-control" and evt.get("request") == "forward":
            num = evt.get("forwardingPhoneNumber", "")
            if re.fullmatch(r"\d{5,6}", num):
                synthetic = {
                    "type": "transfer-destination-request",
                    "phoneNumber": evt.get("callerId", ""),
                    "artifact": {"toolCall": {"arguments": json.dumps({"listing_id": num})}},
                }
                return self._send(*self._handle_transfer(synthetic))

        # 3️⃣ everything else → just forward; no destination!
        _post_to_tixae(raw, dict(self.headers))
        return self._send(*_json(200, {"success": True}))

    # -----------------------------------------------------------------
    def _handle_transfer(self, evt: Dict[str, Any]) -> Tuple[int, list, bytes]:
        """Return destination JSON in both current & legacy schema."""
        args = (evt.get("artifact") or {}).get(
            "toolCall", {}).get("arguments", {})
        if isinstance(args, str):
            try:
                args = json.loads(args)
            except json.JSONDecodeError:
                args = {}

        listing_id = args.get("listing_id")
        if not listing_id:
            LOG.warning("transfer with no listing_id")
            return _json(200, {"error": "missing listing_id"})

        # Mongo lookup
        try:
            rec = COLL.find_one({"_id": listing_id}) or COLL.find_one(
                {"id": listing_id})
        except Exception as exc:
            LOG.error("Mongo error: %s", exc)
            return _json(200, {"error": f"DB error: {exc}"})

        agent = (rec.get("agents") or [{}])[0] if rec else {}
        phones = [agent.get("phone_mobile"), agent.get(
            "phone_direct"), FALLBACK_NUM]
        number = next((n for n in (_norm(p) for p in phones) if n), None)
        if not number:
            LOG.warning("listing %s has no valid phone", listing_id)
            return _json(200, {"error": "no valid phone"})

        if DEBUG:
            LOG.debug(
                "listing %s → %s (%s)",
                listing_id,
                number,
                "fallback" if number == FALLBACK_NUM else "agent",
            )

        dest_core = {
            "type": "number",
            "number": number,
            "callerId": OUTBOUND_CLI or CLI_DEFAULT,
        }
        response = {
            "destination": dest_core,  # new schema (May-2025)
            "transferDestination": {   # legacy schema (pre-2025)
                "type": "phone-number",
                "phoneNumber": number,
                "callerId": dest_core["callerId"],
            },
        }

        if DEBUG:
            LOG.debug("→ returning %s", json.dumps(response, indent=2))

        return _json(200, response)

    # -----------------------------------------------------------------
    def _send(self, code: int, hdrs: list, body: bytes) -> None:
        self.send_response(code)
        for k, v in hdrs:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)


# ── local smoke test ─────────────────────────────────────────────────
if __name__ == "__main__":
    LOG.info("★ proxy listening on http://0.0.0.0:8000 (DEBUG=%s)", DEBUG)
    HTTPServer(("", 8000), handler).serve_forever()        