#!/usr/bin/env python3
"""
Vapi ⇒ TIXAE smart proxy with dynamic warm transfers.

Required env vars (set in Vercel or `.env` locally)

  VAPI_SECRET         shared secret Vapi sends in the request header
  MONGODB_URI         connection string
  DB_NAME             Mongo database name
  COLLECTION_NAME     collection holding the listing docs
  COUNTRY_DIAL_CODE   default +CC for normalisation (default “+44”)
  FALLBACK_NUMBER     duty negotiator’s mobile (E.164)  ← essential
  DEFAULT_CALLER_ID   CLI to present if Vapi omits one  (optional)
  TIXAE_AGENT_ID      “y77c1kx9fboojeu5” etc.
  TIXAE_SECRET        shared secret for proxy → TIXAE   (optional)
  DEBUG               “1” for header dump & verbose logs (optional)
  TIXAE_RETRIES       default 3
  TIXAE_RETRY_DELAY   default 0.8 s
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

# ─────────  Environment & logging  ───────────────────────────────────

load_dotenv()  # Vercel sets env vars; locally you use .env

LOG = logging.getLogger(__name__)
logging.basicConfig(
    stream=sys.stderr,
    level=logging.DEBUG if os.getenv("DEBUG") == "1" else logging.INFO,
    format="%(levelname)s %(message)s",
)

# ─────────  Constants  ───────────────────────────────────────────────

TIXAE_URL = (
    "https://na-gcp-api.vg-stuff.com/v2/agents/"
    f"{os.getenv('TIXAE_AGENT_ID')}/vapi-event"
)

VAPI_SECRET = os.getenv("VAPI_SECRET")
TIXAE_SECRET = os.getenv("TIXAE_SECRET")
MAX_RETRIES = int(os.getenv("TIXAE_RETRIES", "3"))
RETRY_DELAY = float(os.getenv("TIXAE_RETRY_DELAY", "0.8"))

FALLBACK = os.getenv("FALLBACK_NUMBER")
DIAL_CODE = os.getenv("COUNTRY_DIAL_CODE", "+44")
CLI_DEFAULT = os.getenv("DEFAULT_CALLER_ID", "")

COLLECTION = (
    MongoClient(os.environ["MONGODB_URI"], tz_aware=True)
    [os.environ["DB_NAME"]]
    [os.environ["COLLECTION_NAME"]]
)

# ─────────  Tiny helpers  ────────────────────────────────────────────


def _json(code: int, payload: Dict[str, Any] | str) -> Tuple[int, list, bytes]:
    hdr = [("Content-Type", "application/json")]
    body = payload.encode() if isinstance(
        payload, str) else json.dumps(payload).encode()
    return code, hdr, body


def _norm(num: str | None) -> str | None:
    """Normalise to E.164 (“+44…”) or return None."""
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
    """Forward the Vapi event to TIXAE with retries and clean headers."""
    req_id = hdrs.get("x-call-id") or hdrs.get("x-request-id") or ""
    headers = {
        "Content-Type": "application/json",
        "x-request-id": req_id,
        **({"x-tixae-secret": TIXAE_SECRET} if TIXAE_SECRET else {}),
    }
    for h in ("user-agent", "x-forwarded-for"):
        if h in hdrs:
            headers[h] = hdrs[h]

    for attempt in range(1, MAX_RETRIES + 1):
        t0 = time.perf_counter()
        try:
            req = urllib.request.Request(
                TIXAE_URL, data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(req, context=ssl.create_default_context(), timeout=8) as r:
                dur = (time.perf_counter() - t0) * 1000
                LOG.info("→ TIXAE %s (%.0f ms) id=%s", r.status, dur, req_id)
                return
        except HTTPError as exc:      # non-2xx
            body = exc.read().decode("utf-8", "ignore")[:300]
            LOG.warning("✗ TIXAE %s attempt %d/%d id=%s body=%s",
                        exc.code, attempt, MAX_RETRIES, req_id, body)
        except URLError as exc:       # network / TLS
            LOG.warning("✗ TIXAE %s attempt %d/%d id=%s",
                        exc.reason, attempt, MAX_RETRIES, req_id)
        time.sleep(RETRY_DELAY)

# ─────────  HTTP handler  ────────────────────────────────────────────


class handler(BaseHTTPRequestHandler):  # noqa: N801 (Vercel naming)
    def log_message(self, *_):
        return  # silence default HTTP log

    # -----------------------------------------------------------------
    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))

        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Incoming headers: %s", {k: self.headers[k] for k in (
                'x-vapi-secret', 'x-call-id') if k in self.headers})
            LOG.debug("Loaded VAPI_SECRET: %s", VAPI_SECRET)

        incoming_secret = (
            self.headers.get("x-vapi-secret")
            or self.headers.get("x-vapi-signature")
            or self.headers.get("secret")
        )
        if VAPI_SECRET and incoming_secret != VAPI_SECRET:
            LOG.warning("header secret mismatch: %s", incoming_secret)
            return self._send(*_json(401, {"error": "unauthenticated"}))

        try:
            evt = json.loads(raw or "{}")
        except json.JSONDecodeError:
            return self._send(*_json(400, {"error": "invalid JSON"}))

        etype = evt.get("type")

        if etype == "transfer-destination-request":
            return self._send(*self._handle_transfer(evt))

        if etype == "phone-call-control" and evt.get("request") == "forward":
            num = evt.get("forwardingPhoneNumber", "")
            if re.fullmatch(r"\d{5,6}", num):
                synthetic = {
                    "type": "transfer-destination-request",
                    "phoneNumber": evt.get("callerId", ""),
                    "artifact": {"toolCall": {"arguments": json.dumps({"listing_id": num})}},
                }
                return self._send(*self._handle_transfer(synthetic))

        # all other events
        _pipe_to_tixae(raw, dict(self.headers))
        return self._send(*_json(200, {"success": True}))

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
        if not listing_id:
            return _json(200, {"error": "missing listing_id"})

        rec = COLLECTION.find_one(
            {"_id": listing_id}) or COLLECTION.find_one({"id": listing_id})
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


# ─────────  Local smoke-test  ────────────────────────────────────────
if __name__ == "__main__":
    LOG.info("★ proxy listening on http://0.0.0.0:8000")
    HTTPServer(("", 8000), handler).serve_forever()
