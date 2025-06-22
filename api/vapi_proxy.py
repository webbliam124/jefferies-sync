#!/usr/bin/env python3
"""
api/vapi_proxy.py  –  robust, minimal proxy Vapi ⇒ TIXAE
(added one DEBUG line to show the number we return)

Required env vars
  VAPI_SECRET, MONGODB_URI, DB_NAME, COLLECTION_NAME,
  FALLBACK_NUMBER, TIXAE_AGENT_ID

Optional
  COUNTRY_DIAL_CODE  (default “+44”)
  DEFAULT_CALLER_ID
  DEBUG=1            (verbose logs)
  RETRY_TO_TIXAE=1   (one retry on 5xx/timeout)
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
logging.basicConfig(stream=sys.stderr,
                    level=logging.DEBUG if DEBUG else logging.INFO,
                    format="%(levelname)s %(message)s")
LOG = logging.getLogger(__name__)

# ── constants ────────────────────────────────────────────────────────
VAPI_SECRET = os.getenv("VAPI_SECRET")
FALLBACK_NUM = os.getenv("FALLBACK_NUMBER")
DIAL_CODE = os.getenv("COUNTRY_DIAL_CODE", "+44")
CLI_DEFAULT = os.getenv("DEFAULT_CALLER_ID", "")
SHOULD_RETRY = os.getenv("RETRY_TO_TIXAE") == "1"

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


def _forward_to_tixae(payload: bytes, hdrs: dict[str, str]):
    def _post():
        req = urllib.request.Request(
            TIXAE_URL,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "x-request-id": hdrs.get("x-call-id", ""),
                "user-agent": hdrs.get("user-agent", "proxy"),
                "x-forwarded-for": hdrs.get("x-forwarded-for", "")
            },
            method="POST",
        )
        start = time.perf_counter()
        with urllib.request.urlopen(req, context=ssl.create_default_context(), timeout=8) as resp:
            LOG.info("→ TIXAE %s (%.0f ms)", resp.status,
                     (time.perf_counter()-start)*1000)

    try:
        _post()
    except (HTTPError, URLError, ssl.SSLError) as exc:
        if not SHOULD_RETRY:
            LOG.warning("TIXAE forward failed: %s", exc)
            return
        LOG.warning("TIXAE failed (%s) – retrying once …", exc)
        try:
            _post()
        except Exception as exc2:
            LOG.error("TIXAE retry failed: %s", exc2)

# ── HTTP handler ─────────────────────────────────────────────────────


class handler(BaseHTTPRequestHandler):  # noqa: N801
    def log_message(self, *_): return

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))

        # 1  secret check
        inc_secret = (self.headers.get("x-vapi-secret")
                      or self.headers.get("x-vapi-signature")
                      or self.headers.get("secret"))
        if VAPI_SECRET and inc_secret != VAPI_SECRET:
            return self._send(*_json(401, {"error": "unauthenticated"}))

        # 2  parse + unwrap
        try:
            data = json.loads(raw or "{}")
        except json.JSONDecodeError:
            return self._send(*_json(400, {"error": "invalid JSON"}))
        evt = data["message"] if isinstance(
            data.get("message"), dict) else data
        etype = evt.get("type")

        # 3  handle dynamic transfers
        if etype == "transfer-destination-request":
            return self._send(*self._handle_transfer(evt))

        if etype == "phone-call-control" and evt.get("request") == "forward":
            num = evt.get("forwardingPhoneNumber", "")
            if re.fullmatch(r"\d{5,6}", num):
                synthetic = {
                    "type": "transfer-destination-request",
                    "phoneNumber": evt.get("callerId", ""),
                    "artifact": {"toolCall": {"arguments": json.dumps({"listing_id": num})}}
                }
                return self._send(*self._handle_transfer(synthetic))

        # 4  everything else straight to TIXAE
        _forward_to_tixae(raw, dict(self.headers))
        return self._send(*_json(200, {"success": True}))

    # -----------------------------------------------------------------
    def _handle_transfer(self, evt: Dict[str, Any]):
        # extract listing_id
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

        rec = COLL.find_one({"_id": listing_id}) or COLL.find_one(
            {"id": listing_id})
        agent = (rec.get("agents") or [{}])[0] if rec else {}

        phones = [agent.get("phone_mobile"), agent.get(
            "phone_direct"), FALLBACK_NUM]
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
        LOG.debug("→  returning destination: %s", dest["number"])
        return _json(200, {"destination": dest})

    # -----------------------------------------------------------------
    def _send(self, code: int, hdrs: list, body: bytes):
        self.send_response(code)
        for k, v in hdrs:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)


# ── Local smoke-test ─────────────────────────────────────────────────
if __name__ == "__main__":
    LOG.info("★ proxy listening on http://0.0.0.0:8000")
    HTTPServer(("", 8000), handler).serve_forever()
