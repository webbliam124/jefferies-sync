#!/usr/bin/env python3
"""
Vapi proxy:
• If event.type == transfer-destination-request → do DB lookup & return destination
• else → forward unchanged to TIXAE and reply 200
"""

from __future__ import annotations
import os
import json
import re
import sys
import asyncio
import aiohttp
from http.server import BaseHTTPRequestHandler
from urllib.parse import urljoin
from dotenv import load_dotenv
from pymongo import MongoClient

# ── config ───────────────────────────────────────────────────────────
load_dotenv()

TIXAE_URL = "https://na-gcp-api.vg-stuff.com/v2/agents/gFMPPgOtlMcxzrwbUbEl/vapi-event"
MONGO = MongoClient(os.environ["MONGODB_URI"])[os.getenv(
    "DB_NAME", "JefferiesJames")][os.getenv("COLLECTION", "properties")]
FALLBACK = os.getenv("FALLBACK_NUMBER")
DIAL_CODE = os.getenv("COUNTRY_DIAL_CODE", "+27")
CALLER_FWD = os.getenv("DEFAULT_CALLER_ID", "")

# ── helpers ──────────────────────────────────────────────────────────


def _json(code: int, data):                          # unified response
    return code, [("Content-Type", "application/json")], json.dumps(data).encode()


def _norm(num: str | None) -> str | None:                 # -> E.164
    if not num:
        return None
    num = re.sub(r"[^\d+]", "", num)
    if num.startswith("+"):
        return num
    if num.startswith("0"):
        return DIAL_CODE + num.lstrip("0")
    if len(num) > 10:
        return "+"+num
    return None


async def _pipe_to_tixae(payload: bytes, hdrs: dict[str, str] | None):
    async with aiohttp.ClientSession() as s:
        try:
            await s.post(TIXAE_URL, data=payload,
                         headers={"Content-Type": "application/json", **(hdrs or {})})
        except Exception as e:
            print("⚠︎ forward failed:", e, file=sys.stderr)

# ── HTTP handler ─────────────────────────────────────────────────────


class handler(BaseHTTPRequestHandler):                # noqa: N801
    def log_message(self, *_): pass                    # silence default log

    def do_POST(self):
        raw = self.rfile.read(
            int(self.headers.get("Content-Length", "0") or 0))
        try:
            evt = json.loads(raw or "{}")
        except json.JSONDecodeError:
            return self._send(*_json(200, {"error": "invalid JSON"}))

        if evt.get("type") == "transfer-destination-request":
            return self._send(*self._handle_transfer(evt))
        else:
            # fire-and-forget forward; Vapi only needs 2xx
            asyncio.run(_pipe_to_tixae(raw, dict(self.headers)))
            return self._send(*_json(200, {"success": True}))

    # ── dynamic transfer logic ──────────────────────────────────────
    def _handle_transfer(self, evt):
        art = evt.get("artifact") or {}
        tcall = art.get("toolCall") or {}
        args = tcall.get("arguments") or {}
        if isinstance(args, str):
            try:
                args = json.loads(args)
            except:
                args = {}
        lid = args.get("listing_id")
        if not lid:                 # missing argument
            return _json(200, {"error": "missing listing_id"})

        rec = MONGO.find_one({"_id": lid}) or MONGO.find_one({"id": lid})
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
            "callerId": evt.get("phoneNumber", CALLER_FWD),
            "numberE164CheckEnabled": True,
            "transferPlan": {
                "mode": "warm-transfer-experimental",
                "fallbackPlan": {"message": "The agent did not answer.", "endCallEnabled": False}
            }
        }
        return _json(200, {"destination": dest})

    # ── low-level I/O helper ────────────────────────────────────────
    def _send(self, code, hdrs, body):
        self.send_response(code)
        for k, v in hdrs:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)
