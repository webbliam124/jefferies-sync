#!/usr/bin/env python3
"""
Instrumented warm-transfer webhook – prints every event to Vercel logs.
Deploy, trigger a call, then run:

    vercel logs jefferies-sync --prod -f

…to see exactly which events reach the function and what number we return.
If you never see type="transfer-destination-request" the assistant is
calling the wrong tool (or no tools) – not a coding problem.
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

# --- utilities -------------------------------------------------------


def _log(*msg): print(*msg, file=sys.stderr, flush=True)


def _json(code: int, payload: Dict[str, Any] | str):
    hdr = [("Content-Type", "application/json")]
    return code, hdr, (payload if isinstance(payload, str)
                       else json.dumps(payload)).encode()


def _norm(num: str | None):
    if not num:
        return None
    num = re.sub(r"[^\d+]", "", num)
    if num.startswith("+"):
        return num
    if num.startswith("0"):
        return os.getenv("COUNTRY_DIAL_CODE", "+44")+num.lstrip("0")
    if len(num) > 10:
        return "+"+num
    return None

# --- HTTP handler ----------------------------------------------------


class handler(BaseHTTPRequestHandler):                                    # noqa: N801
    # silence std log
    def log_message(self, *_): return

    def do_OPTIONS(self): self._send(
        *_json(204, ""))                     # CORS pre-flight

    def do_POST(self):                                                    # main entry
        try:
            body = self.rfile.read(
                int(self.headers.get("Content-Length", "0")))
            evt = json.loads(body or "{}")
        except Exception:
            return self._send(*_json(200, {"error": "invalid JSON"}))

        etype = evt.get("type")
        _log("► webhook type:", etype)

        if etype != "transfer-destination-request":
            # ignore all others politely
            return self._send(*_json(200, {}))

        self._send(*self._handle(evt))

    # ------------------------------------------------------------------
    def _handle(self, evt: Dict[str, Any]):
        art = evt.get("artifact") or {}
        args = (art.get("toolCall") or {}).get("arguments") or {}
        lid = args.get("listing_id")
        _log("  listing_id:", lid)

        if not lid:
            return _json(200, {"error": "missing listing_id"})

        try:
            load_dotenv()
            col = MongoClient(os.environ["MONGODB_URI"], tz_aware=True)[
                os.getenv("DB_NAME", "JefferiesJames")][
                os.getenv("COLLECTION_NAME", "properties")]
            rec = col.find_one({"_id": lid}) or col.find_one({"id": lid})
        except Exception as exc:
            _log("  DB error:", exc)
            return _json(200, {"error": f"DB error:{exc}"})

        if not rec:
            return _json(200, {"error": "listing not found"})

        ag = (rec.get("agents") or [{}])[0]
        phones = [ag.get("phone_mobile"), ag.get("phone_direct"),
                  os.getenv("FALLBACK_NUMBER")]
        num = next((n for n in (_norm(p) for p in phones) if n), None)
        _log("  number chosen:", num)

        if not num:
            return _json(200, {"error": "no valid phone"})

        dest = {
            "type": "number", "number": num,
            "message": f"Connecting you to {ag.get('name', 'our negotiator')}.",
            "callerId": os.getenv("DEFAULT_CALLER_ID", ""),
            "transferPlan": {
                "mode": "warm-transfer-experimental",
                "fallbackPlan": {"message": "Agent did not answer.",
                                 "endCallEnabled": False}
            }
        }
        return _json(200, {"destination": dest})

    def _send(self, code: int, hdr: list, body: bytes):
        self.send_response(code)
        for k, v in hdr:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)
