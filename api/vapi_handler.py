#!/usr/bin/env python3
# File: api/vapi_handler.py
"""
Vapi → Property search proxy (Vercel-compatible, class-based)

• Exposed at   /api/vapi_handler
• Accepts POST tool-call envelopes from Vapi (see sample body below)
• Optionally fires WhatsApp (unless `"dry": true`)
• Responds with    {"results":[{"toolCallId":"…","result":…}]}

This module **exports a class called `handler` that subclasses
BaseHTTPRequestHandler** – exactly what the Vercel Python runtime
is looking for, avoiding the previous `issubclass` TypeError.
"""

from __future__ import annotations
from property_search import (      # type: ignore  # pylint: disable=import-error
    Settings,
    PropertyRepository,
    summarise,
    send_whatsapp,
)

import io
import json
import sys
import traceback
from http.server import BaseHTTPRequestHandler
from typing import Any, Dict, List

from dotenv import load_dotenv

# helper functions live in lib/
sys.path.append("lib")              # allow import without packaging

# ────────────────────────────────────────────────────────────────
CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


def _json_response(
    code: int,
    payload: Dict[str, Any] | List[Any] | str = "",
) -> tuple[int, list[tuple[str, str]], bytes]:
    """Return (status, headers, body_bytes) – suitable for send_response."""
    headers = [("Content-Type", "application/json"), *CORS.items()]
    body = payload if isinstance(payload, str) else json.dumps(
        payload, ensure_ascii=False)
    return code, headers, body.encode()

# ───────────────────────── entry-point class ───────────────────────────────


class handler(BaseHTTPRequestHandler):  # pylint: disable=invalid-name
    """Vercel expects this exact symbol name (`handler`)."""

    # Silence default logging
    def log_message(self, fmt, *args):  # noqa: D401
        return

    # -------- CORS pre-flight ---------------------------------------------
    def do_OPTIONS(self):  # pylint: disable=invalid-name
        code, hdrs, body = _json_response(204, "")
        self.send_response(code)
        for k, v in hdrs:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    # -------- main POST handler -------------------------------------------
    def do_POST(self):  # pylint: disable=invalid-name
        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            envelope = json.loads(raw or "{}")
        except Exception:
            self._send(*_json_response(400, {"error": "invalid JSON"}))
            return

        res = self._process_envelope(envelope)
        self._send(*res)

    # -------- envelope → results ------------------------------------------
    def _process_envelope(self, env: Dict[str, Any]) -> tuple[int, list[tuple[str, str]], bytes]:
        message = env.get("message") or {}
        calls: list = message.get("toolCallList") or []
        if not calls:
            return _json_response(400, {"error": "no toolCallList in body"})

        # One repo / cfg per lambda invocation
        try:
            load_dotenv()
            cfg = Settings.from_env()
            repo = PropertyRepository(cfg)
            if not repo.ping():
                return _json_response(503, {"error": "database unavailable"})
        except Exception as exc:  # pylint: disable=broad-except
            return _json_response(500, {"error": f"config error: {exc}"})

        results = []
        for call in calls:
            tc_id = call.get("id", "unknown")
            name = call.get("name")
            args = call.get("arguments") or {}

            if name != "find_property":
                results.append(
                    {"toolCallId": tc_id, "result": f"unsupported tool {name}"})
                continue

            # ----- build query --------------------------------------------
            loc = (args.get("location") or "").strip()
            if not loc:
                results.append(
                    {"toolCallId": tc_id, "result": "location is required"})
                continue

            query = {"keyword": loc, "status": args.get("status", "current")}
            for fld in ("beds_min", "baths_min", "price_min", "price_max", "purpose"):
                if fld in args:
                    query[fld] = args[fld]

            # ----- search --------------------------------------------------
            try:
                doc, _tier = repo.find_one(query)
                if not doc:
                    results.append(
                        {"toolCallId": tc_id, "result": "no property found"})
                    continue
                summary = summarise(doc)
            except Exception as exc:  # pylint: disable=broad-except
                traceback.print_exc()
                results.append(
                    {"toolCallId": tc_id, "result": f"search error: {exc}"})
                continue

            # ----- optional WhatsApp -------------------------------------
            phone = (args.get("phone_number") or "").strip()
            if phone and not args.get("dry"):
                try:
                    send_whatsapp(cfg, phone, summary)
                    summary["whatsapp"] = "sent"
                except Exception as exc:  # pylint: disable=broad-except
                    summary["whatsapp"] = f"error: {exc}"

            results.append({"toolCallId": tc_id, "result": summary})

        return _json_response(200, {"results": results})

    # -------- send helper --------------------------------------------------
    def _send(self, code: int, headers: list[tuple[str, str]], body: bytes):
        self.send_response(code)
        for k, v in headers:
            self.send_header(k, v)
        self.end_headers()
        if body:
            # Avoid broken-pipe with large responses
            buf = io.BytesIO(body)
            self.wfile.write(buf.read())
