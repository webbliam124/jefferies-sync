#!/usr/bin/env python3
"""
Vapi → Property search proxy (Vercel deployment).

Accepts POST tool-call envelopes from Vapi, runs the Mongo search, optionally
fires WhatsApp, and returns results in:

    {"results": [{"toolCallId": "…", "result": …}, …]}

NEW 2025-06-01
──────────────
• Supports the `features` array (amenity keywords).
"""

from __future__ import annotations

# local imports
from property_search import (  # type: ignore
    Settings,
    PropertyRepository,
    summarise,
    send_whatsapp,
    normalise_subcategory,
)

# stdlib
import io
import json
import sys
import traceback
from http.server import BaseHTTPRequestHandler
from typing import Any, Dict, List

# third-party
from dotenv import load_dotenv

# allow imports from lib/ dir if you have helpers there
sys.path.append("lib")

# ────────────────────────────────────────────────────────────────────
CORS = {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


def _json_response(code: int, payload) -> tuple[int, list[tuple[str, str]], bytes]:
    headers = [("Content-Type", "application/json"), *CORS.items()]
    body = payload if isinstance(payload, str) else json.dumps(
        payload, ensure_ascii=False)
    return code, headers, body.encode()

# ────────────────────────────────────────────────────────────────────


class handler(BaseHTTPRequestHandler):  # pylint: disable=invalid-name
    """Vercel looks for a symbol literally called `handler`."""

    # quiet default logging
    def log_message(self, *_):  # noqa: D401
        return

    # ----------------------------------------------------------------
    def do_OPTIONS(self):
        code, hdrs, body = _json_response(204, "")
        self.send_response(code)
        for k, v in hdrs:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    # ----------------------------------------------------------------
    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            envelope = json.loads(raw or "{}")
        except Exception:
            self._send(*_json_response(400, {"error": "invalid JSON"}))
            return
        self._send(*self._process_envelope(envelope))

    # ----------------------------------------------------------------
    def _process_envelope(self, env: Dict[str, Any]):
        message = env.get("message") or {}
        calls = message.get("toolCallList") or message.get("toolCalls") or []
        if not calls:
            return _json_response(400, {"error": "no tool calls in body"})

        # shared resources per invocation
        try:
            load_dotenv()
            cfg = Settings.from_env()
            repo = PropertyRepository(cfg)
            if not repo.ping():
                return _json_response(503, {"error": "database unavailable"})
        except Exception as exc:  # pragma: no cover
            return _json_response(500, {"error": f"config error: {exc}"})

        results = []

        for call in calls:
            tc_id = call.get("id", "unknown")

            fn_wrapper = call.get("function") if call.get(
                "type") == "function" else {}
            name = fn_wrapper.get("name") or call.get("name")
            args = fn_wrapper.get("arguments") or call.get("arguments") or {}

            if name != "find_property":
                results.append({"toolCallId": tc_id,
                                "result": f"unsupported tool {name}"})
                continue

            loc = (args.get("location") or "").strip()
            if not loc:
                results.append({"toolCallId": tc_id,
                                "result": "location is required"})
                continue

            # build query ------------------------------------------------
            q: Dict[str, Any] = {
                "keyword": loc,
                "purpose": args.get("purpose", "all"),
            }
            for fld in ("beds_min", "baths_min", "price_min", "price_max"):
                if fld in args and args[fld] is not None:
                    q[fld] = args[fld]

            canon = normalise_subcategory(args.get("subcategory") or "")
            if canon:
                q["subcategories"] = {"$regex": canon, "$options": "i"}

            # amenity keywords
            feats = args.get("features")
            if feats and isinstance(feats, list):
                q["features"] = feats

            # search ----------------------------------------------------
            try:
                doc, _tier = repo.find_one(q)
                if not doc:
                    results.append({"toolCallId": tc_id,
                                    "result": "no property found"})
                    continue
                summary = summarise(doc)
            except Exception as exc:  # pragma: no cover
                traceback.print_exc()
                results.append({"toolCallId": tc_id,
                                "result": f"search error: {exc}"})
                continue

            # WhatsApp (optional) --------------------------------------
            phone = (args.get("phone_number") or "").strip()
            if phone and not args.get("dry"):
                try:
                    send_whatsapp(cfg, phone, summary)
                    summary["whatsapp"] = "sent"
                except Exception as exc:  # pragma: no cover
                    summary["whatsapp"] = f"error: {exc}"

            results.append({"toolCallId": tc_id, "result": summary})

        return _json_response(200, {"results": results})

    # ----------------------------------------------------------------
    def _send(self, code: int, headers: List[tuple[str, str]], body: bytes):
        self.send_response(code)
        for k, v in headers:
            self.send_header(k, v)
        self.end_headers()
        if body:
            self.wfile.write(io.BytesIO(body).read())
