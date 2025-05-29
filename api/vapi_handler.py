#!/usr/bin/env python3
"""
Vapi → Property search proxy (Vercel-compatible).

• Deployed at /api/vapi_handler
• Accepts POST tool-call envelopes from Vapi.
• Optionally sends WhatsApp (unless {"dry": true}).
• Replies with {"results": [{"toolCallId": "…", "result": …}]}.

2025-05-29  No code changes were required for the new *agent* field because
            the handler already forwards the entire summary returned by
            `property_search.summarise`.
2025-05-26  Added support for OpenAI-style nested function envelopes.
"""

from __future__ import annotations

# local
from property_search import (  # type: ignore  # pylint: disable=import-error
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

# helper utilities live in lib/
sys.path.append("lib")

# ──────────────────────────────────────────────────────────────────────
CORS = {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


def _json_response(
    code: int,
    payload: Dict[str, Any] | List[Any] | str = "",
) -> tuple[int, list[tuple[str, str]], bytes]:
    """Return `(status, headers, body_bytes)` for `send_response`."""
    headers = [("Content-Type", "application/json"), *CORS.items()]
    body = payload if isinstance(payload, str) else json.dumps(
        payload, ensure_ascii=False
    )
    return code, headers, body.encode()


# ───────────────────── entry-point class ──────────────────────────────
class handler(BaseHTTPRequestHandler):  # pylint: disable=invalid-name
    """Vercel searches for a symbol literally called `handler`."""

    # suppress default access log
    def log_message(self, *_):  # noqa: D401
        return

    # CORS pre-flight ------------------------------------------------------
    def do_OPTIONS(self):  # pylint: disable=invalid-name
        code, hdrs, body = _json_response(204, "")
        self.send_response(code)
        for k, v in hdrs:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    # POST ----------------------------------------------------------------
    def do_POST(self):  # pylint: disable=invalid-name
        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            envelope = json.loads(raw or "{}")
        except Exception:
            self._send(*_json_response(400, {"error": "invalid JSON"}))
            return

        self._send(*self._process_envelope(envelope))

    # logic ---------------------------------------------------------------
    def _process_envelope(
        self, env: Dict[str, Any]
    ) -> tuple[int, list[tuple[str, str]], bytes]:

        message = env.get("message") or {}
        calls: list = (
            message.get("toolCallList") or  # current Vapi name
            message.get("toolCalls") or []  # legacy alias
        )
        if not calls:
            return _json_response(400, {"error": "no tool calls in body"})

        # shared resources per Lambda invocation
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

            # unwrap OpenAI-style envelope if present
            fn_wrapper = call.get("function") if call.get(
                "type") == "function" else {}
            name = fn_wrapper.get("name") or call.get("name")
            args = fn_wrapper.get("arguments") or call.get("arguments") or {}

            if not name:
                results.append({"toolCallId": tc_id,
                                "result": "tool name missing"})
                continue
            if name != "find_property":
                results.append({"toolCallId": tc_id,
                                "result": f"unsupported tool {name}"})
                continue

            # build query --------------------------------------------------
            loc = (args.get("location") or "").strip()
            if not loc:
                results.append({"toolCallId": tc_id,
                                "result": "location is required"})
                continue

            query: Dict[str, Any] = {
                "keyword": loc,
                "purpose": args.get("purpose", "all"),
            }
            for fld in ("beds_min", "baths_min", "price_min", "price_max"):
                if fld in args and args[fld] is not None:
                    query[fld] = args[fld]

            canon = normalise_subcategory(args.get("subcategory") or "")
            if canon:
                query["subcategories"] = {"$regex": canon, "$options": "i"}

            # search -------------------------------------------------------
            try:
                doc, _tier = repo.find_one(query)
                if not doc:
                    results.append({"toolCallId": tc_id,
                                    "result": "no property found"})
                    continue
                summary = summarise(doc)          # ← includes 'agent'
            except Exception as exc:  # pylint: disable=broad-except
                traceback.print_exc()
                results.append({"toolCallId": tc_id,
                                "result": f"search error: {exc}"})
                continue

            # WhatsApp (optional) -----------------------------------------
            phone = (args.get("phone_number") or "").strip()
            if phone and not args.get("dry"):
                try:
                    send_whatsapp(cfg, phone, summary)
                    summary["whatsapp"] = "sent"
                except Exception as exc:          # pylint: disable=broad-except
                    summary["whatsapp"] = f"error: {exc}"

            results.append({"toolCallId": tc_id, "result": summary})

        return _json_response(200, {"results": results})

    # helper --------------------------------------------------------------
    def _send(self, code: int,
              headers: list[tuple[str, str]],
              body: bytes):
        self.send_response(code)
        for k, v in headers:
            self.send_header(k, v)
        self.end_headers()
        if body:
            buf = io.BytesIO(body)               # avoid broken-pipe
            self.wfile.write(buf.read())
