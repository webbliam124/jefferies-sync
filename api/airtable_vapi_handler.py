#!/usr/bin/env python3
# File: api/airtable_vapi_handler.py
"""
Vapi → Airtable upsert proxy  (Jefferies London edition)

• Deployed at  /api/airtable_vapi_handler
• Accepts POST envelopes from Vapi (tool-calls format)
• Only recognises the tool  "upsert_contact_jefferieslondon"
• Returns   {"results":[{"toolCallId": "...", "result": "success"}]}

Auth:
─────
The request must include   x-api-key: jefferiesLondonSecret-api-key124
(or set the same value in Vercel as VAPI_API_KEY).

Runtime:
────────
Python ≥3.12   ·   Depends on  api/airtable_upsert.py
"""

from __future__ import annotations

import io
import json
import os
import sys
import traceback
from http.server import BaseHTTPRequestHandler
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv

# --------------------------------------------------------------------------- #
#  Bring in the business-logic function (merge-or-create contact in Airtable)
# --------------------------------------------------------------------------- #
try:
    # When "api" is detected as a package (Vercel runtime)
    from .airtable_upsert import upsert_to_airtable
except ModuleNotFoundError:
    # When running locally via  python api/airtable_vapi_handler.py …
    sys.path.append("api")          # add repo/api to path
    from airtable_upsert import upsert_to_airtable  # type: ignore

# --------------------------------------------------------------------------- #
#  Constants
# --------------------------------------------------------------------------- #
TOOL_NAME = "upsert_contact_jefferieslondon"
CORS = {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, x-api-key",
}
# Default API key – override with env var in production
DEFAULT_API_KEY = "jefferiesLondonSecret-api-key124"

# --------------------------------------------------------------------------- #
#  Small helpers
# --------------------------------------------------------------------------- #


def _json_response(
    status: int, payload: Dict[str, Any] | List[Any] | str
) -> Tuple[int, List[Tuple[str, str]], bytes]:
    headers = [("Content-Type", "application/json"), *CORS.items()]
    body = payload if isinstance(payload, str) else json.dumps(
        payload, ensure_ascii=False
    )
    return status, headers, body.encode()


# --------------------------------------------------------------------------- #
#  Vercel entry-point
# --------------------------------------------------------------------------- #
class handler(BaseHTTPRequestHandler):  # pylint: disable=invalid-name
    """Vercel looks for this exact symbol in the module."""

    # Quiet default logging
    def log_message(self, fmt: str, *args) -> None:  # noqa: D401
        return

    # ---------- CORS pre-flight -------------------------------------------
    def do_OPTIONS(self):  # pylint: disable=invalid-name
        code, hdrs, body = _json_response(204, "")
        self.send_response(code)
        for k, v in hdrs:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    # ---------- main POST --------------------------------------------------
    def do_POST(self):  # pylint: disable=invalid-name
        # 0  API-key check
        expected = os.getenv("VAPI_API_KEY", DEFAULT_API_KEY)
        got = next(
            (v for k, v in self.headers.items() if k.lower() == "x-api-key"),
            None,
        )
        if got != expected:
            self._send(*_json_response(403, {"error": "Forbidden"}))
            return

        # 1  parse body
        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            envelope = json.loads(raw or "{}")
        except Exception:
            self._send(*_json_response(400, {"error": "invalid JSON"}))
            return

        # 2  process envelope
        code, hdrs, body = self._process_envelope(envelope)
        self._send(code, hdrs, body)

    # ---------- envelope → results[] --------------------------------------
    def _process_envelope(
        self, env: Dict[str, Any]
    ) -> Tuple[int, List[Tuple[str, str]], bytes]:
        message = env.get("message") or {}
        calls: list = (
            message.get("toolCallList") or message.get("toolCalls") or []
        )
        if not calls:
            return _json_response(400, {"error": "no tool calls in body"})

        # (local dev convenience)
        load_dotenv()

        results = []
        for call in calls:
            tc_id = call.get("id", "unknown")

            # ── unwrap Vapi/OpenAI function wrapper ────────────────────────
            if call.get("type") == "function":
                fn_block = call.get("function") or {}
            else:
                fn_block = {}

            name = fn_block.get("name") or call.get("name")
            args = fn_block.get("arguments") or call.get("arguments") or {}

            if name != TOOL_NAME:
                results.append(
                    {"toolCallId": tc_id, "result": f"unsupported tool {name}"}
                )
                continue

            # ── call Airtable upsert logic ────────────────────────────────
            try:
                res = upsert_to_airtable({"inputVars": args})
                results.append({"toolCallId": tc_id, "result": res["status"]})
            except Exception as exc:  # pylint: disable=broad-except
                traceback.print_exc()
                results.append(
                    {"toolCallId": tc_id, "result": f"error: {exc}"}
                )

        return _json_response(200, {"results": results})

    # ---------- send helper -----------------------------------------------
    def _send(self, code: int, headers: List[Tuple[str, str]], body: bytes):
        self.send_response(code)
        for k, v in headers:
            self.send_header(k, v)
        self.end_headers()
        if body:
            buf = io.BytesIO(body)
            self.wfile.write(buf.read())
