#!/usr/bin/env python3
# File: api/airtable_vapi_handler.py
"""
Vapi → Airtable upsert proxy  (Jefferies London)

• Endpoint :  /api/airtable_vapi_handler
• Tool name:  upsert_contact_jefferieslondon
• Auth     :  x-api-key header matches env VAPI_API_KEY
"""

from __future__ import annotations

import importlib.util
import io
import json
import os
import pathlib
import sys
import traceback
from http.server import BaseHTTPRequestHandler
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────
#  Dynamically load airtable_upsert.py that sits in the same dir
# ─────────────────────────────────────────────────────────────
_THIS_DIR = pathlib.Path(__file__).parent
_UPSERT = _THIS_DIR / "airtable_upsert.py"
if not _UPSERT.exists():
    raise ImportError(
        "airtable_upsert.py missing beside airtable_vapi_handler.py")

spec = importlib.util.spec_from_file_location("airtable_upsert", _UPSERT)
_airtable_upsert = importlib.util.module_from_spec(spec)  # type: ignore
sys.modules["airtable_upsert"] = _airtable_upsert
spec.loader.exec_module(_airtable_upsert)  # type: ignore

upsert_to_airtable = _airtable_upsert.upsert_to_airtable  # type: ignore

# ─────────────────────────────────────────────────────────────
TOOL_NAME = "upsert_contact_jefferieslondon"
CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, x-api-key",
}


def _json_response(
    status: int, payload: Dict[str, Any] | List[Any] | str
) -> Tuple[int, List[Tuple[str, str]], bytes]:
    hdrs = [("Content-Type", "application/json"), *CORS.items()]
    body = payload if isinstance(payload, str) else json.dumps(
        payload, ensure_ascii=False
    )
    return status, hdrs, body.encode()


# ───────────────────────── Vercel entry-point ──────────────────────────────
class handler(BaseHTTPRequestHandler):  # pylint: disable=invalid-name
    """The symbol Vercel looks for."""

    def log_message(self, fmt: str, *args) -> None:  # noqa: D401
        return  # silence

    # ---- CORS pre-flight --------------------------------------------------
    def do_OPTIONS(self):  # pylint: disable=invalid-name
        self._send(*_json_response(204, ""))

    # ---- main POST --------------------------------------------------------
    def do_POST(self):  # pylint: disable=invalid-name
        expected_key = os.getenv("VAPI_API_KEY")
        got_key = next(
            (v for k, v in self.headers.items() if k.lower() == "x-api-key"), None
        )
        if not expected_key or got_key != expected_key:
            self._send(*_json_response(403, {"error": "Forbidden"}))
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            env = json.loads(self.rfile.read(length) or "{}")
        except Exception:
            self._send(*_json_response(400, {"error": "invalid JSON"}))
            return

        self._send(*self._process_envelope(env))

    # ---- Vapi envelope → results[] ---------------------------------------
    def _process_envelope(
        self, env: Dict[str, Any]
    ) -> Tuple[int, List[Tuple[str, str]], bytes]:
        message = env.get("message") or {}
        calls: list = message.get(
            "toolCallList") or message.get("toolCalls") or []
        if not calls:
            return _json_response(400, {"error": "no tool calls in body"})

        load_dotenv()  # useful for `vercel dev`
        results = []

        for call in calls:
            tc_id = call.get("id", "unknown")

            fn_block = call.get("function") if call.get(
                "type") == "function" else {}
            name = fn_block.get("name") or call.get("name")
            args = fn_block.get("arguments") or call.get("arguments") or {}

            if name != TOOL_NAME:
                results.append(
                    {"toolCallId": tc_id, "result": f"unsupported tool {name}"}
                )
                continue

            try:
                res = upsert_to_airtable({"inputVars": args})
                results.append({"toolCallId": tc_id, "result": res["status"]})
            except Exception as exc:  # pylint: disable=broad-except
                traceback.print_exc()
                results.append(
                    {"toolCallId": tc_id, "result": f"error: {exc}"})

        return _json_response(200, {"results": results})

    # ---- send helper ------------------------------------------------------
    def _send(self, code: int, headers: List[Tuple[str, str]], body: bytes):
        self.send_response(code)
        for k, v in headers:
            self.send_header(k, v)
        self.end_headers()
        if body:
            io.BytesIO(body).readinto(self.wfile)
