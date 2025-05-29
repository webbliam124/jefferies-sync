#!/usr/bin/env python3
"""
Dynamic transfer-destination endpoint for Vapi.

• Path: /api/dynamic_transfer_handler
• Triggered by the `transfer-destination-request` webhook.
• Examines previous assistant messages to find the property’s agent,
  then returns a warm-transfer destination with a short summary.

This function is stateless and relies only on the payload Vapi sends.
"""

from __future__ import annotations

import io
import json
import os
import re
import sys
from http.server import BaseHTTPRequestHandler
from typing import Any, Dict, List, Tuple

# ─────────────── helpers ──────────────────────────────────────────────
CORS = {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


def _json_response(code: int,
                   payload: Dict[str, Any] | List[Any] | str
                   ) -> Tuple[int, list[tuple[str, str]], bytes]:
    headers = [("Content-Type", "application/json"), *CORS.items()]
    body = payload if isinstance(payload, str) else json.dumps(payload,
                                                               ensure_ascii=False)
    return code, headers, body.encode()


def _parse_agent_from_messages(msgs: List[Dict[str, Any]]
                               ) -> Tuple[str | None, str | None]:
    """
    Look through assistant messages for a JSON blob that contains an 'agent'
    object with 'phone_mobile' (preferred) or 'phone_direct'.
    Returns (number, name) or (None, None) if not found.
    """
    json_re = re.compile(r"{.*}", re.DOTALL)
    for m in reversed(msgs):                        # newest → oldest
        if m.get("role") != "assistant":
            continue
        # Vapi may wrap JSON in markdown fences – strip them
        content: str = m.get("content", "")
        match = json_re.search(content)
        if not match:
            continue
        try:
            blob = json.loads(match.group())
        except Exception:
            continue
        agent = blob.get("agent") or {}
        number = agent.get("phone_mobile") or agent.get("phone_direct")
        name = agent.get("name")
        if number:
            return number, name
    return None, None


# ─────────────── Vercel entry-point class ─────────────────────────────
class handler(BaseHTTPRequestHandler):                # noqa: N801
    """Vercel looks for a symbol literally called `handler`."""

    # quiet standard logging
    def log_message(self, *_args):  # noqa: D401
        return

    # -- CORS pre-flight --
    def do_OPTIONS(self):  # pylint: disable=invalid-name
        code, hdrs, body = _json_response(204, "")
        self.send_response(code)
        for k, v in hdrs:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    # -- POST (main path) --
    def do_POST(self):  # pylint: disable=invalid-name
        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            payload = json.loads(raw or "{}")
        except Exception:
            self._send(*_json_response(400, {"error": "invalid JSON"}))
            return

        if payload.get("type") != "transfer-destination-request":
            self._send(*_json_response(400, {"error": "unsupported type"}))
            return

        res = self._build_destination(payload)
        self._send(*res)

    # -- core logic --
    def _build_destination(
        self, body: Dict[str, Any]
    ) -> Tuple[int, list[tuple[str, str]], bytes]:

        artefact = body.get("artifact") or {}
        messages = artefact.get(
            "messagesOpenAIFormatted") or artefact.get("messages") or []
        transcript: str = artefact.get("transcript", "")

        number, name = _parse_agent_from_messages(messages)

        if not number:
            fallback = os.getenv("FALLBACK_NUMBER")
            if not fallback:
                return _json_response(200, {"error": "no agent number found"})
            number, name = fallback, "our main line"

        caller_id = os.getenv("DEFAULT_CALLER_ID", "")
        summary_lines = transcript.strip().splitlines()
        summary = summary_lines[-1] if len(
            summary_lines) == 1 else " ".join(summary_lines[-5:])

        response = {
            "destination": {
                "type": "number",
                "number": number,
                "message": f"Putting you through to {name}. Please hold.",
                "callerId": caller_id,
                "numberE164CheckEnabled": True,
                "transferPlan": {
                    "mode": "warm-transfer-with-summary",
                    "summaryPlan": {
                        "enabled": True,
                        "messages": [
                            {
                                "role": "system",
                                "content": "Provide a concise call summary."
                            },
                            {
                                "role": "user",
                                "content": f"Here is the transcript:\n\n{summary}\n"
                            }
                        ]
                    }
                }
            }
        }
        return _json_response(200, response)

    # -- send helper --
    def _send(self, code: int,
              headers: list[tuple[str, str]],
              body: bytes):
        self.send_response(code)
        for k, v in headers:
            self.send_header(k, v)
        self.end_headers()
        if body:
            buf = io.BytesIO(body)
            self.wfile.write(buf.read())
