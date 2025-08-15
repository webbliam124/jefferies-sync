#!/usr/bin/env python3
# api/transfer_webhook_min.py

from __future__ import annotations
import json
import os
import re
import hmac
import hashlib
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Optional

VAPI_SECRET = os.getenv("VAPI_SECRET", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
COUNTRY_DIAL_CODE = os.getenv("COUNTRY_DIAL_CODE", "+44")
OUTBOUND_CLI = os.getenv("OUTBOUND_CLI", os.getenv("DEFAULT_CALLER_ID", ""))


def _load_json_env(name: str) -> dict:
    try:
        return json.loads(os.getenv(name, "{}"))
    except Exception:
        return {}


CONTACTS = _load_json_env("CONTACTS_JSON")      # "Full Name": "+44..."
ASSISTANTS = _load_json_env("ASSISTANTS_JSON")    # "key": "assistantId"
# "spoken variant": "Full Name or assistant key"
ALIASES = _load_json_env("ALIASES_JSON")
PREFERENCES = _load_json_env("PREFERENCES_JSON")   # optional per-target tweaks


def _json(code: int, payload: Dict[str, Any]) -> tuple[int, list[tuple[str, str]], bytes]:
    return code, [("Content-Type", "application/json")], json.dumps(payload).encode()


def _norm(num: Optional[str]) -> Optional[str]:
    if not num:
        return None
    s = re.sub(r"[^\d+]", "", num)
    if s.startswith("+"):
        return s
    if s.startswith("0"):
        return COUNTRY_DIAL_CODE + s.lstrip("0")
    if len(s) > 10:
        return "+" + s
    return None


def _signature_ok(raw: bytes, headers: Dict[str, str]) -> bool:
    sig = headers.get("x-vapi-signature")
    if WEBHOOK_SECRET and sig:
        expected = hmac.new(WEBHOOK_SECRET.encode(), raw,
                            hashlib.sha256).hexdigest()
        try:
            return hmac.compare_digest(sig, expected)
        except Exception:
            return False
    sec = headers.get("x-vapi-secret") or headers.get("secret")
    return (not VAPI_SECRET) or (sec == VAPI_SECRET)


def _get_args(evt: Dict[str, Any]) -> Dict[str, Any]:
    root = evt["message"] if isinstance(evt.get("message"), dict) else evt
    # New style: functionCall.parameters
    fc = root.get("functionCall") or {}
    params = fc.get("parameters")
    if isinstance(params, dict) and params:
        return params
    # Old style: artifact.toolCall.arguments
    args = ((root.get("artifact") or {}).get(
        "toolCall") or {}).get("arguments")
    if isinstance(args, dict):
        return args
    if isinstance(args, str):
        try:
            return json.loads(args)
        except Exception:
            return {}
    return {}


def _canonical_target(name: Optional[str], language: Optional[str]) -> Optional[str]:
    # Language-only routing
    if not name and language:
        lang = language.strip().lower()
        if lang in ("mt", "maltese", "mlt", "mti"):
            return "jessemulti"
        if lang in ("el", "ell", "greek", "gr"):
            return "jessegreek"
    if not name:
        return None

    raw = name.strip()
    lower = raw.lower()

    # Alias collapse (spoken variants → canonical)
    if lower in ALIASES:
        return ALIASES[lower]

    # Assistant keys supported by label or lower-key
    if raw in ASSISTANTS:
        return raw
    if lower in ASSISTANTS:
        return lower

    # Exact contact name or case-insensitive match
    if raw in CONTACTS:
        return raw
    for k in CONTACTS.keys():
        if k.lower() == lower:
            return k

    return None


def _destination_for(target: str, reason: Optional[str], complexity: Optional[str]) -> Dict[str, Any]:
    # Assistant → assistant transfer
    if target in ASSISTANTS:
        return {
            "type": "assistant",
            "assistantId": ASSISTANTS[target],
            "message": f"Connecting you to {target}."
        }

    # Human → phone number (warm transfer, with callerId)
    number = _norm(CONTACTS.get(target))
    if not number:
        return {"error": "no_match", "hint": "unknown target"}

    summary_msgs = [
        {"role": "system", "content": "Provide a concise summary of the call."}]
    extras = []
    if reason:
        extras.append(f"Reason: {reason}.")
    if complexity:
        extras.append(f"Complexity: {complexity}.")
    if extras:
        summary_msgs.append({"role": "user", "content": " ".join(extras)})

    dest = {
        "type": "number",
        "number": number,
        "message": f"Transferring you to {target}. Please hold.",
        "callerId": OUTBOUND_CLI or None,
        "transferPlan": {
            "mode": "warm-transfer-experimental",
            "summaryPlan": {"enabled": True, "messages": summary_msgs},
            "fallbackPlan": {
                "message": "Could not complete the transfer. I’m still here.",
                "endCallEnabled": False
            }
        }
    }
    prefs = PREFERENCES.get(target, {})
    if prefs.get("callerId"):
        dest["callerId"] = prefs["callerId"]
    return dest


class handler(BaseHTTPRequestHandler):
    def log_message(self, *_: Any) -> None:
        return

    def do_POST(self) -> None:  # noqa: N802
        raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        headers = {k.lower(): v for k, v in self.headers.items()}

        if not _signature_ok(raw, headers):
            return self._send(*_json(401, {"error": "unauthenticated"}))

        try:
            data = json.loads(raw or "{}")
        except Exception:
            return self._send(*_json(400, {"error": "invalid JSON"}))

        evt = data["message"] if isinstance(
            data.get("message"), dict) else data
        if evt.get("type") != "transfer-destination-request":
            return self._send(*_json(200, {"success": True}))

        params = _get_args(evt)
        targetName = params.get("targetName")
        language = params.get("language")
        reason = params.get("reason")
        complexity = params.get("complexity")

        canonical = _canonical_target(targetName, language)
        if not canonical:
            return self._send(*_json(200, {"error": "no_match", "hint": "set CONTACTS_JSON/ASSISTANTS_JSON or supply targetName"}))

        dest = _destination_for(canonical, reason, complexity)
        if "error" in dest:
            return self._send(*_json(200, dest))
        return self._send(*_json(200, {"destination": dest}))

    def _send(self, code: int, hdrs: list[tuple[str, str]], body: bytes) -> None:
        self.send_response(code)
        for k, v in hdrs:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    print("★ transfer_webhook_min listening on http://0.0.0.0:8000")
    HTTPServer(("", 8000), handler).serve_forever()
