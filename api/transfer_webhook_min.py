from __future__ import annotations
import json
import os
import re
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Tuple
from dotenv import load_dotenv

load_dotenv()
DEBUG = os.getenv("DEBUG") == "1"


def _log(*a):
    if DEBUG:
        print(*a, file=sys.stderr, flush=True)


DIAL_CODE = os.getenv("COUNTRY_DIAL_CODE", "+44")
FALLBACK_NUMBER = os.getenv("FALLBACK_NUMBER")
OUTBOUND_CLI = os.getenv("OUTBOUND_CLI") or os.getenv(
    "DEFAULT_CALLER_ID") or ""

CONTACTS = json.loads(os.getenv("CONTACTS_JSON", "{}") or "{}")
ASSISTANTS = json.loads(os.getenv("ASSISTANTS_JSON", "{}") or "{}")
ALIASES = json.loads(os.getenv("ALIASES_JSON", "{}") or "{}")
PREFS = json.loads(os.getenv("PREFERENCES_JSON", "{}") or "{}")


def _json(code: int, payload: Dict[str, Any] | str) -> Tuple[int, list, bytes]:
    return code, [("Content-Type", "application/json")], (payload if isinstance(payload, str) else json.dumps(payload)).encode()


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


def _resolve_target(name: str) -> Dict[str, Any]:
    # alias → canonical
    canonical = ALIASES.get(name.lower()) or name
    # assistant?
    if canonical in ASSISTANTS:
        return {"type": "assistant", "assistantId": ASSISTANTS[canonical], "message": f"Connecting you to {canonical}."}
    # human?
    number = _norm(CONTACTS.get(canonical)) or _norm(FALLBACK_NUMBER)
    if not number:
        return {"error": "no valid phone"}
    pref = PREFS.get(canonical, {})
    mode = (pref.get("mode") or "warm-transfer").lower()
    dest: Dict[str, Any] = {
        "type": "number",
        "number": number,
        "message": f"Transferring you to {canonical}. Please hold.",
        "callerId": pref.get("callerId") or OUTBOUND_CLI or None,
        "numberE164CheckEnabled": True,
        "transferPlan": {"mode": "warm-transfer-experimental",
                         "summaryPlan": {"enabled": True,
                                         "messages": [{"role": "system", "content": "Provide a concise summary of the call."},
                                                      {"role": "user", "content": "Here is the transcript:\n\n{{transcript}}\n\n"}]},
                         "fallbackPlan": {"message": "Could not complete the transfer. I’m still here.",
                                          "endCallEnabled": False}}
    }
    if mode.startswith("blind"):
        dest["transferPlan"] = {"mode": "blind-transfer", "sipVerb": "refer"}
    # ensure callerId is set for Twilio
    if dest["type"] == "number" and not dest.get("callerId"):
        dest["callerId"] = OUTBOUND_CLI or ""
    return dest


class handler(BaseHTTPRequestHandler):  # noqa: N801
    def log_message(self, *_): return

    def do_POST(self):
        try:
            raw = self.rfile.read(
                int(self.headers.get("Content-Length", "0")) or 0)
            body = json.loads(raw or "{}")
        except Exception:
            return self._send(*_json(200, {"error": "invalid JSON"}))

        evt = body.get("message") if isinstance(
            body.get("message"), dict) else body
        if evt.get("type") != "transfer-destination-request":
            return self._send(*_json(200, {"ignored": evt.get("type")}))

        args = (evt.get("artifact") or {}).get(
            "toolCall", {}).get("arguments", {})
        if isinstance(args, str):
            try:
                args = json.loads(args)
            except json.JSONDecodeError:
                args = {}
        target = (args.get("targetName") or "").strip()
        if not target:
            return self._send(*_json(200, {"error": "missing targetName"}))

        dest = _resolve_target(target)
        if dest.get("error"):
            return self._send(*_json(200, {"error": dest["error"]}))
        return self._send(*_json(200, {"destination": dest}))

    def _send(self, code: int, hdrs: list, body: bytes):
        self.send_response(code)
        for k, v in hdrs:
            self.send_header(k, v)
        self.end_headers()
        if body:
            self.wfile.write(body)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    _log(f"★ transfer_webhook_min listening on http://0.0.0.0:{port}")
    HTTPServer(("", port), handler).serve_forever()
