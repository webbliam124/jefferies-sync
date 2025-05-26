#!/usr/bin/env python3
# File: api/vapi_handler.py
"""
Vapi.ai → Jefferies bridge (import-driven, no subprocess)
──────────────────────────────────────────────────────────
POST body example

{
  "name":         "Alice",
  "location":     "Hyde Park",
  "purpose":      "sale",             # sale | rental | all (optional, default all)
  "beds_min":     2,                  # optional
  "baths_min":    2,                  # optional
  "price_min":    500000,             # optional
  "price_max":    750000,             # optional
  "phone_number": "27761234567"       # required for WhatsApp send
}

Success response

{
  "message": "Alice – I’ve found a matching property in Bayswater. The brochure is on WhatsApp.",
  "property": { ...summary... }
}
"""

from __future__ import annotations
import importlib.util as _ilu

import json
import os
import sys
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any, Dict

# ───── import property_search.py by absolute path ───────────────────────
ROOT = Path(__file__).resolve().parent.parent
SEARCH_PATH = ROOT / "lib" / "property_search.py"


spec = _ilu.spec_from_file_location("property_search", SEARCH_PATH)
if spec is None or spec.loader is None:
    raise ImportError(f"Cannot load {SEARCH_PATH}")
_property_search = _ilu.module_from_spec(spec)

# --- ADD THIS LINE so dataclasses can find the module -------------------
sys.modules[spec.name] = _property_search
# ------------------------------------------------------------------------

spec.loader.exec_module(_property_search)  # type: ignore[attr-defined]

Settings = _property_search.Settings
PropertyRepository = _property_search.PropertyRepository
summarise = _property_search.summarise
send_whatsapp = _property_search.send_whatsapp

# ───── singletons reused across invocations ─────────────────────────────
CFG = Settings.from_env()
REPO = PropertyRepository(CFG)

# ───── helpers ──────────────────────────────────────────────────────────


def _find_matching(payload: Dict[str, Any]) -> Dict[str, Any] | None:
    qry: Dict[str, Any] = {
        "keyword": payload["location"],
        "purpose": payload.get("purpose", "all"),
    }
    for fld in ("beds_min", "baths_min", "price_min", "price_max"):
        if fld in payload and payload[fld] not in (None, "", 0):
            qry[fld] = payload[fld]

    doc, _tier = REPO.find_one(qry)
    return summarise(doc) if doc else None


def _success_msg(name: str | None, locality: str, sent: bool) -> str:
    if sent:
        return f"{name or 'Hi'} – I’ve found a matching property in {locality}. The brochure is on WhatsApp."
    return f"{name or 'Here are the details'} of a matching property in {locality}."


# ───── HTTP entry-point (works on Vercel/Lambda) ────────────────────────
class handler(BaseHTTPRequestHandler):  # noqa: N801
    def do_POST(self):  # noqa: N802
        try:
            body = self.rfile.read(
                int(self.headers.get("Content-Length", "0"))).decode()
            payload = json.loads(body or "{}")

            # validation
            if "location" not in payload:
                raise ValueError("location is required")
            if "phone_number" not in payload:
                raise ValueError("phone_number is required")

            summary = _find_matching(payload)
            if not summary:
                self._send(
                    200, {"message": "Sorry, no matching property found."})
                return

            phone = payload["phone_number"]
            try:
                send_whatsapp(CFG, phone, summary)
            except Exception as exc:  # pylint: disable=broad-except
                self._send(
                    200,
                    {
                        "message": "Property found but WhatsApp failed.",
                        "error": str(exc),
                        "property": summary,
                    },
                )
                return

            locality = summary["location"].get(
                "locality") or summary["address"]
            reply = {
                "message": _success_msg(payload.get("name"), locality, True),
                "property": summary,
            }
            self._send(200, reply)

        except Exception as exc:  # pylint: disable=broad-except
            if os.getenv("DEBUG"):
                import traceback
                traceback.print_exc()
            self._send(500, {"error": str(exc)})

    # ------------------------------------------------------------
    def _send(self, status: int, obj: Dict[str, Any]):
        blob = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(blob)


# ───── Local dev runner (optional) ──────────────────────────────────────
if __name__ == "__main__":
    import socketserver
    PORT = int(os.getenv("PORT", "8888"))
    with socketserver.TCPServer(("", PORT), handler) as httpd:
        print(f"Serving test endpoint on :{PORT}  (Ctrl-C to quit)")
        httpd.serve_forever()
