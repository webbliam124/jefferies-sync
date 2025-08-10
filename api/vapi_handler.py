#!/usr/bin/env python3
from __future__ import annotations
from lib.property_search import Settings, PropertyRepository, summarise, send_whatsapp

import os
import sys
import json
import logging
from http.server import BaseHTTPRequestHandler
from typing import Any, Dict
from dotenv import load_dotenv

# add repo root so we can import lib/
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

load_dotenv(override=True)

LOG = logging.getLogger("vapi_handler")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
if os.getenv("DEBUG") == "1":
    LOG.setLevel(logging.DEBUG)

CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS, GET",
    "Access-Control-Allow-Headers": "Content-Type, x-vapi-secret, x-vapi-signature, x-call-id",
}

VAPI_SECRET = os.getenv("VAPI_SECRET")


def _json(code: int, payload: Any) -> tuple[int, list[tuple[str, str]], bytes]:
    headers = [("Content-Type", "application/json"), *CORS.items()]
    body = payload if isinstance(payload, str) else json.dumps(
        payload, ensure_ascii=False)
    return code, headers, body.encode()


class handler(BaseHTTPRequestHandler):  # noqa: N801
    def log_message(self, *_: Any) -> None:
        return

    def do_OPTIONS(self) -> None:  # noqa: N802
        code, hdrs, body = _json(200, {"ok": True})
        self._send(code, hdrs, body)

    def do_GET(self) -> None:  # noqa: N802
        if self.path.startswith("/healthz"):
            code, hdrs, body = _json(200, {"ok": True})
            self._send(code, hdrs, body)
            return
        code, hdrs, body = _json(404, {"error": "not_found"})
        self._send(code, hdrs, body)

    def do_POST(self) -> None:  # noqa: N802
        # shared-secret auth
        secret = (
            self.headers.get("x-vapi-secret")
            or self.headers.get("x-vapi-signature")
            or self.headers.get("secret")
        )
        if VAPI_SECRET and secret != VAPI_SECRET:
            code, hdrs, body = _json(401, {"error": "unauthenticated"})
            self._send(code, hdrs, body)
            return

        raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        try:
            data = json.loads(raw or "{}")
        except json.JSONDecodeError:
            code, hdrs, body = _json(400, {"error": "invalid_json"})
            self._send(code, hdrs, body)
            return

        evt = data.get("message") if isinstance(
            data.get("message"), dict) else data
        if (evt or {}).get("type") != "tool-calls":
            code, hdrs, body = _json(
                200, {"success": True, "ignored": (evt or {}).get("type")})
            self._send(code, hdrs, body)
            return

        tool_calls = evt.get("toolCalls") or evt.get("toolCallList") or []
        try:
            cfg = Settings.from_env()
            repo = PropertyRepository(cfg)
        except Exception as exc:
            code, hdrs, body = _json(
                500, {"error": "init_failed", "detail": str(exc)})
            self._send(code, hdrs, body)
            return

        results: List[dict] = []
        for call in tool_calls:
            tool_id = call.get("id") or call.get("toolCallId") or "unknown"
            fn = (call.get("function") or {}).get("name")
            args = (call.get("function") or {}).get("arguments") or {}

            if fn != "find_property":
                results.append({"toolCallId": tool_id, "result": {
                               "error": "unsupported_function", "name": fn}})
                continue

            try:
                if args.get("location") and not args.get("keyword"):
                    args["keyword"] = args["location"]

                doc, tier, debug = repo.find_best(args)
                if not doc:
                    results.append({"toolCallId": tool_id, "result": {
                                   "no_match": True, "tier": tier, "debug": debug}})
                    continue

                out = summarise(doc)
                out["tier"] = tier

                phone = args.get("phone_number")
                dry = bool(args.get("dry", True))
                if phone and not dry:
                    try:
                        send_whatsapp(cfg, phone, out)
                        out["whatsapp"] = "sent"
                    except Exception as exc:
                        out["whatsapp"] = f"failed: {exc}"
                else:
                    out["whatsapp"] = "skipped"

                results.append({"toolCallId": tool_id, "result": out})
            except Exception as exc:
                LOG.exception("search_failed")
                results.append({"toolCallId": tool_id, "result": {
                               "error": "search_failed", "detail": str(exc)}})

        code, hdrs, body = _json(200, {"results": results})
        self._send(code, hdrs, body)

    def _send(self, code: int, hdrs: list[tuple[str, str]], body: bytes) -> None:
        self.send_response(code)
        for k, v in hdrs:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    LOG.info("★ vapi_handler listening on http://0.0.0.0:%s (DEBUG=%s)",
             port, os.getenv("DEBUG") == "1")
    HTTPServer(("", port), handler).serve_forever()
