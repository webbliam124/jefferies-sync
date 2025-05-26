#!/usr/bin/env python3
"""
Vapi Custom-Tool handler
───────────────────────
• Accepts Vapi “tool-calls” POST payloads
• Runs a property search (lib/property_search.py) using the passed arguments
• Optionally fires WhatsApp (unless "dry": true)
• Returns the required   {"results":[{"toolCallId":"…","result":…}]}   structure
• Adds basic CORS so you can hit it from Vapi’s web tester

Expected tool name      :  find_property
Expected argument object:  {
    "name": "Alice",
    "location": "London",
    "beds_min": 2,
    "baths_min": 1,
    "price_min": 500000,
    "price_max": 1000000,
    "phone_number": "2776…",
    "status": "current",
    "purpose": "sale|rental",
    "dry": true
}
"""

from __future__ import annotations

import json
import traceback
from typing import Any, Dict, Tuple, List

from dotenv import load_dotenv

# helper module lives in lib/
from lib.property_search import (  # pylint: disable=import-error
    Settings,
    PropertyRepository,
    summarise,
    send_whatsapp,
)

# ──────────────── simple CORS helpers ───────────────────────────
CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


def _response(
    code: int, payload: Dict[str, Any] | str | List[Any] = ""
) -> Tuple[int, Dict[str, str], str]:
    hdrs = {"Content-Type": "application/json", **CORS}
    body = payload if isinstance(payload, str) else json.dumps(
        payload, ensure_ascii=False)
    return code, hdrs, body


# ──────────────── Vercel entrypoint ─────────────────────────────
def handler(req):  # Vercel passes a werkzeug-like Request object
    # -- Pre-flight ---------------------------------------------------------
    if req.method == "OPTIONS":
        return _response(204)

    if req.method != "POST":
        return _response(405, {"error": "only POST supported"})

    # -- Parse JSON ---------------------------------------------------------
    try:
        data = req.get_json(force=True)
    except Exception:
        return _response(400, {"error": "invalid JSON"})

    if not isinstance(data, dict):
        return _response(400, {"error": "body must be an object"})

    message = data.get("message") or {}
    calls: list = message.get("toolCallList") or []
    if not calls:
        return _response(400, {"error": "no toolCallList in body"})

    results = []

    # -- Load env + repo once ----------------------------------------------
    load_dotenv()
    try:
        cfg = Settings.from_env()
        repo = PropertyRepository(cfg)
        if not repo.ping():
            return _response(503, {"error": "database unavailable"})
    except Exception as exc:  # pylint: disable=broad-except
        return _response(500, {"error": f"config error: {exc}"})

    # -- Process each call --------------------------------------------------
    for call in calls:
        tc_id = call.get("id") or "unknown"
        name = call.get("name")
        args = call.get("arguments") or {}

        if name != "find_property":
            results.append(
                {"toolCallId": tc_id, "result": f"unsupported tool {name}"})
            continue

        # --- Build search query -------------------------------------------
        try:
            kw = args["location"].strip()
        except (KeyError, AttributeError):
            results.append(
                {"toolCallId": tc_id, "result": "location is required"})
            continue

        query = {"keyword": kw, "status": args.get("status", "current")}
        for fld in ("beds_min", "baths_min", "price_min", "price_max", "purpose"):
            if fld in args:
                query[fld] = args[fld]

        # --- Search --------------------------------------------------------
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

        # --- Optional WhatsApp --------------------------------------------
        phone = (args.get("phone_number") or "").strip()
        if phone and not args.get("dry"):
            try:
                send_whatsapp(cfg, phone, summary)
                summary["whatsapp"] = "sent"
            except Exception as exc:  # pylint: disable=broad-except
                summary["whatsapp"] = f"error: {exc}"

        results.append({"toolCallId": tc_id, "result": summary})

    return _response(200, {"results": results})
