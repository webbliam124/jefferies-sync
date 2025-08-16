#!/usr/bin/env python3
# api/vapi_proxy.py
# Vapi ➜ (optional) dynamic resolver ➜ destination JSON
# Works on Vercel (serverless) and locally (HTTPServer below)

from __future__ import annotations
from http.server import BaseHTTPRequestHandler, HTTPServer

import hashlib
import hmac
import json
import logging
import os
import re
import sys
import time
import ssl
import urllib.request
from typing import Any, Dict, Optional, Tuple
from urllib.error import HTTPError, URLError

# ──────────────────────────────────────────────────────────────────────────────
# configuration & logging
# ──────────────────────────────────────────────────────────────────────────────

DEBUG = os.getenv("DEBUG") == "1"


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + f".{int((time.time()%1)*1000):03d}Z"


logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(message)s",
)
LOG = logging.getLogger("vapi-proxy")


def _log(level: str, msg: str, **kv: Any) -> None:
    parts = [f"{_ts()} | {level.upper():5} | {msg}"]
    if kv:
        try:
            parts.append(
                "| " + " ".join(f"{k}={json.dumps(v, ensure_ascii=False)}" for k, v in kv.items()))
        except Exception:
            parts.append("| (kv-encode-failed)")
    print(" ".join(parts), flush=True)


# env
VAPI_SECRET = os.getenv("VAPI_SECRET", "")
DYN_ENABLED = os.getenv("DYNAMIC_TRANSFER_ENABLED") == "1"
DYN_URL = os.getenv("DYNAMIC_TRANSFER_URL", "")
DYN_SECRET = os.getenv("DYNAMIC_TRANSFER_SECRET", VAPI_SECRET)

FORWARD_URL = os.getenv("FORWARD_URL", "")  # optional analytics sink
FORWARD_RETRY = os.getenv("FORWARD_RETRY", "0") == "1"

DIAL_CODE = os.getenv("COUNTRY_DIAL_CODE", "+44")
CLI_DEFAULT = os.getenv("DEFAULT_CALLER_ID", "")
OUTBOUND_CLI = os.getenv("OUTBOUND_CLI", CLI_DEFAULT)

DEFAULT_TRANSFER_MODE = (
    os.getenv("DEFAULT_TRANSFER_MODE") or "warm").lower().strip()

# directories (inline JSON)


def _env_json(name: str) -> dict:
    raw = os.getenv(name, "") or ""
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        _log("warning", f"{name} JSON failed to parse", sample=raw[:120])
        return {}


CONTACTS = _env_json("CONTACTS_JSON")           # "Name" -> "+44..."
ASSISTANTS = _env_json("ASSISTANTS_JSON")       # "alias" -> assistantId
ALIASES = _env_json("ALIASES_JSON")             # "alias" -> "Canonical Name"
# "Canonical Name" -> {mode, callerId}
PREFERENCES = _env_json("PREFERENCES_JSON")

# ──────────────────────────────────────────────────────────────────────────────
# helpers
# ──────────────────────────────────────────────────────────────────────────────

_E164_RE = re.compile(r"^\+\d{6,18}$")


def _norm_e164(num: Optional[str]) -> Optional[str]:
    """Normalize to E.164. Returns None if impossible."""
    if not num:
        return None
    # strip everything except digits and '+'
    s = re.sub(r"[^\d+]", "", str(num))
    if not s:
        return None
    if s.startswith("+"):
        return s if _E164_RE.match(s) else None
    # handle UK-style leading zero
    if s.startswith("0"):
        cand = DIAL_CODE + s.lstrip("0")
        return cand if _E164_RE.match(cand) else None
    # raw national w/o 0 – assume DIAL_CODE
    cand = DIAL_CODE + s
    return cand if _E164_RE.match(cand) else None


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return "<unserializable>"


def _hmac_ok(raw: bytes, signature: str, secret: str) -> bool:
    if not signature or not secret:
        return False
    try:
        mac = hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()
        return hmac.compare_digest(mac, signature.lower())
    except Exception:
        return False


def _auth_ok(headers: dict, raw: bytes) -> Tuple[bool, str]:
    """Allow either x-vapi-secret (plain) OR x-vapi-signature (HMAC SHA256)."""
    plain = headers.get("x-vapi-secret") or headers.get("x-vapi-signature")
    if VAPI_SECRET and plain == VAPI_SECRET:
        _log("info", "auth: ok via x-vapi-secret (plain)")
        return True, "plain"
    sig = headers.get("x-vapi-signature", "")
    if _hmac_ok(raw, sig, VAPI_SECRET):
        _log("info", "auth: ok via x-vapi-signature (hmac)")
        return True, "hmac"
    return False, "none"


def _post(url: str, blob: bytes, headers: dict, timeout: float = 10.0) -> Tuple[int, bytes, dict]:
    req = urllib.request.Request(
        url, data=blob, headers=headers, method="POST")
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, context=ssl.create_default_context(), timeout=timeout) as r:
            body = r.read()
            dt = int((time.perf_counter() - t0) * 1000)
            _log("info", "http", url=url, status=r.status,
                 ms=dt, out_len=len(body))
            return r.status, body, dict(r.headers)
    except HTTPError as e:
        emsg = e.read().decode(errors="ignore")
        dt = int((time.perf_counter() - t0) * 1000)
        _log("warning", "http-error", url=url,
             status=e.code, ms=dt, error=emsg[:400])
        return e.code, emsg.encode(), {}
    except URLError as e:
        dt = int((time.perf_counter() - t0) * 1000)
        _log("warning", "http-error", url=url, status=0, ms=dt, error=str(e))
        return 0, b"", {}


def _forward_elsewhere(raw: bytes, headers: dict) -> None:
    if not FORWARD_URL:
        return
    # strip auth, pass a correlation id if present
    hdrs = {"Content-Type": "application/json"}
    if "x-call-id" in headers:
        hdrs["x-call-id"] = headers["x-call-id"]
    st, _, _ = _post(FORWARD_URL, raw, hdrs, timeout=6.0)
    if st != 200 and FORWARD_RETRY:
        _log("warning", "forward failed; retrying once", status=st)
        _post(FORWARD_URL, raw, hdrs, timeout=6.0)


def _build_transfer_plan(mode: str, summary: bool = True) -> dict:
    mode = (mode or "warm").lower()
    if mode.startswith("blind"):
        return {"mode": "blind-transfer", "sipVerb": "refer"}
    # default warm plan with summary
    plan = {
        "mode": "warm-transfer-experimental",
        "summaryPlan": {
            "enabled": bool(summary),
            "messages": [
                {"role": "system", "content": "Provide a concise summary of the call."},
                {"role": "user", "content": "Here is the transcript:\n\n{{transcript}}\n\n"},
            ],
        },
        "fallbackPlan": {
            "message": "Could not complete the transfer. I’m still here.",
            "endCallEnabled": False,
        },
    }
    return plan


def _choose_cli(canonical: str) -> str:
    # per-contact override via PREFERENCES_JSON
    pref = (PREFERENCES.get(canonical) or {})
    return pref.get("callerId") or OUTBOUND_CLI or CLI_DEFAULT or ""


def _choose_mode(canonical: str) -> str:
    pref = (PREFERENCES.get(canonical) or {})
    return (pref.get("mode") or DEFAULT_TRANSFER_MODE or "warm").lower()


def _resolve_target(target_name: str) -> Tuple[Optional[dict], Optional[str]]:
    """
    Resolve a name into a destination dict. Returns (destination, errorMessage).
    Handles assistants and humans; enforces E.164 for numbers.
    """
    if not target_name:
        return None, "missing targetName"

    raw = target_name.strip()
    key = raw.casefold()

    # alias normalisation
    if ALIASES:
        for alias, canonical in ALIASES.items():
            if alias.casefold() == key:
                raw = canonical
                key = raw.casefold()
                break

    # assistants
    for name, asst_id in ASSISTANTS.items():
        if name.casefold() == key:
            _log("info", "resolve_target → assistant", name=name, id=asst_id)
            return {
                "type": "assistant",
                "assistantId": asst_id,
                "message": f"Connecting you to {name}.",
            }, None

    # humans (contacts)
    for name, phone in CONTACTS.items():
        if name.casefold() == key:
            number = _norm_e164(phone)
            if not number:
                return None, f"invalid phone for {name}"
            mode = _choose_mode(name)
            caller_id = _choose_cli(name)
            _log(
                "info",
                "resolve_target → number",
                name=name, number=number, mode=mode, cli=caller_id
            )
            dest = {
                "type": "number",
                "number": number,
                "callerId": caller_id or None,
                "message": f"Transferring you to {name}. Please hold.",
                "transferPlan": _build_transfer_plan(mode, summary=True),
            }
            return dest, None

    return None, "no_match"


def _extract_args(evt: dict) -> dict:
    """
    Pulls parameters from either:
      - message.functionCall.parameters (new shape)
      - artifact.toolCall.arguments (stringified JSON, legacy)
      - phone-call-control.forwardingPhoneNumber (name fallthrough)
    """
    # new shape
    params = (evt.get("functionCall") or {}).get("parameters")
    if isinstance(params, dict):
        return params

    # legacy artifact
    args = (((evt.get("artifact") or {}).get("toolCall") or {}).get("arguments"))
    if isinstance(args, str):
        try:
            return json.loads(args)
        except Exception:
            pass
    elif isinstance(args, dict):
        return args

    # phone-control forward using a name
    if evt.get("type") == "phone-call-control" and evt.get("request") == "forward":
        fwd = evt.get("forwardingPhoneNumber")
        if fwd and re.search(r"[A-Za-z]", str(fwd)):  # looks like a name, not digits
            return {"targetName": str(fwd)}

    return {}

# ──────────────────────────────────────────────────────────────────────────────
# HTTP handler (works on Vercel as “handler” class)
# ──────────────────────────────────────────────────────────────────────────────


def _json_resp(code: int, payload: Dict[str, Any] | str) -> Tuple[int, list, bytes]:
    body = payload.encode() if isinstance(
        payload, str) else json.dumps(payload).encode()
    return code, [("Content-Type", "application/json")], body


class handler(BaseHTTPRequestHandler):  # noqa: N801
    def log_message(self, *_: Any) -> None:
        return  # silence BaseHTTPRequestHandler's default access log

    # core
    def do_POST(self) -> None:  # noqa: N802
        raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        hdrs = {k.lower(): v for k, v in self.headers.items()}
        body_len = len(raw)
        _log("info", "request", path=self.path, body_len=body_len)

        # auth
        ok, how = _auth_ok(hdrs, raw)
        if not ok:
            self._send(*_json_resp(401, {"error": "unauthenticated"}))
            return

        # parse body
        try:
            data = json.loads(raw or b"{}")
        except Exception:
            self._send(*_json_resp(400, {"error": "invalid JSON"}))
            return

        # events can be naked or nested under "message"
        evt = data.get("message") if isinstance(
            data.get("message"), dict) else data
        etype = evt.get("type")
        _log("info", "event.type="+str(etype or ""))

        # healthcheck for sanity testing
        if etype == "healthcheck":
            self._send(*_json_resp(200, {"ok": True}))
            return

        # main: TRANSFER DESTINATION REQUEST
        if etype == "transfer-destination-request":
            # try dynamic resolver first (if enabled)
            if DYN_ENABLED and DYN_URL:
                # forward entire event; resolver knows how to read it
                blob = json.dumps(evt).encode()
                hdr = {"Content-Type": "application/json",
                       "x-vapi-secret": DYN_SECRET or ""}
                _log("info", "resolver.call",
                     url=DYN_URL, secret=("set" if DYN_SECRET else "missing"),
                     len=len(blob))
                st, out, _ = _post(DYN_URL, blob, hdr, timeout=12.0)
                if st == 200:
                    try:
                        j = json.loads(out or b"{}")
                    except Exception:
                        j = {}
                    if isinstance(j, dict) and j.get("destination"):
                        # log & return (also include legacy shim)
                        resp = _with_legacy(j)
                        _log("info", "OUT", destination=resp.get("destination"))
                        self._send(*_json_resp(200, resp))
                        return
                    else:
                        _log("warning", "resolver: 200 but no destination in body")
                else:
                    _log("warning", "resolver: non-200", status=st)

            # fallback: resolve locally from tool parameters
            args = _extract_args(evt)
            target = (args.get("targetName") or "").strip()
            lang = (args.get("language") or "").strip().lower()

            # language-only hint → route to assistant if configured
            if not target and lang:
                if lang in ("mt", "maltese"):
                    target = "jessemulti"
                elif lang in ("el", "ell", "greek"):
                    target = "jessegreek"

            if not target:
                _log("warning", "no targetName in request")
                self._send(
                    *_json_resp(200, {"error": "no_match", "hint": "supply targetName"}))
                return

            dest, err = _resolve_target(target)
            if not dest:
                self._send(*_json_resp(200, {"error": err or "no_match"}))
                return

            resp = _with_legacy({"destination": dest})
            _log("info", "OUT", destination=resp.get("destination"))
            self._send(*_json_resp(200, resp))
            return

        # optional: intercept a rogue forward with a *name* and answer with a destination
        if etype == "phone-call-control" and evt.get("request") == "forward":
            req = evt.get("forwardingPhoneNumber", "")
            _log("info", "phone-control.forward", request=_safe_json(req))
            # If it's a *name* not a number, try to resolve and answer with a destination anyway
            if req and re.search(r"[A-Za-z]", str(req)):
                dest, err = _resolve_target(str(req))
                if dest:
                    resp = _with_legacy({"destination": dest})
                    _log("info", "OUT (from phone-control)",
                         destination=resp.get("destination"))
                    self._send(*_json_resp(200, resp))
                    return
                else:
                    _log("warning", "forward name not found", name=req, error=err)

        # everything else: forward (optional) and ack
        if FORWARD_URL:
            _forward_elsewhere(raw, hdrs)
        self._send(*_json_resp(200, {"success": True}))

    def _send(self, code: int, hdrs: list, body: bytes) -> None:
        try:
            self.send_response(code)
            for k, v in hdrs:
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)
        except BrokenPipeError:
            pass

# add legacy shim for old SDKs that read transferDestination


def _with_legacy(body: dict) -> dict:
    body = dict(body or {})
    dest = body.get("destination") or {}
    legacy = {}
    if dest.get("type") == "number":
        legacy = {
            "type": "phone-number",
            "phoneNumber": dest.get("number"),
            "assistantId": None,
            "callerId": dest.get("callerId") or None,
        }
    elif dest.get("type") == "assistant":
        legacy = {
            "type": "assistant",
            "assistantId": dest.get("assistantId"),
            "phoneNumber": None,
            "callerId": None,
        }
    if legacy:
        body["transferDestination"] = legacy
    return body

# ──────────────────────────────────────────────────────────────────────────────
# local dev entrypoint
# ──────────────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    port = int(os.getenv("PORT") or "8000")
    _log("info", f"★ vapi_proxy listening on http://0.0.0.0:{port}",
         dyn_enabled=DYN_ENABLED, dyn_url=bool(DYN_URL),
         have_contacts=bool(CONTACTS), have_assts=bool(ASSISTANTS))
    HTTPServer(("", port), handler).serve_forever()
