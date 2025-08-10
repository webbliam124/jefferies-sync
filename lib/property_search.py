#!/usr/bin/env python3
"""
property_search.py — robust Mongo search + WhatsApp notify
==========================================================

- Accepts flexible query dicts coming from Vapi tool calls (or CLI).
- Combines structured filters (purpose/status/type/price) with
  text search (location + features).
- Ranks candidates with a scoring function to pick the best match.
- Optionally sends an interactive WhatsApp e-brochure.

Env (required)
--------------
MONGODB_URI
DB_NAME                 (default: JefferiesJames)
COLLECTION_NAME         (default: properties)

WABA_TOKEN              (for WhatsApp send)
WABA_PHONE_ID
TEMPLATE_NAME           (default: send_property)
TEMPLATE_LANG           (default: en)

Notes
-----
• Works with both your "old" docs and the enriched docs that include:
  - price_sale_gbp / price_sort_gbp / price_rent_pcm_gbp
  - subcategory_canonical
  - attributes/attributes_full
  - location_terms
• Beds/Baths may be strings: we filter leniently and score precisely.
• `find_best(query)` returns (doc, tier, debug) where tier indicates
  which fallback matched.
"""

from __future__ import annotations

# stdlib
import argparse
import json
import logging
import math
import os
import re
import sys
from dataclasses import dataclass
from decimal import Decimal
from difflib import get_close_matches
from typing import Any, Dict, List, Optional, Tuple

# third-party
import requests
from dotenv import load_dotenv
from pymongo import ASCENDING, DESCENDING, MongoClient, TEXT
from pymongo.collection import Collection
from pymongo.errors import OperationFailure
from requests.exceptions import HTTPError, RequestException

# ─────────────────────────── setup ────────────────────────────
load_dotenv(override=True)
LOG = logging.getLogger("property_search")
if os.getenv("DEBUG") == "1":
    logging.basicConfig(level=logging.DEBUG,
                        format="%(levelname)s %(message)s")
else:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

# Rex e-brochure base (kept for compatibility if record lacks a link)
EBROCHURE_BASE = (
    "https://app.rexsoftware.com/public/ebrochure/?region=eu_uk_1&account_id=3877&listing_id="
)

# ─────────────── subcategory canon + feature mapping ───────────────
_SUBCAT_CANON = {
    "house": {
        "detached house", "semi-detached house", "terraced house",
        "end of terrace house", "mid terrace house", "town house", "mews house",
        "character property", "mews", "mews house", "mews home",
    },
    "flat": {
        "apartment", "apartments", "studio", "duplex", "penthouse",
        "maisonette", "flat",
    },
    "other": {"house boat", "houseboat", "boat"},
}

_LOOKUP = {
    syn.lower(): canon for canon, syns in _SUBCAT_CANON.items() for syn in syns
}
# make direct canon keys resolve too
for canon in list(_SUBCAT_CANON.keys()):
    _LOOKUP[canon] = canon

_FEATURE_MAP = {
    "private garden": {"garden", "roof garden", "roof terrace", "terrace"},
    "stairs": {"stairs", "internal staircase", "duplex", "internal stairs"},
    "off-street parking": {"off street", "private parking", "driveway"},
    "double garage": {"double garage", "garage (2 car)", "garage en bloc"},
    "lift": {"lift", "elevator"},
    "balcony": {"balcony", "front terrace"},
    "guest wc": {"guest wc", "guest cloakroom", "cloakroom", "wc"},
}


def canonical_subcategory(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    t = val.strip().lower()
    if t in _LOOKUP:
        return _LOOKUP[t]
    hit = get_close_matches(t, list(_LOOKUP.keys()), n=1, cutoff=0.82)
    return _LOOKUP[hit[0]] if hit else None


def norm_feature(term: str) -> str:
    t = (term or "").strip().lower()
    for canon, syns in _FEATURE_MAP.items():
        if t == canon or t in syns:
            return canon
    return t


def _intish(v: Any) -> Optional[int]:
    if v in (None, "", False):
        return None
    try:
        return int(v)
    except Exception:
        s = str(v)
        s = re.sub(r"[^\d]", "", s)
        return int(s) if s else None


# ─────────────────────────── Settings ────────────────────────────
@dataclass(frozen=True)
class Settings:
    mongodb_uri: str
    db_name: str = os.getenv("DB_NAME", "JefferiesJames")
    collection_name: str = os.getenv("COLLECTION_NAME", "properties")

    # WhatsApp
    waba_token: str = os.getenv("WABA_TOKEN", "")
    waba_phone_id: str = os.getenv("WABA_PHONE_ID", "")
    waba_template: str = os.getenv("TEMPLATE_NAME", "send_property")
    waba_lang: str = os.getenv("TEMPLATE_LANG", "en")

    @classmethod
    def from_env(cls) -> "Settings":
        uri = os.getenv("MONGODB_URI")
        if not uri:
            raise RuntimeError("MONGODB_URI missing in env")
        return cls(uri)

    @property
    def waba_endpoint(self) -> str:
        if not self.waba_phone_id:
            raise RuntimeError("WABA_PHONE_ID missing in env")
        return f"https://graph.facebook.com/v19.0/{self.waba_phone_id}/messages"


# ───────────────────────── Repository ────────────────────────────
class PropertyRepository:
    """DAO + search/ranking."""

    def __init__(self, cfg: Settings):
        self._client = MongoClient(cfg.mongodb_uri, tz_aware=True)
        self._col: Collection = self._client[cfg.db_name][cfg.collection_name]
        self._ensure_indexes()

    def ping(self) -> bool:
        try:
            self._client.admin.command("ping")
            return True
        except Exception:
            return False

    def _ensure_indexes(self) -> None:
        # structured fields
        self._col.create_index([("purpose", ASCENDING)])
        self._col.create_index([("status", ASCENDING)])
        self._col.create_index([("subcategory_canonical", ASCENDING)])
        self._col.create_index([("price_sort_gbp", ASCENDING)])
        self._col.create_index([("price_sale_gbp", ASCENDING)])
        self._col.create_index([("price_rent_pcm_gbp", ASCENDING)])
        self._col.create_index([("updated_at", DESCENDING)])

        # text index
        text_keys = [
            ("display_address", TEXT),
            ("address.formats.full_address", TEXT),
            ("address.formats.hidden_address", TEXT),
            ("address.locality", TEXT),
            ("address.suburb_or_town", TEXT),
            ("address.postcode", TEXT),
            ("advert_internet.heading", TEXT),
            ("advert_internet.body", TEXT),
            ("highlights.description", TEXT),
            ("features", TEXT),
            ("location_terms", TEXT),
            ("tags", TEXT),
            ("subcategories", TEXT),
        ]
        try:
            self._col.create_index(
                text_keys, name="text_search", default_language="english")
        except OperationFailure as exc:
            if exc.code == 85:
                # recreate if changed
                try:
                    self._col.drop_index("text_search")
                except Exception:
                    pass
                self._col.create_index(
                    text_keys, name="text_search", default_language="english")
            else:
                raise

    # ---------- helpers ----------
    @staticmethod
    def _price_from_display(s: Optional[str]) -> Optional[int]:
        if not s:
            return None
        # Extract first £-number sequence
        m = re.search(r"£\s*([\d,]+)", s)
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except Exception:
                return None
        # fall back: any digits number
        m = re.search(r"([\d][\d,]{3,})", s)
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except Exception:
                return None
        return None

    def _price_numeric(self, doc: dict, purpose: Optional[str]) -> Optional[int]:
        """Prefer explicit numeric fields; fallback to parsing formatted strings."""
        # Sale-first order, rental-first order
        keys_sale = (
            "price_sale_gbp",
            "price_sort_gbp",
            "price_match_sale",                # legacy field from Rex
            "listing.price_match_sale",
            "price_match",
            "state_value_price",
        )
        keys_rent = (
            "price_rent_pcm_gbp",
            "price_rent_amount_gbp",
            "price_match_rent_pa_inc_tax_month",
            "price_match",
            "state_value_price",
        )
        keys = keys_rent if (purpose == "rental") else keys_sale

        def _get(d: dict, dotted: str) -> Any:
            if "." not in dotted:
                return d.get(dotted)
            a, b = dotted.split(".", 1)
            sub = d.get(a) or {}
            return sub.get(b) if isinstance(sub, dict) else None

        for k in keys:
            v = _get(doc, k)
            iv = _intish(v)
            if iv:
                return iv

        # fallback: parse strings
        p = self._price_from_display(doc.get("price_display"))
        if p:
            return p
        return self._price_from_display((doc.get("advert_internet") or {}).get("heading"))

    @staticmethod
    def _beds(doc: dict) -> Optional[int]:
        for path in (
            ("attributes", "bedrooms"),
            ("attributes_full", "bedrooms"),
        ):
            v = (doc.get(path[0]) or {}).get(path[1])
            iv = _intish(v)
            if iv is not None:
                return iv
        return _intish(doc.get("beds"))

    @staticmethod
    def _baths(doc: dict) -> Optional[int]:
        for path in (
            ("attributes", "bathrooms"),
            ("attributes_full", "bathrooms"),
        ):
            v = (doc.get(path[0]) or {}).get(path[1])
            iv = _intish(v)
            if iv is not None:
                return iv
        return _intish(doc.get("baths"))

    @staticmethod
    def _has_feat(doc: dict, feat: str) -> bool:
        feat = feat.lower()
        # features array
        for f in (doc.get("features") or []):
            if isinstance(f, str) and feat in f.lower():
                return True
        # highlights
        for h in (doc.get("highlights") or []):
            if isinstance(h, dict) and feat in (h.get("description", "").lower()):
                return True
        # marketing text
        txt = " ".join([
            (doc.get("display_address") or ""),
            (doc.get("advert_internet", {}).get("heading") or ""),
            (doc.get("advert_internet", {}).get("body") or ""),
        ]).lower()
        return feat in txt

    # ---------- scoring ----------
    def _score(self, doc: dict, q: dict, text_score: float = 0.0) -> float:
        score = 0.0

        # 1) text relevance
        score += 1.0 * float(text_score or 0.0)

        # 2) subcategory
        want = canonical_subcategory(
            q.get("subcategory") or q.get("subcategory_canonical") or "")
        have = doc.get("subcategory_canonical")
        if not have:
            # attempt from subcategories list
            subcats = doc.get("subcategories") or []
            for s in subcats:
                cand = canonical_subcategory(str(s))
                if cand:
                    have = cand
                    break
        if want:
            if have == want:
                score += 2.0
            else:
                # light penalty
                score -= 0.75

        # 3) purpose hard filter was applied, but missing data gets small penalty
        if q.get("purpose") and doc.get("purpose") and doc.get("purpose") != q.get("purpose"):
            score -= 2.0

        # 4) price closeness (midpoint in range)
        pmin, pmax = q.get("price_min"), q.get("price_max")
        price = self._price_numeric(doc, q.get("purpose"))
        if price:
            if pmin or pmax:
                lo = pmin or price
                hi = pmax or price
                mid = (lo + hi) / 2.0
                # distance normalized
                dist = abs(price - mid) / max(1.0, hi -
                                              lo if hi > lo else max(1.0, mid))
                closeness = max(0.0, 1.0 - min(1.0, dist))
                score += 2.0 * closeness
            else:
                score += 0.25  # slight bump for known price

        # 5) beds/baths
        want_beds, want_baths = q.get("beds_min"), q.get("baths_min")
        b = self._beds(doc)
        if want_beds:
            if b is None:
                score -= 0.25
            elif b < want_beds:
                score -= (want_beds - b) * 1.0
            else:
                score += min(2.0, 0.6 + 0.2 * (b - want_beds))

        ba = self._baths(doc)
        if want_baths:
            if ba is None:
                score -= 0.25
            elif ba < want_baths:
                score -= (want_baths - ba) * 0.8
            else:
                score += min(1.7, 0.5 + 0.2 * (ba - want_baths))

        # 6) features coverage
        feats = [norm_feature(x) for x in (q.get("features") or []) if x]
        if feats:
            hits = sum(1 for f in feats if self._has_feat(doc, f))
            score += min(1.5, 0.5 * hits)

        # 7) freshness
        # (mongo sort by updated_at desc already; this is just tiny bump)
        if doc.get("updated_at"):
            score += 0.1

        return score

    # ---------- query tier runners ----------
    def _price_filter_or(self, purpose: Optional[str], pmin: Optional[int], pmax: Optional[int]) -> Optional[dict]:
        if pmin is None and pmax is None:
            return None
        bounds = {}
        if pmin is not None:
            bounds["$gte"] = pmin
        if pmax is not None:
            bounds["$lte"] = pmax

        if purpose == "rental":
            keys = ["price_rent_pcm_gbp", "price_match_rent_pa_inc_tax_month"]
        else:
            keys = ["price_sort_gbp", "price_sale_gbp", "price_match_sale"]

        # OR across whichever numeric field is present
        return {"$or": [{k: bounds} for k in keys]}

    def _beds_filter_or(self, min_v: Optional[int]) -> Optional[dict]:
        if not min_v:
            return None
        # handle ints or strings "5","6",...
        str_candidates = [str(i) for i in range(min_v, 21)]
        return {"$or": [
            {"attributes.bedrooms": {"$gte": min_v}},
            {"attributes_full.bedrooms": {"$gte": min_v}},
            {"attributes.bedrooms": {"$in": str_candidates}},
            {"attributes_full.bedrooms": {"$in": str_candidates}},
            {"beds": {"$in": str_candidates}},
        ]}

    def _baths_filter_or(self, min_v: Optional[int]) -> Optional[dict]:
        if not min_v:
            return None
        str_candidates = [str(i) for i in range(min_v, 21)]
        return {"$or": [
            {"attributes.bathrooms": {"$gte": min_v}},
            {"attributes_full.bathrooms": {"$gte": min_v}},
            {"attributes.bathrooms": {"$in": str_candidates}},
            {"attributes_full.bathrooms": {"$in": str_candidates}},
            {"baths": {"$in": str_candidates}},
        ]}

    def _base_filter(self, q: dict, include_type=True) -> dict:
        f: Dict[str, Any] = {}
        if q.get("purpose") in ("sale", "rental"):
            f["purpose"] = q["purpose"]
        if q.get("status") in ("current", "sold"):
            f["status"] = q["status"]

        if include_type:
            want = canonical_subcategory(
                q.get("subcategory") or q.get("subcategory_canonical"))
            if want:
                f["$or"] = [
                    {"subcategory_canonical": want},
                    {"subcategories": {"$regex": want, "$options": "i"}},
                ]
        return f

    def _text_terms(self, q: dict) -> str:
        key = (q.get("location") or q.get("keyword") or "").strip()
        feats = [norm_feature(x) for x in (q.get("features") or []) if x]
        parts = [key, *feats]
        return " ".join([p for p in parts if p]).strip()

    def _run_tier(self, q: dict, tier_name: str, text: bool, apply_price=True, apply_beds=True, limit=40) -> List[dict]:
        base = self._base_filter(q, include_type=True)

        # structured filters
        and_terms: List[dict] = []
        pf = self._price_filter_or(q.get("purpose"), q.get(
            "price_min"), q.get("price_max")) if apply_price else None
        if pf:
            and_terms.append(pf)

        bf = self._beds_filter_or(q.get("beds_min")) if apply_beds else None
        if bf:
            and_terms.append(bf)

        baf = self._baths_filter_or(q.get("baths_min")) if apply_beds else None
        if baf:
            and_terms.append(baf)

        if and_terms:
            base = {"$and": [base, *and_terms]}
        LOG.debug("tier %s base=%s", tier_name, json.dumps(base))

        docs: List[dict] = []
        if text:
            terms = self._text_terms(q)
            if not terms:
                return []
            cur = (self._col.find(base | {"$text": {"$search": terms}},
                                  {"score": {"$meta": "textScore"}})
                   .sort([("score", {"$meta": "textScore"}), ("updated_at", DESCENDING)])
                   .limit(limit))
            for d in cur:
                d["_textScore"] = float(d.get("score", 0.0))  # stash
                docs.append(d)
        else:
            # regex location fallback
            key = (q.get("location") or q.get("keyword") or "").strip()
            if key:
                rx = {"$regex": re.escape(key), "$options": "i"}
                loc_or = {
                    "$or": [
                        {"display_address": rx},
                        {"address.postcode": rx},
                        {"address.formats.full_address": rx},
                        {"address.locality": rx},
                        {"address.suburb_or_town": rx},
                        {"location_terms": rx},
                        {"advert_internet.body": rx},
                        {"highlights.description": rx},
                        {"features": rx},
                    ]
                }
                base = {"$and": [base, loc_or]}
            cur = self._col.find(base).sort(
                [("updated_at", DESCENDING)]).limit(limit)
            docs = list(cur)

        # rank
        for d in docs:
            ts = float(d.get("_textScore", 0.0)) if text else 0.0
            d["_rankScore"] = self._score(d, q, ts)

        docs.sort(key=lambda x: x.get("_rankScore", 0.0), reverse=True)
        return docs

    # ---------- public: find_best ----------
    def find_best(self, query: Dict[str, Any]) -> Tuple[Optional[dict], str, Dict[str, Any]]:
        """
        Try strict text + filters; then relax price, then relax beds/baths,
        then regex location. Return (doc, tier, debug).
        """
        q = dict(query or {})
        # unify location/keyword
        if not q.get("location") and q.get("keyword"):
            q["location"] = q["keyword"]

        # quick exact id
        for key in ("listing_id", "_id", "id"):
            if q.get(key):
                doc = self._col.find_one(
                    {"_id": str(q[key])}) or self._col.find_one({"id": str(q[key])})
                if doc:
                    return doc, "id_exact", {"candidates": 1}

        tiers = [
            ("text_strict",     dict(text=True,  apply_price=True,  apply_beds=True)),
            ("text_no_price",   dict(text=True,  apply_price=False, apply_beds=True)),
            ("text_no_beds",    dict(text=True,  apply_price=True,  apply_beds=False)),
            ("regex_fallback",  dict(text=False, apply_price=False, apply_beds=False)),
        ]
        debug: Dict[str, Any] = {}
        for name, params in tiers:
            docs = self._run_tier(q, name, **params)
            debug[name] = [{"_id": d.get("_id"), "score": round(
                d.get("_rankScore", 0.0), 3)} for d in docs[:5]]
            if docs:
                return docs[0], name, debug
        return None, "none", debug


# ───────────────────── WhatsApp sender ─────────────────────
def _nz(v: Optional[str]) -> str:
    return str(v) if v not in (None, "", "None") else "-"


def send_whatsapp(cfg: Settings, phone: str, summary: Dict[str, Any]) -> None:
    headers = {
        "Authorization": f"Bearer {cfg.waba_token}",
        "Content-Type": "application/json",
    }
    # Map to your template variables (keep these names stable!)
    body_params = [
        {"type": "text", "parameter_name": "location",
            "text": _nz(summary.get("address"))},
        {"type": "text", "parameter_name": "price",      "text": _nz(
            summary.get("price") or summary["marketing"].get("heading"))},
        {"type": "text", "parameter_name": "bedrooms",
            "text": _nz(summary["amenities"].get("beds"))},
        {"type": "text", "parameter_name": "bathrooms",
            "text": _nz(summary["amenities"].get("baths"))},
        {"type": "text", "parameter_name": "size",
            "text": _nz(summary.get("size"))},
    ]
    payload = {
        "messaging_product": "whatsapp",
        "to": phone,
        "type": "template",
        "template": {
            "name": cfg.waba_template,
            "language": {"code": cfg.waba_lang, "policy": "deterministic"},
            "components": [
                {"type": "body", "parameters": body_params},
                {"type": "button", "sub_type": "url", "index": "0",
                 "parameters": [{"type": "text", "text": summary["listing_id"]}]},
            ],
        },
    }
    r = requests.post(cfg.waba_endpoint, headers=headers,
                      json=payload, timeout=12)
    try:
        r.raise_for_status()
    except HTTPError:
        LOG.error("WhatsApp API error %s – %s", r.status_code, r.text)
        raise
    except RequestException as exc:
        LOG.error("WhatsApp request failed – %s", exc)
        raise


# ───────────────────── summary builder ─────────────────────
def _strip_house_number(a: str) -> str:
    return re.sub(r"^\s*\d+\s*", "", a).strip()


def _pick_main_image(rec: Dict[str, Any]) -> str:
    return rec.get("main_image_url") or rec.get("main_image") or ""


def summarise(rec: Dict[str, Any]) -> Dict[str, Any]:
    addr = rec.get("address") or {}
    addr_fmt = addr.get("formats") or {}
    safe_addr = addr_fmt.get("hidden_address") or _strip_house_number(
        rec.get("display_address", "")) or rec.get("display_address", "")

    # price (prefer numeric -> format; else display text)
    def fmt_gbp(v: Optional[int]) -> Optional[str]:
        if v is None:
            return None
        return f"£{v:,}"

    # choose best available price text
    price_text = rec.get("price_display") or fmt_gbp(
        rec.get("price_sort_gbp") or rec.get("price_sale_gbp"))
    if not price_text:
        price_text = fmt_gbp(rec.get("price_match_sale")) or fmt_gbp(
            rec.get("price_rent_pcm_gbp"))

    # canonical subcategory
    canon = rec.get("subcategory_canonical")
    if not canon:
        for s in rec.get("subcategories") or []:
            cs = canonical_subcategory(str(s))
            if cs:
                canon = cs
                break

    # agent
    agents = rec.get("agents") or []
    ag = agents[0] if agents else None
    agent_details = {
        "id": (ag or {}).get("id"),
        "name": (ag or {}).get("name"),
        "email": (ag or {}).get("email"),
        "phone_mobile": (ag or {}).get("phone_mobile"),
        "phone_direct": (ag or {}).get("phone_direct"),
        "position": (ag or {}).get("position"),
        "profile_image_url": (ag or {}).get("profile_image_url") or "",
    } if ag else None

    # amenities
    am = rec.get("attributes") or rec.get("attributes_full") or {}
    beds = am.get("bedrooms") or rec.get("beds")
    baths = am.get("bathrooms") or rec.get("baths")

    return {
        "listing_id": str(rec.get("_id") or rec.get("id")),
        "address": safe_addr,
        "size": rec.get("size_display") or rec.get("size"),
        "price": price_text,
        "ebrochure_url": rec.get("ebrochure_link") or f"{EBROCHURE_BASE}{rec.get('_id')}",
        "main_image_url": _pick_main_image(rec),
        "location": {
            "postcode": addr.get("postcode"),
            "locality": addr.get("locality"),
            "suburb_or_town": addr.get("suburb_or_town"),
            "latitude": addr.get("lat") or addr.get("latitude"),
            "longitude": addr.get("lon") or addr.get("longitude"),
        },
        "features": rec.get("features") or [],
        "highlights": ", ".join([h.get("description") for h in (rec.get("highlights") or []) if isinstance(h, dict) and h.get("description")]),
        "marketing": {
            "heading": (rec.get("advert_internet") or {}).get("heading"),
            "body": (rec.get("advert_internet") or {}).get("body"),
        },
        "amenities": {"beds": str(beds) if beds is not None else None, "baths": str(baths) if baths is not None else None},
        "subcategory": canon,
        "agent": agent_details,
    }


# ───────────────────── CLI helpers ─────────────────────
def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Property search CLI with WhatsApp auto-send")
    p.add_argument("keyword", nargs="?",
                   help="Location / keyword phrase (postcode/area/road)")
    p.add_argument("--json", help="Inline JSON dict of filters")
    p.add_argument(
        "--purpose", choices=["sale", "rental"], help="Sale or rental")
    p.add_argument("--to", help="Destination MSISDN (e.g. 27764121438)")
    p.add_argument("--dry", action="store_true",
                   help="Skip WhatsApp send (print only)")
    return p.parse_args(argv)


def _build_query(ns: argparse.Namespace) -> Dict[str, Any]:
    if ns.json:
        body = json.loads(ns.json)
        if ns.keyword and "location" not in body and "keyword" not in body:
            body["location"] = ns.keyword
    else:
        body = {"location": ns.keyword or ""}
    if ns.purpose and not body.get("purpose"):
        body["purpose"] = ns.purpose
    return body


def main(argv: Optional[List[str]] = None) -> None:
    cfg = Settings.from_env()
    repo = PropertyRepository(cfg)

    ns = _parse_args(argv)
    q = _build_query(ns)

    doc, tier, debug = repo.find_best(q)
    if not doc:
        print(json.dumps(
            {"no_match": True, "tier": tier, "debug": debug}, indent=2))
        sys.exit(4)

    summary = summarise(doc)
    summary["tier"] = tier
    print(json.dumps(summary, indent=2, ensure_ascii=False))

    if ns.to and not ns.dry:
        try:
            send_whatsapp(cfg, ns.to, summary)
            print(f"WhatsApp template '{cfg.waba_template}' sent to {ns.to}")
        except Exception as exc:
            print(f"WhatsApp send failed: {exc}")
            sys.exit(3)


if __name__ == "__main__":
    main(sys.argv[1:])
