#!/usr/bin/env python3
"""
property_search.py — smarter ranking (Jun-2025)
===============================================

This version keeps the original CLI & Vapi surface but:
• filters by `status` (current / sold);
• indexes `location_terms` (Chelsea, SW10, …);
• scores up to 50 candidates on beds/baths/price/feature proximity
  so “4-bed 5-bath flat in Chelsea” finds the best match available.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import sys
from dataclasses import dataclass
from difflib import get_close_matches
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient, TEXT
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure, OperationFailure
from requests.exceptions import HTTPError, RequestException
from rich import print

# ───── helpers (synonyms, etc.) ────────────────────────────────────
_SUBCAT_DICT = {
    "house": {
        "detached house", "semi-detached house", "terraced house",
        "end of terrace house", "mid terrace house", "town house",
        "mews house", "character property",
    },
    "flat": {
        "apartment", "apartments", "studio", "duplex",
        "flat", "penthouse", "maisonette",
    },
    "other": {"house boat", "houseboat"},
}

_LOOKUP = {syn: canon for canon, syns in _SUBCAT_DICT.items() for syn in syns}

_FEATURE_MAP = {
    "private garden":      {"garden", "roof garden", "roof terrace"},
    "stairs":              {"stairs", "internal staircase", "duplex"},
    "off-street parking":  {"off street", "private parking", "driveway"},
    "double garage":       {"double garage", "garage en bloc"},
    "lift":                {"lift", "elevator"},
    "balcony":             {"balcony", "front terrace"},
}


def _normalise_feature(term: str) -> str:
    t = term.casefold().strip()
    for canon, syns in _FEATURE_MAP.items():
        if t == canon or t in syns:
            return canon
    return t


def normalise_subcategory(val: str) -> Optional[str]:
    if not val:
        return None
    val = val.casefold().strip()
    if val in _LOOKUP:
        return _LOOKUP[val]
    hit = get_close_matches(val, _LOOKUP.keys(), n=1, cutoff=0.8)
    return _LOOKUP[hit[0]] if hit else None


# ───── config ──────────────────────────────────────────────────────
load_dotenv(override=True)

EBROCHURE_BASE = (
    "https://app.rexsoftware.com/public/ebrochure/"
    "?region=eu_uk_1&account_id=3877&listing_id="
)


@dataclass(frozen=True)
class Settings:
    mongodb_uri: str
    db_name: str = "JefferiesJames"
    collection_name: str = "properties"

    waba_token: str = os.getenv("WABA_TOKEN", "")
    waba_phone_id: str = os.getenv("WABA_PHONE_ID", "")
    waba_template: str = os.getenv("TEMPLATE_NAME", "send_property")
    waba_lang: str = os.getenv("TEMPLATE_LANG", "en")

    @classmethod
    def from_env(cls) -> "Settings":
        uri = os.getenv("MONGODB_URI")
        if not uri:
            raise RuntimeError("MONGODB_URI missing in .env")
        return cls(
            uri,
            os.getenv("DB_NAME", cls.db_name),
            os.getenv("COLLECTION_NAME", cls.collection_name),
        )

    @property
    def waba_endpoint(self) -> str:
        if not self.waba_phone_id:
            raise RuntimeError("WABA_PHONE_ID missing in .env")
        return f"https://graph.facebook.com/v19.0/{self.waba_phone_id}/messages"


# ───── Mongo DAO ───────────────────────────────────────────────────
class PropertyRepository:
    """Return *best* candidate rather than first hit."""

    def __init__(self, cfg: Settings):
        self._cli = MongoClient(cfg.mongodb_uri, tz_aware=True)
        self._col: Collection = self._cli[cfg.db_name][cfg.collection_name]
        self._ensure_indexes()

    # connectivity --------------------------------------------------
    def ping(self) -> bool:
        try:
            self._cli.admin.command("ping")
            return True
        except ConnectionFailure:
            return False

    # indexes -------------------------------------------------------
    def _ensure_indexes(self):
        self._col.create_index([("purpose", ASCENDING)])
        text = [
            ("address.formats.full_address", TEXT),
            ("address.locality", TEXT),
            ("address.suburb_or_town", TEXT),
            ("advert_internet.heading", TEXT),
            ("advert_internet.body", TEXT),
            ("highlights.description", TEXT),
            ("features", TEXT),
            ("location_terms", TEXT),
        ]
        try:
            self._col.create_index(text, name="text_search",
                                   default_language="english")
        except OperationFailure as exc:
            if exc.code == 85:  # changed definition
                self._col.drop_index("text_search")
                self._col.create_index(text, name="text_search",
                                       default_language="english")
            else:
                raise

    # ----------------------------------------------------------------
    def _fetch_candidates(
        self, base_q: Dict[str, Any], text_terms: str
    ) -> List[Dict[str, Any]]:
        """Return up to 50 docs sorted by textScore (if any)."""
        q = base_q.copy()
        if text_terms:
            q["$text"] = {"$search": text_terms}
            cur = (
                self._col.find(q, {"score": {"$meta": "textScore"}})
                .sort("score", {"$meta": "textScore"})
                .limit(50)
            )
        else:
            cur = self._col.find(q).limit(50)
        return list(cur)

    # ----------------------------------------------------------------
    @staticmethod
    def _num(val) -> Optional[int | float]:
        try:
            return int(val)
        except (TypeError, ValueError):
            try:
                return float(val)
            except Exception:
                return None

    # simple linear penalty scoring ---------------------------------
    def _score(
        self,
        doc: Dict[str, Any],
        kw_feats: List[str],
        beds_min: Optional[int],
        baths_min: Optional[int],
        price_min: Optional[int],
        price_max: Optional[int],
        subcat: Optional[str],
        text_score: float | None,
    ) -> float:
        score = text_score or 0.0

        # beds / baths
        d_beds = self._num(doc.get("beds")) or self._num(
            doc.get("attributes", {}).get("bedrooms"))
        d_baths = self._num(doc.get("baths")) or self._num(
            doc.get("attributes", {}).get("bathrooms"))

        if beds_min is not None:
            score -= max(0, beds_min - (d_beds or 0)) * 5
        if baths_min is not None:
            score -= max(0, baths_min - (d_baths or 0)) * 5

        # price window – we score **penalty per %** outside requested range
        p = self._num(doc.get("price_match_sale"))
        if p is not None:
            if price_min is not None and p < price_min:
                score -= ((price_min - p) / price_min) * 100
            if price_max is not None and p > price_max:
                score -= ((p - price_max) / price_max) * 100

        # features (must-have missing → penalty)
        doc_feats = {f.casefold() for f in (doc.get("features") or [])}
        for f in kw_feats:
            if f not in doc_feats:
                score -= 3

        # subcategory closeness
        if subcat:
            match = normalise_subcategory(doc.get("subcategory") or "")
            if match != subcat:
                if subcat == "flat" and match == "other":
                    score -= 2
                else:
                    score -= 4

        return score

    # ----------------------------------------------------------------
    def find_best(
        self, params: Dict[str, Any]
    ) -> Tuple[Optional[Dict[str, Any]], str]:
        """Return best doc + tier label (for logging)."""

        key = (params.get("keyword") or "").strip()
        if not key:
            raise ValueError("keyword required")

        purpose = params.get("purpose")
        status = params.get("status")        # current / sold
        subcat = normalise_subcategory(params.get("subcategory", ""))
        beds_min, baths_min = params.get("beds_min"), params.get("baths_min")
        price_min, price_max = params.get("price_min"), params.get("price_max")
        kw_feats = [_normalise_feature(t)
                    for t in (params.get("features") or [])]

        base_q: Dict[str, Any] = {}
        if purpose and purpose != "all":
            base_q["purpose"] = purpose
        if status:
            base_q["status"] = status         # Rex -> "current" / "sold"
        if subcat:
            base_q["subcategories"] = {"$regex": subcat, "$options": "i"}

        text_terms = " ".join([key, *kw_feats]).strip()
        cand = self._fetch_candidates(base_q, text_terms)
        if not cand:
            # fall back to regex-only if text search empty
            rx = {"$regex": re.escape(key), "$options": "i"}
            base_q["$or"] = [
                {"address.formats.full_address": rx},
                {"location_terms": rx},
                {"advert_internet.body": rx},
            ]
            cand = self._fetch_candidates(base_q, "")

        if not cand:
            return None, "none"

        # rank by score
        best, best_score = None, -math.inf
        for d in cand:
            tscore = d.get("score") if isinstance(
                d.get("score"), (int, float)) else 0
            scr = self._score(
                d, kw_feats, beds_min, baths_min, price_min, price_max, subcat, tscore
            )
            if scr > best_score:
                best, best_score = d, scr

        return best, "ranked"

# ───── WhatsApp helper (unchanged) ─────────────────────────────────-


def _nz(v: Optional[str]) -> str:
    return v if v and str(v).strip() else "-"


def send_whatsapp(cfg: Settings, phone: str, summary: Dict[str, Any]) -> None:
    headers = {"Authorization": f"Bearer {cfg.waba_token}",
               "Content-Type": "application/json"}
    body_params = [
        {"type": "text", "parameter_name": "location",
         "text": _nz(summary["address"])},
        {"type": "text", "parameter_name": "price",
         "text": _nz(summary.get("price") or summary["marketing"]["heading"])},
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
                      json=payload, timeout=10)
    try:
        r.raise_for_status()
    except HTTPError:
        logging.error("WhatsApp API error %s – %s", r.status_code, r.text)
        raise
    except RequestException as exc:
        logging.error("WhatsApp request failed – %s", exc)
        raise

# ───── summariser (unchanged except marketing fallback) ────────────


def _strip_house_number(a: str) -> str:
    return re.sub(r"^\s*\d+\s*", "", a).strip()


def _pick_main_image(rec: Dict[str, Any]) -> str:
    return rec.get("main_image_url") or rec.get("main_image") or ""


def summarise(rec: Dict[str, Any]) -> Dict[str, Any]:
    am = rec.get("attributes") or rec.get("attributes_full", {})
    addr = rec.get("address", {})
    addr_fmt = addr.get("formats", {})

    safe_addr = addr_fmt.get("hidden_address") or _strip_house_number(
        rec.get("display_address", "")
    )
    price = (rec.get("price_display") or rec.get("guide_price")
             or rec.get("price_formatted") or rec.get("price_match_sale"))

    marketing_blk = (rec.get("advert_internet")
                     or rec.get("advert_brochure")
                     or rec.get("advert_stocklist") or {})

    highlights_val = ""
    if isinstance(rec.get("highlights"), list):
        highlights_val = ", ".join(
            d.get("description") for d in rec["highlights"] if d.get("description")
        )
    elif isinstance(rec.get("highlights"), dict):
        highlights_val = rec["highlights"].get("description", "")

    subcat_list = rec.get("subcategories", [])
    canonical_subcategory = None
    if isinstance(subcat_list, list):
        for entry in subcat_list:
            canon = normalise_subcategory(str(entry))
            if canon:
                canonical_subcategory = canon
                break
    elif isinstance(subcat_list, str):
        canonical_subcategory = normalise_subcategory(subcat_list)

    agents_raw = rec.get("agents") or []
    primary_agent = agents_raw[0] if agents_raw else None
    agent_details = None
    if primary_agent:
        agent_details = {
            "id":                primary_agent.get("id"),
            "name":              primary_agent.get("name"),
            "email":             primary_agent.get("email"),
            "phone_mobile":      primary_agent.get("phone_mobile"),
            "phone_direct":      primary_agent.get("phone_direct"),
            "position":          primary_agent.get("position"),
            "profile_image_url": primary_agent.get("profile_image_url"),
        }

    return {
        "listing_id": str(rec.get("_id")),
        "address":    safe_addr,
        "size":       rec.get("size_display") or rec.get("size"),
        "price":      price,
        "ebrochure_url": rec.get("ebrochure_link")
        or f"{EBROCHURE_BASE}{rec.get('_id')}",
        "main_image_url": _pick_main_image(rec),
        "location": {
            "postcode": addr.get("postcode"),
            "locality": addr.get("locality"),
            "suburb_or_town": addr.get("suburb_or_town"),
            "latitude": addr.get("lat") or addr.get("latitude"),
            "longitude": addr.get("lon") or addr.get("longitude"),
        },
        "features":   rec.get("features") or [],
        "highlights": highlights_val,
        "marketing":  {
            "heading": marketing_blk.get("heading"),
            "body":    marketing_blk.get("body"),
        },
        "amenities": {
            "beds": am.get("bedrooms"),
            "baths": am.get("bathrooms"),
        },
        "subcategory": canonical_subcategory,
        "agent":       agent_details,
    }

# ───── CLI ---------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Property search CLI")
    p.add_argument("keyword", nargs="?", help="Location / keyword phrase")
    p.add_argument("--json", help="Inline JSON dict of filters")
    p.add_argument("--purpose", choices=["sale", "rental", "all"],
                   default="all")
    p.add_argument("--features",
                   help="Comma-separated amenity keywords "
                        "(e.g. 'garden,stairs,parking')")
    p.add_argument("--to", help="Destination MSISDN (e.g. 447700900123)")
    p.add_argument("--dry", action="store_true",
                   help="Skip WhatsApp send")
    return p.parse_args()


def _query(ns: argparse.Namespace) -> Dict[str, Any]:
    if ns.json:
        body = json.loads(ns.json)
        if ns.keyword and "keyword" not in body:
            body["keyword"] = ns.keyword
        body.setdefault("purpose", ns.purpose)
        return body
    q: Dict[str, Any] = {"keyword": ns.keyword or "", "purpose": ns.purpose}
    if ns.features:
        q["features"] = [s.strip() for s in ns.features.split(",") if s.strip()]
    return q

# ───── main --------------------------------------------------------


def main() -> None:
    cfg = Settings.from_env()
    repo = PropertyRepository(cfg)
    if not repo.ping():
        logging.error("MongoDB not reachable")
        sys.exit(1)

    ns = _parse_args()
    try:
        doc, tier = repo.find_best(_query(ns))
    except ValueError as exc:
        logging.error("%s", exc)
        sys.exit(2)

    if not doc:
        print("[yellow]No matching property found[/]")
        sys.exit(4)

    summary = summarise(doc)
    print(f"[bold green]Match found (tier {tier}):[/]")
    print(json.dumps(summary, indent=2, ensure_ascii=False))

    if ns.to:
        if ns.dry:
            print("[cyan]DRY-RUN — WhatsApp skipped[/]")
        else:
            try:
                send_whatsapp(cfg, ns.to, summary)
                print(f"[green]WhatsApp sent to {ns.to}[/]")
            except Exception:
                sys.exit(3)


if __name__ == "__main__":
    main()
