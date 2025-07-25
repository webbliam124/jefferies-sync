#!/usr/bin/env python3
"""
property_search.py — Mongo search + WhatsApp notify
===================================================

Looks up a best-match property in MongoDB with a four-tier fallback
strategy, optionally sends an interactive WhatsApp e-brochure, and
returns a compact JSON summary (includes agent data).

2025-06-30  ✨  New
• Added find_best() wrapper (alias to find_one()) so newer callers work.
"""

from __future__ import annotations

# ── standard library ──────────────────────────────────────────────
import argparse
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from difflib import get_close_matches
from typing import Any, Dict, List, Optional, Tuple

# ── third-party ───────────────────────────────────────────────────
import requests
from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient, TEXT
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure, OperationFailure
from requests.exceptions import HTTPError, RequestException
from rich import print

# ──────────────────────────────────────────────────────────────────
# sub-category + feature synonym helpers
# ──────────────────────────────────────────────────────────────────
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

_LOOKUP = {
    synonym: canon
    for canon, synonyms in _SUBCAT_DICT.items()
    for synonym in synonyms
}

# Minimal amenity-keyword map.  Extend as data warrants.
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


def normalise_subcategory(user_value: str) -> Optional[str]:
    if not user_value:
        return None
    val = user_value.casefold().strip()
    if val in _LOOKUP:
        return _LOOKUP[val]
    hit = get_close_matches(val, _LOOKUP.keys(), n=1, cutoff=0.8)
    return _LOOKUP[hit[0]] if hit else None


# ──────────────────────────────────────────────────────────────────
# configuration
# ──────────────────────────────────────────────────────────────────
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


# ──────────────────────────────────────────────────────────────────
# Mongo repository
# ──────────────────────────────────────────────────────────────────
class PropertyRepository:
    """Thin DAO with four-tier search strategy (feature-aware)."""

    def __init__(self, cfg: Settings):
        self._client = MongoClient(cfg.mongodb_uri, tz_aware=True)
        self._col: Collection = self._client[cfg.db_name][cfg.collection_name]
        self._ensure_indexes()

    # connectivity --------------------------------------------------
    def ping(self) -> bool:
        try:
            self._client.admin.command("ping")
            return True
        except ConnectionFailure:
            return False

    # indexes -------------------------------------------------------
    def _ensure_indexes(self):
        self._col.create_index([("purpose", ASCENDING)])
        text_keys = [
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
            self._col.create_index(text_keys, name="text_search",
                                   default_language="english")
        except OperationFailure as exc:
            if exc.code == 85:  # IndexOptionsConflict
                self._col.drop_index("text_search")
                self._col.create_index(text_keys, name="text_search",
                                       default_language="english")
            else:
                raise

    # search --------------------------------------------------------
    def find_one(self, p: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
        key = (p.get("keyword") or "").strip()
        if not key:
            raise ValueError("keyword required")

        purpose = p.get("purpose")
        canon = normalise_subcategory(p.get("subcategory", ""))

        base: Dict[str, Any] = {}
        if purpose and purpose != "all":
            base["purpose"] = purpose
        if canon:
            base["subcategories"] = {"$regex": canon, "$options": "i"}
        if p.get("status") in ("current", "sold"):
            base["status"] = p["status"]

        # amenity keywords -----------------------------------------
        raw_feats: List[str] = p.get("features") or []
        feats: List[str] = [_normalise_feature(t) for t in raw_feats]

        beds_min, baths_min = p.get("beds_min"), p.get("baths_min")
        price_min, price_max = p.get("price_min"), p.get("price_max")

        # full-text tier
        text_terms = " ".join([key, *feats]).strip()
        text_stage = {"$text": {"$search": text_terms}}

        # regex fallback tier
        rx_key = {"$regex": re.escape(key), "$options": "i"}
        or_bucket = [
            {"address.postcode": rx_key},
            {"address.locality": rx_key},
            {"address.suburb_or_town": rx_key},
            {"address.formats.full_address": rx_key},
            {"advert_internet.heading": rx_key},
            {"advert_internet.body": rx_key},
            {"highlights.description": rx_key},
            {"features": rx_key},
        ]
        for fv in feats:
            rx_f = {"$regex": re.escape(fv), "$options": "i"}
            or_bucket.extend([
                {"features": rx_f},
                {"highlights.description": rx_f},
                {"advert_internet.body": rx_f},
            ])
        regex_stage = {"$or": or_bucket}

        def apply_nums(q: Dict[str, Any], tier: str) -> Dict[str, Any]:
            if tier != "no_price":
                if price_min is not None:
                    q.setdefault("price_match_sale", {})["$gte"] = price_min
                if price_max is not None:
                    q.setdefault("price_match_sale", {})["$lte"] = price_max
            if tier not in ("no_beds_baths", "location_only"):
                if beds_min is not None:
                    q["attributes.bedrooms"] = {"$gte": beds_min}
                if baths_min is not None:
                    q["attributes.bathrooms"] = {"$gte": baths_min}
            return q

        tiers: List[Tuple[str, Dict[str, Any]]] = [
            ("full",           apply_nums(base | text_stage, "full")),
            ("no_price",       apply_nums(base | text_stage, "no_price")),
            ("no_beds_baths",  apply_nums(base | text_stage, "no_beds_baths")),
            ("location_only",  base | regex_stage),
        ]

        for name, q in tiers:
            if "$text" in q:
                cur = (self._col.find(q, {"score": {"$meta": "textScore"}})
                       .sort("score", {"$meta": "textScore"})
                       .limit(1))
                doc = next(cur, None)
            else:
                doc = self._col.find_one(q)
            if doc:
                return doc, name
        return None, "none"

    # ------------------------------------------------------------------
    def find_best(self, query: Dict[str, Any]):
        """
        Alias kept for backward compatibility – Vapi handler now calls
        PropertyRepository.find_best(); internally we still use find_one()
        as the canonical implementation.
        """
        return self.find_one(query)

# ──────────────────────────────────────────────────────────────────
# WhatsApp helper
# ──────────────────────────────────────────────────────────────────


def _nz(v: Optional[str]) -> str:
    return v if v and str(v).strip() else "-"


def send_whatsapp(cfg: Settings, phone: str, summary: Dict[str, Any]) -> None:
    headers = {
        "Authorization": f"Bearer {cfg.waba_token}",
        "Content-Type": "application/json",
    }

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
                {
                    "type": "button",
                    "sub_type": "url",
                    "index": "0",
                    "parameters": [{"type": "text",
                                    "text": summary["listing_id"]}],
                },
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


# ──────────────────────────────────────────────────────────────────
# misc helpers + summary builder (agent support)
# ──────────────────────────────────────────────────────────────────
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

    price = (
        rec.get("price_display")
        or rec.get("guide_price")
        or rec.get("price_formatted")
        or rec.get("price_match_sale")
    )

    # highlights: list OR list-of-dicts
    highlights_val = ""
    if isinstance(rec.get("highlights"), list):
        highlights_val = ", ".join(
            d.get("description") for d in rec["highlights"] if d.get("description")
        )
    elif isinstance(rec.get("highlights"), dict):
        highlights_val = rec["highlights"].get("description", "")

    # first canonical sub-category
    subcat_list = rec.get("subcategories", [])
    canonical_subcategory = None
    if isinstance(subcat_list, list) and subcat_list:
        for entry in subcat_list:
            canon = normalise_subcategory(str(entry))
            if canon:
                canonical_subcategory = canon
                break
    elif isinstance(subcat_list, str):
        canonical_subcategory = normalise_subcategory(subcat_list)

    # agent ------------------------------------------
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
        "address": safe_addr,
        "size": rec.get("size_display") or rec.get("size"),
        "price": price,
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
        "features": rec.get("features") or [],
        "highlights": highlights_val,
        "marketing": {
            "heading": rec.get("advert_internet", {}).get("heading"),
            "body": rec.get("advert_internet", {}).get("body"),
        },
        "amenities": {
            "beds": am.get("bedrooms"),
            "baths": am.get("bathrooms"),
        },
        "subcategory": canonical_subcategory,
        "agent": agent_details,
    }


# ──────────────────────────────────────────────────────────────────
# CLI helpers
# ──────────────────────────────────────────────────────────────────
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Property search CLI with WhatsApp auto-send")
    p.add_argument("keyword", nargs="?", help="Location / keyword phrase")
    p.add_argument("--json", help="Inline JSON dict of filters")
    p.add_argument("--purpose", choices=["sale", "rental", "all"],
                   default="all", help="Sale, rental, or all (default)")
    p.add_argument("--features",
                   help="Comma-separated amenity keywords "
                        "(e.g. 'garden,stairs,parking')")
    p.add_argument("--to", help="Destination MSISDN (e.g. 27764121438)")
    p.add_argument("--dry", action="store_true",
                   help="Print payload but skip WhatsApp send")
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


# ──────────────────────────────────────────────────────────────────
# main
# ──────────────────────────────────────────────────────────────────
def main() -> None:
    cfg = Settings.from_env()
    repo = PropertyRepository(cfg)

    if not repo.ping():
        logging.error("MongoDB not reachable — check .env / Atlas rules")
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
            print("[cyan]DRY-RUN — WhatsApp dispatch skipped[/]")
        else:
            try:
                send_whatsapp(cfg, ns.to, summary)
                print(f"[green]WhatsApp template '{cfg.waba_template}' "
                      f"sent to {ns.to}[/]")
            except Exception:  # pragma: no cover
                sys.exit(3)


if __name__ == "__main__":
    main()
