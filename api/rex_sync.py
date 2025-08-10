#!/usr/bin/env python3
# File: api/rex_sync.py
"""
Rex → Mongo synchroniser for Jefferies London (normalised)
──────────────────────────────────────────────────────────
• GET  /api/rex_sync  — manual trigger (Vercel function)
• Cron /api/rex_sync  — scheduled via vercel.json

What’s new (2025-08-10):
• Canonical numeric price fields:
    - price_sale_gbp
    - price_rent_amount_gbp   (native amount as advertised)
    - price_rent_pcm_gbp      (normalised to per-calendar-month)
    - price_sort_gbp          (sale price or rent pcm, unified)
    - price_min_gbp / price_max_gbp (semantic bounds: guide, offers over, etc)
• Beds/baths hardened: beds_int, baths_int
• Canonical subcategory: subcategory_canonical in {"house","flat","other"}
• Fixed postcode tokenising for location_terms (no more “SE1” from “SE14 5DN”)
• Keeps originals: price_display, attributes, subcategories, features, etc
• Idempotent indexes for fast search

Env:
  ENABLE_REX_CRON=1              toggle cron
  SYNC_INCLUDE_SOLD=1            also fetch sold listings for comparables
  PAGE_SIZE=100                  batch size
  MAX_DURATION=55                safety cut-off (seconds)
  REX_*                          existing Rex creds
  MONGODB_URI, DB_NAME           existing Mongo creds
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import pathlib
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Generator, List, Tuple

import httpx
from dotenv import load_dotenv
from http.server import BaseHTTPRequestHandler
from pymongo import ASCENDING, TEXT, MongoClient, UpdateOne
from pymongo.errors import OperationFailure

# ── env & logging ────────────────────────────────────────────────
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | rex_sync | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── helpers ──────────────────────────────────────────────────────


def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        log.critical("Missing env var: %s", name)
        sys.exit(1)
    return v


def https(u: str) -> str:
    return ("https:" + u) if isinstance(u, str) and u.startswith("//") else u


FT2_PER_M2 = Decimal("10.76391041671")

RUN_ENABLED = os.getenv("ENABLE_REX_CRON", "1") == "1"
INCLUDE_SOLD = os.getenv("SYNC_INCLUDE_SOLD", "1") == "1"
TTL = int(os.getenv("REX_TOKEN_TTL", 604_800))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", 100))
HTTP_TIMEOUT = 15.0
MAX_DURATION = int(os.getenv("MAX_DURATION", 55))

# Rex config
REX_EMAIL = env("REX_EMAIL")
REX_PASSWORD = env("REX_PASSWORD")
REX_BASE_URL = env("REX_BASE_URL").rstrip("/")
REX_ACCOUNT_ID = env("REX_ACCOUNT_ID", "3877")


def rex_url(service: str, method: str) -> str:
    return f"{REX_BASE_URL}/v1/rex/{service}/{method}"


# Mongo config
MONGO_URI = env("MONGODB_URI")
DB_NAME = env("DB_NAME", "JefferiesJames")
client = MongoClient(MONGO_URI, tz_aware=True)
col_prop = client[DB_NAME][env("MONGO_COLLECTION", "properties")]
col_run = client[DB_NAME][env("MONGO_RUN_COLLECTION", "listing_changes")]
col_dupe = client[DB_NAME][env("MONGO_DUPE_COLLECTION", "duplicate_clusters")]

# Only fetch fields we actually use + extras we normalise
STATIC_EXTRAS: List[str] = [
    "address",
    "attributes",
    "highlights",
    "tags",
    "features",
    "subcategories",
    "images",
    "listing_sale_or_rental",
    "advert_internet",
    "advert_brochure",
    "advert_stocklist",
    # if present, Rex will also return the numeric matches we want:
    # price_match_sale, price_match_rent_pa_inc_tax_week/fortnight/month
]

DROP_KEYS = ["images", "photos", "floorplans", "raw", "media"]

# ── subcategory canonicaliser ────────────────────────────────────
_SUBCAT_DICT = {
    "house": {
        "detached house", "semi-detached house", "terraced house",
        "end of terrace house", "mid terrace house", "town house",
        "mews house", "mews", "character property", "house",
    },
    "flat": {
        "apartment", "apartments", "studio", "duplex",
        "flat", "penthouse", "maisonette",
    },
    "other": {"house boat", "houseboat"},
}
_SUB_LOOKUP = {syn.lower(): canon for canon, syns in _SUBCAT_DICT.items()
               for syn in syns}


def normalise_subcategory_value(s: str | None) -> str | None:
    if not s:
        return None
    t = s.strip().lower()
    if t in _SUB_LOOKUP:
        return _SUB_LOOKUP[t]
    # fuzzy-ish fallbacks
    if "mews" in t or "terrace" in t or "house" in t:
        return "house"
    if "apartment" in t or "flat" in t or "penthouse" in t or "maisonette" in t or "studio" in t or "duplex" in t:
        return "flat"
    return None


# ── postcode tokenising (UK) ─────────────────────────────────────
# Matches e.g. SW1W 9HH → area=SW, district=1W, outward=SW1W, sector=9, unit=HH
PC_RE = re.compile(
    r"^\s*([A-Z]{1,2})(\d[A-Z\d]?)\s*(\d)\s*([A-Z]{2})\s*$", re.I)


def postcode_tokens(pc: str | None) -> List[str]:
    if not pc or not isinstance(pc, str):
        return []
    t = pc.strip().upper()
    m = PC_RE.match(t)
    if not m:
        # fall back to outward if possible
        parts = t.split()
        return list({t, parts[0]} - {""}) if parts else [t]
    area, district, sector_digit, unit = m.groups()
    outward = f"{area}{district}"
    sector = f"{outward} {sector_digit}"
    return [t, outward, sector]


def location_terms(addr: dict) -> List[str]:
    terms = set()
    for tok in postcode_tokens(addr.get("postcode")):
        terms.add(tok.lower())
    for k in ("locality", "suburb_or_town", "state_or_region"):
        v = (addr.get(k) or "").strip().lower()
        if v:
            terms.add(v)
    street = " ".join(
        x for x in [(addr.get("street_name") or "").strip().lower()] if x
    )
    if street:
        terms.add(street)
    return sorted(t for t in terms if t)

# ── numeric helpers ──────────────────────────────────────────────


def to_int(x: Any) -> int | None:
    if x in (None, "", "null"):
        return None
    try:
        return int(x)
    except Exception:
        try:
            return int(float(str(x)))
        except Exception:
            return None


def to_float_stripped(x: Any) -> float | None:
    if x in (None, "", "null"):
        return None
    s = str(x)
    s = re.sub(r"[^0-9.]", "", s)
    try:
        return float(s) if s else None
    except Exception:
        return None


def sqm_sqft(attrs: dict) -> Tuple[float | None, float | None]:
    # prefer explicit m2 if present
    for k in ("attr_buildarea_m2", "attr_landarea_m2", "buildarea_m2", "landarea_m2"):
        v = attrs.get(k)
        if v not in (None, "", 0):
            sqm = float(v)
            sqft = float((Decimal(sqm) * FT2_PER_M2).quantize(Decimal("0.01")))
            return sqm, sqft
    # fallback on ambiguous units
    v = attrs.get("buildarea") or attrs.get("landarea")
    unit = (attrs.get("buildarea_unit") or "").lower()
    if v not in (None, "", 0):
        try:
            v = float(v)
        except Exception:
            return None, None
        if unit in ("m2", "m²"):
            sqm = v
            sqft = float((Decimal(sqm) * FT2_PER_M2).quantize(Decimal("0.01")))
            return sqm, sqft
        if unit in ("ft2", "ft²", "sq ft", "sqft"):
            sqft = v
            sqm = float((Decimal(sqft) / FT2_PER_M2).quantize(Decimal("0.01")))
            return sqm, sqft
    return None, None


def classify_price_text(text: str | None) -> str:
    if not text:
        return "unknown"
    t = text.lower()
    if re.search(r"\boffers?\s*over\b|\boieo\b|\boiro\b", t):
        return "offers_over"
    if "guide" in t:
        return "guide"
    if "fixed" in t:
        return "fixed"
    if "poa" in t or "price on application" in t:
        return "poa"
    return "unknown"


def purpose_of(row: dict) -> str | None:
    v = row.get("listing_sale_or_rental") or row.get("sale_or_rental")
    return v.lower() if isinstance(v, str) else None


def status_of(row: dict) -> str | None:
    # keep Rex’s raw string; common values are "current", "sold", "withdrawn"
    v = row.get("system_listing_state")
    return v.lower() if isinstance(v, str) else None


def pick_main_image(imgs: List[dict]) -> str:
    for im in imgs or []:
        url = im.get("url") or ""
        if url:
            return https(url)
    return ""

# ── indexes ──────────────────────────────────────────────────────


def ensure_indexes():
    # numeric index for fast filtering
    try:
        col_prop.create_index(
            [
                ("purpose", ASCENDING),
                ("status", ASCENDING),
                ("subcategory_canonical", ASCENDING),
                ("price_sort_gbp", ASCENDING),
                ("beds_int", ASCENDING),
                ("baths_int", ASCENDING),
            ],
            name="filter_idx_v2",
            background=True,
        )
    except OperationFailure as exc:
        if exc.code != 85:  # 85 IndexOptionsConflict
            raise
    # text index for keyword search
    text_keys = [
        ("address.formats.full_address", TEXT),
        ("address.locality", TEXT),
        ("address.suburb_or_town", TEXT),
        ("advert_internet.heading", TEXT),
        ("advert_internet.body", TEXT),
        ("advert_brochure.heading", TEXT),
        ("advert_brochure.body", TEXT),
        ("advert_stocklist.heading", TEXT),
        ("advert_stocklist.body", TEXT),
        ("highlights.description", TEXT),
        ("features", TEXT),
        ("location_terms", TEXT),
    ]
    try:
        col_prop.create_index(text_keys, name="text_search",
                              default_language="english", background=True)
    except OperationFailure as exc:
        if exc.code == 85:
            try:
                col_prop.drop_index("text_search")
            except Exception:
                pass
            col_prop.create_index(
                text_keys, name="text_search", default_language="english", background=True)
        else:
            raise

# ── flattener ────────────────────────────────────────────────────


def flatten(row: dict) -> dict:
    addr = row.get("address", {}) or {}
    attrs = row.get("attributes", {}) or {}
    sqm, sqft = sqm_sqft(attrs)

    # base address bits
    display_addr = (addr.get("formats", {}) or {}).get("display_address", "")
    lat = addr.get("latitude") or addr.get("lat")
    lon = addr.get("longitude") or addr.get("lng")

    # media
    main_image_url = pick_main_image(row.get("images") or [])

    # agents (Rex provides listing_agent_1/_2 with portal overrides)
    def clean_agent(a: dict | None) -> dict:
        if not isinstance(a, dict):
            return {}
        img = a.get("profile_image") if isinstance(
            a.get("profile_image"), dict) else None
        return {
            "id": a.get("id"),
            "name": a.get("name"),
            "first_name": a.get("first_name"),
            "last_name": a.get("last_name"),
            "email": a.get("email_address"),
            "phone_mobile": a.get("phone_mobile"),
            "phone_direct": a.get("phone_direct"),
            "position": a.get("position"),
            "profile_image_url": https(img.get("url", "")) if img else "",
        }

    agents: List[dict] = []
    for raw in (row.get("listing_agent_1"), row.get("listing_agent_2")):
        ca = clean_agent(raw)
        if ca:
            agents.append(ca)

    # size display
    if sqm and sqft:
        size_display = f"{sqm:.0f} m² / {sqft:.0f} ft²"
    else:
        size_display = ""

    # time
    mod_iso = datetime.fromtimestamp(int(row.get(
        "system_modtime", 0) or 0), tz=timezone.utc).isoformat(timespec="seconds")

    # price normalisation
    purpose = purpose_of(row)
    price_text = row.get("price_advertise_as") or row.get(
        "price_display") or ""
    price_type = classify_price_text(price_text)

    # numeric sale and rent fields from Rex if present
    sale_raw = to_float_stripped(
        row.get("price_match_sale") or row.get("state_value_price"))
    rent_pw = to_float_stripped(row.get("price_match_rent_pa_inc_tax_week"))
    rent_fn = to_float_stripped(
        row.get("price_match_rent_pa_inc_tax_fortnight"))
    rent_pm = to_float_stripped(row.get("price_match_rent_pa_inc_tax_month"))
    rent_pa = to_float_stripped(
        row.get("price_match_rent_pa_inc_tax") or row.get("price_rent"))

    # derive PCM
    rent_pcm = None
    rent_amount_native = None
    price_period = None
    if rent_pm:
        rent_pcm = rent_pm
        rent_amount_native = rent_pm
        price_period = "pcm"
    elif rent_pw:
        rent_pcm = float((Decimal(rent_pw) * Decimal(52) /
                         Decimal(12)).quantize(Decimal("0.01")))
        rent_amount_native = rent_pw
        price_period = "pw"
    elif rent_fn:
        rent_pcm = float((Decimal(rent_fn) * Decimal(26) /
                         Decimal(12)).quantize(Decimal("0.01")))
        rent_amount_native = rent_fn
        price_period = "pfn"
    elif rent_pa:
        rent_pcm = float((Decimal(rent_pa) / Decimal(12)
                          ).quantize(Decimal("0.01")))
        rent_amount_native = rent_pa
        price_period = "pa"

    price_sale_gbp = sale_raw if purpose == "sale" else None
    price_rent_amount_gbp = rent_amount_native if purpose == "rental" else None
    price_rent_pcm_gbp = rent_pcm if purpose == "rental" else None
    price_sort_gbp = price_sale_gbp if purpose == "sale" else price_rent_pcm_gbp

    # semantic bounds (offers_over = min only)
    price_min_gbp = None
    price_max_gbp = None
    if purpose == "sale" and price_sale_gbp:
        if price_type == "offers_over":
            price_min_gbp = price_sale_gbp
            price_max_gbp = None
        else:
            price_min_gbp = price_sale_gbp
            price_max_gbp = price_sale_gbp
    elif purpose == "rental" and price_rent_pcm_gbp:
        # treat rent as an exact pcm figure
        price_min_gbp = price_rent_pcm_gbp
        price_max_gbp = price_rent_pcm_gbp

    # beds/baths hardened
    beds_int = to_int(attrs.get("bedrooms") or row.get("beds"))
    baths_int = to_int(attrs.get("bathrooms") or row.get("baths"))

    # canonical subcategory
    canon_subcat = None
    subs = row.get("subcategories") or []
    if isinstance(subs, list):
        for s in subs:
            canon_subcat = normalise_subcategory_value(str(s))
            if canon_subcat:
                break
    elif isinstance(subs, str):
        canon_subcat = normalise_subcategory_value(subs)

    doc = {
        "_id": str(row["id"]),
        "id": row["id"],
        "purpose": purpose,                       # "sale" | "rental"
        "status": status_of(row),                 # "current" | "sold" | etc
        # location
        "address": addr,
        "display_address": display_addr,
        "lat": lat,
        "lon": lon,
        "location_terms": location_terms(addr),
        # price (display + canonical numeric)
        "price_display": price_text,
        "price_type": price_type,
        # for rental only (pw/pfn/pcm/pa)
        "price_period": price_period,
        "price_sale_gbp": price_sale_gbp,
        "price_rent_amount_gbp": price_rent_amount_gbp,
        "price_rent_pcm_gbp": price_rent_pcm_gbp,
        "price_sort_gbp": price_sort_gbp,
        "price_min_gbp": price_min_gbp,
        "price_max_gbp": price_max_gbp,
        # size
        "size_sqm": sqm,
        "size_sqft": sqft,
        "size_display": size_display,
        # media
        "main_image_url": main_image_url,
        "ebrochure_link": row.get("ebrochure_link"),
        # facets
        "tags": row.get("tags", []),
        "subcategories": subs if isinstance(subs, list) else ([subs] if subs else []),
        "subcategory_canonical": canon_subcat,
        "features": [s.strip() for s in (row.get("features") or []) if isinstance(s, str) and s.strip()],
        # agents
        "agents": agents,
        # marketing
        "advert_internet": row.get("advert_internet", {}) or {},
        "advert_brochure": row.get("advert_brochure", {}) or {},
        "advert_stocklist": row.get("advert_stocklist", {}) or {},
        # attributes for downstream UI
        "attributes": attrs,
        "attributes_full": attrs,
        # housekeeping
        "system_modtime_iso": datetime.fromtimestamp(int(row.get("system_modtime", 0) or 0), tz=timezone.utc).isoformat(timespec="seconds"),
        "updated_at": datetime.now(timezone.utc),
    }

    return doc

# ── duplicates (unchanged) ───────────────────────────────────────


def find_duplicates(docs: List[dict]) -> List[dict]:
    buckets: Dict[Tuple[str, str, str], List[dict]] = defaultdict(list)
    for d in docs:
        if d.get("lat") is None or d.get("lon") is None:
            continue
        key = (
            (d.get("display_address") or "").lower(),
            f"{float(d['lat']):.6f}",
            f"{float(d['lon']):.6f}",
        )
        buckets[key].append(d)
    return [
        {"address": k[0], "lat": k[1], "lon": k[2],
            "ids": [x["id"] for x in v]}
        for k, v in buckets.items()
        if len(v) > 1
    ]


def log_to_tmp(filename: str, data: dict) -> None:
    try:
        tmp = pathlib.Path("/tmp/logs")
        tmp.mkdir(parents=True, exist_ok=True)
        tmp.joinpath(filename).write_text(
            json.dumps(data, indent=2, default=str))
    except Exception as exc:
        log.warning("could not write /tmp log: %s", exc)

# ── core sync ────────────────────────────────────────────────────


async def sync() -> Dict[str, Any]:
    if not RUN_ENABLED:
        return {"disabled": True}

    ensure_indexes()

    deadline = asyncio.get_running_loop().time() + MAX_DURATION - 5
    rows: List[dict] = []

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as s:
        # login
        tok_raw = (
            await s.post(
                rex_url("Authentication", "login"),
                json={
                    "email": REX_EMAIL,
                    "password": REX_PASSWORD,
                    "account_id": REX_ACCOUNT_ID,
                    "token_lifetime": TTL,
                },
            )
        ).json()["result"]
        s.headers["Authorization"] = f"Bearer {tok_raw['token'] if isinstance(tok_raw, dict) else tok_raw}"
        log.info("Rex auth OK")

        # discover extra fields the account exposes
        meta = (await s.post(rex_url("PublishedListings", "describe-model"), json={})).json().get("result", {})
        extras = sorted(
            {*STATIC_EXTRAS, *meta.get("read_extra_fields", {}).keys()})

        async def fetch_state(state: str):
            nonlocal rows
            offset = 0
            while True:
                if asyncio.get_running_loop().time() > deadline:
                    raise RuntimeError("time limit hit")
                payload = {
                    "criteria": [{"name": "system_listing_state", "value": state}],
                    "offset": offset,
                    "limit": PAGE_SIZE,
                    "result_format": "default_no_stubs",
                    "order_by": {"system_modtime": "ASC"},
                    "extra_options": {"extra_fields": extras},
                }
                res = await s.post(rex_url("PublishedListings", "search"), json=payload)
                res.raise_for_status()
                batch = res.json().get("result", {}).get("rows", [])
                if not batch:
                    break
                rows.extend(batch)
                offset += PAGE_SIZE
                log.info("Fetched %d %s (total %d)",
                         len(batch), state, len(rows))

        # always fetch current; optionally also sold
        await fetch_state("current")
        if INCLUDE_SOLD:
            await fetch_state("sold")

    log.info("Listings fetched: %d", len(rows))
    if not rows:
        return {"created": 0, "updated": 0, "unchanged": 0, "deleted": 0, "duplicates": 0}

    docs = [flatten(r) for r in rows]
    ids_now = {d["_id"] for d in docs}

    existing = {e["_id"]: e for e in col_prop.find(
        {"_id": {"$in": list(ids_now)}})}

    created = updated = unchanged = 0
    changes: Dict[str, Any] = {}
    ops: List[UpdateOne] = []

    for d in docs:
        before = existing.get(d["_id"])
        if before is None:
            created += 1
            changes[d["_id"]] = {"created": True}
        else:
            # light diff to record changes
            changed = [k for k in d.keys() if before.get(k) != d.get(k)]
            if changed:
                updated += 1
                changes[d["_id"]] = {"changed": changed}
            else:
                unchanged += 1

        ops.append(
            UpdateOne(
                {"_id": d["_id"]},
                {"$set": d, "$unset": {k: "" for k in DROP_KEYS}},
                upsert=True,
            )
        )

    if ops:
        col_prop.bulk_write(ops, ordered=False)

    # purge anything not returned this run (only if we did not include sold)
    deleted = 0
    if not INCLUDE_SOLD:
        deleted = col_prop.delete_many(
            {"_id": {"$nin": list(ids_now)}}).deleted_count
        if deleted:
            log.info("Listings deleted: %d", deleted)

    dupes = find_duplicates(docs)
    if dupes:
        col_dupe.insert_one(
            {"ts": datetime.now(timezone.utc), "clusters": dupes})

    run_doc = {
        "ts": datetime.now(timezone.utc),
        "created": created,
        "updated": updated,
        "unchanged": unchanged,
        "deleted": deleted,
        "duplicates": len(dupes),
    }
    col_run.insert_one(run_doc)
    log_to_tmp(f"run_{run_doc['ts']:%Y-%m-%d_%H%M}.json",
               {**run_doc, "changes": changes})

    return run_doc

# ── HTTP handler (Vercel) ────────────────────────────────────────


class handler(BaseHTTPRequestHandler):  # noqa: N801
    def log_message(self, *_):  # silence default access log
        return

    def do_GET(self):  # pylint: disable=invalid-name
        try:
            body = json.dumps(asyncio.run(sync()), default=str).encode()
            status = 200
        except Exception as exc:  # pylint: disable=broad-except
            log.exception("sync failed")
            body, status = json.dumps(
                {"error": str(exc)}, default=str).encode(), 500
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

# ── WSGI fallback ────────────────────────────────────────────────


def app(environ, start_response) -> Generator[bytes, None, None]:
    try:
        body = json.dumps(asyncio.run(sync()), default=str).encode()
        status = b"200 OK"
    except Exception as exc:  # pylint: disable=broad-except
        log.exception("sync failed")
        body = json.dumps({"error": str(exc)}, default=str).encode()
        status = b"500 Internal Server Error"
    headers = [(b"Content-Type", b"application/json"),
               (b"Content-Length", str(len(body)).encode())]
    start_response(status, headers)
    yield body
