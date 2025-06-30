#!/usr/bin/env python3
# File: api/rex_sync.py
"""
Rex → Mongo synchroniser for Jefferies London
─────────────────────────────────────────────
• GET  /api/rex_sync — manual trigger (Vercel function)
• Cron /api/rex_sync — scheduled via vercel.json

Stores (per listing)
• Core: id, purpose, status, price_display, beds, baths, size
• Location: full address, lat/lon, postcode/locality tokens
• Media: hero image URL + e-brochure link
• Facets: tags, subcategories, features
• People: agents[]
• Marketing: advert_internet / brochure / stocklist
• House-keeping: system_modtime_iso, updated_at

2025-06-30  ✨  New
• Handles April-2025 Rex API changes (attr_* keys, advert_* now optional)
• Deletes Mongo docs no longer returned by Rex
• Adds location_terms for richer search
• Exposes a WSGI `handler` callable (required by Vercel ≥ 2024-12)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import pathlib
import sys
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Tuple, Generator

import httpx
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

# ───── env & logging ──────────────────────────────────────────────
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | rex_sync | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ───── helpers ────────────────────────────────────────────────────


def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        log.critical("Missing env var: %s", name)
        sys.exit(1)
    return v


def https(u: str) -> str:
    return ("https:" + u) if isinstance(u, str) and u.startswith("//") else u


FT2_PER_M2 = Decimal("10.76391041671")  # exact conversion

# ───── runtime constants ─────────────────────────────────────────
RUN_ENABLED = os.getenv("ENABLE_REX_CRON", "1") == "1"
TTL = int(os.getenv("REX_TOKEN_TTL", 604_800))  # 7 days default
PAGE_SIZE = int(os.getenv("PAGE_SIZE", 100))
HTTP_TIMEOUT = 15.0
MAX_DURATION = int(os.getenv("MAX_DURATION", 55))  # Lambda safety cut-off

# ───── Rex config ────────────────────────────────────────────────
REX_EMAIL = env("REX_EMAIL")
REX_PASSWORD = env("REX_PASSWORD")
REX_BASE_URL = env("REX_BASE_URL").rstrip("/")
REX_ACCOUNT_ID = env("REX_ACCOUNT_ID", "3877")


def rex_url(service: str, method: str) -> str:
    return f"{REX_BASE_URL}/v1/rex/{service}/{method}"


# ───── Mongo config ──────────────────────────────────────────────
MONGO_URI = env("MONGODB_URI")
DB_NAME = env("DB_NAME", "JefferiesJames")
client = MongoClient(MONGO_URI, tz_aware=True)
col_prop = client[DB_NAME][env("MONGO_COLLECTION", "properties")]
col_run = client[DB_NAME][env("MONGO_RUN_COLLECTION", "listing_changes")]
col_dupe = client[DB_NAME][env("MONGO_DUPE_COLLECTION", "duplicate_clusters")]

# ───── Rex → only request what we keep ───────────────────────────
STATIC_EXTRAS: List[str] = [
    "address",
    "attributes",
    "highlights",
    "tags",
    "features",
    "subcategories",
    "images",                     # to derive hero image
    "listing_sale_or_rental",
    # became optional April-2025
    "advert_internet",
    "advert_brochure",
    "advert_stocklist",
]

# ───── keys to strip from Mongo on every upsert ──────────────────
DROP_KEYS = ["images", "photos", "floorplans", "raw", "media"]

# ───── flatten helpers ───────────────────────────────────────────


def _best_area(a: dict) -> Tuple[float | None, str | None]:
    for k in (
        "attr_buildarea_m2",
        "attr_landarea_m2",
        "buildarea_m2",
        "landarea_m2",
    ):
        if (v := a.get(k)) not in (None, "", 0):
            return float(v), "m2"
    for k, u in (
        ("attr_buildarea", "buildarea_unit"),
        ("attr_landarea", "landarea_unit"),
        ("buildarea", "buildarea_unit"),
        ("landarea", "landarea_unit"),
    ):
        if (v := a.get(k)) not in (None, "", 0):
            unit = (a.get(u) or "").lower()
            if unit in ("m2", "m²"):
                return float(v), "m2"
            if unit in ("ft2", "ft²", "sq ft"):
                return float(v), "ft2"
    return None, None


def _sqm_sqft(a: dict) -> Tuple[float | None, float | None]:
    v, u = _best_area(a)
    if v is None:
        return None, None
    if u == "m2":
        sqm = v
        sqft = float((Decimal(v) * FT2_PER_M2).quantize(Decimal("0.01")))
    else:
        sqft = v
        sqm = float((Decimal(v) / FT2_PER_M2).quantize(Decimal("0.01")))
    return sqm, sqft


def _norm_imgs(imgs: List[dict]) -> List[str]:
    return [https(im.get("url", "")) for im in imgs if im.get("url")]


def _agent_clean(a: dict | None) -> dict:
    if not isinstance(a, dict):
        return {}
    img = a.get("profile_image", {}) if isinstance(
        a.get("profile_image"), dict) else {}
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


def _list(val: Any) -> List[str]:
    if not isinstance(val, list):
        return []
    out: List[str] = []
    for item in val:
        if isinstance(item, str):
            out.append(item.strip())
        elif isinstance(item, dict):
            out.append(item.get("value") or item.get(
                "text") or item.get("name") or "")
    return [x for x in out if x]


def _purpose(rec: dict) -> str | None:
    v = rec.get("listing_sale_or_rental") or rec.get("sale_or_rental")
    return v.lower() if isinstance(v, str) else None


def _postcode_tokens(pc: str | None) -> List[str]:
    if not pc:
        return []
    t = pc.upper().strip()
    parts = t.split()
    area = parts[0]
    sector = t if len(parts) == 2 else ""
    return list({t, area, sector[:3]} - {""})


def _location_terms(addr: dict) -> List[str]:
    return list(
        {
            *(t.lower() for t in _postcode_tokens(addr.get("postcode"))),
            *(addr.get(k, "").lower()
              for k in ("locality", "suburb_or_town", "state_or_region")),
        }
        - {""}
    )


def _flatten(rec: dict) -> dict:
    attrs = rec.get("attributes", {})
    sqm, sqft = _sqm_sqft(attrs)

    addr: dict = rec.get("address", {})
    hero_img = _norm_imgs(rec.get("images") or [])[:1]

    mod_iso = datetime.fromtimestamp(
        int(rec.get("system_modtime", 0)), tz=timezone.utc
    ).isoformat(timespec="seconds")

    agents: List[dict] = []
    for raw in (rec.get("listing_agent_1"), rec.get("listing_agent_2")):
        clean = _agent_clean(raw)
        if clean:
            agents.append(clean)

    return {
        "_id": str(rec["id"]),
        "id": rec["id"],
        "purpose": _purpose(rec),
        # location
        "address": addr,
        "display_address": addr.get("formats", {}).get("display_address", ""),
        "lat": addr.get("latitude") or addr.get("lat"),
        "lon": addr.get("longitude") or addr.get("lng"),
        "location_terms": _location_terms(addr),
        # core
        "price_display": rec.get("price_advertise_as", ""),
        "status": rec.get("system_listing_state", ""),
        "beds": attrs.get("bedrooms"),
        "baths": attrs.get("bathrooms"),
        "size_sqm": sqm,
        "size_sqft": sqft,
        "size_display": f"{sqm:.0f} m² / {sqft:.0f} ft²" if sqm and sqft else "",
        # media
        "main_image_url": hero_img[0] if hero_img else "",
        "ebrochure_link": rec.get("ebrochure_link"),
        # facets
        "tags": rec.get("tags", []),
        "subcategories": rec.get("subcategories", []),
        "features": _list(rec.get("features")),
        # people
        "agents": agents,
        # marketing blocks
        "advert_internet": rec.get("advert_internet", {}),
        "advert_brochure": rec.get("advert_brochure", {}),
        "advert_stocklist": rec.get("advert_stocklist", {}),
        # housekeeping
        "system_modtime_iso": mod_iso,
        "updated_at": datetime.now(timezone.utc),
    }


def _diff(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, List[str]]:
    added = set(b) - set(a)
    removed = set(a) - set(b)
    changed = [k for k in b if k in a and b[k] != a[k]]
    return (
        {"added": sorted(added), "removed": sorted(
            removed), "changed": changed}
        if (added or removed or changed)
        else {}
    )


def _find_duplicates(docs: List[dict]) -> List[dict]:
    buckets: Dict[Tuple[str, str, str], List[dict]] = defaultdict(list)
    for d in docs:
        if d.get("lat") is None or d.get("lon") is None:
            continue
        key = (
            d.get("display_address", "").lower(),
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


def _log_to_tmp(filename: str, data: dict) -> None:
    try:
        tmp = pathlib.Path("/tmp/logs")
        tmp.mkdir(parents=True, exist_ok=True)
        tmp.joinpath(filename).write_text(
            json.dumps(data, indent=2, default=str))
    except Exception as exc:  # pylint: disable=broad-except
        log.warning("could not write /tmp log: %s", exc)

# ───── sync core ─────────────────────────────────────────────────


async def sync() -> Dict[str, Any]:
    if not RUN_ENABLED:
        return {"disabled": True}

    deadline = asyncio.get_running_loop().time() + MAX_DURATION - 5

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as s:
        # 1. login
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
        s.headers["Authorization"] = (
            f"Bearer {tok_raw['token'] if isinstance(tok_raw, dict) else tok_raw}"
        )
        log.info("Rex auth OK")

        # 2. discover extra fields
        meta = (
            await s.post(rex_url("PublishedListings", "describe-model"), json={})
        ).json().get("result", {})
        extras = sorted(
            {*STATIC_EXTRAS, *meta.get("read_extra_fields", {}).keys()})

        # 3. fetch pages
        rows, offset = [], 0
        while True:
            if asyncio.get_running_loop().time() > deadline:
                raise RuntimeError("time limit hit")
            payload = {
                "criteria": [{"name": "system_listing_state", "value": "current"}],
                "offset": offset,
                "limit": PAGE_SIZE,
                "result_format": "default_no_stubs",
                "order_by": {"system_modtime": "ASC"},
                "extra_options": {"extra_fields": extras},
            }
            batch = (
                await s.post(rex_url("PublishedListings", "search"), json=payload)
            ).json().get("result", {}).get("rows", [])
            if not batch:
                break
            rows.extend(batch)
            offset += PAGE_SIZE

    log.info("Listings fetched: %d", len(rows))
    if not rows:
        return {
            "created": 0,
            "updated": 0,
            "unchanged": 0,
            "deleted": 0,
            "duplicates": 0,
        }

    docs = [_flatten(r) for r in rows]
    ids_current = {d["_id"] for d in docs}

    existing = {e["_id"]: e for e in col_prop.find(
        {"_id": {"$in": list(ids_current)}})}

    created = updated = unchanged = 0
    changes: Dict[str, Any] = {}

    for d in docs:
        before = existing.get(d["_id"])
        if before is None:
            created += 1
            changes[d["_id"]] = {"created": True}
        else:
            diff = _diff(before, d)
            if diff:
                updated += 1
                changes[d["_id"]] = diff
            else:
                unchanged += 1

    # 4. bulk upsert
    ops = [
        UpdateOne(
            {"_id": d["_id"]},
            {"$set": d, "$unset": {k: "" for k in DROP_KEYS}},
            upsert=True,
        )
        for d in docs
    ]
    col_prop.bulk_write(ops, ordered=False)

    # 5. purge removed listings
    deleted = col_prop.delete_many(
        {"_id": {"$nin": list(ids_current)}}).deleted_count
    if deleted:
        log.info("Listings deleted: %d", deleted)

    # 6. record duplicates
    dupes = _find_duplicates(docs)
    if dupes:
        col_dupe.insert_one(
            {"ts": datetime.now(timezone.utc), "clusters": dupes}
        )

    # 7. run log
    run_doc = {
        "ts": datetime.now(timezone.utc),
        "created": created,
        "updated": updated,
        "unchanged": unchanged,
        "deleted": deleted,
        "duplicates": len(dupes),
    }
    col_run.insert_one(run_doc)
    _log_to_tmp(
        f"run_{run_doc['ts']:%Y-%m-%d_%H%M}.json",
        {**run_doc, "changes": changes},
    )

    return run_doc

# ───── WSGI entry-point for Vercel ────────────────────────────────


def handler(environ, start_response) -> Generator[bytes, None, None]:
    """
    Minimal WSGI callable expected by Vercel’s Python runtime.
    • Always returns JSON (success or error)
    """
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
