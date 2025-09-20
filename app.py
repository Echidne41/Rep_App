import os
import re
import csv
import io
import json
import time
import typing as t
from datetime import datetime

import requests
from flask import Flask, request, jsonify
from flask_cors import CORS

# =========================
# ENV & CONSTANTS
# =========================
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

OPENSTATES_API_KEY = os.getenv("OPENSTATES_API_KEY", "")
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*")

VOTES_CSV_URL = os.getenv("VOTES_CSV_URL", "")
FLOTERIAL_BASE_CSV_URL = os.getenv("FLOTERIAL_BASE_CSV_URL", "")
FLOTERIAL_TOWN_CSV_URL = os.getenv("FLOTERIAL_TOWN_CSV_URL", "")

OS_ROOT = "https://v3.openstates.org"
OS_PEOPLE = f"{OS_ROOT}/people"
OS_PEOPLE_GEO = f"{OS_ROOT}/people.geo"

CENSUS_GEOCODER_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# =========================
# APP
# =========================
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ALLOWED_ORIGINS.split(",") if ALLOWED_ORIGINS else ["*"]}})

# =========================
# Helpers — tiny utils
# =========================

def _http_get(url: str, params: dict | None = None, headers: dict | None = None, timeout: int = 20):
    r = requests.get(url, params=params or {}, headers=headers or {}, timeout=timeout)
    r.raise_for_status()
    return r


def _title(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).title()


# =========================
# Floterial CSV loaders (tiny, cached)
# =========================
_csv_cache: dict[str, tuple[float, list[dict[str, str]]]] = {}


def _read_csv_url(url: str) -> list[dict[str, str]]:
    if not url:
        return []
    now = time.time()
    cached = _csv_cache.get(url)
    if cached and now - cached[0] < 300:  # 5 min TTL
        return cached[1]
    if url.startswith("file://"):
        path = url.replace("file://", "")
        with open(path, "r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    else:
        r = _http_get(url)
        content = r.content.decode("utf-8", errors="replace")
        rows = list(csv.DictReader(io.StringIO(content)))
    _csv_cache[url] = (now, rows)
    return rows


def load_floterial_maps():
    base_rows = _read_csv_url(FLOTERIAL_BASE_CSV_URL)
    town_rows = _read_csv_url(FLOTERIAL_TOWN_CSV_URL)
    base_map = {}
    for r in base_rows:
        b = (r.get("base_district") or "").strip()
        f = (r.get("floterial_district") or "").strip()
        if b and f:
            base_map.setdefault(_norm_district(b), set()).add(_norm_district(f))
    town_map = {}
    for r in town_rows:
        tname = _title(r.get("town", ""))
        county = _title((r.get("county", "").replace(" County", "")).strip())
        f = _norm_district(r.get("floterial_district", ""))
        if tname and county and f:
            town_map.setdefault((tname, county), set()).add(f)
    return base_map, town_map


def _norm_district(label: str) -> str:
    m = re.search(r"([A-Za-z]+)\s*0*([0-9]+)", (label or ""))
    if not m:
        return (label or "").strip()
    return f"{m.group(1).title()} {int(m.group(2))}"


# =========================
# Geocoding (Census → Nominatim fallback)
# =========================

def geocode_oneline(addr: str) -> tuple[float, float, dict]:
    """Return (lat, lon, meta) where meta has town/county strings.
    ELI5: try US Census first (fast, NH-friendly). If it fails, try OSM.
    """
    # Census
    try:
        r = _http_get(CENSUS_GEOCODER_URL, params={
            "address": addr,
            "benchmark": "Public_AR_Current",
            "format": "json",
        })
        j = r.json()
        res = (j.get("result") or {}).get("addressMatches") or []
        if res:
            m = res[0]
            coords = m.get("coordinates") or {}
            lon = float(coords.get("x"))
            lat = float(coords.get("y"))
            comps = (m.get("addressComponents") or {})
            town = _title(comps.get("city"))
            county = _title(comps.get("county"))
            return lat, lon, {"town": town, "county": county, "geocoder": "census"}
    except Exception:
        pass

    # Nominatim
    r = _http_get(NOMINATIM_URL, params={"q": addr, "format": "json", "addressdetails": 1, "countrycodes": "us", "state": "New Hampshire", "limit": 1}, headers={"User-Agent": "NH-Rep-Finder/1.0"})
    j = r.json()
    if not j:
        raise ValueError("geocoding_failed")
    it = j[0]
    lat = float(it.get("lat"))
    lon = float(it.get("lon"))
    ad = it.get("address") or {}
    town = _title(ad.get("city") or ad.get("town") or ad.get("village") or ad.get("hamlet") or ad.get("municipality"))
    county = _title((ad.get("county") or "").replace(" County", ""))
    return lat, lon, {"town": town, "county": county, "geocoder": "nominatim"}


# =========================
# OpenStates helpers — TRUST people.geo
# =========================

def openstates_people_geo(lat: float, lon: float) -> list[dict]:
    params = {"lat": lat, "lng": lon}
    headers = {"X-API-KEY": OPENSTATES_API_KEY} if OPENSTATES_API_KEY else {}
    r = _http_get(OS_PEOPLE_GEO, params=params, headers=headers)
    j = r.json()
    return j or []


def _pick_house_members_from_people_geo(people: list[dict]) -> list[dict]:
    """ELI5: Take OpenStates result and keep NH House only. Do *not* over-filter.
    We accept either `current_role` or scan `roles` for a matching lower-chamber role.
    We do not hard-enforce start/end dates (they're often null)."""
    out: list[dict] = []
    for p in people or []:
        role = p.get("current_role") or {}
        def _is_lower_nh(r: dict) -> bool:
            return (r.get("chamber") == "lower") and ("new hampshire" in (r.get("jurisdiction", "").lower() or ""))
        if not _is_lower_nh(role):
            roles = p.get("roles") or []
            role = next((r for r in roles if _is_lower_nh(r)), None)
            if not role:
                continue
        out.append({
            "name": p.get("name"),
            "party": (p.get("party") or role.get("party") or "Unknown"),
            "district": role.get("district") or "",
            "email": p.get("email"),
            "phone": p.get("voice"),
            "links": [{"url": L.get("url")} for L in (p.get("links") or []) if L.get("url")],
        })
    return out


# =========================
# API ROUTES
# =========================

@app.get("/health")
def health():
    base_rows = _read_csv_url(FLOTERIAL_BASE_CSV_URL)
    town_rows = _read_csv_url(FLOTERIAL_TOWN_CSV_URL)
    return jsonify({
        "status": "ok",
        "time": datetime.utcnow().isoformat() + "Z",
        "floterial_base_csv_set": bool(base_rows),
        "floterial_town_csv_set": bool(town_rows),
        "votes_csv_set": bool(VOTES_CSV_URL),
        "openstates_api_key": bool(OPENSTATES_API_KEY),
    })


@app.get("/debug/floterials")
def debug_floterials():
    base_map, town_map = load_floterial_maps()
    return jsonify({
        "base_to_floterial_count": sum(len(v) for v in base_map.values()),
        "town_to_floterial_count": sum(len(v) for v in town_map.values()),
        "examples": {
            "base": list(base_map.items())[:5],
            "town": [
                {"town": k[0], "county": k[1], "floterials": list(v)}
                for k, v in list(town_map.items())[:5]
            ],
        },
    })


@app.get("/api/lookup-legislators")
def lookup_legislators():
    addr = (request.args.get("address") or request.json.get("address") if request.is_json else request.args.get("address")) or ""
    if not addr:
        return jsonify({"success": False, "error": "missing address"}), 400

    # 1) Geocode
    lat, lon, meta = geocode_oneline(addr)
    town = meta.get("town")
    county = meta.get("county")

    # 2) Ask OpenStates which people represent this point
    people = openstates_people_geo(lat, lon)
    house_from_geo = _pick_house_members_from_people_geo(people)

    # 3) Overlay labels (from our CSVs), informational
    base_map, town_map = load_floterial_maps()
    overlay_labels = sorted(list(town_map.get((town, county), set())))

    # 4) If OpenStates returned House members, **trust it** and return.
    if house_from_geo:
        return jsonify({
            "success": True,
            "address": addr,
            "geographies": {
                "town_county": [town, county],
            },
            "source": {
                "geocoder": meta.get("geocoder"),
                "openstates_geo": True,
                "overlay_labels": overlay_labels,
            },
            "stateRepresentatives": house_from_geo,
        })

    # 5) Fallback path (rare): build district list from overlays and query /people
    reps: list[dict] = []
    try:
        districts_to_try = set(overlay_labels)
        # Optionally add base districts mapped to those floterials (reverse mapping)
        for b, fset in base_map.items():
            if fset & districts_to_try:
                districts_to_try.add(b)
        headers = {"X-API-KEY": OPENSTATES_API_KEY} if OPENSTATES_API_KEY else {}
        for d in districts_to_try:
            q = {"jurisdiction": "New Hampshire", "chamber": "lower", "district": d}
            r = _http_get(OS_PEOPLE, params=q, headers=headers)
            for p in (r.json() or []):
                reps.append({
                    "name": p.get("name"),
                    "party": p.get("party") or "Unknown",
                    "district": d,
                    "email": p.get("email"),
                    "phone": p.get("voice"),
                    "links": [{"url": L.get("url")} for L in (p.get("links") or []) if L.get("url")],
                })
    except Exception:
        pass

    return jsonify({
        "success": True,
        "address": addr,
        "geographies": {"town_county": [town, county]},
        "source": {"geocoder": meta.get("geocoder"), "openstates_geo": bool(people), "overlay_labels": overlay_labels},
        "stateRepresentatives": reps,
    })

@app.get("/debug/peek")
def debug_peek():
    import itertools
    def head(url):
        try:
            if url.startswith("file://"):
                p = url.replace("file://","")
                with open(p, "r", encoding="utf-8") as f:
                    return {"ok": True, "path": p, "head": list(itertools.islice(f, 3))}
            else:
                r = requests.get(url, timeout=10)
                r.raise_for_status()
                txt = r.text.splitlines()[:3]
                return {"ok": True, "url": url, "head": txt}
        except Exception as e:
            return {"ok": False, "err": str(e)}
    return jsonify({
        "base": head(os.getenv("FLOTERIAL_BASE_CSV_URL","")),
        "town": head(os.getenv("FLOTERIAL_TOWN_CSV_URL","")),
    })


# -------------------------
# Optional helpers: votes & bill link (simple, safe defaults)
# -------------------------

@app.get("/api/vote-map")
def vote_map():
    bill = (request.args.get("bill") or "").strip()
    if not VOTES_CSV_URL:
        return jsonify({"success": False, "error": "VOTES_CSV_URL not set"}), 500
    rows = _read_csv_url(VOTES_CSV_URL)
    if bill:
        rows = [r for r in rows if (r.get("bill") or "").strip().lower() == bill.lower()]
    return jsonify({"success": True, "count": len(rows), "rows": rows[:1000]})


@app.get("/api/bill-link")
def bill_link():
    bill = (request.args.get("bill") or "").strip()
    # Safe default: OpenStates search page for the bill token (works for HB/SB/etc.)
    if not bill:
        return jsonify({"success": False, "error": "missing bill"}), 400
    link = f"https://openstates.org/nh/bills/?q={requests.utils.quote(bill)}"
    return jsonify({"success": True, "bill": bill, "url": link})


# Root -> health (kept on purpose)
@app.get("/")
def root():
    return jsonify({"ok": True, "see": "/health"})


# =========================
# MAIN (for local dev)
# =========================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)

