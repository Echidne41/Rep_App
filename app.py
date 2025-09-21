import os
import re
import csv
import io
import time
from datetime import datetime
from typing import List, Dict, Tuple

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
ALLOWED_ORIGINS    = os.getenv("ALLOWED_ORIGINS", "*")

VOTES_CSV_URL          = os.getenv("VOTES_CSV_URL", "")
FLOTERIAL_BASE_CSV_URL = os.getenv("FLOTERIAL_BASE_CSV_URL", "")
FLOTERIAL_TOWN_CSV_URL = os.getenv("FLOTERIAL_TOWN_CSV_URL", "")

OS_ROOT       = "https://v3.openstates.org"
OS_PEOPLE     = f"{OS_ROOT}/people"
OS_PEOPLE_GEO = f"{OS_ROOT}/people.geo"

CENSUS_GEOCODER_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
NOMINATIM_URL       = "https://nominatim.openstreetmap.org/search"

# =========================
# APP
# =========================
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ALLOWED_ORIGINS.split(",") if ALLOWED_ORIGINS else ["*"]}})

# =========================
# Utilities
# =========================

def _http_get(url: str, params: dict | None = None, headers: dict | None = None, timeout: int = 18):
    r = requests.get(url, params=params or {}, headers=headers or {}, timeout=timeout)
    app.logger.info(f"GET {url} {r.status_code} params={params}")
    r.raise_for_status()
    return r


def _title(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).title()


def _norm_district(label: str) -> str:
    m = re.search(r"([A-Za-z]+)\s*0*([0-9]+)", (label or ""))
    if not m:
        return (label or "").strip()
    return f"{m.group(1).title()} {int(m.group(2))}"

# =========================
# CSV load (cached)
# =========================
_csv_cache: Dict[str, Tuple[float, List[dict]]] = {}

def _read_csv_url(url: str) -> List[dict]:
    if not url:
        return []
    now = time.time()
    cached = _csv_cache.get(url)
    if cached and now - cached[0] < 300:
        return cached[1]
    if url.startswith("file://"):
        path = url.replace("file://", "")
        with open(path, "r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    else:
        r = _http_get(url)
        txt = r.content.decode("utf-8", errors="replace")
        rows = list(csv.DictReader(io.StringIO(txt)))
    _csv_cache[url] = (now, rows)
    return rows


def load_floterial_maps() -> Tuple[Dict[str, set], Dict[Tuple[str, str], set]]:
    base_rows = _read_csv_url(FLOTERIAL_BASE_CSV_URL)
    town_rows = _read_csv_url(FLOTERIAL_TOWN_CSV_URL)

    base_map: Dict[str, set] = {}
    for r in base_rows:
        b = (r.get("base_district") or "").strip()
        f = (r.get("floterial_district") or r.get("district") or "").strip()
        if b and f:
            base_map.setdefault(_norm_district(b), set()).add(_norm_district(f))

    town_map: Dict[Tuple[str, str], set] = {}
    for r in town_rows:
        town   = _title(r.get("town", ""))
        county = _title((r.get("county", "").replace(" County", "")).strip())
        fd     = (r.get("floterial_district") or r.get("district") or "").strip()
        if town and county and fd:
            town_map.setdefault((town, county), set()).add(fd)
    return base_map, town_map

# =========================
# Geocoding (Census → Nominatim)
# =========================

def geocode_oneline(addr: str) -> Tuple[float, float, dict]:
    # Census
    try:
        r = _http_get(CENSUS_GEOCODER_URL, params={"address": addr, "benchmark": "Public_AR_Current", "format": "json"})
        j = r.json(); matches = (j.get("result") or {}).get("addressMatches") or []
        if matches:
            m = matches[0]; lon = float(m["coordinates"]["x"]); lat = float(m["coordinates"]["y"])
            comps = m.get("addressComponents") or {}
            return lat, lon, {"town": _title(comps.get("city")), "county": _title(comps.get("county")), "geocoder": "census"}
    except Exception:
        pass
    # Nominatim
    r = _http_get(NOMINATIM_URL, params={"q": addr, "format": "json", "addressdetails": 1, "countrycodes": "us", "state": "New Hampshire", "limit": 1}, headers={"User-Agent": "NH-Rep-Finder/1.0"})
    j = r.json() or []
    if not j:
        raise ValueError("geocoding_failed")
    it = j[0]
    lat = float(it.get("lat")); lon = float(it.get("lon"))
    ad = it.get("address") or {}
    town   = _title(ad.get("city") or ad.get("town") or ad.get("village") or ad.get("hamlet") or ad.get("municipality"))
    county = _title((ad.get("county") or "").replace(" County", ""))
    return lat, lon, {"town": town, "county": county, "geocoder": "nominatim"}

# =========================
# OpenStates helpers — FIXED HOUSE FILTER
# =========================

def openstates_people_geo(lat: float, lon: float) -> List[dict]:
    params = {"lat": lat, "lng": lon}
    headers = {"X-API-KEY": OPENSTATES_API_KEY} if OPENSTATES_API_KEY else {}
    r = _http_get(OS_PEOPLE_GEO, params=params, headers=headers)
    j = r.json()
    if isinstance(j, dict) and "results" in j:
        return j.get("results") or []
    if isinstance(j, list):
        return j
    return []


def _pick_house_members_from_people_geo(people: List[dict]) -> List[dict]:
    def j_name(j):
        if isinstance(j, dict):
            return (j.get("name") or "").lower()
        return (j or "").lower()

    def is_lower_nh(role: dict) -> bool:
        lower = role.get("org_classification") == "lower" or role.get("chamber") == "lower"
        return lower and ("new hampshire" in j_name(role.get("jurisdiction")))

    out: List[dict] = []
    for p in people or []:
        role = p.get("current_role") or {}
        if not is_lower_nh(role):
            role = next((r for r in (p.get("roles") or []) if is_lower_nh(r)), None)
            if not role:
                continue
        out.append({
            "name": p.get("name"),
            "party": (p.get("party") or role.get("party") or "Unknown"),
            "district": role.get("district") or "",
            "email": p.get("email"),
            "phone": p.get("voice"),
            "links": [{"url": L.get("url")} for L in (p.get("links") or []) if L and L.get("url")],
        })
    return out

# =========================
# Routes
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

@app.get("/debug/peek")
def debug_peek():
    import itertools
    def head(url):
        try:
            if url.startswith("file://"):
                p = url.replace("file://", "")
                with open(p, "r", encoding="utf-8") as f:
                    return {"ok": True, "path": p, "head": list(itertools.islice(f, 3))}
            else:
                r = requests.get(url, timeout=10)
                r.raise_for_status()
                return {"ok": True, "url": url, "head": r.text.splitlines()[:3]}
        except Exception as e:
            return {"ok": False, "err": str(e)}
    return jsonify({
        "base": head(os.getenv("FLOTERIAL_BASE_CSV_URL", "")),
        "town": head(os.getenv("FLOTERIAL_TOWN_CSV_URL", "")),
    })

@app.get("/debug/floterials")
def debug_floterials():
    try:
        base_map, town_map = load_floterial_maps()
        base_sample = [
            {"base": k, "floterials": sorted(list(v))}
            for k, v in list(base_map.items())[:5]
        ]
        town_sample = [
            {"town": k[0], "county": k[1], "floterials": sorted(list(v))}
            for k, v in list(town_map.items())[:5]
        ]
        return jsonify({
            "base_to_floterial_count": sum(len(v) for v in base_map.values()),
            "town_to_floterial_count": sum(len(v) for v in town_map.values()),
            "samples": {"base": base_sample, "town": town_sample},
        })
    except Exception as e:
        return jsonify({"ok": False, "error": type(e).__name__, "detail": str(e)}), 500

@app.get("/debug/openstates-probe")
def openstates_probe():
    try:
        r = requests.get(OS_PEOPLE_GEO, params={"lat": 43.36, "lng": -72.30}, headers={"X-API-KEY": OPENSTATES_API_KEY} if OPENSTATES_API_KEY else {}, timeout=12)
        try:
            body = r.json()
        except Exception:
            body = None
        if isinstance(body, dict):
            samp = {k: body.get(k) for k in list(body.keys())[:5]}
        elif isinstance(body, list):
            samp = body[:1]
        else:
            samp = None
        return jsonify({"has_key": bool(OPENSTATES_API_KEY), "status_code": r.status_code, "ok": r.ok, "rate_headers": {k: v for k, v in r.headers.items() if "ratelimit" in k.lower() or k.lower() == "retry-after"}, "sample": samp})
    except Exception as e:
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}", "has_key": bool(OPENSTATES_API_KEY)}), 500

@app.get("/debug/row")
def debug_row():
    qtown = _title(request.args.get("town", ""))
    town_rows = _read_csv_url(FLOTERIAL_TOWN_CSV_URL)
    hits = [r for r in town_rows if _title(r.get("town", "")) == qtown]
    return jsonify({"town": qtown, "rows": hits[:10], "count": len(hits)})

@app.get("/debug/diag")
def debug_diag():
    addr = (request.args.get("address") or "").strip()
    if not addr:
        return jsonify({"ok": False, "error": "missing address"}), 400

    # geocode
    try:
        lat, lon, meta = geocode_oneline(addr)
    except Exception as e:
        return jsonify({"ok": False, "stage": "geocode", "error": f"{type(e).__name__}: {e}"}), 500
    town = meta.get("town"); county = meta.get("county")

    base_map, town_map = load_floterial_maps()
    inferred = None
    if (not county) and town:
        poss = {c for (t, c) in town_map.keys() if t == town}
        if len(poss) == 1:
            county = inferred = next(iter(poss))

    raw_ov = sorted(list(town_map.get((town, county), set())))
    fixed = []
    for lbl in raw_ov:
        s = str(lbl or "").strip()
        if re.fullmatch(r"\d+", s) and county:
            fixed.append(f"{county} {int(s)}")
        else:
            fixed.append(s)
    ov_labels = [_norm_district(x) for x in fixed]

    # people.geo
    people_raw = []
    people_house = []
    geo_status = None
    try:
        r = requests.get(OS_PEOPLE_GEO, params={"lat": lat, "lng": lon}, headers={"X-API-KEY": OPENSTATES_API_KEY} if OPENSTATES_API_KEY else {}, timeout=12)
        geo_status = r.status_code
        j = r.json()
        if isinstance(j, dict) and "results" in j:
            people_raw = j["results"] or []
        elif isinstance(j, list):
            people_raw = j
        people_house = _pick_house_members_from_people_geo(people_raw)
    except Exception as e:
        geo_status = f"error:{type(e).__name__}"

    # fallback preview
    districts_to_try = set(ov_labels)
    for b, fset in base_map.items():
        if fset & districts_to_try:
            districts_to_try.add(b)
    districts_to_try = sorted(list(districts_to_try))

    fallback_try = []
    headers = {"X-API-KEY": OPENSTATES_API_KEY} if OPENSTATES_API_KEY else {}
    for d in districts_to_try[:3]:
        variants = [
            {"jurisdiction": "New Hampshire", "chamber": "lower", "district": d},
            {"state": "NH", "chamber": "lower", "district": d},
        ]
        m = re.search(r"(\d+)$", d)
        if m:
            variants.append({"state": "NH", "chamber": "lower", "district": m.group(1)})
        vstats = []
        for params in variants:
            try:
                rr = requests.get(OS_PEOPLE, params=params, headers=headers, timeout=12)
                ok = rr.ok
                cnt = len(rr.json() or []) if ok else None
                vstats.append({"params": params, "status": rr.status_code, "count": cnt})
            except Exception as e:
                vstats.append({"params": params, "status": f"error:{type(e).__name__}"})
        fallback_try.append({"district": d, "variants": vstats})

    return jsonify({
        "ok": True,
        "addr": addr,
        "geocode": {"lat": lat, "lon": lon, "town": town, "county": county, "inferred_county": inferred, "geocoder": meta.get("geocoder")},
        "people_geo": {"status": geo_status, "raw_count": len(people_raw), "house_count": len(people_house), "house_sample": people_house[:3]},
        "overlays": {"raw": raw_ov, "normalized": ov_labels},
        "fallback_preview": fallback_try
    })

@app.get("/api/lookup-legislators")
def lookup_legislators():
    addr = (request.args.get("address") or (request.json.get("address") if request.is_json else None) or "").strip()
    if not addr:
        return jsonify({"success": False, "error": "missing address"}), 400

    # 1) Geocode
    try:
        lat, lon, meta = geocode_oneline(addr)
    except Exception:
        return jsonify({"success": False, "error": "geocoding_failed"}), 400
    town = meta.get("town"); county = meta.get("county")

    # 2) people.geo (never crash); FIXED house filter
    house_from_geo = []
    try:
        people = openstates_people_geo(lat, lon)
        house_from_geo = _pick_house_members_from_people_geo(people)
    except Exception as e:
        app.logger.warning(f"people.geo failed: {type(e).__name__}: {e}")

    # 3) overlays + county inference
    base_map, town_map = load_floterial_maps()
    if (not county) and town:
        poss = {c for (t, c) in town_map.keys() if t == town}
        if len(poss) == 1:
            county = next(iter(poss))
    overlay_labels = sorted(list(town_map.get((town, county), set())))

    fixed = []
    for lbl in overlay_labels:
        s = str(lbl or "").strip()
        if re.fullmatch(r"\d+", s) and county:
            fixed.append(f"{county} {int(s)}")
        else:
            fixed.append(s)
    overlay_labels = [_norm_district(x) for x in fixed]

    if house_from_geo:
        return jsonify({
            "success": True,
            "address": addr,
            "geographies": {"town_county": [town, county]},
            "source": {"geocoder": meta.get("geocoder"), "openstates_geo": True, "overlay_labels": overlay_labels},
            "stateRepresentatives": house_from_geo,
        })

    # 5) Fallback: /people variants (prefer jurisdiction form first)
    reps: List[dict] = []
    try:
        districts_to_try = set(overlay_labels)
        for b, fset in base_map.items():
            if fset & districts_to_try:
                districts_to_try.add(b)
        headers = {"X-API-KEY": OPENSTATES_API_KEY} if OPENSTATES_API_KEY else {}
        def _fetch(params):
            try:
                r = _http_get(OS_PEOPLE, params=params, headers=headers)
                return r.json() or []
            except Exception:
                return []
        seen = set()
        for d in districts_to_try:
            if not d:
                continue
            variants = [
                {"jurisdiction": "New Hampshire", "chamber": "lower", "district": d},
                {"state": "NH", "chamber": "lower", "district": d},
            ]
            m = re.search(r"(\d+)$", d)
            if m:
                variants.append({"state": "NH", "chamber": "lower", "district": m.group(1)})
            for params in variants:
                for p in _fetch(params):
                    k = p.get("id") or (p.get("name"), p.get("email"))
                    if k in seen:
                        continue
                    seen.add(k)
                    reps.append({
                        "name": p.get("name"),
                        "party": p.get("party") or "Unknown",
                        "district": params.get("district") or d,
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
        "source": {"geocoder": meta.get("geocoder"), "openstates_geo": False, "overlay_labels": overlay_labels},
        "stateRepresentatives": reps,
    })

# Simple helpers
@app.get("/api/vote-map")
def vote_map():
    if not VOTES_CSV_URL:
        return jsonify({"success": False, "error": "VOTES_CSV_URL not set"}), 500
    bill = (request.args.get("bill") or "").strip()
    rows = _read_csv_url(VOTES_CSV_URL)
    if bill:
        rows = [r for r in rows if (r.get("bill") or "").strip().lower() == bill.lower()]
    return jsonify({"success": True, "count": len(rows), "rows": rows[:1000]})

@app.get("/api/bill-link")
def bill_link():
    bill = (request.args.get("bill") or "").strip()
    if not bill:
        return jsonify({"success": False, "error": "missing bill"}), 400
    url = f"https://openstates.org/nh/bills/?q={requests.utils.quote(bill)}"
    return jsonify({"success": True, "bill": bill, "url": url})

@app.get("/")
def root():
    return jsonify({"ok": True, "see": "/health"})

# =========================
# MAIN (local dev)
# =========================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)
