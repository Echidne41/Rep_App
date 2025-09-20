import os
import re
import csv
import io
import requests
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime

# =========================
# ENV
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

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ALLOWED_ORIGINS.split(",") if ALLOWED_ORIGINS else ["*"]}})

# =========================
# Helpers
# =========================

def _http_get(url, params=None, headers=None, timeout=20):
    r = requests.get(url, params=params or {}, headers=headers or {}, timeout=timeout)
    r.raise_for_status()
    return r

_csv_cache = {}

def _read_csv_url(url):
    if not url:
        return []
    if url.startswith("file://"):
        path = url.replace("file://", "")
        with open(path, "r", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    r = _http_get(url)
    content = r.content.decode("utf-8", errors="replace")
    return list(csv.DictReader(io.StringIO(content)))

def _title(s):
    return re.sub(r"\s+", " ", (s or "").strip()).title()

def _norm_district(label: str) -> str:
    m = re.search(r"([A-Za-z]+)\s*0*([0-9]+)", (label or ""))
    if not m:
        return (label or "").strip()
    return f"{m.group(1).title()} {int(m.group(2))}"

def load_floterial_maps():
    base_rows = _read_csv_url(FLOTERIAL_BASE_CSV_URL)
    town_rows = _read_csv_url(FLOTERIAL_TOWN_CSV_URL)
    base_map, town_map = {}, {}
    for r in base_rows:
        b = (r.get("base_district") or "").strip()
        f = (r.get("floterial_district") or "").strip()
        if b and f:
            base_map.setdefault(_norm_district(b), set()).add(_norm_district(f))
    for r in town_rows:
        tname = _title(r.get("town", ""))
        county = _title((r.get("county", "").replace(" County", "")).strip())
        fd = r.get("floterial_district") or r.get("district") or ""
        f = _norm_district(fd)
        if tname and county and f:
            town_map.setdefault((tname, county), set()).add(f)
    return base_map, town_map

def geocode_oneline(addr: str):
    try:
        r = _http_get(CENSUS_GEOCODER_URL, params={"address": addr, "benchmark": "Public_AR_Current", "format": "json"})
        j = r.json()
        res = (j.get("result") or {}).get("addressMatches") or []
        if res:
            m = res[0]
            coords = m.get("coordinates") or {}
            lon, lat = float(coords.get("x")), float(coords.get("y"))
            comps = (m.get("addressComponents") or {})
            return lat, lon, {"town": _title(comps.get("city")), "county": _title(comps.get("county")), "geocoder": "census"}
    except Exception:
        pass
    r = _http_get(NOMINATIM_URL, params={"q": addr, "format": "json", "addressdetails": 1, "countrycodes": "us", "state": "New Hampshire", "limit": 1}, headers={"User-Agent": "NH-Rep-Finder/1.0"})
    j = r.json()
    it = j[0]
    return float(it.get("lat")), float(it.get("lon")), {"town": _title(it["address"].get("city") or it["address"].get("town")), "county": _title((it["address"].get("county") or "").replace(" County", "")), "geocoder": "nominatim"}

def openstates_people_geo(lat, lon):
    headers = {"X-API-KEY": OPENSTATES_API_KEY} if OPENSTATES_API_KEY else {}
    r = _http_get(OS_PEOPLE_GEO, params={"lat": lat, "lng": lon}, headers=headers)
    return r.json() or []

def _pick_house_members_from_people_geo(people):
    out = []
    for p in people or []:
        role = p.get("current_role") or {}
        def _is_lower_nh(r):
            return r.get("chamber") == "lower" and "new hampshire" in (r.get("jurisdiction", "").lower() or "")
        if not _is_lower_nh(role):
            roles = p.get("roles") or []
            role = next((r for r in roles if _is_lower_nh(r)), None)
            if not role:
                continue
        out.append({
            "name": p.get("name"),
            "party": p.get("party") or role.get("party") or "Unknown",
            "district": role.get("district") or "",
            "email": p.get("email"),
            "phone": p.get("voice"),
            "links": [{"url": L.get("url")} for L in (p.get("links") or []) if L.get("url")],
        })
    return out

# =========================
# Routes
# =========================
@app.get("/health")
def health():
    base_rows = _read_csv_url(FLOTERIAL_BASE_CSV_URL)
    town_rows = _read_csv_url(FLOTERIAL_TOWN_CSV_URL)
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat() + "Z", "floterial_base_csv_set": bool(base_rows), "floterial_town_csv_set": bool(town_rows), "votes_csv_set": bool(VOTES_CSV_URL), "openstates_api_key": bool(OPENSTATES_API_KEY)})

@app.get("/debug/floterials")
def debug_floterials():
    try:
        base_map, town_map = load_floterial_maps()
        return jsonify({"base_to_floterial_count": sum(len(v) for v in base_map.values()), "town_to_floterial_count": sum(len(v) for v in town_map.values())})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/debug/peek")
def debug_peek():
    import itertools
    def head(url):
        try:
            if url.startswith("file://"):
                p = url.replace("file://", "")
                with open(p, "r", encoding="utf-8") as f:
                    return {"ok": True, "path": p, "head": list(itertools.islice(f, 3))}
        except Exception as e:
            return {"ok": False, "err": str(e)}
    return jsonify({"base": head(FLOTERIAL_BASE_CSV_URL), "town": head(FLOTERIAL_TOWN_CSV_URL)})

@app.get("/api/lookup-legislators")
def lookup_legislators():
    addr = request.args.get("address") or ""
    if not addr:
        return jsonify({"success": False, "error": "missing address"}), 400
    try:
        lat, lon, meta = geocode_oneline(addr)
    except Exception:
        return jsonify({"success": False, "error": "geocoding_failed"}), 400

    town, county = meta.get("town"), meta.get("county")

    house_from_geo, people = [], []
    try:
        people = openstates_people_geo(lat, lon)
        house_from_geo = _pick_house_members_from_people_geo(people)
    except Exception:
        people = []

    base_map, town_map = load_floterial_maps()
    # If county is missing from geocoder, infer it from the CSV keys for this town
    if (not county) and town:
        possible = {c for (t, c) in town_map.keys() if t == town}
        if len(possible) == 1:
            county = next(iter(possible))
    overlay_labels = sorted(list(town_map.get((town, county), set())))

    # Normalize numeric-only labels by prefixing county
    fixed = []
    for lbl in overlay_labels:
        if lbl and re.fullmatch(r"\d+", str(lbl).strip()):
            fixed.append(f"{county} {int(lbl)}")
        else:
            fixed.append(lbl)
    overlay_labels = [_norm_district(x) for x in fixed]

    if house_from_geo:
        return jsonify({"success": True, "address": addr, "geographies": {"town_county": [town, county]}, "source": {"geocoder": meta.get("geocoder"), "openstates_geo": True, "overlay_labels": overlay_labels}, "stateRepresentatives": house_from_geo})

    reps = []
    headers = {"X-API-KEY": OPENSTATES_API_KEY} if OPENSTATES_API_KEY else {}
    for d in overlay_labels:
        try:
            q = {"jurisdiction": "New Hampshire", "chamber": "lower", "district": d}
            r = _http_get(OS_PEOPLE, params=q, headers=headers)
            for p in (r.json() or []):
                reps.append({"name": p.get("name"), "party": p.get("party") or "Unknown", "district": d, "email": p.get("email"), "phone": p.get("voice"), "links": [{"url": L.get("url")} for L in (p.get("links") or []) if L.get("url")],})
        except Exception:
            continue

    return jsonify({"success": True, "address": addr, "geographies": {"town_county": [town, county]}, "source": {"geocoder": meta.get("geocoder"), "openstates_geo": bool(people), "overlay_labels": overlay_labels}, "stateRepresentatives": reps})

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
    if not bill:
        return jsonify({"success": False, "error": "missing bill"}), 400
    link = f"https://openstates.org/nh/bills/?q={requests.utils.quote(bill)}"
    return jsonify({"success": True, "bill": bill, "url": link})

@app.get("/")
def root():
    return jsonify({"ok": True, "see": "/health"})

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)
