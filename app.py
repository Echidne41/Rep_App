import os, re, csv, io, time, unicodedata
from datetime import datetime
from typing import List, Dict, Tuple, Iterable, Optional

import requests
from flask import Flask, request, jsonify
from flask_cors import CORS

# ---------------- ENV ----------------
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

OPENSTATES_API_KEY      = os.getenv("OPENSTATES_API_KEY", "")
ALLOWED_ORIGINS         = os.getenv("ALLOWED_ORIGINS", "*")

VOTES_CSV_URL           = os.getenv("VOTES_CSV_URL", "")
FLOTERIAL_BASE_CSV_URL  = os.getenv("FLOTERIAL_BASE_CSV_URL", "")
FLOTERIAL_TOWN_CSV_URL  = os.getenv("FLOTERIAL_TOWN_CSV_URL", "")

OS_ROOT       = "https://v3.openstates.org"
OS_PEOPLE     = f"{OS_ROOT}/people"
OS_PEOPLE_GEO = f"{OS_ROOT}/people.geo"

CENSUS_ADDR_URL  = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
CENSUS_GEOG_URL  = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
NOMINATIM_URL    = "https://nominatim.openstreetmap.org/search"

# ---------------- APP ----------------
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ALLOWED_ORIGINS.split(",") if ALLOWED_ORIGINS else ["*"]}})

# ---------------- HELPERS ----------------
def _http_get(url: str, params: dict | None = None, headers: dict | None = None, timeout: int = 18):
    r = requests.get(url, params=params or {}, headers=headers or {}, timeout=timeout)
    app.logger.info(f"GET {url} {r.status_code} params={params}")
    r.raise_for_status()
    return r

def _title(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).title()

def _deaccent(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c))

def _key(town: str, county: str) -> Tuple[str, str]:
    return (_title(_deaccent(town)), _title(_deaccent(county)))

# --- expand county abbreviations like "Me 30" -> "Merrimack 30"
ABBR = {
  'Be':'Belknap','Ca':'Carroll','Ch':'Cheshire','Co':'Coos','Gr':'Grafton',
  'Hi':'Hillsborough','Me':'Merrimack','Ro':'Rockingham','St':'Strafford','Su':'Sullivan'
}
def _expand_label(s: str) -> str:
    s = (s or "").strip()
    m = re.match(r'^(..)\s+(\d+)$', s)
    if m and m.group(1) in ABBR:
        return f"{ABBR[m.group(1)]} {int(m.group(2))}"
    return s

def _norm_district(label: str) -> str:
    """
    Normalize 'Sullivan 02' -> 'Sullivan 2', '02' -> '2'.
    Accepts abbrev, numeric-only, or 'County N' and returns consistent form.
    """
    s = _expand_label((label or "").strip())
    m = re.match(r"^([A-Za-z]+)\s*0*([0-9]+)$", s)
    if m:
        return f"{m.group(1).title()} {int(m.group(2))}"
    m2 = re.match(r"^0*([0-9]+)$", s)
    if m2:
        return str(int(m2.group(1)))
    return s

def _dedupe_people(people: List[dict]) -> List[dict]:
    seen = set()
    out = []
    for p in people or []:
        k = p.get("id") or (p.get("name"), p.get("email"))
        if k in seen:
            continue
        seen.add(k)
        out.append(p)
    return out

# ---------------- CSV LOAD (CACHED) ----------------
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
    """
    base_map:  'Sullivan 2' -> {'Sullivan 10', ...}
    town_map:  ('Cornish','Sullivan') -> {'Sullivan 10', ...}
    Robust to column name variants and abbrev/full county names.
    """
    base_rows = _read_csv_url(FLOTERIAL_BASE_CSV_URL)
    town_rows = _read_csv_url(FLOTERIAL_TOWN_CSV_URL)

    base_map: Dict[str, set] = {}
    for r in base_rows:
        b = _norm_district(r.get("base_district") or r.get("base") or r.get("base_label") or "")
        f = _norm_district(r.get("floterial_district") or r.get("district") or r.get("floterial") or "")
        if b and f:
            base_map.setdefault(b, set()).add(f)

    town_map: Dict[Tuple[str, str], set] = {}
    for r in town_rows:
        town   = _title(r.get("town",""))
        county = _title((r.get("county","")).replace(" County",""))
        fd     = (r.get("floterial_district") or r.get("district") or r.get("floterial") or "").strip()
        if re.fullmatch(r"\d+", fd) and county:  # number only -> prefix with county
            fd = f"{county} {int(fd)}"
        fd = _norm_district(fd)
        if town and county and fd:
            town_map.setdefault(_key(town, county), set()).add(fd)

    return base_map, town_map

# ---------------- GEOCODING ----------------
def geocode_oneline(addr: str) -> Tuple[float, float, dict]:
    """
    Census onelineaddress → lat/lon; fallback to Nominatim. Returns (lat, lon, {town, county, geocoder})
    """
    try:
        r = _http_get(CENSUS_ADDR_URL, params={"address": addr, "benchmark": "Public_AR_Current", "format": "json"})
        j = r.json()
        matches = (j.get("result") or {}).get("addressMatches") or []
        if matches:
            m = matches[0]
            lon = float(m["coordinates"]["x"]); lat = float(m["coordinates"]["y"])
            comps = m.get("addressComponents") or {}
            return lat, lon, {"town": _title(comps.get("city")), "county": _title(comps.get("county")), "geocoder": "census"}
    except Exception:
        pass

    r = _http_get(NOMINATIM_URL, params={"q": addr, "format": "json", "addressdetails": 1, "countrycodes": "us", "state": "New Hampshire", "limit": 1},
                  headers={"User-Agent": "NH-Rep-Finder/1.0"})
    j = r.json() or []
    if not j:
        raise ValueError("geocoding_failed")
    it = j[0]
    lat = float(it.get("lat")); lon = float(it.get("lon"))
    ad = it.get("address") or {}
    town   = _title(ad.get("city") or ad.get("town") or ad.get("village") or ad.get("hamlet") or ad.get("municipality"))
    county = _title((ad.get("county") or "").replace(" County", ""))
    return lat, lon, {"town": town, "county": county, "geocoder": "nominatim"}

def census_sldl_from_coords(lat: float, lon: float) -> Optional[str]:
    """
    Use Census geographies to get SLDL (base) district number as a string, e.g., '2'.
    """
    try:
        r = _http_get(CENSUS_GEOG_URL, params={
            "x": lon, "y": lat, "benchmark": "Public_AR_Current",
            "vintage": "Current_Current", "format": "json"
        })
        j = r.json() or {}
        geogs = ((j.get("result") or {}).get("geographies") or {})
        sldl = geogs.get("State Legislative Districts - Lower", []) or geogs.get("State Legislative Districts - Lower Chamber", [])
        if sldl:
            rec = sldl[0]
            base = str(rec.get("BASENAME") or "").strip()
            if base:
                return str(int(base))  # strip leading zeros
            name = str(rec.get("NAME") or "")
            m = re.search(r"(\d+)", name)
            if m:
                return str(int(m.group(1)))
    except Exception as e:
        app.logger.warning(f"census_sldl_from_coords failed: {type(e).__name__}: {e}")
    return None

# ---------------- OPENSTATES ----------------
def _is_lower_nh(role: dict) -> bool:
    lower = (role.get("org_classification") == "lower") or (role.get("chamber") == "lower")
    jur = role.get("jurisdiction")
    if isinstance(jur, dict):
        jur = (jur.get("name") or "").lower()
    else:
        jur = (jur or "").lower()
    return lower and ("new hampshire" in jur)

def openstates_people_geo(lat: float, lon: float) -> List[dict]:
    headers = {"X-API-KEY": OPENSTATES_API_KEY} if OPENSTATES_API_KEY else {}
    r = _http_get(OS_PEOPLE_GEO, params={"lat": lat, "lng": lon}, headers=headers)
    j = r.json()
    if isinstance(j, dict) and "results" in j:
        return j["results"] or []
    if isinstance(j, list):
        return j
    return []

def _pick_house_members_from_people_geo(people: List[dict]) -> List[dict]:
    out: List[dict] = []
    for p in people or []:
        role = p.get("current_role") or {}
        if not _is_lower_nh(role):
            role = next((r for r in (p.get("roles") or []) if _is_lower_nh(r)), None)
            if not role:
                continue
        out.append({
            "id": p.get("id"),
            "name": p.get("name"),
            "party": (p.get("party") or role.get("party") or "Unknown"),
            "district": _norm_district(role.get("district") or ""),
            "email": p.get("email"),
            "phone": p.get("voice"),
            "links": [{"url": L.get("url")} for L in (p.get("links") or []) if L and L.get("url")],
        })
    return out

def openstates_search_lower_nh(params: dict) -> List[dict]:
    headers = {"X-API-KEY": OPENSTATES_API_KEY} if OPENSTATES_API_KEY else {}
    try:
        r = _http_get(OS_PEOPLE, params=params, headers=headers)
        j = r.json() or []
        if isinstance(j, dict) and "results" in j:
            j = j["results"] or []
        out = []
        for p in j:
            role = p.get("current_role") or {}
            if not _is_lower_nh(role):
                role = next((r for r in (p.get("roles") or []) if _is_lower_nh(r)), {})
            out.append({
                "id": p.get("id"),
                "name": p.get("name"),
                "party": p.get("party") or role.get("party") or "Unknown",
                "district": _norm_district(role.get("district") or params.get("district") or ""),
                "email": p.get("email"),
                "phone": p.get("voice"),
                "links": [{"url": L.get("url")} for L in (p.get("links") or []) if L and L.get("url")],
            })
        return out
    except Exception:
        return []

# ---------------- ROUTES ----------------
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
    try:
        base_map, town_map = load_floterial_maps()
        base_sample = [{"base": k, "floterials": sorted(list(v))} for k, v in list(base_map.items())[:5]]
        town_sample = [{"town": k[0], "county": k[1], "floterials": sorted(list(v))} for k, v in list(town_map.items())[:5]]
        return jsonify({
            "base_to_floterial_count": sum(len(v) for v in base_map.values()),
            "town_to_floterial_count": sum(len(v) for v in town_map.values()),
            "samples": {"base": base_sample, "town": town_sample},
        })
    except Exception as e:
        return jsonify({"ok": False, "error": type(e).__name__, "detail": str(e)}), 500

@app.get("/debug/row")
def debug_row():
    qtown = _title(request.args.get("town", ""))
    town_rows = _read_csv_url(FLOTERIAL_TOWN_CSV_URL)
    hits = [r for r in town_rows if _title(r.get("town","")) == qtown]
    return jsonify({"town": qtown, "rows": hits[:10], "count": len(hits)})

@app.get("/api/lookup-legislators")
def lookup_legislators():
    addr = (request.args.get("address") or (request.json.get("address") if request.is_json else None) or "").strip()
    force_fallback = request.args.get("force_fallback", "") in ("1","true","yes")
    if not addr:
        return jsonify({"success": False, "error": "missing address"}), 400

    # 1) Geocode
    try:
        lat, lon, meta = geocode_oneline(addr)
    except Exception:
        return jsonify({"success": False, "error": "geocoding_failed"}), 400
    town = meta.get("town"); county = meta.get("county")

    # 2) Base SLDL from Census (CSV-independent)
    sldl_num = census_sldl_from_coords(lat, lon)  # e.g., '2'
    sldl_label = f"{county} {sldl_num}" if county and sldl_num else None

    # 3) people.geo (no early return)
    house_from_geo: List[dict] = []
    if not force_fallback:
        try:
            people = openstates_people_geo(lat, lon)
            house_from_geo = _pick_house_members_from_people_geo(people)
        except Exception as e:
            app.logger.warning(f"people.geo failed: {type(e).__name__}: {e}")

    # 4) Overlays + county inference
    base_map, town_map = load_floterial_maps()
    if (not county) and town:
        poss = {c for (t, c) in town_map.keys() if t == _title(_deaccent(town))}
        if len(poss) == 1:
            county = next(iter(poss))
    overlay_labels = sorted(list(town_map.get(_key(town, county), set())))

    # 5) Districts to query (robust)
    districts_to_try = set()
    for p in (house_from_geo or []):
        if p.get("district"):
            districts_to_try.add(_norm_district(p["district"]))
    for d in overlay_labels:
        if d:
            districts_to_try.add(_norm_district(d))
    if sldl_num:
        districts_to_try.add(_norm_district(sldl_num))      # '2'
    if sldl_label:
        districts_to_try.add(_norm_district(sldl_label))    # 'Sullivan 2', etc.

    # link base <- floterial via CSV
    for b, fset in (base_map or {}).items():
        if fset & districts_to_try:
            districts_to_try.add(_norm_district(b))

    # 6) Query OpenStates /people for each district; merge with people.geo; dedupe
    reps: List[dict] = house_from_geo[:]

    for d in sorted(districts_to_try):
        if not d:
            continue
        variants = [
            {"jurisdiction": "New Hampshire", "chamber": "lower", "district": d},
            {"state": "NH", "chamber": "lower", "district": d},
        ]
        if re.fullmatch(r"(\d+)", d):  # numeric only
            variants.append({"state": "NH", "chamber": "lower", "district": d})
        for params in variants:
            reps.extend(openstates_search_lower_nh(params))

    reps = _dedupe_people(reps)

    return jsonify({
        "success": True,
        "address": addr,
        "geographies": {
            "town_county": [_title(town), _title(county)],
            "sldl": sldl_num,
        },
        "source": {
            "geocoder": meta.get("geocoder"),
            "openstates_geo_used": bool(house_from_geo) and not force_fallback,
            "overlay_labels": sorted(list(overlay_labels)),
            "districts_queried": sorted(list(districts_to_try)),
        },
        "stateRepresentatives": reps,
    })

@app.get("/")
def root():
    return jsonify({"ok": True, "see": "/health"})

# ---------------- MAIN ----------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)
