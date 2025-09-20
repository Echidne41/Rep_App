"""
NH Rep Finder backend — clean drop-in.

Routes:
  GET /health
  GET /api/vote-map
  GET /api/bill-link
  GET|POST /api/lookup-legislators
  GET /house_key_votes.csv
  GET /debug/floterials
  GET /debug/district
"""

import os, re, io, csv, time
from urllib.parse import urlparse
import requests

from flask import Flask, request, jsonify, abort, Response
from flask_cors import CORS

import logging, traceback
logging.basicConfig(level=logging.INFO)
def _err_json(stage: str, exc: Exception):
    return {
        "error": f"{stage} failed",
        "type": exc.__class__.__name__,
        "message": str(exc),
    }


# =========================
# APP & CORS (create app first!)
# =========================
app = Flask(__name__)

ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",") if o.strip()]
if "*" in ALLOWED_ORIGINS:
    CORS(app)
else:
    CORS(app, resources={r"/*": {"origins": ALLOWED_ORIGINS}})

# =========================
# SECURITY HEADERS
# =========================
@app.after_request
def _security_headers(resp):
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["Referrer-Policy"] = "same-origin"
    resp.headers["Permissions-Policy"] = "geolocation=()"
    return resp

# =========================
# GENTLE RATE LIMIT (60/min per IP+path; 30s retry)
# =========================
RATE_LIMIT_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MIN", "60"))
RATE_WINDOW_SECS = 60
COOL_OFF_SECS = 30
_hits = {}  # (ip, path) -> [timestamps]

@app.before_request
def _rate_limit_guard():
    if not request.path.startswith("/api/"):
        return
    ip = (request.headers.get("x-forwarded-for") or request.remote_addr or "?").split(",")[0].strip()
    key = (ip, request.path)
    now = time.time()
    recent = [t for t in _hits.get(key, []) if now - t < RATE_WINDOW_SECS]
    if len(recent) >= RATE_LIMIT_PER_MIN:
        resp = jsonify({"error": "Too many requests, please retry shortly."})
        resp.status_code = 429
        resp.headers["Retry-After"] = str(COOL_OFF_SECS)
        return resp
    recent.append(now)
    _hits[key] = recent

# =========================
# ENV
# =========================
OPENSTATES_API_KEY = os.getenv("OPENSTATES_API_KEY", "").strip()
VOTES_CSV_URL = os.getenv("VOTES_CSV_URL", "").strip()
VOTES_TTL_SECONDS = int(os.getenv("VOTES_TTL_SECONDS", "300"))
NOMINATIM_FALLBACK = os.getenv("NOMINATIM_FALLBACK", "1") == "1"
NOMINATIM_EMAIL = os.getenv("NOMINATIM_EMAIL", "").strip()

FLOTERIAL_BASE_CSV_URL = os.getenv("FLOTERIAL_BASE_CSV_URL", "").strip()
FLOTERIAL_TOWN_CSV_URL = os.getenv("FLOTERIAL_TOWN_CSV_URL", "").strip()

# =========================
# HEALTH
# =========================
@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "has_openstates_key": bool(OPENSTATES_API_KEY),
        "votes_csv_url_set": bool(VOTES_CSV_URL),
        "floterial_base_csv_set": bool(FLOTERIAL_BASE_CSV_URL),
        "floterial_town_csv_set": bool(FLOTERIAL_TOWN_CSV_URL),
    })

# =========================
# HELPERS
# =========================
def _read_text_from_url(url: str) -> str:
    if not url:
        return ""
    if url.startswith("file://"):
        path = urlparse(url).path
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.text

def _normkey(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s or "").lower()).strip()

def _unique_reps(reps):
    out = {}
    for r in reps:
        k = r.get("openstates_person_id") or r.get("name")
        if k: out[k] = r
    return list(out.values())

def _parse_csv_text(text: str):
    rdr = csv.reader(io.StringIO(text))
    rows = list(rdr)
    if not rows:
        return [], []
    headers = [h.strip() for h in rows[0]]
    out = []
    for r in rows[1:]:
        d = {}
        for i, h in enumerate(headers):
            d[h] = r[i] if i < len(r) else ""
        out.append(d)
    return headers, out

def _extract_bill_key(s: str) -> str:
    m = re.search(r"(HB|SB|HR|HCR|SCR)\s*-?\s*(\d{1,4})(?:.*?(\d{4}))?", str(s or ""), re.I)
    if m:
        bill = f"{m.group(1).upper()}{m.group(2)}"
        yr = m.group(3)
        return f"{bill}_{yr}" if yr else bill
    return re.sub(r"[^A-Za-z0-9]+", "_", str(s or "")).upper()

def _get_address_from_request():
    addr = (request.args.get("address") or request.args.get("addr") or "").strip()
    if not addr and request.is_json:
        j = request.get_json(silent=True) or {}
        addr = (j.get("address") or j.get("addr") or "").strip()
    if not addr and request.form:
        addr = (request.form.get("address") or request.form.get("addr") or "").strip()
    return addr or None

# =========================
# VOTE CSV → JSON MAP (long or wide)
# =========================
_vote_cache = {"t": 0.0, "rows": [], "columns": []}

def _load_votes_rows_cached():
    now = time.time()
    if _vote_cache["t"] and now - _vote_cache["t"] < VOTES_TTL_SECONDS:
        return _vote_cache["rows"], _vote_cache["columns"]
    if not VOTES_CSV_URL:
        return [], []
    txt = _read_text_from_url(VOTES_CSV_URL)
    headers, rows = _parse_csv_text(txt)
    cols = [h for h in headers if h.lower() not in ("openstates_person_id","person_id","id","name","district","party","bill","vote")]
    _vote_cache.update({"t": now, "rows": rows, "columns": cols})
    return rows, cols

def _build_vote_map(rows):
    """Return dict: person_key -> { column_or_bill_label: rawVote }."""
    out = {}
    is_long = any(("bill" in r and "vote" in r) for r in rows)

    if is_long:
        for r in rows:
            pid = (r.get("openstates_person_id") or r.get("person_id") or r.get("id") or "").strip()
            bill_label = r.get("bill", "") or ""
            bill_key = bill_label or _extract_bill_key(bill_label)
            if not bill_key:
                continue
            val = str(r.get("vote", ""))
            name_key = _normkey(r.get("name", ""))
            dist_key = _normkey(r.get("district", ""))

            if pid:
                out.setdefault(pid, {})[bill_label] = val
            if name_key:
                out.setdefault(f"name:{name_key}", {})[bill_label] = val
            out.setdefault(f"nd:{name_key}|{dist_key}", {})[bill_label] = val
        return out

    # wide
    for r in rows:
        pid = (r.get("openstates_person_id") or r.get("person_id") or r.get("id") or "").strip()
        row = {}
        for k, v in r.items():
            lk = k.lower()
            if lk in ("openstates_person_id","person_id","id","name","district","party","bill","vote"):
                continue
            row[k] = str(v or "")
        name_key = _normkey(r.get("name", ""))
        dist_key = _normkey(r.get("district", ""))

        if pid: out[pid] = row
        if name_key: out[f"name:{name_key}"] = row
        out[f"nd:{name_key}|{dist_key}"] = row

    return out

@app.get("/api/vote-map")
def api_vote_map():
    rows, cols = _load_votes_rows_cached()
    vote_map = _build_vote_map(rows)
    colset = set()
    for d in vote_map.values():
        colset.update(d.keys())
    for junk in ("openstates_person_id","person_id","id","name","district","party","bill","vote"):
        colset.discard(junk)
    columns = sorted(colset) if colset else cols
    return jsonify({"columns": columns, "votes": vote_map, "rows": len(rows), "source": VOTES_CSV_URL})

@app.get("/house_key_votes.csv")
def house_key_votes():
    if not VOTES_CSV_URL:
        abort(404)
    txt = _read_text_from_url(VOTES_CSV_URL)
    return Response(txt, mimetype="text/csv")

# =========================
# FLOTERIAL OVERLAY (base CSV + town CSV)
# =========================
_FLOTERIAL_CACHE = {"t": 0.0, "by_base": {}, "by_town": {}}
_FLOTERIAL_TTL = 3600  # 1h

def _norm_district_label(s: str) -> str:
    m = re.search(r"([A-Za-z]+)\s*0*([0-9]+)", str(s or ""))
    if not m:
        return (s or "").strip()
    county = m.group(1).title().strip()
    num = int(m.group(2))
    return f"{county} {num}"

def _norm_town(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).title()

def _load_floterial_csv(url: str):
    if not url: return []
    txt = _read_text_from_url(url)
    rdr = csv.DictReader(io.StringIO(txt))
    return list(rdr)

def _load_floterials_cached():
    now = time.time()
    if _FLOTERIAL_CACHE["t"] and now - _FLOTERIAL_CACHE["t"] < _FLOTERIAL_TTL:
        return _FLOTERIAL_CACHE["by_base"], _FLOTERIAL_CACHE["by_town"]
    by_base, by_town = {}, {}
    # base mapping
    for r in _load_floterial_csv(FLOTERIAL_BASE_CSV_URL):
        b = _norm_district_label(r.get("base_district", ""))
        f = _norm_district_label(r.get("floterial_district", ""))
        if b and f:
            by_base.setdefault(b, set()).add(f)
    # town mapping
    for r in _load_floterial_csv(FLOTERIAL_TOWN_CSV_URL):
        t = _norm_town(r.get("town", ""))
        c = (r.get("county", "") or "").strip()
        if c:
            c = c.replace(" County", "").strip().title()
        f = _norm_district_label(r.get("floterial_district", ""))
        if t and c and f:
            by_town.setdefault((t, c), set()).add(f)
    _FLOTERIAL_CACHE.update({"t": now, "by_base": by_base, "by_town": by_town})
    return by_base, by_town

# =========================
# BILL LINK VIA OPENSTATES
# =========================
def _extract_bill_code_year(s: str):
    m = re.search(r"(HB|SB|HR|HCR|SCR)\s*[-_ ]?\s*(\d{1,4})(?:.*?(\d{4}))?", str(s or ""), re.I)
    if not m: return None, None
    return f"{m.group(1).upper()}{m.group(2)}", (m.group(3) or None)

def _pick_best_bill_url(item: dict) -> str:
    for src in (item.get("sources") or []):
        u = src.get("url")
        if u: return u
    for lk in (item.get("links") or []):
        u = lk.get("url")
        if u: return u
    return item.get("openstates_url") or ""

@app.get("/api/bill-link")
def api_bill_link():
    if not OPENSTATES_API_KEY:
        return jsonify({"error": "OPENSTATES_API_KEY not set"}), 500

    bill_param = (request.args.get("bill") or "").strip()
    year_param = (request.args.get("year") or "").strip()
    if not bill_param:
        raw = (request.args.get("label") or "").strip()
        bill_param, y = _extract_bill_code_year(raw)
        if y and not year_param:
            year_param = y

    bill_code, year = _extract_bill_code_year(bill_param or "")
    if not bill_code:
        # Not parseable → harmless empty URL
        return jsonify({"bill": bill_param or "", "year": year_param or "", "url": ""})

    cache_key = f"{bill_code}:{year or ''}"
    now = time.time()
    # tiny in-memory cache (1h)
    if not hasattr(app, "_BILL_CACHE"):
        app._BILL_CACHE = {}
    cache = app._BILL_CACHE
    if cache_key in cache and now - cache[cache_key]["t"] < 3600:
        return jsonify({"bill": bill_code, "year": year, "url": cache[cache_key]["url"]})

    params = {"jurisdiction": "New Hampshire", "q": bill_code.replace(" ", ""), "per_page": 3}
    if year: params["session"] = year
    r = requests.get("https://v3.openstates.org/bills",
                     headers={"X-API-KEY": OPENSTATES_API_KEY},
                     params=params, timeout=15)
    r.raise_for_status()
    data = r.json() or {}
    items = data.get("results") or data.get("data") or []
    url = _pick_best_bill_url(items[0]) if items else ""
    cache[cache_key] = {"url": url, "t": now}
    return jsonify({"bill": bill_code, "year": year, "url": url})

# =========================
# OPENSTATES LOOKUPS
# =========================
CENSUS_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

def _geocode_address(addr: str):
    # Try Census first
    try:
        params = {"address": addr, "benchmark": "Public_AR_Census2020", "format": "json"}
        r = requests.get(CENSUS_URL, params=params, timeout=15)
        r.raise_for_status()
        j = r.json()
        matches = (j.get("result") or {}).get("addressMatches") or []
        if matches:
            loc = matches[0]["coordinates"]
            return float(loc["y"]), float(loc["x"])
    except Exception:
        pass
    # Fallback Nominatim (polite UA)
    if NOMINATIM_FALLBACK:
        try:
            h = {"User-Agent": f"NH-Rep-Finder/1.0 ({NOMINATIM_EMAIL or 'contact@example.com'})"}
            params = {"q": addr, "format": "json", "limit": 1, "countrycodes": "us"}
            r = requests.get(NOMINATIM_URL, params=params, headers=h, timeout=15)
            r.raise_for_status()
            arr = r.json()
            if arr:
                return float(arr[0]["lat"]), float(arr[0]["lon"])
        except Exception:
            pass
    return None, None

def _openstates_people_geo(lat: float, lon: float):
    if not OPENSTATES_API_KEY:
        return []
    url = "https://v3.openstates.org/people.geo"
    params = {"lat": lat, "lon": lon, "per_page": 50}
    r = requests.get(url, params=params, headers={"X-API-KEY": OPENSTATES_API_KEY}, timeout=20)
    r.raise_for_status()
    data = r.json() or {}
    results = data.get("results") or data.get("data") or []
    reps = []
    for item in results:
        person = item.get("person") or item
        district = item.get("district") or {}
        name = person.get("name") or person.get("given_name") or ""
        party = None
        parties = person.get("party") or person.get("current_parties") or []
        if isinstance(parties, list) and parties:
            party = (parties[0].get("name") if isinstance(parties[0], dict) else str(parties[0])) or None
        reps.append({
            "openstates_person_id": person.get("id") or person.get("openstates_id") or "",
            "name": name,
            "party": party,
            "district": district.get("name") or district.get("label") or None,
            "email": person.get("email"),
            "phone": person.get("phone"),
            "links": person.get("links") or []
        })
    return reps

def _openstates_people_by_district_label(label: str):
    if not OPENSTATES_API_KEY:
        return []
    url = "https://v3.openstates.org/people"
    params = {"jurisdiction": "New Hampshire", "district": _norm_district_label(label), "per_page": 50}
    r = requests.get(url, params=params, headers={"X-API-KEY": OPENSTATES_API_KEY}, timeout=20)
    r.raise_for_status()
    data = r.json() or {}
    results = data.get("results") or data.get("data") or []
    reps = []
    for item in results:
        p = item.get("person") or item
        d = item.get("district") or {}
        parties = p.get("party") or p.get("current_parties") or []
        party = None
        if isinstance(parties, list) and parties:
            party = (parties[0].get("name") if isinstance(parties[0], dict) else str(parties[0])) or None
        reps.append({
            "openstates_person_id": p.get("id") or "",
            "name": p.get("name") or p.get("given_name") or "",
            "party": party,
            "district": d.get("name") or d.get("label") or _norm_district_label(label),
            "email": p.get("email"),
            "phone": p.get("phone"),
            "links": p.get("links") or []
        })
    return reps

# =========================
# FLOTERIAL OVERLAY UNION
# =========================
def _nominatim_reverse(lat: float, lon: float):
    try:
        ua = {"User-Agent": f"NH-Rep-Finder/1.0 ({NOMINATIM_EMAIL or 'contact@example.com'})"}
        params = {"lat": lat, "lon": lon, "format": "json", "zoom": 10, "addressdetails": 1}
        r = requests.get("https://nominatim.openstreetmap.org/reverse", params=params, headers=ua, timeout=15)
        r.raise_for_status()
        a = (r.json() or {}).get("address") or {}
        town = a.get("town") or a.get("city") or a.get("village") or a.get("municipality") or ""
        county = (a.get("county") or "").replace(" County","").strip()
        return _norm_town(town), county.title()
    except Exception:
        return "", ""

def _overlay_district_labels(lat: float, lon: float, openstates_labels):
    """Union of labels from OpenStates + base→floterials + town→floterials."""
    by_base, by_town = _load_floterials_cached()
    current = {_norm_district_label(x) for x in openstates_labels if x}
    overlay = set(current)
    for lbl in list(current):
        for f in by_base.get(lbl, set()):
            overlay.add(_norm_district_label(f))
    town, county = _nominatim_reverse(lat, lon)
    if town and county:
        for f in by_town.get((town, county), set()):
            overlay.add(_norm_district_label(f))
    return overlay

# =========================
# LOOKUP ROUTE
# =========================
@app.route("/api/lookup-legislators", methods=["GET", "POST"])
def api_lookup_legislators():
    try:
        # accept address from query, JSON, or form
        addr = (request.args.get("address") or request.args.get("addr") or "").strip()
        if not addr and request.is_json:
            j = request.get_json(silent=True) or {}
            addr = (j.get("address") or j.get("addr") or "").strip()
        if not addr and request.form:
            addr = (request.form.get("address") or request.form.get("addr") or "").strip()
        if not addr:
            return jsonify({"error": "address is required", "hint": "Send ?address=... or JSON {address: ...}"}), 422

        # geocode
        try:
            lat, lon = _geocode_address(addr)
        except Exception as e:
            logging.exception("geocode error")
            return jsonify(_err_json("geocode", e)), 500

        if lat is None or lon is None:
            return jsonify({
                "address": addr,
                "geographies": {},
                "stateRepresentatives": [],
                "source": {"geocoder": "none"}
            })

        # OpenStates at the point
        try:
            reps_point = _openstates_people_geo(lat, lon)
        except Exception as e:
            logging.exception("openstates people.geo error")
            return jsonify(_err_json("people.geo", e)), 500

        os_labels = [(r.get("district") or "") for r in reps_point]

        # overlay union (handles base→floterial + town→floterial)
        try:
            want_labels = _overlay_district_labels(lat, lon, os_labels)
        except Exception as e:
            logging.exception("overlay compute error")
            return jsonify(_err_json("overlay", e)), 500

        # fill any missing district labels via /people
        have_by_label = {_norm_district_label(r.get("district") or "") for r in reps_point}
        reps_extra = []
        try:
            for lbl in want_labels:
                if lbl and lbl not in have_by_label:
                    reps_extra.extend(_openstates_people_by_district_label(lbl))
        except Exception as e:
            logging.exception("openstates people by district error")
            return jsonify(_err_json("people(district)", e)), 500

        reps_all = _unique_reps(reps_point + reps_extra)

        # reverse for town/county (informational; never fatal)
        try:
            town, county = _nominatim_reverse(lat, lon)
        except Exception:
            town, county = "", ""

        return jsonify({
            "address": addr,
            "geographies": {"town_county": [town, county]},
            "stateRepresentatives": reps_all,
            "source": {
                "geocoder": "census_or_nominatim",
                "openstates_geo": True,
                "overlay_labels": sorted(list(want_labels))
            }
        })
    except Exception as e:
        # last-ditch guard: never 500 without context
        logging.exception("lookup-legislators unhandled")
        return jsonify(_err_json("lookup-legislators", e)), 500


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=os.getenv("FLASK_DEBUG","0") == "1")

