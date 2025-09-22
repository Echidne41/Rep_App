"""
NH Rep Finder backend — NH House only + floterials + retries + security.

Routes:
  GET /health
  GET /api/vote-map
  GET /api/bill-link
  GET|POST /api/lookup-legislators
  GET /house_key_votes.csv
  GET /debug/floterials
  GET /debug/floterial-headers
  GET /debug/base-map
  GET /debug/district
  GET /version
"""

import os, re, io, csv, time, logging
from urllib.parse import urlparse
import requests

from flask import Flask, request, jsonify, abort, Response
from flask_cors import CORS

logging.basicConfig(level=logging.INFO)

# =========================
# APP & CORS
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
# RATE LIMIT (60/min per IP+path)
# =========================
RATE_LIMIT_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MIN", "60"))
RATE_WINDOW_SECS = 60
COOL_OFF_SECS = 30
_hits: dict[tuple[str, str], list[float]] = {}

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
# HEALTH (verbose)
# =========================
@app.get("/health")
def health():
    by_base, by_town = _load_floterials_cached()
    def _raw_first(url):
        try:
            t = _read_text_from_url(url)
            return t.splitlines()[0] if t else ""
        except Exception as e:
            return f"ERR:{type(e).__name__}"
    return jsonify({
        "ok": True,
        "has_openstates_key": bool(OPENSTATES_API_KEY),
        "votes_csv_url_set": bool(VOTES_CSV_URL),
        "floterial_base_csv_set": bool(FLOTERIAL_BASE_CSV_URL),
        "floterial_town_csv_set": bool(FLOTERIAL_TOWN_CSV_URL),
        "base_to_floterial_count": sum(len(v) for v in by_base.values()),
        "town_to_floterial_count": sum(len(v) for v in by_town.values()),
        "base_keys_sample": sorted(list(by_base.keys()))[:5],
        "town_keys_sample": sorted([f"{t},{c}" for (t,c) in by_town.keys()])[:5],
        "base_csv_first_line": _raw_first(FLOTERIAL_BASE_CSV_URL),
        "town_csv_first_line": _raw_first(FLOTERIAL_TOWN_CSV_URL),
        "commit": os.getenv("RENDER_GIT_COMMIT","local")
    })

# =========================
# HELPERS
# =========================

def _census_sldl_from_coords(lat: float, lon: float) -> str | None:
    try:
        url = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
        params = {"x": lon, "y": lat, "benchmark": "Public_AR_Census2020", "vintage": "Current", "format": "json"}
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        g = ((r.json() or {}).get("result") or {}).get("geographies") or {}
        rows = g.get("State Legislative Districts - Lower") or g.get("State Legislative Districts - Lower (SLDL)") or []
        if not rows: return None
        label = rows[0].get("BASENAME") or rows[0].get("NAME") or ""
        return _norm_district_label(label)
    except Exception:
        return None

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

def _extract_bill_code_year(s: str):
    m = re.search(r"(HB|SB|HR|HCR|SCR)\s*[-_ ]?\s*(\d{1,4})(?:.*?(\d{4}))?", str(s or ""), re.I)
    if not m: return None, None
    return f"{m.group(1).upper()}{m.group(2)}", (m.group(3) or None)

def _get_address_from_request():
    addr = (request.args.get("address") or request.args.get("addr") or "").strip()
    if not addr and request.is_json:
        j = request.get_json(silent=True) or {}
        addr = (j.get("address") or j.get("addr") or "").strip()
    if not addr and request.form:
        addr = (request.form.get("address") or request.form.get("addr") or "").strip()
    return addr or None

def _err_json(stage: str, exc: Exception):
    return {"error": f"{stage} failed", "message": str(exc), "type": exc.__class__.__name__}

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
    out = {}
    is_long = any(("bill" in r and "vote" in r) for r in rows)

    if is_long:
        for r in rows:
            pid = (r.get("openstates_person_id") or r.get("person_id") or r.get("id") or "").strip()
            bill_label = r.get("bill", "") or ""
            if not bill_label:
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
    """CSV loader with BOM/header normalization."""
    if not url:
        return []
    txt = _read_text_from_url(url)
    if txt.startswith("\ufeff"):
        txt = txt.lstrip("\ufeff")
    f = io.StringIO(txt)
    rdr = csv.reader(f)
    rows = list(rdr)
    if not rows:
        return []
    def norm(h: str) -> str:
        return re.sub(r"\s+", "_", (h or "").strip().strip('"').strip("'").lower().lstrip("\ufeff"))
    headers = [norm(h) for h in rows[0]]
    out = []
    for r in rows[1:]:
        d = {}
        for i, h in enumerate(headers):
            d[h] = (r[i].strip() if i < len(r) else "")
        out.append(d)
    return out

def _load_floterials_cached():
    now = time.time()
    if _FLOTERIAL_CACHE["t"] and now - _FLOTERIAL_CACHE["t"] < _FLOTERIAL_TTL:
        return _FLOTERIAL_CACHE["by_base"], _FLOTERIAL_CACHE["by_town"]
    by_base, by_town = {}, {}
    # base mapping
    for r in _load_floterial_csv(FLOTERIAL_BASE_CSV_URL):
        b = _norm_district_label(r.get("base_district", "") or r.get("district", ""))
        f = _norm_district_label(r.get("floterial_district", "") or r.get("floterial", ""))
        if b and f:
            by_base.setdefault(b, set()).add(f)
    # town mapping
    for r in _load_floterial_csv(FLOTERIAL_TOWN_CSV_URL):
        t = _norm_town(r.get("town", ""))
        c = (r.get("county", "") or "").replace(" County", "").strip().title()
        f = _norm_district_label(r.get("floterial_district", "") or r.get("district", ""))
        if t and c and f:
            by_town.setdefault((t, c), set()).add(f)
    _FLOTERIAL_CACHE.update({"t": now, "by_base": by_base, "by_town": by_town})
    return by_base, by_town

# =========================
# BILL LINK VIA OPENSTATES (with retry)
# =========================
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
        return jsonify({"bill": bill_param or "", "year": year_param or "", "url": ""})

    url = "https://v3.openstates.org/bills"
    params = {"jurisdiction": "New Hampshire", "q": bill_code.replace(" ", ""), "per_page": 3}
    if year: params["session"] = year
    r = requests.get(url, headers={"X-API-KEY": OPENSTATES_API_KEY}, params=params, timeout=15)
    try:
        r.raise_for_status()
    except requests.HTTPError:
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(0.5)
            r = requests.get(url, headers={"X-API-KEY": OPENSTATES_API_KEY}, params=params, timeout=15)
            r.raise_for_status()
        else:
            raise

    items = (r.json() or {}).get("results") or (r.json() or {}).get("data") or []
    url_out = _pick_best_bill_url(items[0]) if items else ""
    if not hasattr(app, "_BILL_CACHE"):
        app._BILL_CACHE = {}
    app._BILL_CACHE[f"{bill_code}:{year or ''}"] = {"url": url_out, "t": time.time()}
    return jsonify({"bill": bill_code, "year": year, "url": url_out})

# =========================
# OPENSTATES LOOKUPS
# =========================
CENSUS_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

def _geocode_address(addr: str):
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

def _is_lower_house(item, fallback_label=""):
    org = (item.get("organization") or item.get("org") or {})
    if (org.get("classification") or "").lower() == "lower":
        return True
    person = item.get("person") or item
    roles = person.get("roles") or person.get("current_roles") or []
    if isinstance(roles, list):
        for r in roles:
            if (r.get("org_classification") or "").lower() == "lower": return True
            if (r.get("chamber") or "").lower() == "lower": return True
    d = item.get("district") or {}
    text = " ".join([str(d.get("name") or ""), str(d.get("label") or ""), str(fallback_label or "")]).lower()
    if "senate" in text or "congress" in text or "united states" in text: return False
    if "house" in text: return True
    norm = _norm_district_label(d.get("name") or d.get("label") or fallback_label or "")
    return bool(re.match(r"^[A-Za-z]+\s+\d+$", norm))

def _openstates_people_geo(lat: float, lon: float):
    if not OPENSTATES_API_KEY:
        return []
    url = "https://v3.openstates.org/people.geo"
    params  = {"lat": lat, "lng": lon, "per_page": 50, "jurisdiction": "New Hampshire"}
    headers = {"X-API-KEY": OPENSTATES_API_KEY}

    r = requests.get(url, params=params, headers=headers, timeout=20)
    try:
        r.raise_for_status()
    except requests.HTTPError:
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(0.5)
            r = requests.get(url, params=params, headers=headers, timeout=20)
            r.raise_for_status()
        else:
            raise

    data = r.json() or {}
    results = data.get("results") or data.get("data") or []

    reps = []
    for item in results:
        if not _is_lower_house(item):
            continue
        person   = item.get("person") or item
        district = item.get("district") or {}
        name     = person.get("name") or person.get("given_name") or ""
        parties  = person.get("party") or person.get("current_parties") or []
        party    = (parties[0].get("name") if isinstance(parties[0], dict) else str(parties[0])) if parties else None
        reps.append({
            "openstates_person_id": person.get("id") or person.get("openstates_id") or "",
            "name": name,
            "party": party,
            "district": _norm_district_label(district.get("name") or district.get("label") or ""),
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
    headers = {"X-API-KEY": OPENSTATES_API_KEY}

    r = requests.get(url, params=params, headers=headers, timeout=20)
    try:
        r.raise_for_status()
    except requests.HTTPError:
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(0.5)
            r = requests.get(url, params=params, headers=headers, timeout=20)
            r.raise_for_status()
        else:
            raise

    data = r.json() or {}
    results = data.get("results") or data.get("data") or []
    reps = []
    for item in results:
        if not _is_lower_house(item, fallback_label=label):
            continue
        p = item.get("person") or item
        d = item.get("district") or {}
        parties = p.get("party") or p.get("current_parties") or []
        party = (parties[0].get("name") if isinstance(parties[0], dict) else str(parties[0])) if parties else None
        reps.append({
            "openstates_person_id": p.get("id") or "",
            "name": p.get("name") or p.get("given_name") or "",
            "party": party,
            "district": _norm_district_label(d.get("name") or d.get("label") or label),
            "email": p.get("email"),
            "phone": p.get("phone"),
            "links": p.get("links") or []
        })
    return reps

# =========================
# FLOTERIAL OVERLAY UNION (town first, then inversion)
# =========================
def _nominatim_reverse(lat: float, lon: float):
    try:
        ua = {"User-Agent": f"NH-Rep-Finder/1.0 ({NOMINATIM_EMAIL or 'contact@example.com'})"}
        params = {"lat": lat, "lon": lon, "format": "json", "zoom": 10, "addressdetails": 1}
        r = requests.get("https://nominatim.openstreetmap.org/reverse", params=params, headers=ua, timeout=15)
        r.raise_for_status()
        a = (r.json() or {}).get("address") or {}
        town = a.get("town") or a.get("city") or a.get("village") or a.get("municipality") or ""
        county = (a.get("county") or "").replace(" County", "").strip()
        return _norm_town(town), county.title()
    except Exception:
        return "", ""
# Census SLDL → "Sullivan 2" style
def _census_sldl_from_coords(lat: float, lon: float) -> str | None:
    try:
        url = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
        params = {
            "x": lon, "y": lat, "benchmark": "Public_AR_Census2020",
            "vintage": "Current", "format": "json"
        }
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        g = ((r.json() or {}).get("result") or {}).get("geographies") or {}
        rows = g.get("State Legislative Districts - Lower") or g.get("State Legislative Districts - Lower (SLDL)") or []
        if not rows:
            return None
        row = rows[0]
        # Try BASENAME first (often just "Sullivan 2"), then NAME
        label = row.get("BASENAME") or row.get("NAME") or ""
        return _norm_district_label(label)
    except Exception:
        return None

def _overlay_district_labels(lat: float, lon: float, openstates_labels, addr_text: str | None = None):
    by_base, by_town = _load_floterials_cached()

    # ---- 1) Decide the single true BASE list (no inversion) ----
    bases = set()
    # a) Any base labels OpenStates already gave us
    for lbl in openstates_labels or []:
        n = _norm_district_label(lbl)
        # Heuristic: if label exists as a key in by_base, treat it as a base
        if n in by_base:
            bases.add(n)
    # b) Always add Census SLDL (authoritative one base)
    sldl = _census_sldl_from_coords(lat, lon)
    if sldl:
        bases.add(_norm_district_label(sldl))

    # If still empty, we have no base; we’ll still compute floterials from town.
    # ---- 2) Compute floterials from chosen bases ----
    flos = set()
    for b in bases:
        for f in by_base.get(b, set()):
            flos.add(_norm_district_label(f))

    # ---- 3) Add floterials from town ----
    town, county = _nominatim_reverse(lat, lon)
    if (not town or not county) and addr_text:
        at = addr_text.lower()
        for (t, c) in by_town.keys():
            if t.lower() in at:
                town, county = t, c
                break
    if town and county:
        for f in by_town.get((town, county), set()):
            flos.add(_norm_district_label(f))

    # ---- 4) Final overlay = bases + floterials (no floterial→base) ----
    return set(bases) | set(flos)



# =========================
# LOOKUP
# =========================
@app.route("/api/lookup-legislators", methods=["GET", "POST"])
def api_lookup_legislators():
    try:
        addr = _get_address_from_request()
        if not addr:
            return jsonify({"error": "address is required", "hint": "Send ?address=... or JSON {address: ...}"}), 422

        try:
            lat, lon = _geocode_address(addr)
        except Exception as e:
            logging.exception("geocode error")
            return jsonify(_err_json("geocode", e)), 500

        if lat is None or lon is None:
            return jsonify({"address": addr, "geographies": {}, "stateRepresentatives": [], "source": {"geocoder": "none"}})

        try:
            reps_point = _openstates_people_geo(lat, lon)
        except Exception as e:
            logging.exception("people.geo error")
            return jsonify(_err_json("people.geo", e)), 500

        os_labels = [(r.get("district") or "") for r in reps_point]

        try:
            want_labels = _overlay_district_labels(lat, lon, os_labels, addr_text=addr)

        except Exception as e:
            logging.exception("overlay error")
            return jsonify(_err_json("overlay", e)), 500

        have_by_label = {_norm_district_label(r.get("district") or "") for r in reps_point}
        reps_extra = []
        try:
            for lbl in want_labels:
                if lbl and lbl not in have_by_label:
                    reps_extra.extend(_openstates_people_by_district_label(lbl))
        except Exception as e:
            logging.exception("people-by-district error")
            return jsonify(_err_json("people(district)", e)), 500

        reps_all = _unique_reps(reps_point + reps_extra)

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
                "openstates_geo_used": bool(reps_point),
                "overlay_labels": sorted(list(want_labels))
            },
            "success": True
        })
    except Exception as e:
        logging.exception("lookup-legislators unhandled")
        return jsonify(_err_json("lookup-legislators", e)), 500

# =========================
# DEBUG ROUTES
# =========================
@app.get("/debug/floterials")
def debug_floterials():
    by_base, by_town = _load_floterials_cached()
    return jsonify({
        "base_to_floterial_count": sum(len(v) for v in by_base.values()),
        "town_to_floterial_count": sum(len(v) for v in by_town.values()),
        "samples": {
            "base": sorted(list(by_base.keys()))[:10],
            "town": [
                {"town": t, "county": c, "floterials": sorted(list(v))}
                for (t,c), v in list(by_town.items())[:5]
            ],
        }
    })

def _peek_csv_headers_and_rows(url: str, n: int = 3):
    try:
        txt = _read_text_from_url(url)
        raw_first = txt.splitlines()[0] if txt else ""
        rdr = csv.DictReader(io.StringIO(txt))
        rows = []
        for i, r in enumerate(rdr):
            if i >= n: break
            rows.append(r)
        return {"url": url, "fieldnames": rdr.fieldnames, "raw_first_line": raw_first, "sample_rows": rows}
    except Exception as e:
        return {"url": url, "error": f"{type(e).__name__}: {e}"}

@app.get("/debug/floterial-headers")
def debug_floterial_headers():
    return jsonify({
        "base": _peek_csv_headers_and_rows(FLOTERIAL_BASE_CSV_URL),
        "town": _peek_csv_headers_and_rows(FLOTERIAL_TOWN_CSV_URL),
    })

@app.get("/debug/base-map")
def debug_base_map():
    label = _norm_district_label(request.args.get("label",""))
    by_base, _ = _load_floterials_cached()
    vals = sorted(list(by_base.get(label, set())))
    return jsonify({"label": label, "has_label": label in by_base, "count": len(vals), "floterials": vals})

@app.get("/debug/district")
def debug_district():
    label = request.args.get("label", "")
    reps = _openstates_people_by_district_label(label) if label else []
    return jsonify({"label": _norm_district_label(label), "count": len(reps), "reps": reps})

# Version/commit probe
BUILD_SHA = os.getenv("RENDER_GIT_COMMIT","local")
@app.get("/version")
def version():
    return {"commit": BUILD_SHA}

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=os.getenv("FLASK_DEBUG","0") == "1")


