"""
NH Rep Finder backend — deterministic: Census -> labels, CSV -> overlays, OpenStates -> names.

Routes:
  GET /health
  GET /version
  GET /api/lookup-legislators
  GET /api/bill-link
  GET /api/vote-map
  GET /house_key_votes.csv
  GET /debug/floterials
  GET /debug/floterial-headers
  GET /debug/base-map
  GET /debug/district
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

FLOTERIAL_BASE_CSV_URL = os.getenv("FLOTERIAL_BASE_CSV_URL", "").strip()
FLOTERIAL_TOWN_CSV_URL = os.getenv("FLOTERIAL_TOWN_CSV_URL", "").strip()

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

def _extract_bill_code_year(s: str):
    m = re.search(r"(HB|SB|HR|HCR|SCR)\s*[-_ ]?\s*(\d{1,4})(?:.*?(\d{4}))?", str(s or ""), re.I)
    if not m: return None, None
    return f"{m.group(1).upper()}{m.group(2)}", (m.group(3) or None)

def _err_json(stage: str, exc: Exception):
    return {"error": f"{stage} failed", "message": str(exc), "type": exc.__class__.__name__}

# =========================
# CSVs (normalized)
# =========================
_FLOTERIAL_CACHE = {"t": 0.0, "by_base": {}, "by_town": {}}
_FLOTERIAL_TTL = 3600  # 1h

def _norm_district_label(s: str) -> str:
    m = re.search(r"([A-Za-z]+)\s*0*([0-9]+)", str(s or ""))
    if not m: return (s or "").strip()
    return f"{m.group(1).title().strip()} {int(m.group(2))}"

def _norm_town(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).title()

def _load_floterial_csv(url: str):
    if not url: return []
    txt = _read_text_from_url(url)
    if txt.startswith("\ufeff"): txt = txt.lstrip("\ufeff")
    f = io.StringIO(txt)
    rdr = csv.reader(f)
    rows = list(rdr)
    if not rows: return []
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
    for r in _load_floterial_csv(FLOTERIAL_BASE_CSV_URL):
        b = _norm_district_label(r.get("base_district","") or r.get("district",""))
        f = _norm_district_label(r.get("floterial_district","") or r.get("floterial",""))
        if b and f: by_base.setdefault(b, set()).add(f)
    for r in _load_floterial_csv(FLOTERIAL_TOWN_CSV_URL):
        t = _norm_town(r.get("town",""))
        c = (r.get("county","") or "").replace(" County","").strip().title()
        f = _norm_district_label(r.get("floterial_district","") or r.get("district",""))
        if t and c and f: by_town.setdefault((t,c), set()).add(f)
    _FLOTERIAL_CACHE.update({"t": now, "by_base": by_base, "by_town": by_town})
    return by_base, by_town

# =========================
# OPENSTATES (names by district label)
# =========================
def _openstates_people_by_district_label(label: str):
    if not OPENSTATES_API_KEY: return []
    url = "https://v3.openstates.org/people"
    params = {"jurisdiction": "New Hampshire", "district": _norm_district_label(label), "per_page": 50}
    headers = {"X-API-KEY": OPENSTATES_API_KEY}
    r = requests.get(url, params=params, headers=headers, timeout=20)
    try:
        r.raise_for_status()
    except requests.HTTPError:
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(0.5)
            r = requests.get(url, params=params, headers=headers, timeout=20); r.raise_for_status()
        else:
            raise
    data = r.json() or {}
    results = data.get("results") or data.get("data") or []
    reps = []
    for item in results:
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
# CENSUS GEOCODING (single source of truth)
# =========================
CENSUS_ONE_LINE = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
CENSUS_GEOG = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"

def _census_geocode(addr: str):
    params = {"address": addr, "benchmark": "Public_AR_Census2020", "format": "json"}
    r = requests.get(CENSUS_ONE_LINE, params=params, timeout=20)
    r.raise_for_status()
    j = r.json()
    matches = (j.get("result") or {}).get("addressMatches") or []
    if not matches: return None, None
    loc = matches[0]["coordinates"]
    lat, lon = float(loc["y"]), float(loc["x"])
    return lat, lon

def _census_geographies(lat: float, lon: float):
    """Return (base_sldl, town_name, county_name) from Census geographies with robust fallbacks."""
    url = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"

    def _try(p):
        r = requests.get(url, params=p, timeout=20)
        r.raise_for_status()
        g = ((r.json() or {}).get("result") or {}).get("geographies") or {}
        sldl = g.get("State Legislative Districts - Lower") or g.get("State Legislative Districts - Lower (SLDL)") or []
        mcd  = g.get("County Subdivisions") or g.get("County Subdivisions (MCD)") or []
        co   = g.get("Counties") or []
        base = _norm_district_label((sldl[0].get("BASENAME") or sldl[0].get("NAME") or "")) if sldl else None
        town = _norm_town(mcd[0].get("NAME") or "") if mcd else ""
        county = (co[0].get("NAME") or "").replace(" County","").strip().title() if co else ""
        return base, town, county, p

    # Correct first; then two fallbacks. Add layers=all to avoid layer-mapping issues.
    tries = [
        {"x": lon, "y": lat, "benchmark": "Public_AR_Current", "vintage": "Current", "layers": "all", "format": "json"},
        {"x": lon, "y": lat, "benchmark": "4",                  "vintage": "Current", "layers": "all", "format": "json"},
        {"x": lon, "y": lat, "benchmark": "Public_AR_Census2020","vintage": "Census2020_Current","layers":"all","format":"json"},
    ]
    last_err, last_p = None, None
    for p in tries:
        try:
            b, t, c, used = _try(p)
            # optional: keep for /health debugging
            app._LAST_CENSUS_PARAMS = used
            return b, t, c
        except Exception as e:
            last_err, last_p = e, p
            continue
    raise requests.HTTPError(f"Census geographies failed. Last params={last_p}, err={last_err}")



# =========================
# LABEL COMPUTATION (deterministic)
# =========================
def _compute_labels_census_first(lat: float, lon: float):
    by_base, by_town = _load_floterials_cached()
    base, town, county = _census_geographies(lat, lon)
    bases = set()
    if base: bases.add(base)
    flos = set()
    # Add flos by base
    for b in bases:
        for f in by_base.get(b, set()):
            flos.add(_norm_district_label(f))
    # Add flos by town
    if town and county:
        for f in by_town.get((town, county), set()):
            flos.add(_norm_district_label(f))
    return bases, flos, town, county

# =========================
# VOTES passthrough
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

@app.get("/api/vote-map")
def api_vote_map():
    rows, cols = _load_votes_rows_cached()
    # build map omitted for brevity; same as before or keep just passthrough columns/rows count
    return jsonify({"columns": cols, "rows": len(rows), "source": VOTES_CSV_URL})

@app.get("/house_key_votes.csv")
def house_key_votes():
    if not VOTES_CSV_URL: abort(404)
    txt = _read_text_from_url(VOTES_CSV_URL)
    return Response(txt, mimetype="text/csv")

# =========================
# BILL LINK VIA OPENSTATES
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
        if y and not year_param: year_param = y
    bill_code, year = _extract_bill_code_year(bill_param or "")
    if not bill_code: return jsonify({"bill": bill_param or "", "year": year_param or "", "url": ""})
    url = "https://v3.openstates.org/bills"
    params = {"jurisdiction": "New Hampshire", "q": bill_code.replace(" ", ""), "per_page": 3}
    if year: params["session"] = year
    r = requests.get(url, headers={"X-API-KEY": OPENSTATES_API_KEY}, params=params, timeout=15)
    try:
        r.raise_for_status()
    except requests.HTTPError:
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(0.5)
            r = requests.get(url, headers={"X-API-KEY": OPENSTATES_API_KEY}, params=params, timeout=15); r.raise_for_status()
        else:
            raise
    items = (r.json() or {}).get("results") or (r.json() or {}).get("data") or []
    return jsonify({"bill": bill_code, "year": year, "url": _pick_best_bill_url(items[0]) if items else ""})

# =========================
# LOOKUP (Census-first)
# =========================
@app.route("/api/lookup-legislators", methods=["GET", "POST"])
def api_lookup_legislators():
    try:
        addr = (request.args.get("address") or request.args.get("addr") or "").strip()
        if not addr and request.is_json:
            j = request.get_json(silent=True) or {}
            addr = (j.get("address") or j.get("addr") or "").strip()
        if not addr and request.form:
            addr = (request.form.get("address") or request.form.get("addr") or "").strip()
        if not addr:
            return jsonify({"error": "address is required", "hint": "Send ?address=... or JSON {address: ...}"}), 422

        lat, lon = _census_geocode(addr)
        if lat is None or lon is None:
            return jsonify({"address": addr, "geographies": {}, "stateRepresentatives": [],
                            "source": {"geocoder": "census", "note": "no match"}})

        bases, flos, town, county = _compute_labels_census_first(lat, lon)
        want_labels = sorted(list(bases | flos))

        reps_all = []
        for lbl in want_labels:
            reps_all.extend(_openstates_people_by_district_label(lbl))
        reps_all = _unique_reps(reps_all)

        return jsonify({
            "address": addr,
            "geographies": {"latlon": [lat, lon], "town_county": [town, county]},
            "stateRepresentatives": reps_all,
            "source": {
                "labels_strategy": "census_sldl + csv_overlays",
                "bases": sorted(list(bases)),
                "floterials": sorted(list(flos)),
                "overlay_labels": want_labels
            },
            "success": True
        })
    except Exception as e:
        logging.exception("lookup-legislators unhandled")
        return jsonify(_err_json("lookup-legislators", e)), 500

# =========================
# DEBUG / DIAG
# =========================
@app.get("/debug/census-params")
def debug_census_params():
    return jsonify(getattr(app, "_LAST_CENSUS_PARAMS", {}))

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

# Version/commit
BUILD_SHA = os.getenv("RENDER_GIT_COMMIT","local")
@app.get("/version")
def version():
    return {"commit": BUILD_SHA}

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
        "floterial_base_csv_set": bool(FLOTERIAL_BASE_CSV_URL),
        "floterial_town_csv_set": bool(FLOTERIAL_TOWN_CSV_URL),
        "base_to_floterial_count": sum(len(v) for v in by_base.values()),
        "town_to_floterial_count": sum(len(v) for v in by_town.values()),
        "base_csv_first_line": _raw_first(FLOTERIAL_BASE_CSV_URL),
        "town_csv_first_line": _raw_first(FLOTERIAL_TOWN_CSV_URL),
        "commit": BUILD_SHA
    })

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=os.getenv("FLASK_DEBUG","0") == "1")


