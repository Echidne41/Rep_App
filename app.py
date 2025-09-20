import os, re, io, csv, time
from urllib.parse import urlparse, urlencode
import requests

from flask import Flask, request, jsonify, abort, Response
from flask_cors import CORS

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

# =========================
# HEALTH
# =========================
@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "has_openstates_key": bool(OPENSTATES_API_KEY),
        "votes_csv_url_set": bool(VOTES_CSV_URL)
    })

# =========================
# HELPERS: HTTP/file text
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

# =========================
# VOTE CSV → JSON MAP (long or wide)
# =========================
_vote_cache = {"t": 0.0, "rows": [], "columns": []}

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
    # fallback (keep original label uppercased/sanitized)
    return re.sub(r"[^A-Za-z0-9]+", "_", str(s or "")).upper()

def _load_votes_rows_cached():
    now = time.time()
    if _vote_cache["t"] and now - _vote_cache["t"] < VOTES_TTL_SECONDS:
        return _vote_cache["rows"], _vote_cache["columns"]
    if not VOTES_CSV_URL:
        return [], []
    txt = _read_text_from_url(VOTES_CSV_URL)
    headers, rows = _parse_csv_text(txt)
    # hide obvious non-vote keys if present in wide CSV headers
    cols = [h for h in headers if h.lower() not in ("openstates_person_id","person_id","id","name","district","party","bill","vote")]
    _vote_cache.update({"t": now, "rows": rows, "columns": cols})
    return rows, cols

def _normkey(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s or "").lower()).strip()

def _build_vote_map(rows):
    """
    Return dict: person_key -> { column_or_bill_label: rawVote }
    Supports:
      - LONG: columns 'bill' + 'vote' (labels can be human like 'HB148 - LGBTQ Rights')
      - WIDE: one row per person with many bill columns
    Adds alias keys for name and name|district, so joins tolerate ID gaps.
    """
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

    # WIDE
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

        if pid:
            out[pid] = row
        if name_key:
            out[f"name:{name_key}"] = row
        out[f"nd:{name_key}|{dist_key}"] = row
    return out

@app.get("/api/vote-map")
def api_vote_map():
    rows, cols = _load_votes_rows_cached()
    vote_map = _build_vote_map(rows)
    # union of keys from map (covers long CSV labels)
    colset = set()
    for d in vote_map.values():
        colset.update(d.keys())
    for junk in ("openstates_person_id","person_id","id","name","district","party","bill","vote"):
        colset.discard(junk)
    columns = sorted(colset) if colset else cols
    return jsonify({"columns": columns, "votes": vote_map, "rows": len(rows), "source": VOTES_CSV_URL})

# Also serve the CSV directly for debugging
@app.get("/house_key_votes.csv")
def house_key_votes():
    if not VOTES_CSV_URL:
        abort(404)
    txt = _read_text_from_url(VOTES_CSV_URL)
    return Response(txt, mimetype="text/csv")

# =========================
# BILL LINK VIA OPENSTATES
# =========================
_OS_KEY = OPENSTATES_API_KEY
_BILL_CACHE = {}
_BILL_TTL = 3600  # 1h

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
    if not _OS_KEY:
        return jsonify({"error": "OPENSTATES_API_KEY not set"}), 500
    bill_param = request.args.get("bill", "")
    year_param = request.args.get("year", "")
    if not bill_param:
        # allow 'label' like "HB148 - LGBTQ Rights"
        raw = request.args.get("label", "")
        bill_param, y = _extract_bill_code_year(raw)
        if y and not year_param:
            year_param = y

    bill_code, year = _extract_bill_code_year(bill_param or "")
    if not bill_code:
        return jsonify({"error": "bill not parseable"}), 400

    cache_key = f"{bill_code}:{year or ''}"
    now = time.time()
    if cache_key in _BILL_CACHE and now - _BILL_CACHE[cache_key]["t"] < _BILL_TTL:
        return jsonify({"bill": bill_code, "year": year, "url": _BILL_CACHE[cache_key]["url"]})

    params = {"jurisdiction": "New Hampshire", "q": bill_code.replace(" ", ""), "per_page": 3}
    if year:
        params["session"] = year
    r = requests.get("https://v3.openstates.org/bills",
                     headers={"X-API-KEY": _OS_KEY},
                     params=params, timeout=15)
    r.raise_for_status()
    data = r.json() or {}
    items = data.get("results") or data.get("data") or []
    url = _pick_best_bill_url(items[0]) if items else ""
    _BILL_CACHE[cache_key] = {"url": url, "t": now}
    return jsonify({"bill": bill_code, "year": year, "url": url})

# =========================
# ADDRESS → REPS (OpenStates)
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
    if not _OS_KEY:
        return []
    url = "https://v3.openstates.org/people.geo"
    params = {"lat": lat, "lon": lon, "per_page": 50}
    r = requests.get(url, params=params, headers={"X-API-KEY": _OS_KEY}, timeout=20)
    r.raise_for_status()
    data = r.json() or {}
    results = data.get("results") or data.get("data") or []
    reps = []
    for item in results:
        person = item.get("person") or item  # shapes vary
        district = item.get("district") or {}
        name = person.get("name") or person.get("given_name") or ""
        party = None
        parties = person.get("party") or person.get("current_parties") or []
        if isinstance(parties, list) and parties:
            # party objects have "name"
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
    # Keep only NH House reps if extra chambers leak in
    # (Some responses include Senate; you can filter by 'district' naming if needed.)
    return reps

@app.route("/api/lookup-legislators", methods=["GET", "POST"])
def api_lookup_legislators():
    addr = request.args.get("address") or (request.json or {}).get("address") if request.is_json else None
    if not addr:
        return jsonify({"error": "address is required"}), 400

    lat, lon = _geocode_address(addr)
    if lat is None or lon is None:
        return jsonify({"address": addr, "geographies": {}, "stateRepresentatives": [], "source": {"geocoder": "none"}})

    reps = _openstates_people_geo(lat, lon)
    return jsonify({
        "address": addr,
        "geographies": {},  # (optional) you can enrich later with sldl if you want
        "stateRepresentatives": reps,
        "source": {"geocoder": "census_or_nominatim", "openstates_geo": True}
    })

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    # Local dev: python app.py
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=os.getenv("FLASK_DEBUG","0") == "1")
