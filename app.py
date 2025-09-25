import os, time, csv, io, re, requests, signal
from typing import Dict, Any, List, Tuple, Optional
from flask import Flask, jsonify, request
from flask_cors import CORS

# ----- local utils (your repo) -----
from utils.geocode import geocode_address, GeocodeError
from utils.districts import DistrictIndex  # SU2 -> "Sullivan 2" normalization

# ===================== Flask & CORS =====================
app = Flask(__name__)
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").strip()
if ALLOWED_ORIGINS in ("", "*"):
    CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=False)
else:
    origins = [o.strip() for o in ALLOWED_ORIGINS.split(",") if o.strip()]
    CORS(app, resources={r"/*": {"origins": origins}}, supports_credentials=False)

@app.route("/")
def index():
    return "OK", 200  # Render health probe

# ===================== Env / Config =====================
OPENSTATES_API_KEY    = os.getenv("OPENSTATES_API_KEY", "")
NOMINATIM_EMAIL       = os.getenv("NOMINATIM_EMAIL", "rep-app@yourorg.org")

FLOTERIAL_BASE_CSV_URL = os.getenv("FLOTERIAL_BASE_CSV_URL")
FLOTERIAL_TOWN_CSV_URL = os.getenv("FLOTERIAL_TOWN_CSV_URL")
FLOTERIAL_BY_BASE_PATH = os.getenv("FLOTERIAL_BY_BASE_PATH", "floterial_by_base.csv")
FLOTERIAL_MAP_PATH     = os.getenv("FLOTERIAL_MAP_PATH", "floterial_by_town.csv")

# Votes: prefer env URL, else local file in repo root
VOTES_CSV_URL     = (os.getenv("VOTES_CSV_URL") or "").strip().strip("'\"")
VOTES_TTL_SECONDS = int(os.getenv("VOTES_TTL_SECONDS", os.getenv("OS_TTL_SECONDS", "300")))

OS_MIN_DELAY_MS = int(os.getenv("OS_MIN_DELAY_MS", "350"))
OS_TTL_SECONDS  = int(os.getenv("OS_TTL_SECONDS", "180"))  # people cache

RENDER_COMMIT      = os.getenv("RENDER_GIT_COMMIT", "")
HOUSE_GEOJSON_PATH = os.getenv("HOUSE_GEOJSON_PATH", "data/nh_house_districts.json")

# ===================== OpenStates client =====================
OS_BASE = "https://v3.openstates.org"
_last_call_ts = 0.0
_os_people_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}

def _os_throttle():
    global _last_call_ts
    now = time.time()
    need = OS_MIN_DELAY_MS / 1000.0
    wait = (_last_call_ts + need) - now
    if wait > 0:
        time.sleep(wait)
    _last_call_ts = time.time()

def _os_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if not OPENSTATES_API_KEY:
        return {"error": "no_api_key", "status": 500, "detail": "OPENSTATES_API_KEY not set"}
    _os_throttle()
    headers = {"X-API-Key": OPENSTATES_API_KEY, "Accept": "application/json"}
    try:
        r = requests.get(f"{OS_BASE}{path}", params=params, headers=headers, timeout=8)  # 8s HTTP
    except requests.RequestException as e:
        return {"error": "transport", "status": 502, "detail": str(e)[:200]}
    if r.status_code == 429:
        return {"error": "rate_limited", "status": 429, "detail": r.text[:200]}
    if r.status_code >= 400:
        return {"error": "upstream", "status": r.status_code, "detail": r.text[:200]}
    try:
        return r.json()
    except ValueError as e:
        return {"error": "bad_json", "status": 502, "detail": str(e)[:200]}

def os_people_by_district(label: str) -> Dict[str, Any]:
    """TTL cache; never store errors."""
    now = time.time()
    key = str(label).strip()
    if OS_TTL_SECONDS > 0:
        cached = _os_people_cache.get(key)
        if cached:
            ts, payload = cached
            if now - ts < OS_TTL_SECONDS:
                return payload
            _os_people_cache.pop(key, None)
    payload = _os_get("/people", {
        "jurisdiction": "New Hampshire",
        "chamber": "lower",
        "district": key,
        "per_page": 50,
    })
    if payload.get("error"):
        _os_people_cache.pop(key, None)
        return payload
    if OS_TTL_SECONDS > 0:
        _os_people_cache[key] = (time.time(), payload)
    return payload

def _extract_people(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if "error" in payload:
        return []
    data = payload.get("results") or payload.get("data") or payload.get("people") or []
    out: List[Dict[str, Any]] = []
    for p in data:
        out.append({
            "id": p.get("id") or p.get("openstates_id"),
            "name": p.get("name"),
            "party": (p.get("party") or [{}])[0].get("name") if isinstance(p.get("party"), list) else p.get("party"),
            "email": p.get("email"),
            "links": p.get("links", []),
        })
    return out

# ===================== CSV helpers (floterials) =====================
def _read_csv_from(url_or_path: Optional[str], fallback_path: str) -> Tuple[List[str], List[List[str]]]:
    path = (url_or_path or "").strip()
    if path.startswith("file:///"):
        real = path[len("file://"):]
    elif path:
        real = path
    else:
        real = fallback_path
    headers: List[str] = []
    rows: List[List[str]] = []
    try:
        with open(real, newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if i == 0: headers = row
                else: rows.append(row)
    except FileNotFoundError:
        if real != fallback_path:
            try:
                with open(fallback_path, newline="", encoding="utf-8-sig") as f:
                    reader = csv.reader(f)
                    for i, row in enumerate(reader):
                        if i == 0: headers = row
                        else: rows.append(row)
            except Exception:
                pass
    except Exception:
        pass
    return headers, rows

def _h_norm(h: str) -> str: return h.strip().lower().replace(" ", "_")

def _pick_col(row: dict, candidates: List[str]) -> Optional[str]:
    # normalize headers
    norm_map = { _nrm(k): k for k in row.keys() if k is not None }
    wants = [ _nrm(w) for w in candidates ]  # <-- normalize wants too

    # exact match first
    for want in wants:
        if want in norm_map:
            return norm_map[want]

    # loose contains match (only as a last resort)
    for nk, original in norm_map.items():
        for want in wants:
            if want and want in nk:
                return original
    return None


def _group_sample(headers: List[str], rows: List[List[str]]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    if not headers: return out
    key_idx, val_idx = _pick_cols(headers)
    if key_idx is None or val_idx is None: return out
    for r in rows[:50]:
        if len(r) <= max(key_idx, val_idx): continue
        base = r[key_idx].strip(); flo = r[val_idx].strip()
        if base and flo:
            out.setdefault(base, [])
            if flo not in out[base]: out[base].append(flo)
    return out

def _csv_counts() -> Dict[str, Any]:
    by_base_h, by_base_r = _read_csv_from(FLOTERIAL_BASE_CSV_URL, FLOTERIAL_BY_BASE_PATH)
    by_town_h, by_town_r = _read_csv_from(FLOTERIAL_TOWN_CSV_URL, FLOTERIAL_MAP_PATH)
    return {
        "by_base_path": FLOTERIAL_BY_BASE_PATH,
        "by_base_count": len(by_base_r),
        "by_town_path": FLOTERIAL_MAP_PATH,
        "by_town_count": len(by_town_r),
        "headers": {"by_base": by_base_h, "by_town": by_town_h},
        "sample_by_base": dict(list(_group_sample(by_base_h, by_base_r).items())[:3]),
        "sample_by_town": dict(list(_group_sample(by_town_h, by_town_r).items())[:3]),
    }

# ===================== Votes (CSV URL or local file) =====================
_VOTES_CACHE = {"t": 0, "rows": [], "src": ""}

def _pick_col(row: dict, candidates: List[str]) -> Optional[str]:
    # normalize both headers and candidate names
    norm_map = {_nrm(k): k for k in row.keys() if k is not None}
    wants = [_nrm(w) for w in candidates]

    for want in wants:
        if want in norm_map:
            return norm_map[want]

    # loose contains match (handles minor header variants)
    for nk, original in norm_map.items():
        for want in wants:
            if want in nk:
                return original
    return None

def _votes_csv_url() -> str:
    if VOTES_CSV_URL:
        return VOTES_CSV_URL
    local = os.path.join(os.path.dirname(__file__), "house_key_votes.csv").replace("\\", "/")
    return f"file://{local}"

def _http_get_text(url: str) -> str:
    if url.lower().startswith("file://"):
        p = url[7:].strip().strip("'\"")
        if os.name != "nt" and not p.startswith("/"): p = "/" + p
        if os.name == "nt":
            if p.startswith("/"): p = p[1:]
            p = p.replace("/", "\\")
        with open(p, "r", encoding="utf-8") as f:
            return f.read()
    s = requests.Session()
    s.headers.update({"User-Agent":"nh-rep-finder/1","Accept":"text/csv, text/plain, */*"})
    r = s.get(url, timeout=30, allow_redirects=True)
    r.raise_for_status()
    return r.text

def _fetch_votes_rows(force_refresh: bool = False):
    now = time.time()
    if not force_refresh and _VOTES_CACHE["rows"] and now - _VOTES_CACHE["t"] < VOTES_TTL_SECONDS:
        return _VOTES_CACHE["rows"], None
    url = _votes_csv_url()
    try:
        text = _http_get_text(url)
        t = (text or "").lstrip()
        if t.lower().startswith("<!doctype html") or "<html" in t[:1000].lower():
            return [], "Votes CSV URL returned HTML"
        csv.field_size_limit(min((1 << 31) - 1, 10_000_000))
        rdr = csv.DictReader(io.StringIO(text))
        rows = [{(k or "").strip(): (v.strip() if isinstance(v, str) else v) for k, v in (row or {}).items()} for row in rdr]
        _VOTES_CACHE.update({"t": now, "rows": rows, "src": url})
        return rows, None
    except Exception as e:
        return [], f"votes fetch error: {e}"

def _nrm(s: Optional[str]) -> str:
    if not s: return ""
    return re.sub(r"\s+", " ", re.sub(r"[^A-Za-z0-9\s]", "", s)).strip().lower()

def _pick_col(row: dict, candidates: List[str]) -> Optional[str]:
    norm_map = { _nrm(k): k for k in row.keys() if k is not None }
    for want in candidates:
        if want in norm_map: return norm_map[want]
    for nk, original in norm_map.items():
        for want in candidates:
            if want in nk: return original
    return None

def _district_equiv(a: str, b: str) -> bool:
    an = _nrm(a); bn = _nrm(b)
    if an == bn: return True
    ad = re.findall(r"\d+", an); bd = re.findall(r"\d+", bn)
    if not ad or not bd or ad[0] != bd[0]: return False
    aletters = re.sub(r"[^a-z]", "", an); bletters = re.sub(r"[^a-z]", "", bn)
    if not aletters or not bletters: return True
    return aletters[:3] == bletters[:3]

def _bill_key(s: str) -> str:
    m = re.search(r"(HB|SB|HR|HCR|SCR)\s*[-_ ]?\s*(\d{1,4})(?:.*?(\d{4}))?", str(s or ""), re.I)
    if m:
        code = f"{m.group(1).upper()}{m.group(2)}"
        return f"{code}_{m.group(3)}" if m.group(3) else code
    return str(s or "").strip()

def _canon_vote_value(v: str) -> str:
    s = _nrm(v)
    if s in {"y","yes","yea","aye","support","supported","for","pro","infavor","affirmative"}:
        return "For"
    if s in {"n","no","nay","oppose","opposed","against","con"}:
        return "Against"
    if s in {"present","abstain","abstained","excused","nv","notvoting","recused"}:
        return "Other"
    return (v or "").strip()

def _match_row_for_rep(rows, *, person_id: str = "", name: str = "", district: str = "") -> Optional[dict]:
    # 1) by OpenStates ID
    for r in rows:
        col = _pick_col(r, ["openstates_person_id","openstates_id","person_id","openstates id","os id"])
        if col and person_id and (r.get(col) or "").strip() == person_id:
            return r
    # 2) fallback by name (+ district if provided)
    name_hits: List[dict] = []
    name_n = _nrm(name); dist_n = _nrm(district)
    for r in rows:
        ncol = _pick_col(r, ["name","full name","representative","representative name","member","rep"])
        if not ncol: continue
        rname_n = _nrm(r.get(ncol, ""))
        if rname_n == name_n or (name_n and name_n in rname_n):
            name_hits.append(r)
    if not name_hits: return None
    if dist_n:
        for r in name_hits:
            dcol = _pick_col(r, ["district","district label","house district","state house district","sldl","sldl name","sldl label"])
            if dcol and _district_equiv(r.get(dcol, ""), district):
                return r
    return name_hits[0] if len(name_hits) == 1 else None

def _row_to_vote_list_wide(row: dict) -> List[dict]:
    if not row: return []
    meta = {
        "openstates_person_id","openstates_id","person_id","os id",
        "name","full name","representative","representative name","member","rep",
        "district","district label","house district","state house district",
        "sldl","sldl name","sldl label","party","town","county"
    }
    meta_norm = set(_nrm(x) for x in meta)
    votes: List[dict] = []
    for k, v in (row or {}).items():
        if not k or _nrm(k) in meta_norm: continue
        if v is None or str(v).strip() == "": continue
        votes.append({"bill": _bill_key(k), "vote": _canon_vote_value(str(v))})
    return votes

def _collect_votes_for_rep(rows, *, person_id: str = "", name: str = "", district: str = "") -> Tuple[List[dict], Optional[dict]]:
    def has_long_keys(r: dict) -> bool:
        keys = { _nrm(k) for k in r.keys() if k is not None }
        return ("bill" in keys) and ("vote" in keys)  # exact, not substring
    is_long = any(has_long_keys(r) for r in rows) if rows else False

    if not is_long:
        row = _match_row_for_rep(rows, person_id=person_id, name=name, district=district)
        return _row_to_vote_list_wide(row), row

    # long format: aggregate multiple rows
    out: Dict[str, str] = {}
    first_row: Optional[dict] = None
    for r in rows:
        idcol = _pick_col(r, ["openstates_person_id","openstates_id","person_id","openstates id","os id"])
        namecol = _pick_col(r, ["name","full name","representative","representative name","member","rep"])
        distcol = _pick_col(r, ["district","district label","house district","state house district","sldl","sldl name","sldl label"])
        id_match  = bool(person_id and idcol and (r.get(idcol) or "").strip() == person_id)
        name_match= bool(name and namecol and _nrm(r.get(namecol,"")) == _nrm(name))
        dist_match= bool(district and distcol and _district_equiv(r.get(distcol,""), district))
        if id_match or (name_match and (not district or dist_match)):
            bcol = _pick_col(r, ["bill"]); vcol = _pick_col(r, ["vote"])
            if bcol and vcol:
                first_row = first_row or r
                out[_bill_key(r.get(bcol,""))] = _canon_vote_value(str(r.get(vcol,"")))
    votes = [{"bill": k, "vote": v} for k, v in sorted(out.items())]
    return votes, first_row

# ===================== District polygons =====================
DISTRICTS = DistrictIndex.from_geojson_path(HOUSE_GEOJSON_PATH)

# ===================== Hard route timeout (SIGALRM) =====================
class TimeoutException(Exception): ...
def run_with_alarm(seconds: int, fn):
    def handler(signum, frame): raise TimeoutException()
    old = signal.signal(signal.SIGALRM, handler)
    signal.alarm(seconds)
    try:
        return fn()
    finally:
        signal.alarm(0); signal.signal(signal.SIGALRM, old)

# ===================== Routes =====================
@app.route("/health")
def health():
    return jsonify({
        "ok": True,
        "env": {
            "has_openstates_key": bool(OPENSTATES_API_KEY),
            "nominatim_email": NOMINATIM_EMAIL,
            "os_min_delay_ms": OS_MIN_DELAY_MS,
            "os_ttl_seconds": OS_TTL_SECONDS,
            "house_geojson": HOUSE_GEOJSON_PATH,
            "allowed_origins": ALLOWED_ORIGINS or "*",
        },
        "csv": _csv_counts(),
        "votes": {"source": _votes_csv_url(), "cache_age": (time.time() - _VOTES_CACHE["t"]) if _VOTES_CACHE["t"] else None},
        "commit": RENDER_COMMIT,
    })

@app.route("/debug/trace")
def debug_trace():
    addr = (request.args.get("address") or "").strip()
    if not addr:
        return jsonify({"ok": False, "error": "Missing address"}), 400
    try:
        lat, lon, raw = geocode_address(addr, email=NOMINATIM_EMAIL)
    except GeocodeError as e:
        return jsonify({"ok": False, "error": f"geocode_failed: {e}"}), 502
    match = DISTRICTS.find(lat, lon)
    base_label = match[0] if match else None
    return jsonify({
        "ok": True, "inputAddress": addr, "lat": lat, "lon": lon, "geocode": raw,
        "base_district_label": base_label,
    })

@app.route("/debug/district")
def debug_district():
    label = (request.args.get("label") or "").strip()
    if not label:
        return jsonify({"ok": False, "error": "Missing label"}), 400
    try:
        payload = run_with_alarm(12, lambda: os_people_by_district(label))
    except TimeoutException:
        return jsonify({"ok": False, "error": "OpenStates timeout"}), 504
    if "error" in payload:
        return jsonify({"ok": False, **payload}), 429 if payload.get("status") == 429 else 502
    return jsonify({"ok": True, "district": label, "people": _extract_people(payload)})

@app.route("/debug/votes-preview")
def debug_votes_preview():
    rows, err = _fetch_votes_rows(force_refresh=True)
    if err:
        return {"error": err}, 400
    # show first row and detected schema kind
    def has_long(r): return bool(_pick_col(r, ["bill"])) and bool(_pick_col(r, ["vote"]))
    kind = "long" if rows and has_long(rows[0]) else "wide"
    return {"using": _VOTES_CACHE.get("src",""), "schema": kind, "row0": rows[0] if rows else None}, 200

@app.route("/debug/votes-audit")
def debug_votes_audit():
    name = (request.args.get("name") or "").strip()
    district = (request.args.get("district") or "").strip()
    pid = (request.args.get("person_id") or "").strip()
    rows, err = _fetch_votes_rows(force_refresh=True)
    if err:
        return {"error": err}, 400
    votes, row = _collect_votes_for_rep(rows, person_id=pid, name=name, district=district)
    return {
        "person_id": pid or None, "name": name or None, "district": district or None,
        "matched": bool(row) or bool(votes),
        "using": _VOTES_CACHE.get("src",""),
        "votes_count": len(votes),
        "row_keys": list(row.keys()) if row else [],
    }, 200

@app.route("/api/lookup-legislators")
def api_lookup_legislators():
    addr = request.args.get("address"); lat = request.args.get("lat"); lon = request.args.get("lon")
    latf = lonf = None; geocode_raw: Optional[Dict[str, Any]] = None
    if addr:
        try:
            latf, lonf, geocode_raw = geocode_address(addr, email=NOMINATIM_EMAIL)
        except GeocodeError as e:
            return jsonify({"success": False, "error": f"geocode_failed: {e}", "stateRepresentatives": []}), 502
    elif lat and lon:
        try: latf = float(lat); lonf = float(lon)
        except Exception: return jsonify({"success": False, "error": "invalid lat/lon"}), 400
    else:
        return jsonify({"success": False, "error": "provide address or lat/lon"}), 400

    match = DISTRICTS.find(latf, lonf)
    if not match:
        return jsonify({"success": True, "formattedAddress": geocode_raw.get("display_name") if geocode_raw else None,
                        "lat": latf, "lon": lonf, "stateRepresentatives": [],
                        "note": "No base district found in GeoJSON."})
    base_label, _props = match

    try:
        payload = run_with_alarm(12, lambda: os_people_by_district(base_label))
    except TimeoutException:
        return jsonify({"success": False, "error": "OpenStates timeout", "stateRepresentatives": []}), 504
    if "error" in payload:
        status = 429 if payload.get("status") == 429 else 502
        return jsonify({"success": False, "error": payload, "stateRepresentatives": []}), status

    reps = _extract_people(payload)
    return jsonify({"success": True, "formattedAddress": geocode_raw.get("display_name") if geocode_raw else None,
                    "lat": latf, "lon": lonf, "district": base_label, "stateRepresentatives": reps})

# ---------- votes endpoints ----------
@app.get("/api/key-votes")
def api_key_votes():
    person_id = (request.args.get("person_id") or "").strip()
    name = (request.args.get("name") or "").strip()
    district = (request.args.get("district") or "").strip()
    refresh = (request.args.get("refresh") or "").strip() in ("1", "true", "yes")

    rows, err = _fetch_votes_rows(force_refresh=refresh)
    if err:
        return jsonify({"success": False, "error": {"message": err}}), 400

    votes, a_row = _collect_votes_for_rep(rows, person_id=person_id, name=name, district=district)
    # Backfill rep fields from CSV if caller omitted them
    if a_row:
        if not name:
            ncol = _pick_col(a_row, ["name","full name","representative","representative name","member","rep"])
            name = (a_row.get(ncol) if ncol else name) or name
        if not district:
            dcol = _pick_col(a_row, ["district","district label","house district","state house district","sldl","sldl name","sldl label"])
            district = (a_row.get(dcol) if dcol else district) or district

    return jsonify({"success": True,
                    "data": {"matched": bool(a_row) or bool(votes),
                             "rep": {"person_id": person_id or None, "name": name or None, "district": district or None},
                             "votes": votes}})

@app.get("/api/lookup-with-votes")
def api_lookup_with_votes():
    addr = (request.args.get("address") or "").strip()
    refresh = (request.args.get("refreshVotes") or "").strip() in ("1","true","yes")
    if not addr:
        return jsonify({"success": False, "error": {"message": "Missing address"}}), 400

    # Base lookup
    base_resp = api_lookup_legislators()
    if isinstance(base_resp, tuple):
        resp, status = base_resp
        if status != 200: return base_resp
        data = resp.get_json()
    else:
        data = base_resp.get_json()
    reps = data.get("stateRepresentatives") or []

    rows, err = _fetch_votes_rows(force_refresh=refresh)
    votes_src = _VOTES_CACHE.get("src", "")
    if err:
        return jsonify({**data, "votesError": err, "votesSource": votes_src})

    out_reps = []
    for r in reps:
        pid = r.get("id") or ""
        nm  = r.get("name") or ""
        dist= r.get("district") or ""
        votes, _ = _collect_votes_for_rep(rows, person_id=pid, name=nm, district=dist)
        out_reps.append({**r, "votes": votes})

    data["stateRepresentatives"] = out_reps
    data["votesSource"] = votes_src
    return jsonify(data)

# ===================== main =====================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))


