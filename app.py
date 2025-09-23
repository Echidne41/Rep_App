"""
NH Rep Finder backend — resilient OpenStates-first lookup (probe ring) + CSV overlays.

Pipeline:
  1) Geocode: Nominatim (optional Census oneline fallback) -> lat/lon
  2) District labels: OpenStates people.geo (probe ring) -> base + floterials
  3) Reverse geocode (Nominatim) -> town/county (for town-overlay CSV)
  4) CSV overlays -> add floterials by BASE label and (TOWN, COUNTY)
  5) Names: OpenStates /people (by district label)
  6) Votes: CSV-backed endpoints

Debug:
  /health, /version, /debug/floterials, /debug/floterial-headers, /debug/trace, /debug/district
"""

import os, re, io, csv, time, logging
from urllib.parse import urlparse
from typing import Dict, List, Tuple, Optional

import requests
from flask import Flask, request, jsonify, abort, Response
from flask_cors import CORS

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)
CORS(app)

# =========================
# ENV
# =========================
OPENSTATES_API_KEY = (os.getenv("OPENSTATES_API_KEY", "") or "").strip()

# CSV sources: support both styles (baked file URLs OR repo-relative paths)
FLOTERIAL_BASE_CSV_URL = (os.getenv("FLOTERIAL_BASE_CSV_URL", "") or "").strip()
FLOTERIAL_TOWN_CSV_URL = (os.getenv("FLOTERIAL_TOWN_CSV_URL", "") or "").strip()
FLOTERIAL_BY_BASE_PATH = (os.getenv("FLOTERIAL_BY_BASE_PATH", "") or "").strip()
FLOTERIAL_MAP_PATH     = (os.getenv("FLOTERIAL_MAP_PATH", "") or "").strip()

# Votes CSV
VOTES_CSV_URL = (os.getenv("VOTES_CSV_URL", "") or "").strip()
VOTES_TTL_SECONDS = int(os.getenv("VOTES_TTL_SECONDS", "300"))

# Geocoding
NOMINATIM_EMAIL   = (os.getenv("NOMINATIM_EMAIL", "ops@example.org") or "").strip()
GEOCODE_FALLBACK  = (os.getenv("GEOCODE_FALLBACK", "") or "").lower()  # "" or "census"

# Probe ring for people.geo (fixes sparse returns at exact point)
PROBE_START_DEG = float(os.getenv("PROBE_START_DEG", "0.01"))
PROBE_STEP_DEG  = float(os.getenv("PROBE_STEP_DEG",  "0.01"))
PROBE_MAX_RINGS = int(os.getenv("PROBE_MAX_RINGS",  "2"))

# =========================
# Helpers
# =========================
def _read_text_from_url(url: str) -> str:
    if not url: return ""
    if url.lower().startswith("file://"):
        path = urlparse(url).path
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.text

def _file_url_from_rel(rel_path: str) -> str:
    if not rel_path: return ""
    base = os.path.dirname(__file__)
    p = os.path.join(base, rel_path)
    return f"file://{p}"

def _norm_label(s: str) -> str:
    s = str(s or "").strip()
    m = re.search(r"([A-Za-z]+)\s*0*([0-9]+)$", s)
    return f"{m.group(1).title()} {int(m.group(2))}" if m else s

def _norm_town(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).title()

def _unique_reps(reps: List[dict]) -> List[dict]:
    out = {}
    for r in reps:
        k = r.get("openstates_person_id") or r.get("id") or r.get("name")
        if k: out[k] = r
    return list(out.values())

def _err_json(stage: str, exc: Exception):
    return {"error": f"{stage} failed", "message": str(exc), "type": exc.__class__.__name__}

# =========================
# CSV Loaders (accept both schemas)
# by_town:  old -> town,county,floterial_district ; new -> town,district (list)
# by_base:  old -> base_district,floterial_district ; new -> base_label,floterials (list)
# =========================
_FLOTERIAL_TTL = 3600
_FLOTERIAL_CACHE = {"t": 0.0, "by_base": {}, "by_town": {}}

def _load_floterial_town(text: str) -> Dict[Tuple[str,str], set]:
    out: Dict[Tuple[str,str], set] = {}
    if text.startswith("\ufeff"): text = text.lstrip("\ufeff")
    rdr = csv.DictReader(io.StringIO(text))
    for r in rdr:
        town = _norm_town((r.get("town") or r.get("Town") or "").strip())
        county = (r.get("county") or r.get("County") or "").replace(" County","").strip().title()
        cell = (r.get("district") or r.get("District") or
                r.get("floterial_district") or r.get("Floterial_District") or "").strip()
        if not town or not cell: 
            continue
        for d in re.split(r"[;,]", cell):
            lab = _norm_label(re.sub(r"\s+", " ", d.strip()))
            if lab:
                out.setdefault((town, county), set()).add(lab)
    return out

def _load_floterial_base(text: str) -> Dict[str, set]:
    out: Dict[str, set] = {}
    if text.startswith("\ufeff"): text = text.lstrip("\ufeff")
    rdr = csv.DictReader(io.StringIO(text))
    for r in rdr:
        base = _norm_label((r.get("base_label") or r.get("base") or r.get("base_district") or "").strip().title())
        many = (r.get("floterials") or r.get("floterial") or "").strip()
        one  = (r.get("floterial_district") or "").strip()
        cell = many if many else one
        if not base or not cell: 
            continue
        labels = re.split(r"[;,]", cell) if many else [cell]
        for d in labels:
            lab = _norm_label(re.sub(r"\s+", " ", d.strip()))
            if lab:
                out.setdefault(base, set()).add(lab)
    return out

def _load_floterials_cached():
    now = time.time()
    if _FLOTERIAL_CACHE["t"] and now - _FLOTERIAL_CACHE["t"] < _FLOTERIAL_TTL:
        return _FLOTERIAL_CACHE["by_base"], _FLOTERIAL_CACHE["by_town"]

    base_url = FLOTERIAL_BASE_CSV_URL or (_file_url_from_rel(FLOTERIAL_BY_BASE_PATH) if FLOTERIAL_BY_BASE_PATH else "")
    town_url = FLOTERIAL_TOWN_CSV_URL or (_file_url_from_rel(FLOTERIAL_MAP_PATH)     if FLOTERIAL_MAP_PATH     else "")

    by_base, by_town = {}, {}
    if base_url:
        try: by_base = _load_floterial_base(_read_text_from_url(base_url))
        except Exception as e: logging.error("load base CSV failed: %s", e)
    if town_url:
        try: by_town = _load_floterial_town(_read_text_from_url(town_url))
        except Exception as e: logging.error("load town CSV failed: %s", e)

    _FLOTERIAL_CACHE.update({"t": now, "by_base": by_base, "by_town": by_town})
    return by_base, by_town

# =========================
# OpenStates
# =========================
def _os_headers():
    return {"X-API-KEY": OPENSTATES_API_KEY, "Accept": "application/json", "User-Agent": "nh-rep-finder/2"}

def _os_get(url: str, params: dict) -> dict:
    r = requests.get(url, params=params, headers=_os_headers(), timeout=20)
    if r.status_code in (429, 500, 502, 503, 504):
        time.sleep(0.6); r = requests.get(url, params=params, headers=_os_headers(), timeout=20)
    r.raise_for_status()
    return r.json() or {}

def _os_people_geo(lat: float, lon: float) -> list:
    url = "https://v3.openstates.org/people.geo"
    params = {"lat": lat, "lng": lon, "per_page": 50}
    try:
        r = requests.get(url, params=params, headers=_os_headers(), timeout=20)
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(0.6); r = requests.get(url, params=params, headers=_os_headers(), timeout=20)
        r.raise_for_status()
        j = r.json() or {}
        return j.get("results") or j.get("data") or []
    except Exception:
        return []

def _labels_probe_union(lat: float, lon: float) -> set:
    labels: set = set()
    # center first, then neighbors until we get something
    def neighbors():
        yield (lat, lon)
        for r in range(1, PROBE_MAX_RINGS + 1):
            d = PROBE_START_DEG + (r - 1) * PROBE_STEP_DEG
            for dy in (-d, 0, d):
                for dx in (-d, 0, d):
                    if dx == 0 and dy == 0: continue
                    yield (lat + dy, lon + dx)
    for y, x in neighbors():
        items = _os_people_geo(y, x)
        if not items: 
            continue
        for it in items:
            roles = (it.get("roles") or []) + (it.get("person", {}).get("roles") or [])
            for role in roles:
                role_type = (role.get("type") or role.get("role") or "").lower()
                chamber   = (role.get("chamber") or role.get("org_classification") or "").lower()
                if role_type in ("legislator","member") and chamber == "lower":
                    lbl = _norm_label(role.get("district") or role.get("label") or "")
                    if lbl: labels.add(lbl)
        if labels:
            break
    return labels

def _openstates_people_by_label(label: str) -> List[dict]:
    if not OPENSTATES_API_KEY or not label: return []
    url = "https://v3.openstates.org/people"
    params = {"jurisdiction": "New Hampshire", "org_classification": "lower",
              "district": _norm_label(label), "per_page": 50}
    data = _os_get(url, params)
    out = []
    for item in (data.get("results") or data.get("data") or []):
        p = item.get("person") or item
        party = p.get("party")
        if isinstance(party, list): party = (party[0] or {}).get("name")
        out.append({
            "openstates_person_id": p.get("id") or "",
            "id": p.get("id") or "",
            "name": p.get("name") or "",
            "party": party,
            "district": _norm_label(item.get("district") or p.get("district") or ""),
            "email": (p.get("email") if isinstance(p.get("email"), str) else None),
            "phone": None,
            "links": [{"url": l.get("url")} for l in (p.get("links") or []) if isinstance(l, dict) and l.get("url")],
        })
    return out

# =========================
# Geocoding (Nominatim + optional Census oneline fallback)
# =========================
NOM_SEARCH  = "https://nominatim.openstreetmap.org/search"
NOM_REVERSE = "https://nominatim.openstreetmap.org/reverse"
NOM_UA      = {"User-Agent": f"NH-Rep-Finder/1.0 ({NOMINATIM_EMAIL})"}

def _nom_search(addr: str) -> Tuple[Optional[float], Optional[float]]:
    params = {"q": addr, "format": "json", "limit": 1, "addressdetails": 1, "email": NOMINATIM_EMAIL}
    r = requests.get(NOM_SEARCH, params=params, headers=NOM_UA, timeout=20)
    r.raise_for_status()
    arr = r.json() or []
    if not arr: return None, None
    return float(arr[0]["lat"]), float(arr[0]["lon"])

def _nom_reverse(lat: float, lon: float) -> Tuple[str, str]:
    params = {"lat": lat, "lon": lon, "format": "json", "zoom": 10, "addressdetails": 1, "email": NOMINATIM_EMAIL}
    r = requests.get(NOM_REVERSE, params=params, headers=NOM_UA, timeout=20)
    r.raise_for_status()
    a = (r.json() or {}).get("address") or {}
    town = a.get("town") or a.get("village") or a.get("city") or a.get("hamlet") or a.get("municipality") or ""
    county = (a.get("county") or "").replace(" County","").strip().title()
    return _norm_town(town), county

def _census_oneline_geocode(addr: str) -> Tuple[Optional[float], Optional[float]]:
    url = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
    params = {"address": addr, "benchmark": "Public_AR_Current", "format": "json"}
    r = requests.get(url, params=params, timeout=20); r.raise_for_status()
    matches = (r.json().get("result") or {}).get("addressMatches") or []
    if not matches: return None, None
    c = matches[0]["coordinates"]; return float(c["y"]), float(c["x"])

# =========================
# District computation (with probe ring)
# =========================
def _compute_labels(lat: float, lon: float) -> Tuple[set, set, str, str]:
    by_base, by_town = _load_floterials_cached()

    # Robust labels from OpenStates point service (probe ring)
    bases = _labels_probe_union(lat, lon)

    # Town/County for town overlays
    town, county = "", ""
    try:
        town, county = _nom_reverse(lat, lon)
    except Exception:
        pass

    # CSV overlays: by base + by town/county
    flos = set()
    for b in bases:
        for f in by_base.get(_norm_label(b), set()):
            flos.add(_norm_label(f))
    if town and county:
        for f in by_town.get((town, county), set()):
            flos.add(_norm_label(f))

    return bases, flos, town, county

# =========================
# Votes support
# =========================
_vote_cache = {"t": 0.0, "rows": [], "columns": []}

def _read_text_maybe_file(url: str) -> str:
    if url.lower().startswith("file://"):
        path = urlparse(url).path
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    r = requests.get(url, timeout=30); r.raise_for_status(); return r.text

def _parse_csv_text(text: str):
    rdr = csv.reader(io.StringIO(text))
    rows = list(rdr)
    if not rows: return [], []
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

def _load_votes_rows_cached():
    now = time.time()
    if _vote_cache["t"] and now - _vote_cache["t"] < VOTES_TTL_SECONDS:
        return _vote_cache["rows"], _vote_cache["columns"]
    if not VOTES_CSV_URL:
        local = os.path.join(os.path.dirname(__file__), "house_key_votes.csv").replace("\\", "/")
        if os.path.exists(local):
            text = _read_text_maybe_file(f"file://{local}")
        else:
            _vote_cache.update({"t": now, "rows": [], "columns": []})
            return [], []
    else:
        text = _read_text_maybe_file(VOTES_CSV_URL)
    headers, rows = _parse_csv_text(text)
    cols = [h for h in headers if h.lower() not in ("openstates_person_id","person_id","id","name","district","party")]
    _vote_cache.update({"t": now, "rows": rows, "columns": cols})
    return rows, cols

def _match_row_for_rep(rows, *, person_id: str, name: str, district: str) -> Optional[dict]:
    # by id
    id_keys = {"openstates_person_id","openstates_id","person_id","id","os id"}
    for r in rows:
        for k,v in (r or {}).items():
            if (k or "").strip().lower() in id_keys and v and str(v).strip() == (person_id or ""):
                return r
    # by name + district (fuzzy)
    def _norm(s): return re.sub(r"\s+"," ",re.sub(r"[^A-Za-z0-9\s]","",str(s or ""))).strip().lower()
    n = _norm(name); d = _norm(district)
    hits = [r for r in rows if _norm(r.get("name")) == n or n in _norm(r.get("name"))]
    if d and hits:
        for r in hits:
            rd = _norm(r.get("district"))
            if rd and (rd == d or re.findall(r"\d+", rd)[:1] == re.findall(r"\d+", d)[:1]):
                return r
    return hits[0] if hits else None

def _row_to_votes(row: dict) -> List[dict]:
    if not row: return []
    meta = {"openstates_person_id","person_id","id","name","district","party","town","county"}
    out = []
    for k,v in (row or {}).items():
        if k and k.strip().lower() not in meta and str(v or "").strip():
            out.append({"bill": k.strip(), "vote": str(v).strip()})
    return out

# =========================
# Routes
# =========================
@app.route("/api/lookup-legislators", methods=["GET","POST"])
def api_lookup_legislators():
    try:
        # address OR lat/lon
        if request.method == "POST":
            j = request.get_json(silent=True) or {}
            addr = (j.get("address") or j.get("addr") or "").strip()
        else:
            addr = (request.args.get("address") or request.args.get("addr") or "").strip()
        qlat = request.args.get("lat"); qlon = request.args.get("lon")

        if qlat and qlon:
            lat, lon = float(qlat), float(qlon)
        else:
            if not addr:
                return jsonify({"success": False, "error": "address is required"}), 422
            # geocode
            lat = lon = None
            try: lat, lon = _nom_search(addr)
            except Exception: pass
            if (lat is None or lon is None) and GEOCODE_FALLBACK == "census":
                try: lat, lon = _census_oneline_geocode(addr)
                except Exception: pass
            if lat is None or lon is None:
                return jsonify({"success": False, "error": "geocoding failed"}), 400

        # labels
        bases, flos, town, county = _compute_labels(lat, lon)

        # ensure BASE roster is included even if people.geo omitted it
        reps_all = []
        for base_lbl in sorted(list(bases)):
            reps_all.extend(_openstates_people_by_label(base_lbl))
        for lbl in sorted(list(flos)):
            reps_all.extend(_openstates_people_by_label(lbl))
        reps_all = _unique_reps(reps_all)

        # Response (flat data object for frontend)
        return jsonify({
            "success": True,
            "data": {
                "address": addr or None,
                "geographies": {"town_county": [town, county], "sldl": (sorted(list(bases))[0] if bases else None)},
                "stateRepresentatives": reps_all
            }
        })
    except Exception as e:
        logging.exception("lookup-legislators unhandled")
        return jsonify(_err_json("lookup-legislators", e)), 500

@app.get("/api/key-votes")
def api_key_votes():
    person_id = (request.args.get("person_id") or "").strip()
    name      = (request.args.get("name") or "").strip()
    district  = (request.args.get("district") or "").strip()
    refresh   = (request.args.get("refresh") or "").strip() in ("1","true","yes")
    try:
        if refresh: _vote_cache.update({"t": 0.0})
        rows, _ = _load_votes_rows_cached()
        row = _match_row_for_rep(rows, person_id=person_id, name=name, district=district)
        votes = _row_to_votes(row)
        return jsonify({"success": True, "data": {"matched": bool(row), "votes": votes}})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.get("/api/vote-map")
def api_vote_map():
    try:
        rows, cols = _load_votes_rows_cached()
        out = {}
        def _norm(s): return re.sub(r"[^a-z0-9]+","",str(s or "").lower())
        for r in rows:
            pid = (r.get("openstates_person_id") or r.get("person_id") or r.get("id") or "").strip()
            name = r.get("name") or ""
            dist = r.get("district") or ""
            key1 = pid or ""
            key2 = f"name:{_norm(name)}"
            key3 = f"nd:{_norm(name)}|{_norm(dist)}"
            rowmap = {}
            for k,v in r.items():
                if k in ("openstates_person_id","person_id","id","name","district","party"): continue
                if str(v or "").strip(): rowmap[k] = str(v).strip()
            if rowmap:
                if key1: out[key1] = rowmap
                out[key2] = rowmap
                out[key3] = rowmap
        return jsonify({"columns": cols, "votes": out, "rows": len(rows), "source": VOTES_CSV_URL})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/house_key_votes.csv")
def house_key_votes():
    local = os.path.join(os.path.dirname(__file__), "house_key_votes.csv")
    if os.path.exists(local):
        with open(local, "r", encoding="utf-8") as f:
            return Response(f.read(), mimetype="text/csv")
    if VOTES_CSV_URL:
        try:
            return Response(_read_text_maybe_file(VOTES_CSV_URL), mimetype="text/csv")
        except Exception:
            pass
    abort(404)

# =========================
# Debug / Health
# =========================
@app.get("/debug/floterials")
def debug_floterials():
    by_base, by_town = _load_floterials_cached()
    return jsonify({
        "base_to_floterial_count": sum(len(v) for v in by_base.values()),
        "town_to_floterial_count": sum(len(v) for v in by_town.values()),
        "sample_by_base": {k: sorted(list(v)) for k,v in list(by_base.items())[:5]},
        "sample_by_town": [
            {"town": t, "county": c, "floterials": sorted(list(v))}
            for (t,c), v in list(by_town.items())[:5]
        ]
    })

def _peek_csv_headers_and_rows(url: str, n: int = 3):
    try:
        t = _read_text_from_url(url)
        raw_first = t.splitlines()[0] if t else ""
        rdr = csv.DictReader(io.StringIO(t))
        rows = []
        for i, r in enumerate(rdr):
            if i >= n: break
            rows.append(r)
        return {"url": url, "fieldnames": rdr.fieldnames, "raw_first_line": raw_first, "sample_rows": rows}
    except Exception as e:
        return {"url": url, "error": f"{type(e).__name__}: {e}"}

@app.get("/debug/floterial-headers")
def debug_floterial_headers():
    base_url = FLOTERIAL_BASE_CSV_URL or (_file_url_from_rel(FLOTERIAL_BY_BASE_PATH) if FLOTERIAL_BY_BASE_PATH else "")
    town_url = FLOTERIAL_TOWN_CSV_URL or (_file_url_from_rel(FLOTERIAL_MAP_PATH)     if FLOTERIAL_MAP_PATH     else "")
    return jsonify({
        "base": _peek_csv_headers_and_rows(base_url) if base_url else {"error": "unset"},
        "town": _peek_csv_headers_and_rows(town_url) if town_url else {"error": "unset"},
    })

@app.get("/debug/trace")
def debug_trace():
    addr = (request.args.get("address") or "").strip()
    if not addr:
        return jsonify({"error": "address required"}), 400
    lat = lon = None
    try: lat, lon = _nom_search(addr)
    except Exception: pass
    if (lat is None or lon is None) and GEOCODE_FALLBACK == "census":
        try: lat, lon = _census_oneline_geocode(addr)
        except Exception: pass
    if lat is None or lon is None:
        return jsonify({"error": "geocode failed"}), 400
    bases, flos, town, county = _compute_labels(lat, lon)
    by_base, by_town = _load_floterials_cached()
    base_over = sorted(list({f for b in bases for f in by_base.get(_norm_label(b), set())}))
    town_over = sorted(list(by_town.get((_norm_town(town), county), set())))
    return jsonify({
        "input": addr,
        "latlon": [lat, lon],
        "sldl_clean": sorted(list(bases))[0] if bases else None,
        "town": town, "county": county,
        "base_overlays": base_over,
        "town_overlays": town_over
    })

@app.get("/debug/district")
def debug_district():
    label = _norm_label(request.args.get("label",""))
    reps = _openstates_people_by_label(label) if label else []
    return jsonify({"label": label, "count": len(reps), "names": [r.get("name") for r in reps]})

BUILD_SHA = os.getenv("RENDER_GIT_COMMIT","local")
@app.get("/version")
def version():
    return {"commit": BUILD_SHA}

@app.get("/health")
def health():
    by_base, by_town = _load_floterials_cached()
    return jsonify({
        "ok": True,
        "has_openstates_key": bool(OPENSTATES_API_KEY),
        "floterial_base_csv_set": bool(FLOTERIAL_BASE_CSV_URL or FLOTERIAL_BY_BASE_PATH),
        "floterial_town_csv_set": bool(FLOTERIAL_TOWN_CSV_URL or FLOTERIAL_MAP_PATH),
        "base_to_floterial_count": sum(len(v) for v in by_base.values()),
        "town_to_floterial_count": sum(len(v) for v in by_town.values()),
        "commit": BUILD_SHA
    })

# =========================
# Main (local dev only)
# =========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","5000")), debug=True)
