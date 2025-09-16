import os
import re
import sys
import csv, io, time, urllib.parse
from typing import Dict, Any, Optional, Tuple, List

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import requests

# =========================
# ENV & CONSTANTS
# =========================
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

OPENSTATES_API_KEY = os.getenv("OPENSTATES_API_KEY", "")

OS_ROOT = "https://v3.openstates.org"
OS_PEOPLE = f"{OS_ROOT}/people"
OS_PEOPLE_GEO = f"{OS_ROOT}/people.geo"
CENSUS_GEOCODER_URL = "https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress"

# Probing controls
PROBE_START_DEG = float(os.getenv("PROBE_START_DEG", "0.01"))
PROBE_STEP_DEG  = float(os.getenv("PROBE_STEP_DEG", "0.01"))
PROBE_MAX_RINGS = int(os.getenv("PROBE_MAX_RINGS", "2"))

# Votes sheet config
VOTES_GSHEET_DOC_ID = os.getenv("VOTES_GSHEET_DOC_ID", "")
VOTES_GSHEET_GID   = os.getenv("VOTES_GSHEET_GID", "")
VOTES_SHEET_NAME   = os.getenv("VOTES_SHEET_NAME", "House Key Votes")
# sanitize env (strip whitespace & quotes)
VOTES_CSV_URL      = (os.getenv("VOTES_CSV_URL", "") or "").strip().strip("'\"")
VOTES_TTL          = int(os.getenv("VOTES_TTL_SECONDS", "900"))  # seconds

# Floterial CSV mapping
FLOTERIAL_MAP_PATH = os.getenv("FLOTERIAL_MAP_PATH", "floterial_by_town.csv")          # Town -> [District,...]
FLOTERIAL_BY_BASE_PATH = os.getenv("FLOTERIAL_BY_BASE_PATH", "floterial_by_base.csv")  # BaseLabel -> [District,...]

# Geocoder fallback
NOMINATIM_FALLBACK = (os.getenv("NOMINATIM_FALLBACK", "1") or "1").strip().lower() in ("1","true","yes")
NOMINATIM_EMAIL    = os.getenv("NOMINATIM_EMAIL","")  # TODO: your email (optional)

# Caches
PROBE_CACHE: Dict[str, Any] = {}
VOTES_CACHE: Dict[str, Any] = {"at": 0, "rows": [], "src": ""}

# Mappings
FLOTERIAL_MAP_TOWN: Dict[str, List[str]] = {}
FLOTERIAL_MAP_BASE: Dict[str, List[str]] = {}

# Built-in NH floterial assist (seed)
NH_FLOTERIAL_BY_TOWN_BUILTIN = {
    "Cornish": "Sullivan 7",
    "Plainfield": "Sullivan 7",
    "Charlestown": "Sullivan 7",
    "Newport": "Sullivan 7",
    "Unity": "Sullivan 7",
}

# =========================
# APP & CORS
# =========================
app = Flask(__name__, static_url_path="", static_folder=".")

ALLOWED = (os.getenv("ALLOWED_ORIGINS", "*") or "").strip()
if not ALLOWED or ALLOWED == "*":
    CORS(app, resources={r"/*": {"origins": "*"}})  # staging-safe default
else:
    CORS(app, resources={r"/*": {"origins": [o.strip() for o in ALLOWED.split(",") if o.strip()]}})

# =========================
# HELPERS: GEOCODING / LABELS
# =========================
def sldl_to_openstates(name: str) -> str:
    if not name:
        return ""
    m = re.search(r"([A-Za-z]+)\s+0*(\d+)$", name.strip())
    return f"{m.group(1)} {m.group(2)}" if m else name.strip()

def county_label_from_sldl(cleaned: str) -> str:
    parts = (cleaned or "").strip().split()
    return " ".join(parts[:-1]) if len(parts) >= 2 else ""

def parse_town_from_matched(addr: str) -> Optional[str]:
    if not addr:
        return None
    parts = [p.strip() for p in addr.split(",")]
    return parts[1].title() if len(parts) >= 2 else None

def geocode_nominatim(address: str) -> Optional[Dict[str, Any]]:
    try:
        ua = f"nh-rep-finder/1 ({NOMINATIM_EMAIL})" if NOMINATIM_EMAIL else "nh-rep-finder/1"
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"format":"jsonv2","q":address,"addressdetails":1,"limit":1},
            headers={"User-Agent": ua}, timeout=15
        )
        r.raise_for_status()
        arr = r.json() or []
        if not arr: return None
        rec = arr[0]
        a = rec.get("address") or {}
        town = a.get("town") or a.get("village") or a.get("city") or a.get("hamlet") or a.get("municipality")
        return {
            "formattedAddress": rec.get("display_name"),
            "lat": float(rec.get("lat")),
            "lon": float(rec.get("lon")),
            "town": (town or "").title(),
            "sldl": {"name": None, "geoid": None},
        }
    except Exception:
        return None

def geocode_address(address: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    params = {"address": address, "benchmark": "Public_AR_Current", "vintage": "Current_Current", "format": "json"}
    try:
        r = requests.get(CENSUS_GEOCODER_URL, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        matches = data.get("result", {}).get("addressMatches", [])
        if matches:
            m = matches[0]
            coords = m.get("coordinates", {})
            geos = m.get("geographies", {})
            sldl_name = sldl_geoid = None
            for key, arr in geos.items():
                if isinstance(arr, list) and "State Legislative Districts - Lower" in key and arr:
                    rec = arr[0]; sldl_name = rec.get("NAME"); sldl_geoid = rec.get("GEOID")
            out = {
                "formattedAddress": m.get("matchedAddress"),
                "lat": coords.get("y"),
                "lon": coords.get("x"),
                "town": parse_town_from_matched(m.get("matchedAddress") or ""),
                "sldl": {"name": sldl_name, "geoid": sldl_geoid},
            }
            return out, None
        if NOMINATIM_FALLBACK:
            alt = geocode_nominatim(address)
            if alt: return alt, None
        return None, "Address not found by geocoders."
    except requests.RequestException as e:
        if NOMINATIM_FALLBACK:
            alt = geocode_nominatim(address)
            if alt: return alt, None
        return None, f"Census Geocoder error: {e}"

# =========================
# OPENSTATES v3
# =========================
def _headers() -> Dict[str, str]:
    return {"X-API-KEY": OPENSTATES_API_KEY, "Accept": "application/json", "User-Agent": "nh-legislator-lookup/2.2"}

def _get(url: str, params: dict) -> Tuple[Optional[dict], Optional[str]]:
    try:
        r = requests.get(url, headers=_headers(), params=params, timeout=20)
        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            return None, f"OpenStates error: rate limited. Retry after {ra or 'a bit'}."
        r.raise_for_status()
        return r.json() or {}, None
    except requests.HTTPError:
        try:
            detail = r.json().get("detail")
        except Exception:
            detail = (r.text or "")[:300]
        return None, f"OpenStates error: {detail or f'HTTP {r.status_code}'}"
    except requests.RequestException as e:
        return None, f"OpenStates request failed: {e}"

def fetch_people_geo(lat: float, lon: float) -> Tuple[List[dict], Optional[str]]:
    data, err = _get(OS_PEOPLE_GEO, {"lat": float(lat), "lng": float(lon)})
    if err:
        return [], err
    return (data or {}).get("results") or [], None

def fetch_people_by_district(district: str) -> Tuple[List[dict], Optional[str]]:
    data, err = _get(OS_PEOPLE, {
        "jurisdiction": "New Hampshire",
        "org_classification": "lower",
        "district": district,
        "per_page": 50,
        "page": 1
    })
    if err:
        return [], err
    return (data or {}).get("results") or [], None

# Retry helper for floterials (handles intermittent failures)
def _fetch_district_retry(label: str, retries: int = 3, delay: float = 4.0) -> List[dict]:
    for i in range(retries + 1):
        data, err = fetch_people_by_district(label)
        if not err and data:
            return data
        if i == retries:
            print(f"[overlay] {label}: failed after {retries} retries — {err or 'empty'}")
            return []
        time.sleep(delay + 2 * i)
    return []

def _ingest(collected: Dict[str, dict], results: List[dict]) -> None:
    for rec in results or []:
        p = rec.get("person") or rec
        pid = (
            p.get("id")
            or p.get("openstates_id")
            or p.get("open_states_id")
            or ((p.get("name") or "") + "|" + (rec.get("district") or p.get("district") or ""))
        )
        jur = p.get("jurisdiction") or rec.get("jurisdiction")
        jur_name = jur.get("name") if isinstance(jur, dict) else (jur if isinstance(jur, str) else None)
        cr = p.get("current_role") or {}
        org = (cr.get("org_classification") or "").lower()
        district = (rec.get("district") or p.get("district") or cr.get("district") or "").strip()

        party_val = p.get("party")
        if isinstance(party_val, list):
            party_val = (party_val[0] or {}).get("name")

        emails: List[str] = []
        if isinstance(p.get("email_addresses"), list):
            emails.extend([e.get("address") for e in p["email_addresses"] if isinstance(e, dict) and e.get("address")])
        if isinstance(p.get("emails"), list):
            emails.extend([e if isinstance(e, str) else e.get("address") for e in p["emails"]])
        if isinstance(p.get("email"), str):
            emails.append(p["email"])
        emails = [e for e in emails if e]

        links = [{"url": l["url"]} for l in (p.get("links") or []) if isinstance(l, dict) and l.get("url")]

        if pid not in collected:
            collected[pid] = {
                "id": pid,
                "name": p.get("name"),
                "party": party_val,
                "emails": emails,
                "offices": p.get("offices") or [],
                "district": district,
                "currentRole": {"orgClassification": org, "district": district},
                "jurisdiction": jur_name,
                "links": links,
            }

def _neighbors(lat: float, lon: float, start_deg: float, step_deg: float, rings: int):
    yield (lat, lon)
    for r in range(1, rings + 1):
        d = start_deg + (r - 1) * step_deg
        for dy in (-d, 0, d):
            for dx in (-d, 0, d):
                if dy == 0 and dx == 0:
                    continue
                yield (lat + dy, lon + dx)

def union_people_geo_statewide(lat: float, lon: float, min_nh_lower: int = 2) -> Dict[str, dict]:
    collected: Dict[str, dict] = {}

    def probe(y: float, x: float):
        key = f"{round(y,6)},{round(x,6)}"
        if key in PROBE_CACHE:
            _ingest(collected, PROBE_CACHE[key]); return
        res, err = fetch_people_geo(y, x)
        if not err:
            PROBE_CACHE[key] = res
            _ingest(collected, res)

    for (y, x) in _neighbors(lat, lon, PROBE_START_DEG, PROBE_STEP_DEG, PROBE_MAX_RINGS):
        probe(y, x)
        n = sum(1 for v in collected.values()
                if v.get("jurisdiction") == "New Hampshire"
                and (v.get("currentRole") or {}).get("orgClassification") == "lower")
        if n >= min_nh_lower:
            break
    return collected

# allow big Google Sheets cells
try:
    import csv as _csv
    _csv.field_size_limit(min(sys.maxsize, 10_000_000))
except Exception:
    pass

def normalize_person(person: Dict[str, Any]) -> Dict[str, Any]:
    district = person.get("district") or (person.get("currentRole") or {}).get("district") or "Unknown"
    emails = person.get("emails") or []
    email = emails[0] if emails and isinstance(emails[0], str) else (emails[0].get("address") if emails else None)
    phone = None
    for off in (person.get("offices") or []):
        if isinstance(off, dict) and off.get("voice"):
            phone = off["voice"]; break
    party = person.get("party") or "Unknown"
    if isinstance(party, list):
        party = (party[0] or {}).get("name") or "Unknown"
    party = str(party).replace("Democratic", "Democrat")
    return {
        "id": person.get("id") or person.get("openstates_id") or person.get("open_states_id"),
        "name": person.get("name"),
        "party": party,
        "district": district,
        "email": email,
        "phone": phone,
        "links": [l.get("url") for l in (person.get("links") or []) if isinstance(l, dict) and l.get("url")],
    }

# =========================
# VOTES: GOOGLE SHEETS / CSV
# =========================
def _votes_csv_url() -> Optional[str]:
    # prefer env, else fallback to local file next to app.py
    if VOTES_CSV_URL:
        return VOTES_CSV_URL
    local = os.path.join(os.path.dirname(__file__), "house_key_votes.csv").replace("\\", "/")
    return f"file://{local}"

def _alt_csv_urls(primary: str) -> List[str]:
    alts: List[str] = []
    m = re.match(r"(https://docs\.google\.com/spreadsheets/d/e/[^/]+)/pub\?(.+)$", primary)
    if m:
        base, q = m.group(1), m.group(2)
        params = dict(urllib.parse.parse_qsl(q))
        gid = params.get("gid") or (VOTES_GSHEET_GID or "")
        if gid:
            alts.append(f"{base}/gviz/tq?tqx=out:csv&gid={gid}")
        if VOTES_SHEET_NAME:
            alts.append(f"{base}/gviz/tq?tqx=out:csv&sheet={urllib.parse.quote(VOTES_SHEET_NAME)}")
    if VOTES_GSHEET_DOC_ID and VOTES_GSHEET_GID:
        alts.append(f"https://docs.google.com/spreadsheets/d/{VOTES_GSHEET_DOC_ID}/export?format=csv&gid={VOTES_GSHEET_GID}")
    return [u for u in alts if u and u != primary]

def _http_get_text(url: str) -> Tuple[Optional[str], Optional[str]]:
    # Local file support (accept file:// or file:///), keep leading '/' on Linux
    if url.lower().startswith("file://"):
        try:
            p = url[7:]  # drop 'file://', keep rest
            p = p.strip().strip("'\"")
            if os.name != "nt" and not p.startswith("/"):
                p = "/" + p
            if os.name == "nt":
                if p.startswith("/"):
                    p = p[1:]
                p = p.replace("/", "\\")
            with open(p, "r", encoding="utf-8") as f:
                return f.read(), None
        except Exception as e:
            return None, f"file error: {e}"

    try:
        headers = {
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/124.0 Safari/537.36"),
            "Accept": "text/csv, text/plain, */*",
            "Referer": "https://docs.google.com/",
        }
        with requests.Session() as s:
            s.headers.update(headers)
            r = s.get(url, timeout=30, allow_redirects=True)
            r.raise_for_status()
            return r.text, None
    except requests.HTTPError as e:
        return None, f"HTTP {e.response.status_code}: {e.response.url}"
    except Exception as e:
        return None, str(e)

def _fetch_votes_rows(force_refresh: bool = False):
    now = time.time()
    if not force_refresh and VOTES_CACHE["rows"] and now - VOTES_CACHE["at"] < VOTES_TTL:
        return VOTES_CACHE["rows"], None

    primary = _votes_csv_url()
    if not primary:
        return [], "Votes source not configured."

    tried = [primary] + _alt_csv_urls(primary)
    last_err = None
    for url in tried:
        text, err = _http_get_text(url)
        if err:
            last_err = f"Votes CSV fetch error: {err}"; continue
        t = (text or "").lstrip()
        if t.lower().startswith("<!doctype html") or "<html" in t[:1000].lower():
            last_err = "Votes CSV URL returned HTML (fix sharing or use published CSV)."; continue
        try:
            csv.field_size_limit(min(sys.maxsize, 10_000_000))
            buf = io.StringIO(text)
            reader = csv.DictReader(buf)
            rows = [{(k or "").strip(): (v.strip() if isinstance(v, str) else v) for k, v in (row or {}).items()} for row in reader]
            VOTES_CACHE.update({"at": now, "rows": rows, "src": url})
            return rows, None
        except Exception as e:
            last_err = f"Votes CSV parse error: {e}"
    return [], (last_err or "Failed to fetch votes CSV")

def _norm(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"[^A-Za-z0-9\s]", "", s)).strip().lower()

def _pick_col(row: dict, candidates: List[str]) -> Optional[str]:
    norm_map = { _norm(k): k for k in row.keys() if k is not None }
    for want in candidates:
        if want in norm_map:
            return norm_map[want]
    for nk, original in norm_map.items():
        for want in candidates:
            if want in nk:
                return original
    return None

def _district_equiv(a: str, b: str) -> bool:
    an = _norm(a); bn = _norm(b)
    if an == bn:
        return True
    ad = re.findall(r"\d+", an); bd = re.findall(r"\d+", bn)
    if not ad or not bd or ad[0] != bd[0]:
        return False
    aletters = re.sub(r"[^a-z]", "", an); bletters = re.sub(r"[^a-z]", "", bn)
    if not aletters or not bletters:
        return True
    return aletters[:3] == bletters[:3]

def _match_row_for_rep(rows, *, person_id: str = "", name: str = "", district: str = "") -> Optional[dict]:
    name_n = _norm(name); district_n = _norm(district)
    id_candidates = ["openstates_person_id", "openstates_id", "person_id", "openstates id", "os id"]
    for r in rows:
        for pid_key in id_candidates:
            col = _pick_col(r, [pid_key])
            if not col:
                continue
            pid = (r.get(col) or "").strip()
            if pid and person_id and pid == person_id:
                return r
    name_candidates = ["name", "full name", "representative", "representative name", "member", "rep"]
    dist_candidates = ["district", "district label", "house district", "state house district", "sldl", "sldl name", "sldl label"]
    name_hits: List[dict] = []
    for r in rows:
        ncol = _pick_col(r, name_candidates)
        if not ncol:
            continue
        rname_n = _norm(r.get(ncol, ""))
        if rname_n == name_n or (name_n and name_n in rname_n):
            name_hits.append(r)
    if not name_hits:
        return None
    if district_n:
        for r in name_hits:
            dcol = _pick_col(r, dist_candidates)
            if dcol and _district_equiv(r.get(dcol, ""), district):
                return r
    return name_hits[0] if len(name_hits) == 1 else None

def _row_to_vote_list(row: dict) -> List[dict]:
    if not row:
        return []
    meta = {
        "openstates_person_id","openstates_id","person_id","os id",
        "name","full name","representative","representative name","member","rep",
        "district","district label","house district","state house district",
        "sldl","sldl name","sldl label","party","town","county"
    }
    meta_norm = set(_norm(x) for x in meta)
    votes: List[dict] = []
    for k, v in (row or {}).items():
        if not k or _norm(k) in meta_norm:
            continue
        if v is None or str(v).strip() == "":
            continue
        votes.append({"bill": k.strip(), "vote": str(v).strip()})
    return votes

# =========================
# FLOTERIAL MAP LOADERS (Option B)
# =========================
def _load_town_map(path: str) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for t, d in NH_FLOTERIAL_BY_TOWN_BUILTIN.items():
        out.setdefault(t, []).append(d)
    if not path or not os.path.exists(path):
        return out
    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                town = (row.get("town") or row.get("Town") or "").strip().title()
                cell = (row.get("district") or row.get("District") or "").strip()
                if not town or not cell:
                    continue
                for d in [p.strip() for p in cell.split(";") if p.strip()]:
                    out.setdefault(town, []).append(d)
    except Exception:
        pass
    return out

def _load_base_map(path: str) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    if not path or not os.path.exists(path):
        return out
    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                base = (row.get("base_label") or row.get("base") or "").strip().title()
                cell = (row.get("floterials") or row.get("floterial") or "").strip()
                if not base or not cell:
                    continue
                base = re.sub(r"\s+", " ", base)
                values = [re.sub(r"\s+", " ", p.strip()) for p in cell.split(";") if p.strip()]
                if values:
                    out.setdefault(base, [])
                    for d in values:
                        if d not in out[base]:
                            out[base].append(d)
    except Exception:
        pass
    return out

FLOTERIAL_MAP_TOWN = _load_town_map(FLOTERIAL_MAP_PATH)
FLOTERIAL_MAP_BASE = _load_base_map(FLOTERIAL_BY_BASE_PATH)

# =========================
# CORE LOOKUP
# =========================
def _apply_county_hint(raw_people: Dict[str, dict], sldl_clean: str) -> None:
    county_hint = county_label_from_sldl(sldl_clean)
    if not county_hint:
        return
    for v in raw_people.values():
        lab = (v.get("district") or "").strip()
        if lab and re.fullmatch(r"\d+", lab):
            v["district"] = f"{county_hint} {lab}"

def _lookup_core(address: str, *, include_votes: bool = False, refresh_votes: bool = False):
    if not OPENSTATES_API_KEY:
        return jsonify({"success": False, "error": {"message": "Missing OPENSTATES_API_KEY"}}), 400

    geo, err = geocode_address(address)
    if err:
        return jsonify({"success": False, "error": {"message": err}}), 400

    lat, lon = geo.get("lat"), geo.get("lon")
    if lat is None or lon is None:
        return jsonify({"success": False, "error": {"message": "Geocoder did not return coordinates"}}), 400

    # probe for people
    raw_people = union_people_geo_statewide(lat, lon)

    # NH lower only
    raw_people = {
        k: v for k, v in raw_people.items()
        if v.get("jurisdiction") == "New Hampshire"
        and (v.get("currentRole") or {}).get("orgClassification") == "lower"
    }

    # county-number normalize
    sldl_clean = sldl_to_openstates((geo.get("sldl") or {}).get("name") or "")
    _apply_county_hint(raw_people, sldl_clean)

    # ---- PRIMARY: base-label floterials union (Option B) ----
    base_label = sldl_clean.strip()
    if base_label and base_label in FLOTERIAL_MAP_BASE:
        for flab in FLOTERIAL_MAP_BASE[base_label]:
            _ingest(raw_people, _fetch_district_retry(flab))

    # ---- FALLBACK: town→floterial (if still only 1) ----
    uniq_labels = {(v.get("district") or "").strip() for v in raw_people.values() if v.get("district")}
    town = geo.get("town")
    if town in FLOTERIAL_MAP_TOWN and len(uniq_labels) < 2:
        for flab in FLOTERIAL_MAP_TOWN[town]:
            _ingest(raw_people, _fetch_district_retry(flab))

    # re-filter to NH lower
    raw_people = {
        k: v for k, v in raw_people.items()
        if v.get("jurisdiction") == "New Hampshire"
        and (v.get("currentRole") or {}).get("orgClassification") == "lower"
    }

    reps = [normalize_person(p) for p in raw_people.values()]

    # fallback district label on reps
    for r in reps:
        if not r.get("district") or r["district"] == "Unknown":
            r["district"] = sldl_clean

    # attach votes if requested
    if include_votes:
        rows, verr = _fetch_votes_rows(force_refresh=refresh_votes)
        for r in reps:
            row = None
            if not verr:
                row = _match_row_for_rep(rows, person_id=r.get("id") or "", name=r.get("name") or "", district=r.get("district") or "")
            r["votes"] = _row_to_vote_list(row) if row else []
        return jsonify({
            "success": True,
            "data": {
                "formattedAddress": geo.get("formattedAddress"),
                "inputAddress": address,
                "stateRepresentatives": reps,
                "geographies": {"sldl": geo.get("sldl")},
                "votesSource": VOTES_CACHE.get("src", ""),
                "votesError": verr,
            },
        })

    return jsonify({
        "success": True,
        "data": {
            "formattedAddress": geo.get("formattedAddress"),
            "inputAddress": address,
            "stateRepresentatives": reps,
            "geographies": {"sldl": geo.get("sldl")},
        },
    })

# =========================
# ROUTES
# =========================
@app.get("/debug/nh-house-ids.csv")
def nh_house_ids_csv():
    counties = ["Belknap","Carroll","Cheshire","Coos","Grafton","Hillsborough",
                "Merrimack","Rockingham","Strafford","Sullivan"]
    seen = set()
    rows = []

    def grab(label: str):
        data, err = _get(OS_PEOPLE, {
            "jurisdiction": "New Hampshire",
            "org_classification": "lower",
            "district": label,
            "per_page": 50,
            "page": 1
        })
        if err and "rate limited" in err.lower():
            time.sleep(8)
            data, err = _get(OS_PEOPLE, {
                "jurisdiction": "New Hampshire",
                "org_classification": "lower",
                "district": label,
                "per_page": 50,
                "page": 1
            })
        if err:
            return
        for rec in (data or {}).get("results") or []:
            p = rec.get("person") or {}
            pid = p.get("id")
            if not pid or pid in seen:
                continue
            party = p.get("party")
            if isinstance(party, list):
                party = (party[0] or {}).get("name")
            district = rec.get("district") or (p.get("current_role") or {}).get("district") or ""
            rows.append([pid, p.get("name"), district, party or ""])
            seen.add(pid)

    for c in counties:
        for n in range(1, 80):
            grab(f"{c} {n}")
            time.sleep(0.6)

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["openstates_person_id","name","district","party"])
    for r in sorted(rows, key=lambda r: (str(r[2]), str(r[1]))):
        w.writerow(r)
    return buf.getvalue(), 200, {"Content-Type": "text/csv"}

@app.get("/house_key_votes.csv")
def serve_local_votes():
    return send_from_directory(".", "house_key_votes.csv", mimetype="text/csv")

@app.get("/debug/votes-source")
def debug_votes_source():
    return {"url": _votes_csv_url(), "using": VOTES_CACHE.get("src", "")}, 200

@app.get("/debug/votes-preview")
def debug_votes_preview():
    rows, err = _fetch_votes_rows(force_refresh=True)
    if err:
        return {"error": err}, 400
    return {"rows": rows[:2], "using": VOTES_CACHE.get("src", "")}, 200

@app.get("/debug/floterials")
def debug_floterials():
    return {
        "by_town_path": FLOTERIAL_MAP_PATH,
        "by_town_count": sum(len(v) for v in FLOTERIAL_MAP_TOWN.values()),
        "by_base_path": FLOTERIAL_BY_BASE_PATH,
        "by_base_count": sum(len(v) for v in FLOTERIAL_MAP_BASE.values()),
        "sample_by_base": {k: v for k, v in list(FLOTERIAL_MAP_BASE.items())[:3]},
    }, 200

@app.get("/debug/trace")
def debug_trace():
    addr = (request.args.get("address") or "").strip()
    geo, err = geocode_address(addr)
    if err: return {"error": err}, 400
    sldl_clean = sldl_to_openstates((geo.get("sldl") or {}).get("name") or "")
    return {
        "input": addr,
        "sldl_clean": sldl_clean,
        "town": geo.get("town"),
        "base_overlays": FLOTERIAL_MAP_BASE.get(sldl_clean, []),
        "town_overlays": FLOTERIAL_MAP_TOWN.get(geo.get("town") or "", []),
    }, 200

@app.get("/debug/district")
def debug_district():
    label = (request.args.get("label") or "").strip()
    data, err = fetch_people_by_district(label)
    names = []
    for r in (data or []):
        p = (r.get("person") if isinstance(r, dict) else None) or r
        names.append(p.get("name"))
    return {"label": label, "count": len(data or []), "names": names, "error": err}, 200

# Votes audit: shows why certain reps didn't match the CSV
@app.get("/debug/votes-audit")
def debug_votes_audit():
    addr = (request.args.get("address") or "").strip()
    if not addr:
        return {"error": "address required"}, 400
    core = _lookup_core(addr, include_votes=False)
    resp = core[0] if isinstance(core, tuple) else core
    data = resp.get_json().get("data", {})
    reps = data.get("stateRepresentatives") or []

    rows, err = _fetch_votes_rows(force_refresh=True)
    if err:
        return {"error": err}, 400

    id_cols = {"openstates_person_id","openstates id","openstates_id","person_id","os id"}
    def csv_has_id(pid: str) -> bool:
        for r in rows:
            for k, v in (r or {}).items():
                if (k or "").strip().lower() in id_cols and (v or "").strip() == pid:
                    return True
        return False

    report = []
    for r in reps:
        pid = (r.get("id") or "").strip()
        nm  = (r.get("name") or "").strip()
        dist= (r.get("district") or "").strip()
        row = _match_row_for_rep(rows, person_id=pid, name=nm, district=dist)
        if row:
            report.append({"id":pid,"name":nm,"district":dist,"matched":True,"votes":len(_row_to_vote_list(row))})
        else:
            why = "unknown"
            if not pid:                why = "rep has no OpenStates id"
            elif not csv_has_id(pid):  why = "id not found in CSV"
            else:                      why = "name+district mismatch"
            report.append({"id":pid,"name":nm,"district":dist,"matched":False,"why":why})
    return {"address":addr, "using": VOTES_CACHE.get("src",""), "result":report}, 200

@app.get("/api/key-votes")
def api_key_votes():
    person_id = (request.args.get("person_id") or "").strip()
    name = (request.args.get("name") or "").strip()
    district = (request.args.get("district") or "").strip()
    refresh = (request.args.get("refresh") or "").strip() in ("1", "true", "yes")

    rows, err = _fetch_votes_rows(force_refresh=refresh)
    if err:
        return jsonify({"success": False, "error": {"message": err}}), 400

    row = _match_row_for_rep(rows, person_id=person_id, name=name, district=district)
    votes = _row_to_vote_list(row) if row else []

    return jsonify({
        "success": True,
        "data": {
            "matched": bool(row),
            "rep": {
                "person_id": person_id or (row.get("openstates_person_id") if row else None),
                "name": name or (row.get("name") or row.get("Representative") if row else None),
                "district": district or (row.get("district") or row.get("District") if row else None),
            },
            "votes": votes
        }
    })

@app.post("/api/lookup-legislators")
def lookup_legislators():
    payload = request.get_json(force=True) or {}
    address = (payload.get("address") or "").strip()
    if not address:
        return jsonify({"success": False, "error": {"message": "Missing 'address' in body"}}), 400
    return _lookup_core(address)

@app.get("/api/lookup-legislators")
def lookup_legislators_get():
    addr = (request.args.get("address") or "").strip()
    if not addr:
        return jsonify({"success": False, "error": {"message": "Missing address query param"}}), 400
    return _lookup_core(addr)

@app.post("/api/lookup-with-votes")
def lookup_with_votes_post():
    payload = request.get_json(force=True) or {}
    address = (payload.get("address") or "").strip()
    refresh_votes = bool(payload.get("refreshVotes", False))
    if not address:
        return jsonify({"success": False, "error": {"message": "Missing 'address' in body"}}), 400
    return _lookup_core(address, include_votes=True, refresh_votes=refresh_votes)

@app.get("/api/lookup-with-votes")
def lookup_with_votes_get():
    addr = (request.args.get("address") or "").strip()
    refresh_votes = (request.args.get("refreshVotes") or "").strip() in ("1", "true", "yes")
    if not addr:
        return jsonify({"success": False, "error": {"message": "Missing address query param"}}), 400
    return _lookup_core(addr, include_votes=True, refresh_votes=refresh_votes)

@app.get("/health")
def health():
    return {
        "ok": True,
        "has_openstates_key": bool(OPENSTATES_API_KEY),
        "openstates_url": {"rest": OS_ROOT},
        "probe": {"start_deg": PROBE_START_DEG, "step_deg": PROBE_STEP_DEG, "max_rings": PROBE_MAX_RINGS},
        "floterials": {
            "by_town_path": FLOTERIAL_MAP_PATH,
            "by_town_count": sum(len(v) for v in FLOTERIAL_MAP_TOWN.values()),
            "by_base_path": FLOTERIAL_BY_BASE_PATH,
            "by_base_count": sum(len(v) for v in FLOTERIAL_MAP_BASE.values()),
        },
    }, 200

@app.get("/")
def root():
    return send_from_directory(".", "index.html")

if __name__ == "__main__":
    print(f"OPENSTATES_API_KEY loaded: {bool(OPENSTATES_API_KEY)}")
    app.run(host="127.0.0.1", port=int(os.getenv("PORT", "5000")), debug=True)
