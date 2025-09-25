import os, time, json, csv, requests
from typing import Dict, Any, List, Tuple, Optional
from flask import Flask, jsonify, request
from flask_cors import CORS

from utils.geocode import geocode_address, GeocodeError
from utils.districts import DistrictIndex  # does SU2 -> Sullivan 2 normalization

app = Flask(__name__)

# ---- CORS ----
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").strip()
if ALLOWED_ORIGINS in ("", "*"):
    CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=False)
else:
    origins = [o.strip() for o in ALLOWED_ORIGINS.split(",") if o.strip()]
    CORS(app, resources={r"/*": {"origins": origins}}, supports_credentials=False)

# ---- Config ----
OPENSTATES_API_KEY = os.getenv("OPENSTATES_API_KEY", "")
NOMINATIM_EMAIL    = os.getenv("NOMINATIM_EMAIL", "rep-app@yourorg.org")

FLOTERIAL_BASE_CSV_URL = os.getenv("FLOTERIAL_BASE_CSV_URL")
FLOTERIAL_TOWN_CSV_URL = os.getenv("FLOTERIAL_TOWN_CSV_URL")
FLOTERIAL_BY_BASE_PATH = os.getenv("FLOTERIAL_BY_BASE_PATH", "floterial_by_base.csv")
FLOTERIAL_MAP_PATH     = os.getenv("FLOTERIAL_MAP_PATH", "floterial_by_town.csv")

VOTES_CSV_URL     = os.getenv("VOTES_CSV_URL")
VOTES_TTL_SECONDS = int(os.getenv("VOTES_TTL_SECONDS", os.getenv("OS_TTL_SECONDS", "300")))

OS_MIN_DELAY_MS = int(os.getenv("OS_MIN_DELAY_MS", "350"))
OS_TTL_SECONDS  = int(os.getenv("OS_TTL_SECONDS", "180"))

RENDER_COMMIT       = os.getenv("RENDER_GIT_COMMIT", "")
HOUSE_GEOJSON_PATH  = os.getenv("HOUSE_GEOJSON_PATH", "data/nh_house_districts.json")

# ---- OpenStates client (with throttle + TTL cache, never cache errors) ----
OS_BASE = "https://v3.openstates.org"
_last_call_ts = 0.0
_os_people_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}

def _os_throttle():
    """Ensure minimum spacing between OpenStates calls."""
    global _last_call_ts
    now = time.time()
    need = OS_MIN_DELAY_MS / 1000.0
    wait = (_last_call_ts + need) - now
    if wait > 0:
        time.sleep(wait)
    _last_call_ts = time.time()

def _os_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if not OPENSTATES_API_KEY:
        raise RuntimeError("OPENSTATES_API_KEY not set")
    _os_throttle()
    headers = {"X-API-Key": OPENSTATES_API_KEY, "Accept": "application/json"}
    r = requests.get(f"{OS_BASE}{path}", params=params, headers=headers, timeout=20)
    if r.status_code == 429:
        # small backoff+jitter to reduce stampede
        time.sleep(min(1.0 + (time.time() % 0.5), 2.0))
        return {"error": "rate_limited", "status": 429, "detail": r.text[:200]}
    r.raise_for_status()
    return r.json()

def os_people_by_district(label: str) -> Dict[str, Any]:
    """TTL-aware cache. Never cache error payloads."""
    now = time.time()
    key = str(label).strip()

    if OS_TTL_SECONDS > 0:
        cached = _os_people_cache.get(key)
        if cached:
            ts, payload = cached
            if now - ts < OS_TTL_SECONDS:
                return payload
            else:
                _os_people_cache.pop(key, None)

    try:
        payload = _os_get(
            "/people",
            {
                "jurisdiction": "New Hampshire",
                "chamber": "lower",
                "district": key,
                "per_page": 50,
            },
        )
    except Exception as e:
        return {"error": "transport", "detail": str(e)}

    # never cache an error
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

# ---- CSV helpers (unchanged) ----
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

def _norm(h: str) -> str: return h.strip().lower().replace(" ", "_")

def _pick_cols(headers: List[str]) -> Tuple[Optional[int], Optional[int]]:
    key_idx = val_idx = None
    for i, h in enumerate(headers):
        hn = _norm(h)
        if hn in ("base","base_district","base_label"): key_idx = i
        if hn in ("floterial","overlay","floterial_label","floterial_district"): val_idx = i
    return key_idx, val_idx

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

# ---- District polygons (GeoJSON) ----
DISTRICTS = DistrictIndex.from_geojson_path(HOUSE_GEOJSON_PATH)

# ================== ROUTES ==================
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
        "commit": RENDER_COMMIT,
    })

@app.route("/debug/trace")
def debug_trace():
    addr = request.args.get("address", "").strip()
    if not addr:
        return jsonify({"ok": False, "error": "Missing address"}), 400
    try:
        lat, lon, raw = geocode_address(addr, email=NOMINATIM_EMAIL)
    except GeocodeError as e:
        return jsonify({"ok": False, "error": f"geocode_failed: {e}"}), 502
    match = DISTRICTS.find(lat, lon)
    base_label = match[0] if match else None
    return jsonify({
        "ok": True,
        "inputAddress": addr,
        "lat": lat, "lon": lon,
        "geocode": raw,
        "base_district_label": base_label,
    })

@app.route("/debug/district")
def debug_district():
    label = request.args.get("label", "").strip()
    if not label:
        return jsonify({"ok": False, "error": "Missing label"}), 400
    payload = os_people_by_district(label)
    if "error" in payload:
        return jsonify({"ok": False, **payload}), 429 if payload.get("status") == 429 else 502
    return jsonify({"ok": True, "district": label, "people": _extract_people(payload)})

@app.route("/api/lookup-legislators")
def api_lookup_legislators():
    addr = request.args.get("address")
    lat  = request.args.get("lat")
    lon  = request.args.get("lon")

    if addr:
        try:
            latf, lonf, geocode_raw = geocode_address(addr, email=NOMINATIM_EMAIL)
        except GeocodeError as e:
            return jsonify({"success": False, "error": f"geocode_failed: {e}", "stateRepresentatives": []}), 502
    elif lat and lon:
        try:
            latf = float(lat); lonf = float(lon); geocode_raw = None
        except Exception:
            return jsonify({"success": False, "error": "invalid lat/lon"}), 400
    else:
        return jsonify({"success": False, "error": "provide address or lat/lon"}), 400

    match = DISTRICTS.find(latf, lonf)
    if not match:
        return jsonify({
            "success": True,
            "formattedAddress": geocode_raw.get("display_name") if geocode_raw else None,
            "lat": latf, "lon": lonf,
            "stateRepresentatives": [],
            "note": "No base district found in GeoJSON."
        })

    base_label, _props = match
    payload = os_people_by_district(base_label)
    if "error" in payload:
        status = 429 if payload.get("status") == 429 else 502
        return jsonify({"success": False, "error": payload, "stateRepresentatives": []}), status

    reps = _extract_people(payload)
    return jsonify({
        "success": True,
        "formattedAddress": geocode_raw.get("display_name") if geocode_raw else None,
        "lat": latf, "lon": lonf,
        "district": base_label,
        "stateRepresentatives": reps
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
