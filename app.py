import os
import time
import json
import csv
from functools import lru_cache
from typing import Dict, Any, List, Tuple, Optional
from flask import Flask, jsonify, request
import requests

# --- Geocoder (Nominatim-only) ---
from utils.geocode import geocode_address, GeocodeError

app = Flask(__name__)

# -------- Config from ENV ----------
OPENSTATES_API_KEY = os.getenv("OPENSTATES_API_KEY", "")
NOMINATIM_EMAIL    = os.getenv("NOMINATIM_EMAIL", "rep-app@yourorg.org")

# CSV locations (Render "file://..." or local paths)
FLOTERIAL_BASE_CSV_URL = os.getenv("FLOTERIAL_BASE_CSV_URL")
FLOTERIAL_TOWN_CSV_URL = os.getenv("FLOTERIAL_TOWN_CSV_URL")
FLOTERIAL_BY_BASE_PATH = os.getenv("FLOTERIAL_BY_BASE_PATH", "floterial_by_base.csv")
FLOTERIAL_MAP_PATH     = os.getenv("FLOTERIAL_MAP_PATH", "floterial_by_town.csv")

VOTES_CSV_URL   = os.getenv("VOTES_CSV_URL")  # optional override
VOTES_TTL_SECONDS = int(os.getenv("VOTES_TTL_SECONDS", os.getenv("OS_TTL_SECONDS", "300")))

# Rate-limit knobs (OpenStates)
OS_MIN_DELAY_MS = int(os.getenv("OS_MIN_DELAY_MS", "350"))
OS_TTL_SECONDS  = int(os.getenv("OS_TTL_SECONDS", "180"))

# Probe ring (kept for compatibility; not used if district-by-label is called explicitly)
PROBE_START_DEG = float(os.getenv("PROBE_START_DEG", "0.02"))
PROBE_STEP_DEG  = float(os.getenv("PROBE_STEP_DEG", "0.01"))
PROBE_MAX_RINGS = int(os.getenv("PROBE_MAX_RINGS", "3"))

RENDER_COMMIT   = os.getenv("RENDER_GIT_COMMIT", "")  # Render injects this on deploys

# -------- OpenStates helpers --------
OS_BASE = "https://v3.openstates.org"

_last_call_ts = 0.0
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
        raise RuntimeError("OPENSTATES_API_KEY not set")
    _os_throttle()
    headers = {"X-API-Key": OPENSTATES_API_KEY, "Accept": "application/json"}
    r = requests.get(f"{OS_BASE}{path}", params=params, headers=headers, timeout=20)
    if r.status_code == 429:
        # serve structured error so frontend knows it's rate-limit
        return {"error": "rate_limited", "status": 429, "detail": r.text[:200]}
    r.raise_for_status()
    return r.json()

@lru_cache(maxsize=2048)
def os_people_by_district(label: str) -> Dict[str, Any]:
    # cache w/ soft TTL: bake TTL into cache key by minute bucket
    return _os_get(
        "/people",
        {
            "jurisdiction": "New Hampshire",
            "chamber": "lower",
            "district": label,
            "per_page": 50,
        },
    )

def _extract_people(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if "error" in payload:
        return []
    data = payload.get("results") or payload.get("data") or payload.get("people") or []
    out = []
    for p in data:
        out.append({
            "id": p.get("id") or p.get("openstates_id"),
            "name": p.get("name"),
            "party": (p.get("party") or [{}])[0].get("name") if isinstance(p.get("party"), list) else p.get("party"),
            "email": p.get("email"),
            "links": p.get("links", []),
        })
    return out

# -------- Floterial CSV loading --------
def _read_csv_from(url_or_path: Optional[str], fallback_path: str) -> Tuple[List[str], List[List[str]]]:
    path = (url_or_path or "").strip()
    if path.startswith("file:///"):
        # Render "file:///" points to container path
        real = path[len("file://"):]  # keep leading /opt/...
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
                if i == 0:
                    headers = row
                else:
                    rows.append(row)
    except FileNotFoundError:
        # best-effort: try fallback_path if the url/path failed
        if real != fallback_path:
            try:
                with open(fallback_path, newline="", encoding="utf-8-sig") as f:
                    reader = csv.reader(f)
                    for i, row in enumerate(reader):
                        if i == 0:
                            headers = row
                        else:
                            rows.append(row)
            except Exception:
                pass
    except Exception:
        pass
    return headers, rows

def _csv_counts() -> Dict[str, Any]:
    by_base_h, by_base_r = _read_csv_from(FLOTERIAL_BASE_CSV_URL, FLOTERIAL_BY_BASE_PATH)
    by_town_h, by_town_r = _read_csv_from(FLOTERIAL_TOWN_CSV_URL, FLOTERIAL_MAP_PATH)
    return {
        "by_base_path": FLOTERIAL_BY_BASE_PATH,
        "by_base_count": len(by_base_r),
        "by_town_path": FLOTERIAL_MAP_PATH,
        "by_town_count": len(by_town_r),
        "headers": {
            "by_base": by_base_h,
            "by_town": by_town_h,
        },
        "sample_by_base": dict(list(_group_sample(by_base_h, by_base_r).items())[:3]),
        "sample_by_town": dict(list(_group_sample(by_town_h, by_town_r).items())[:3]),
    }

def _group_sample(headers: List[str], rows: List[List[str]]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    if not headers:
        return out
    # try to find expected columns
    key_idx = None
    val_idx = None
    # common header names
    for i, h in enumerate(headers):
        if h.strip().lower() in ("base", "base_district", "base_label"):
            key_idx = i
        if h.strip().lower() in ("floterial", "overlay", "floterial_label"):
            val_idx = i
    if key_idx is None or val_idx is None:
        return out
    for r in rows[:20]:
        if len(r) <= max(key_idx, val_idx):
            continue
        base = r[key_idx].strip()
        flo = r[val_idx].strip()
        out.setdefault(base, [])
        if flo and flo not in out[base]:
            out[base].append(flo)
    return out

# -------- Votes (CSV) --------
_votes_cache: Dict[str, Any] = {"ts": 0.0, "data": None}

def _load_votes() -> Any:
    now = time.time()
    if _votes_cache["data"] is not None and now - _votes_cache["ts"] < VOTES_TTL_SECONDS:
        return _votes_cache["data"]
    path = None
    if VOTES_CSV_URL:
        path = VOTES_CSV_URL[len("file://"):] if VOTES_CSV_URL.startswith("file://") else VOTES_CSV_URL
    else:
        path = os.path.join(os.path.dirname(__file__), "house_key_votes.csv")  # bundled default if present
    out = {"ok": False, "count": 0, "rows": []}
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            out = {"ok": True, "count": len(rows), "rows": rows}
    except Exception as e:
        out = {"ok": False, "error": str(e)}
    _votes_cache["ts"] = now
    _votes_cache["data"] = out
    return out

# ================== ROUTES ==================

@app.route("/health")
def health():
    csv_info = _csv_counts()
    return jsonify({
        "ok": True,
        "env": {
            "has_openstates_key": bool(OPENSTATES_API_KEY),
            "nominatim_email": NOMINATIM_EMAIL,
            "os_min_delay_ms": OS_MIN_DELAY_MS,
            "os_ttl_seconds": OS_TTL_SECONDS,
        },
        "csv": csv_info,
        "commit": RENDER_COMMIT,
    })

@app.route("/version")
def version():
    return jsonify({"commit": RENDER_COMMIT})

@app.route("/debug/floterials")
def debug_floterials():
    return jsonify(_csv_counts())

@app.route("/debug/floterial-headers")
def debug_flot_headers():
    by_base_h, _ = _read_csv_from(FLOTERIAL_BASE_CSV_URL, FLOTERIAL_BY_BASE_PATH)
    by_town_h, _ = _read_csv_from(FLOTERIAL_TOWN_CSV_URL, FLOTERIAL_MAP_PATH)
    return jsonify({"by_base": by_base_h, "by_town": by_town_h})

@app.route("/debug/trace")
def debug_trace():
    addr = request.args.get("address", "").strip()
    if not addr:
        return jsonify({"ok": False, "error": "Missing address"}), 400
    try:
        lat, lon, raw = geocode_address(addr, email=NOMINATIM_EMAIL)
    except GeocodeError as e:
        return jsonify({"ok": False, "error": f"geocode_failed: {e}"}), 502

    # NOTE: base district discovery is implementation-specific.
    # We return the lat/lon + echo back so downstream can be tested.
    return jsonify({
        "ok": True,
        "inputAddress": addr,
        "lat": lat,
        "lon": lon,
        "geocode": raw,
        "note": "Geocode succeeded. If base-district lookup still fails, use /debug/district?label=… to test OpenStates path while rate-limiting is tuned."
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

@app.route("/api/vote-map")
def api_vote_map():
    return jsonify(_load_votes())

@app.route("/api/lookup-legislators")
def api_lookup_legislators():
    # Supports address OR lat/lon passthrough
    addr = request.args.get("address")
    lat = request.args.get("lat")
    lon = request.args.get("lon")

    latf: Optional[float] = None
    lonf: Optional[float] = None
    geocode_raw: Optional[Dict[str, Any]] = None

    if addr:
        try:
            latf, lonf, geocode_raw = geocode_address(addr, email=NOMINATIM_EMAIL)
        except GeocodeError as e:
            return jsonify({"success": False, "error": f"geocode_failed: {e}", "stateRepresentatives": []}), 502
    elif lat and lon:
        try:
            latf = float(lat); lonf = float(lon)
        except Exception:
            return jsonify({"success": False, "error": "invalid lat/lon"}), 400
    else:
        return jsonify({"success": False, "error": "provide address or lat/lon"}), 400

    # IMPORTANT:
    # If you already have working base-district detection elsewhere in your codebase,
    # replace the placeholder below with that logic. For now, we accept an explicit
    # district ?label= in the query to keep end-to-end fetch testable under rate limits.
    label = request.args.get("label")
    if not label:
        # Placeholder: require label until shapefile/label-probe is re-integrated.
        # This ensures we can still test OpenStates + floterial overlay path.
        return jsonify({
            "success": True,
            "formattedAddress": geocode_raw.get("display_name") if geocode_raw else None,
            "lat": latf, "lon": lonf,
            "stateRepresentatives": [],
            "note": "Base-district lookup not executed in this minimal patch. Call /debug/district?label=… (e.g., Sullivan 2) or pass &label=Sullivan%202 here while we finalize shapefile/probe integration."
        })

    payload = os_people_by_district(label)
    if "error" in payload:
        status = 429 if payload.get("status") == 429 else 502
        return jsonify({"success": False, "error": payload, "stateRepresentatives": []}), status

    reps = _extract_people(payload)
    return jsonify({
        "success": True,
        "formattedAddress": geocode_raw.get("display_name") if geocode_raw else None,
        "lat": latf, "lon": lonf,
        "district": label,
        "stateRepresentatives": reps
    })

# -------------- main ---------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
