# utils/geocode.py
import time
import requests
from typing import Optional, Tuple, Dict, Any, List

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
DEFAULT_LIMIT = 1

class GeocodeError(Exception):
    pass

def _req(params: Dict[str, Any], email: str, timeout=12) -> List[Dict[str, Any]]:
    headers = {
        "User-Agent": email or "rep-app/1.0",
        "Accept": "application/json",
    }
    delay = 1.0
    for _ in range(6):
        r = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 502, 503, 504):
            time.sleep(delay)
            delay = min(delay * 2, 8.0)
            continue
        raise GeocodeError(f"Nominatim HTTP {r.status_code}: {r.text[:200]}")
    raise GeocodeError("Nominatim: exhausted retries")

def geocode_address(
    address: str,
    email: str,
    zip_hint: Optional[str] = None,
    city_hint: Optional[str] = None,
    state_hint: str = "NH",
    country_hint: str = "USA",
) -> Tuple[float, float, Dict[str, Any]]:
    """
    Nominatim-only geocoder. Tries structured + freeform variants commonly needed in NH.
    Returns (lat, lon, raw_json). Raises GeocodeError on failure.
    """
    address = (address or "").strip()
    if not address:
        raise GeocodeError("Empty address")

    def first_line(st: str) -> str:
        return st.split(",")[0].strip()

    street_variants = [
        address.replace(" NH RT ", " NH-").replace(" NH RTE ", " NH-"),
        address.replace(" NH RT ", " NH ").replace(" NH RTE ", " NH "),
        address.replace(" RT ", " Route ").replace(" RTE ", " Route "),
        address.replace(" NH ", " NH-"),
        address,  # as-is last
    ]
    structured_attempts: List[Dict[str, Any]] = []
    for sv in street_variants:
        street = first_line(sv)
        params = {
            "format": "json",
            "limit": DEFAULT_LIMIT,
            "street": street,
            "state": state_hint,
            "country": country_hint,
        }
        # add hints if present/derivable
        city = city_hint or ("Cornish" if "Cornish" in address else None)
        if city: params["city"] = city
        zipc = zip_hint or ("03745" if "03745" in address or "Cornish" in address else None)
        if zipc: params["postalcode"] = zipc
        structured_attempts.append(params)

    freeform = []
    for v in street_variants:
        vv = v
        if "Cornish" in address and "03745" not in vv:
            vv = vv + " 03745"
        freeform.append(vv)

    attempts = []
    attempts += structured_attempts
    attempts += [{"format": "json", "limit": DEFAULT_LIMIT, "q": v} for v in freeform]

    last_err: Optional[Exception] = None
    for params in attempts:
        try:
            res = _req(params, email=email)
            if res:
                lat = float(res[0]["lat"])
                lon = float(res[0]["lon"])
                return lat, lon, res[0]
        except Exception as e:
            last_err = e
            continue
    raise GeocodeError(f"All Nominatim attempts returned empty. Last error: {last_err}")
