# utils/districts.py
import json, re
from typing import Any, Dict, List, Optional, Tuple

# Two-letter county codes -> full county names (2022 House map)
COUNTY_CODE = {
    "BE": "Belknap", "CA": "Carroll", "CH": "Cheshire", "CO": "Coos",
    "GR": "Grafton", "HI": "Hillsborough", "ME": "Merrimack",
    "RO": "Rockingham", "ST": "Strafford", "SU": "Sullivan",
}

# e.g. "SU2", "SU02", "HI12" -> ("SU","2")
CODE_RE = re.compile(r"^([A-Z]{2})0*([0-9]+)$")

def _point_in_ring(lon: float, lat: float, ring: List[List[float]]) -> bool:
    inside = False
    n = len(ring)
    if n < 3: return False
    for i in range(n):
        x1, y1 = ring[i]; x2, y2 = ring[(i + 1) % n]
        if (y1 > lat) != (y2 > lat):
            x_int = (x2 - x1) * (lat - y1) / (y2 - y1 + 1e-15) + x1
            if x_int > lon: inside = not inside
    return inside

def _point_in_polygon(lon: float, lat: float, coords: List) -> bool:
    if not coords or not _point_in_ring(lon, lat, coords[0]): return False
    for hole in coords[1:]:
        if _point_in_ring(lon, lat, hole): return False
    return True

def _point_in_multipolygon(lon: float, lat: float, mcoords: List) -> bool:
    return any(_point_in_polygon(lon, lat, poly) for poly in mcoords)

_LABEL_KEYS = [
    "district","district_name","districtlabel","district_label",
    "name","label","DISTRICT","DIST_LABEL",
    "HOUSE_DIST","HSE_DIST","HSE_DIST_N","HSE_DIST_NA",
    "basehse22","BASEHSE22","BASE_LABEL",
]

def _normalize_label(val: str, props: Dict[str, Any]) -> Optional[str]:
    v = (val or "").strip()
    # Already "Sullivan 2"
    if " " in v and any(v.startswith(n) for n in COUNTY_CODE.values()):
        return v
    # Code form (SU2, SU02, etc.)
    m = CODE_RE.match(v)
    if m:
        county = COUNTY_CODE.get(m.group(1))
        if county:
            return f"{county} {int(m.group(2))}"
    # Compose from county + number fields if present
    county = None; num = None
    for ck in ("county","COUNTY","CNTY_NAME","County"):
        if props.get(ck):
            county = str(props[ck]).strip().title(); break
    for nk in ("district_n","district_no","DIST_NO","DISTRICT_N","HSE_DISTNO"):
        if props.get(nk) not in (None, ""):
            try: num = str(int(props[nk]))
            except Exception: num = str(props[nk]).strip()
            break
    if county and num:
        return f"{county} {int(num)}"
    return v or None

def _coerce_label(props: Dict[str, Any]) -> Optional[str]:
    for k in _LABEL_KEYS:
        if props.get(k):
            out = _normalize_label(str(props[k]), props)
            if out: return out
    for k in ("id","OBJECTID","FID"):
        if props.get(k): return str(props[k])
    return None

class DistrictIndex:
    def __init__(self, features: List[Dict[str, Any]]):
        self._items: List[Tuple[str, Any, str, Dict[str, Any]]] = []
        for f in features:
            geom = f.get("geometry") or {}
            gtype = geom.get("type"); coords = geom.get("coordinates")
            props = f.get("properties") or {}
            label = _coerce_label(props) or "UNKNOWN"
            if not gtype or coords is None: continue
            self._items.append((gtype, coords, label, props))

    @classmethod
    def from_geojson_path(cls, path: str) -> "DistrictIndex":
        with open(path, "r", encoding="utf-8") as fh:
            gj = json.load(fh)
        feats = gj.get("features") or []
        return cls(feats)

    def find(self, lat: float, lon: float) -> Optional[Tuple[str, Dict[str, Any]]]:
        for gtype, coords, label, props in self._items:
            if gtype == "Polygon":
                if _point_in_polygon(lon, lat, coords): return (label, props)
            elif gtype == "MultiPolygon":
                if _point_in_multipolygon(lon, lat, coords): return (label, props)
        return None
