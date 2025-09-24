# utils/districts.py
# Lat/lon -> NH House base district using the GeoJSON you uploaded.
# No external geo deps; simple point-in-polygon (ray casting) for Polygon/MultiPolygon.

import json
from typing import Any, Dict, List, Optional, Tuple

# ---- basic geometry (lon, lat order in GeoJSON) ----
def _point_in_ring(lon: float, lat: float, ring: List[List[float]]) -> bool:
    # ray casting; ring is [ [lon,lat], ... ]
    inside = False
    n = len(ring)
    if n < 3:
        return False
    for i in range(n):
        x1, y1 = ring[i]
        x2, y2 = ring[(i + 1) % n]
        # Check edges that straddle the horizontal ray
        cond = ((y1 > lat) != (y2 > lat))
        if cond:
            # Intersection x at this y = lat
            x_int = (x2 - x1) * (lat - y1) / (y2 - y1 + 1e-15) + x1
            if x_int > lon:
                inside = not inside
    return inside

def _point_in_polygon(lon: float, lat: float, coords: List) -> bool:
    # coords = [ outer_ring, hole1, hole2, ... ]
    if not coords:
        return False
    if not _point_in_ring(lon, lat, coords[0]):
        return False
    # if inside outer, exclude holes
    for hole in coords[1:]:
        if _point_in_ring(lon, lat, hole):
            return False
    return True

def _point_in_multipolygon(lon: float, lat: float, mcoords: List) -> bool:
    for poly in mcoords:
        if _point_in_polygon(lon, lat, poly):
            return True
    return False

# ---- label helpers ----
_LABEL_KEYS = [
    # common guesses in NH data
    "district", "district_name", "districtlabel", "district_label",
    "name", "label", "DISTRICT", "DIST_LABEL",
    "HOUSE_DIST", "HSE_DIST", "HSE_DIST_N", "HSE_DIST_NA",
    "basehse22", "BASEHSE22", "BASE_LABEL",
]

def _coerce_label(props: Dict[str, Any]) -> Optional[str]:
    # Try direct label fields first
    for k in _LABEL_KEYS:
        if k in props and props[k]:
            val = str(props[k]).strip()
            if val:
                # Normalize common formats like "Sullivan 02"
                return val.replace(" 0", " ")
    # Try composing from county + number
    county_keys = ["county", "COUNTY", "CNTY_NAME", "County"]
    num_keys = ["district_n", "district_no", "DIST_NO", "DISTRICT_N", "HSE_DISTNO"]
    county = None
    num = None
    for ck in county_keys:
        if ck in props and props[ck]:
            county = str(props[ck]).strip().title()
            break
    for nk in num_keys:
        if nk in props and props[nk] not in (None, ""):
            try:
                num = str(int(props[nk]))
            except Exception:
                num = str(props[nk]).strip()
            break
    if county and num:
        return f"{county} {num}"
    # Fallback: stringify a couple props so we see *something*
    if props:
        for key in ("id", "OBJECTID", "FID"):
            if key in props:
                return str(props[key])
    return None

# ---- loader ----
class DistrictIndex:
    def __init__(self, features: List[Dict[str, Any]]):
        # store tuples: (geom_type, coords, label, properties)
        self._items: List[Tuple[str, Any, str, Dict[str, Any]]] = []
        for f in features:
            geom = f.get("geometry") or {}
            gtype = geom.get("type")
            coords = geom.get("coordinates")
            props = f.get("properties") or {}
            label = _coerce_label(props) or "UNKNOWN"
            if not gtype or coords is None:
                continue
            self._items.append((gtype, coords, label, props))

    @classmethod
    def from_geojson_path(cls, path: str) -> "DistrictIndex":
        with open(path, "r", encoding="utf-8") as fh:
            gj = json.load(fh)
        feats = gj.get("features") or []
        return cls(feats)

    def find(self, lat: float, lon: float) -> Optional[Tuple[str, Dict[str, Any]]]:
        # GeoJSON coordinates are [lon, lat] – careful with order.
        for gtype, coords, label, props in self._items:
            if gtype == "Polygon":
                if _point_in_polygon(lon, lat, coords):
                    return label, props
            elif gtype == "MultiPolygon":
                if _point_in_multipolygon(lon, lat, coords):
                    return label, props
        return None
