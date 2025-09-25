import json, os, re
import importlib

# Import the Flask app instance from app.py
app_mod = importlib.import_module("app")
app = getattr(app_mod, "app")

def _get(path):
    c = app.test_client()
    r = c.get(path)
    assert r.status_code == 200, f"HTTP {r.status_code}: {r.data[:200]}"
    return r.get_json()

def test_lookup_returns_base_and_floterials():
    js = _get("/api/lookup?address=667 NH RT 120, Cornish, NH")
    reps = js["stateRepresentatives"]
    assert isinstance(reps, list) and len(reps) > 0
    dnames = [ (r.get("district") or "").lower().strip() for r in reps ]
    assert any("sullivan 2" in d for d in dnames), "Missing base district Sullivan 2"
    assert len(dnames) != len(set(dnames)), "Expected at least one floterial in addition to base"

def test_vote_labels_are_normalized():
    js = _get("/api/lookup?address=667 NH RT 120, Cornish, NH")
    for rep in js.get("stateRepresentatives", []):
        vm = rep.get("voteMap") or {}
        for v in vm.values():
            assert v in {"For","Against","No Vote"}, f"Invalid vote label: {v}"
