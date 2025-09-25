#!/usr/bin/env bash
set -euo pipefail
ADDR="667%20NH%20RT%20120,%20Cornish,%20NH"
echo "== Smoke: /api/lookup =="
curl -fsS "http://localhost:5000/api/lookup?address=${ADDR}" \
  | python - <<'PY'
import sys, json
js=json.load(sys.stdin)
reps=js.get("stateRepresentatives", [])
assert isinstance(reps, list) and len(reps)>0, "no reps returned"
names=[(r.get("district","") or "").lower() for r in reps]
assert any("sullivan 2" in d for d in names), "missing base district Sullivan 2"
print("OK")
PY
echo "All good."
