# NH Rep Finder — Handoff (authoritative)

## Repo
- Branch: `main` (protected)
- Last known good commit: `d6a73f0`
- Backend: Flask (Python 3.13.4) — file: `app.py`
- Data: `floterial_by_base.csv`, `floterial_by_town.csv` (in repo root unless moved to /data)

## API contract (do not change without bumping version)
GET /api/lookup?address=<string>
200 JSON:
{
  "formattedAddress": "…",
  "stateRepresentatives": [
    {
      "id": "ocd-person/…",
      "name": "…",
      "district": "…",         // includes base AND floterial reps
      "voteMap": { "HB####": "For" | "Against" | "No Vote" }
    }
  ],
  "diagnostics": { … }        // presence optional, never removed once added
}

**Invariants**
- Vote labels **only**: "For", "Against", "No Vote".
- Names matched case-insensitively, trimmed.
- Always return base **and** all applicable floterials for the address.

## Golden addresses (must pass)
- 667 NH RT 120, Cornish, NH — includes Sullivan 2 + correct floterials
- (add 4 more once verified)

## How we verify (local)
- `pytest -q`
- `./scripts/smoke.sh` (requires server running locally on :5000)

## Known issues / next work
- Keep a short bullet list here with acceptance checks.
