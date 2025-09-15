import os, csv, time, requests
from dotenv import load_dotenv

load_dotenv()  # reads your backend .env
KEY = os.getenv("OPENSTATES_API_KEY")
if not KEY:
    raise SystemExit("Missing OPENSTATES_API_KEY in .env or env vars.")

S = requests.Session()
S.headers.update({
    "X-API-KEY": KEY,
    "Accept": "application/json",
    "User-Agent": "nh-rep-id-builder/1.0"
})

rows = []
page = 1
while True:
    params = {
        "jurisdiction": "New Hampshire",
        "org_classification": "lower",
        "per_page": 50,  # OpenStates cap
        "page": page
    }
    r = S.get("https://v3.openstates.org/people", params=params, timeout=30)
    if r.status_code == 429:
        # Respect Retry-After or back off a bit
        ra = int(r.headers.get("Retry-After", "8"))
        time.sleep(min(ra, 60))
        continue
    r.raise_for_status()
    results = r.json().get("results", [])
    if not results:
        break

    for rec in results:
        p = rec.get("person") or {}
        pid = p.get("id")
        name = p.get("name")
        district = rec.get("district") or (p.get("current_role") or {}).get("district") or ""
        party = p.get("party")
        if isinstance(party, list):
            party = (party[0] or {}).get("name")

        # email (best-effort)
        emails = []
        if isinstance(p.get("email_addresses"), list):
            emails += [e.get("address") for e in p["email_addresses"] if isinstance(e, dict) and e.get("address")]
        if isinstance(p.get("emails"), list):
            for e in p["emails"]:
                emails.append(e if isinstance(e, str) else e.get("address"))
        email = next((e for e in emails if e), "")

        # phone (first office voice)
        phone = ""
        for off in (p.get("offices") or []):
            if isinstance(off, dict) and off.get("voice"):
                phone = off["voice"]
                break

        rows.append([pid, name, district, party or "", email, phone])

    if len(results) < 50:
        break
    page += 1
    time.sleep(0.9)  # gentle throttle

rows.sort(key=lambda r: ((r[2] or ""), (r[1] or "")))
with open("nh_house_ids.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["openstates_person_id","name","district","party","email","phone"])
    w.writerows(rows)

print(f"Wrote nh_house_ids.csv ({len(rows)} rows)")
