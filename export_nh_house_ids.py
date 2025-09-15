#!/usr/bin/env python3
import os, sys, csv, time, argparse, requests
from typing import List, Dict, Any

# Optional .env support
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv(), override=True)
except Exception:
    pass

API_KEY = os.getenv("OPENSTATES_API_KEY", "").strip()
OS_PEOPLE = "https://v3.openstates.org/people"

def die(msg: str, code: int = 1):
    print(msg, file=sys.stderr)
    sys.exit(code)

def party_str(p) -> str:
    if isinstance(p, list) and p:
        return (p[0] or {}).get("name") or ""
    return p or ""

def first_email(p: Dict[str, Any]) -> str:
    emails: List[str] = []
    if isinstance(p.get("email_addresses"), list):
        emails += [e.get("address") for e in p["email_addresses"] if isinstance(e, dict) and e.get("address")]
    if isinstance(p.get("emails"), list):
        for e in p["emails"]:
            emails.append(e if isinstance(e, str) else e.get("address"))
    return next((e for e in emails if e), "")

def first_phone(p: Dict[str, Any]) -> str:
    for off in (p.get("offices") or []):
        if isinstance(off, dict) and off.get("voice"):
            return off["voice"]
    return ""

def fetch_page(sess: requests.Session, page: int, per_page: int = 50, max_retries: int = 6) -> List[Dict[str, Any]]:
    """Fetch one page with polite backoff on 429."""
    params = {
        "jurisdiction": "New Hampshire",
        "org_classification": "lower",
        "per_page": per_page,
        "page": page,
    }
    attempt = 0
    while True:
        attempt += 1
        r = sess.get(OS_PEOPLE, params=params, timeout=30)
        if r.status_code == 429:
            ra = int(r.headers.get("Retry-After", "8"))
            wait = min(ra if ra > 0 else 8, 60)
            print(f"[page {page}] 429 rate-limited — sleeping {wait}s…", flush=True)
            time.sleep(wait)
            continue
        try:
            r.raise_for_status()
            data = r.json()
            return data.get("results", []) or []
        except requests.HTTPError as e:
            if attempt < max_retries:
                wait = min(2 ** attempt, 30)
                print(f"[page {page}] HTTP {r.status_code} — retrying in {wait}s…", flush=True)
                time.sleep(wait)
                continue
            raise e

def main():
    if not API_KEY:
        die("Missing OPENSTATES_API_KEY (set in .env or environment).")

    ap = argparse.ArgumentParser(description="Export all NH House OpenStates IDs to CSV.")
    ap.add_argument("--out", default="nh_house_ids.csv", help="Output CSV path (default: nh_house_ids.csv)")
    args = ap.parse_args()

    out_final = os.path.abspath(args.out)
    out_part  = out_final + ".part"

    sess = requests.Session()
    sess.headers.update({
        "X-API-KEY": API_KEY,
        "Accept": "application/json",
        "User-Agent": "nh-rep-id-export/1.2",
    })

    # stream rows directly to .part so we never lose progress
    total = 0
    with open(out_part, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["openstates_person_id","name","district","party","email","phone"])

        page = 1
        while True:
            results = fetch_page(sess, page)
            count = len(results)
            print(f"page {page}: {count} results (total so far {total + count})", flush=True)
            if count == 0:
                break

            for rec in results:
                p = rec.get("person") or {}
                pid = p.get("id")
                name = p.get("name")
                district = rec.get("district") or (p.get("current_role") or {}).get("district") or ""
                party = party_str(p.get("party"))
                email = first_email(p)
                phone = first_phone(p)
                w.writerow([pid, name, district, party, email, phone])

            total += count
            if count < 50:
                break
            page += 1
            time.sleep(0.9)  # polite throttle

    # Atomic replace to final path
    os.replace(out_part, out_final)
    print(f"Done. Wrote {out_final} ({total} rows)")

if __name__ == "__main__":
    main()
