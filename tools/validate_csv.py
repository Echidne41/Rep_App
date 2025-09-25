import sys, csv, os

FILES = ["floterial_by_base.csv", "floterial_by_town.csv"]
REQUIRED_BASE = {"district","rep_name","openstates_id"}
REQUIRED_TOWN = {"town","county","floterial"}

def check(path, required):
    with open(path, newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        missing = required - set(r.fieldnames or [])
        assert not missing, f"{path}: missing headers {missing}"
        for i,row in enumerate(r, start=2):
            for k in required:
                assert (row.get(k) or "").strip(), f"{path}:{i} empty {k}"
            if "openstates_id" in row and row["openstates_id"]:
                assert row["openstates_id"].startswith("ocd-person/"), f"{path}:{i} bad OpenStates ID"

def main():
    root = os.getcwd()
    for fname in FILES:
        path = os.path.join(root, fname)
        if os.path.exists(path):
            if fname == "floterial_by_town.csv":
                check(path, REQUIRED_TOWN)
            else:
                check(path, REQUIRED_BASE)
            print(f"{fname}: OK")
        else:
            print(f"{fname}: not present, skipped")
if __name__ == "__main__":
    main()
