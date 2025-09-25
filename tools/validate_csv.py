#!/usr/bin/env python3
import sys, os, csv, re

# ---- what each CSV must contain (any one of these header-sets is OK) ----
RULES = {
    # Your file: base_district + floterial_district (or legacy/alt names)
    "floterial_by_base.csv": [
        {"base_district", "floterial_district"},
        {"base_label", "floterials"},
        {"base", "floterials"},
        {"base", "floterial_district"},
        {"base_label", "floterial_district"},
    ],
    # Town mapping (county optional if you ever drop it)
    "floterial_by_town.csv": [
        {"town", "county", "district"},
        {"town", "district"},
        {"town", "county", "floterials"},
        {"town", "floterial_district"},
    ],
    # Roster of OS person IDs
    "nh_house_ids.csv": [
        {"openstates_person_id", "name", "district"},
        {"openstates_id", "name", "district"},
        {"person_id", "name", "district"},
        {"openstates_person_id", "rep_name", "district"},
        {"openstates_person_id", "name", "district_label"},
    ],
}

def norm(s: str) -> str:
    """lowercase, strip BOM/space/punct so 'openstates_person_id' == 'OpenStates Person ID'."""
    if s is None: return ""
    s = s.replace("\ufeff", "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "", s)  # drop spaces/
