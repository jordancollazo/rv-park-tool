"""
populate_county_names.py
Populate county names for leads that have county_fips but missing county_name.
"""
import sqlite3
import json
from pathlib import Path

DB_PATH = Path("data/leads.db")

# Florida county FIPS to name mapping
FLORIDA_COUNTIES = {
    "12001": "Alachua",
    "12003": "Baker",
    "12005": "Bay",
    "12007": "Bradford",
    "12009": "Brevard",
    "12011": "Broward",
    "12013": "Calhoun",
    "12015": "Charlotte",
    "12017": "Citrus",
    "12019": "Clay",
    "12021": "Collier",
    "12023": "Columbia",
    "12027": "DeSoto",
    "12029": "Dixie",
    "12031": "Duval",
    "12033": "Escambia",
    "12035": "Flagler",
    "12037": "Franklin",
    "12039": "Gadsden",
    "12041": "Gilchrist",
    "12043": "Glades",
    "12045": "Gulf",
    "12047": "Hamilton",
    "12049": "Hardee",
    "12051": "Hendry",
    "12053": "Hernando",
    "12055": "Highlands",
    "12057": "Hillsborough",
    "12059": "Holmes",
    "12061": "Indian River",
    "12063": "Jackson",
    "12065": "Jefferson",
    "12067": "Lafayette",
    "12069": "Lake",
    "12071": "Lee",
    "12073": "Leon",
    "12075": "Levy",
    "12077": "Liberty",
    "12079": "Madison",
    "12081": "Manatee",
    "12083": "Marion",
    "12085": "Martin",
    "12086": "Miami-Dade",
    "12087": "Monroe",
    "12089": "Nassau",
    "12091": "Okaloosa",
    "12093": "Okeechobee",
    "12095": "Orange",
    "12097": "Osceola",
    "12099": "Palm Beach",
    "12101": "Pasco",
    "12103": "Pinellas",
    "12105": "Polk",
    "12107": "Putnam",
    "12109": "St. Johns",
    "12111": "St. Lucie",
    "12113": "Santa Rosa",
    "12115": "Sarasota",
    "12117": "Seminole",
    "12119": "Sumter",
    "12121": "Suwannee",
    "12123": "Taylor",
    "12125": "Union",
    "12127": "Volusia",
    "12129": "Wakulla",
    "12131": "Walton",
    "12133": "Washington",
}

def populate_county_names():
    """Populate county names for leads with FIPS but missing names."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get leads with FIPS but missing name
    cursor.execute("""
        SELECT id, county_fips
        FROM leads
        WHERE county_fips IS NOT NULL
        AND (county_name IS NULL OR county_name = '')
    """)

    leads = cursor.fetchall()

    if not leads:
        print("All leads with county FIPS already have county names.")
        conn.close()
        return

    print(f"Found {len(leads)} leads with FIPS but missing county name.")

    updates = []
    for lead_id, fips in leads:
        if fips in FLORIDA_COUNTIES:
            county_name = FLORIDA_COUNTIES[fips]
            updates.append((county_name, lead_id))

    if updates:
        cursor.executemany("UPDATE leads SET county_name = ? WHERE id = ?", updates)
        conn.commit()
        print(f"Updated {len(updates)} leads with county names.")
    else:
        print("No matching FIPS codes found in mapping.")

    conn.close()

if __name__ == "__main__":
    populate_county_names()
