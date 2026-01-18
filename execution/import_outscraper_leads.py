"""
import_outscraper_leads.py

Imports leads from Outscraper xlsx/csv exports into the leads database.
Filters out non-MHP/RV properties and maps columns to our schema.

Usage:
    python execution/import_outscraper_leads.py .tmp/500leads.xlsx
    python execution/import_outscraper_leads.py .tmp/leads.csv --no-filter
"""

import argparse
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd


DB_PATH = Path("data/leads.db")

# Category patterns that indicate valid MHP/RV properties
VALID_CATEGORIES = [
    "mobile home park",
    "rv park",
    "trailer park",
    "manufactured home",
    "campground",
    "mobile home dealer",  # Sometimes parks are miscategorized
]

# Name patterns that indicate non-MHP/RV properties (filter these out)
INVALID_NAME_PATTERNS = [
    r"\bapartment",
    r"\bapts?\b",
    r"\brestaurant\b",
    r"\bgrill\b",
    r"\bbar\b",
    r"\bpizza\b",
    r"\bcafe\b",
    r"\bcoffee\b",
    r"\bhotel\b",
    r"\bmotel\b",
    r"\binn\b",
    r"\bself.?storage\b",
    r"\bstorage\b unit",
    r"\bwarehouse\b",
    r"\boffice\b",
    r"\bplaza\b",
    r"\bshopping\b",
    r"\bretail\b",
    r"\bbank\b",
    r"\bchurch\b",
    r"\bschool\b",
    r"\bhospital\b",
    r"\bclinic\b",
    r"\bdentist\b",
    r"\bdoctor\b",
    r"\bsalon\b",
    r"\bspa\b",
    r"\bgym\b",
    r"\bfitness\b",
]


def is_valid_mhp_rv(row: pd.Series) -> tuple[bool, str]:
    """
    Check if a row represents a valid MHP/RV property.
    Returns (is_valid, reason).
    """
    name = str(row.get("name", "")).lower()
    category = str(row.get("category", "")).lower()
    subtypes = str(row.get("subtypes", "")).lower()
    
    # Check if category matches valid types
    category_match = any(valid in category or valid in subtypes 
                         for valid in VALID_CATEGORIES)
    
    # Check for invalid name patterns
    for pattern in INVALID_NAME_PATTERNS:
        if re.search(pattern, name, re.IGNORECASE):
            return False, f"Name matches invalid pattern: {pattern}"
    
    # If no category match and name doesn't contain MHP/RV keywords, flag it
    name_keywords = ["mobile", "rv", "trailer", "manufactured", "mhp", "campground"]
    name_match = any(kw in name for kw in name_keywords)
    
    if not category_match and not name_match:
        return False, "No MHP/RV category or name keywords"
    
    return True, "Valid"


def import_leads(
    input_path: Path,
    apply_filter: bool = True,
    dry_run: bool = False
) -> dict:
    """
    Import leads from Outscraper export file.
    
    Returns stats dict with counts.
    """
    # Read input file
    if input_path.suffix == ".xlsx":
        df = pd.read_excel(input_path)
    else:
        df = pd.read_csv(input_path)
    
    print(f"Loaded {len(df)} rows from {input_path}")
    
    stats = {
        "total": len(df),
        "valid": 0,
        "filtered": 0,
        "duplicates": 0,
        "imported": 0,
        "filtered_reasons": {},
    }
    
    # Filter if requested
    if apply_filter:
        valid_rows = []
        for idx, row in df.iterrows():
            is_valid, reason = is_valid_mhp_rv(row)
            if is_valid:
                valid_rows.append(row)
                stats["valid"] += 1
            else:
                stats["filtered"] += 1
                stats["filtered_reasons"][reason] = stats["filtered_reasons"].get(reason, 0) + 1
        
        df = pd.DataFrame(valid_rows)
        print(f"After filtering: {len(df)} valid MHP/RV properties")
        print(f"Filtered out: {stats['filtered']} non-MHP/RV entries")
    else:
        stats["valid"] = len(df)
    
    if dry_run:
        print("\n[DRY RUN] Would import these leads:")
        for _, row in df.head(10).iterrows():
            print(f"  - {row.get('name')} | {row.get('city')}, {row.get('state_code')}")
        if len(df) > 10:
            print(f"  ... and {len(df) - 10} more")
        return stats
    
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get existing place_ids to avoid duplicates (check all, place_id is globally unique)
    cursor.execute("SELECT place_id FROM leads")
    existing_ids = {row[0] for row in cursor.fetchall()}
    
    # Import each lead
    for _, row in df.iterrows():
        place_id = row.get("place_id")
        
        if place_id in existing_ids:
            stats["duplicates"] += 1
            continue
        
        # Map Outscraper columns to our schema
        lead_data = {
            "place_id": place_id,
            "name": row.get("name"),
            "address": row.get("address"),
            "city": row.get("city"),
            "state": row.get("state_code", row.get("state")),
            "zip_code": row.get("postal_code"),
            "phone": row.get("phone"),
            "website": row.get("website"),
            "latitude": row.get("latitude"),
            "longitude": row.get("longitude"),
            "google_rating": row.get("rating"),
            "review_count": row.get("reviews"),
            "google_id": row.get("google_id"),
            "maps_url": f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id else None,
            "business_status": row.get("business_status"),
            "category": row.get("category"),
            "subtypes": row.get("subtypes"),
            "county": row.get("county"),
            "archived": 0,
            "first_scraped_at": datetime.now().isoformat(),
            "last_scraped_at": datetime.now().isoformat(),
        }
        
        # Insert into database
        columns = ", ".join(lead_data.keys())
        placeholders = ", ".join(["?" for _ in lead_data])
        values = list(lead_data.values())
        
        try:
            cursor.execute(f"""
                INSERT INTO leads ({columns})
                VALUES ({placeholders})
            """, values)
            stats["imported"] += 1
            existing_ids.add(place_id)  # Track to avoid dups in same batch
        except sqlite3.IntegrityError:
            stats["duplicates"] += 1
        except sqlite3.Error as e:
            print(f"  Error importing {row.get('name')}: {e}")
    
    conn.commit()
    conn.close()
    
    return stats


def main():
    parser = argparse.ArgumentParser(description="Import Outscraper leads")
    parser.add_argument("input", type=str, help="Input xlsx or csv file path")
    parser.add_argument("--no-filter", action="store_true", 
                        help="Skip MHP/RV filtering (import all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview what would be imported without making changes")
    
    args = parser.parse_args()
    input_path = Path(args.input)
    
    if not input_path.exists():
        print(f"ERROR: File not found: {input_path}")
        return
    
    print("=" * 60)
    print("OUTSCRAPER LEAD IMPORT")
    print("=" * 60)
    
    stats = import_leads(
        input_path,
        apply_filter=not args.no_filter,
        dry_run=args.dry_run
    )
    
    print("\n" + "=" * 60)
    print("IMPORT SUMMARY")
    print("=" * 60)
    print(f"Total rows in file:    {stats['total']}")
    print(f"Valid MHP/RV leads:    {stats['valid']}")
    print(f"Filtered out:          {stats['filtered']}")
    print(f"Duplicates skipped:    {stats['duplicates']}")
    print(f"Successfully imported: {stats['imported']}")
    
    if stats['filtered_reasons']:
        print("\nFilter breakdown:")
        for reason, count in sorted(stats['filtered_reasons'].items(), 
                                    key=lambda x: -x[1]):
            print(f"  {count:4d} - {reason}")


if __name__ == "__main__":
    main()
