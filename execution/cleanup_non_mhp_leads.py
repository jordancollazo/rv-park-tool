"""
cleanup_non_mhp_leads.py

Removes or flags existing Crexi leads that are not MHP/RV parks.
Uses the same validation logic as ingest_crexi_leads.py.

Usage:
    python execution/cleanup_non_mhp_leads.py --dry-run   # Preview what would be removed
    python execution/cleanup_non_mhp_leads.py --flag      # Mark as excluded (keeps in DB)
    python execution/cleanup_non_mhp_leads.py --delete    # Actually remove from DB
"""

import argparse
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"

# Keywords indicating MHP/RV parks (positive signals)
MHP_RV_POSITIVE_KEYWORDS = [
    "mobile home", "mobile-home", "mobilehome",
    "manufactured home", "manufactured housing",
    "mhp", "mhc",  # Mobile Home Park/Community
    "rv park", "rv resort", "rv community",
    "trailer park", "trailer court",
    "camp ground", "campground",
]

# Keywords indicating NOT an MHP/RV (negative signals)  
NON_MHP_RV_KEYWORDS = [
    "retail", "restaurant", "office", "warehouse", "industrial",
    "grocery", "market", "produce", "liquor", "store", "shop",
    "hotel", "motel", "apartment", "condo", "townhouse",
    "medical", "dental", "clinic", "hospital",
    "church", "school", "daycare",
    "gas station", "car wash", "auto",
    "land only", "vacant land", "raw land",
    "self storage", "mini storage", "self-storage",
]


def is_valid_mhp_rv(name: str, description: str, category: str) -> bool:
    """Check if a lead appears to be a valid MHP/RV park.
    
    AGGRESSIVE filter - requires explicit MHP/RV signals in the name.
    """
    name_lower = (name or "").lower()
    desc_lower = (description or "").lower()
    
    # Must have positive signal in the NAME to be kept
    # These are the definitive keywords that identify MHP/RV properties
    mhp_keywords = ["mobile home", "mobile-home", "mobilehome", "mhp", "mhc", 
                    "manufactured", "trailer park", "trailer court"]
    rv_keywords = ["rv park", "rv resort", "rv community", "campground", "camp ground"]
    
    # Check name for positive keywords
    has_mhp_signal = any(kw in name_lower for kw in mhp_keywords)
    has_rv_signal = any(kw in name_lower for kw in rv_keywords)
    
    if has_mhp_signal or has_rv_signal:
        return True
    
    # Also check description for strong signals if name doesn't have them
    # But be more strict - require multiple signals or very strong signals
    strong_desc_signals = ["mobile home park", "rv park", "rv resort", "trailer park", 
                          "manufactured housing community", "campground"]
    
    for signal in strong_desc_signals:
        if signal in desc_lower:
            return True
    
    # No strong signals - reject
    return False


def cleanup_leads(action: str = "dry-run"):
    """Clean up non-MHP/RV leads from the database."""
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Get all Crexi leads
    cursor.execute("""
        SELECT id, name, description, category
        FROM leads
        WHERE scrape_source = 'crexi'
    """)
    
    all_leads = cursor.fetchall()
    print(f"Found {len(all_leads)} total Crexi leads")
    
    # Find invalid leads
    invalid_leads = []
    for lead in all_leads:
        if not is_valid_mhp_rv(lead['name'], lead['description'], lead['category']):
            invalid_leads.append(lead)
    
    print(f"Found {len(invalid_leads)} leads that appear to be non-MHP/RV properties")
    
    if not invalid_leads:
        print("Nothing to clean up!")
        conn.close()
        return
    
    # Show sample
    print("\nSample invalid leads:")
    for lead in invalid_leads[:10]:
        print(f"  - [{lead['id']}] {lead['name']} (category: {lead['category']})")
    if len(invalid_leads) > 10:
        print(f"  ... and {len(invalid_leads) - 10} more")
    
    if action == "dry-run":
        print("\n[DRY RUN] No changes made.")
        conn.close()
        return
    
    if action == "flag":
        print("\nFlagging leads as 'excluded'...")
        for lead in invalid_leads:
            cursor.execute(
                "UPDATE leads SET status = 'excluded' WHERE id = ?",
                (lead['id'],)
            )
        conn.commit()
        print(f"Done! Flagged {len(invalid_leads)} leads as 'excluded'.")
    
    elif action == "delete":
        print("\nDeleting invalid leads...")
        for lead in invalid_leads:
            cursor.execute("DELETE FROM leads WHERE id = ?", (lead['id'],))
        conn.commit()
        print(f"Done! Deleted {len(invalid_leads)} invalid leads.")
    
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Clean up non-MHP/RV Crexi leads")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true", help="Preview changes without modifying")
    group.add_argument("--flag", action="store_true", help="Mark invalid leads as 'excluded'")
    group.add_argument("--delete", action="store_true", help="Remove invalid leads from database")
    
    args = parser.parse_args()
    
    if args.flag:
        action = "flag"
    elif args.delete:
        action = "delete"
    else:
        action = "dry-run"
    
    cleanup_leads(action=action)


if __name__ == "__main__":
    main()
