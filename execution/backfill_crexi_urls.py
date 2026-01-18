"""
backfill_crexi_urls.py

Backfills listing_url for Crexi leads that are missing it.
Constructs URLs from crexi_id using the pattern:
  https://www.crexi.com/properties/{crexi_id}/{state-slugified-name}

Usage:
    python execution/backfill_crexi_urls.py --dry-run   # Preview changes
    python execution/backfill_crexi_urls.py             # Apply changes
"""

import argparse
import re
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"


def slugify(text: str) -> str:
    """Convert text to URL-friendly slug (lowercase, hyphens, no special chars)."""
    if not text:
        return ""
    # Lowercase
    text = text.lower()
    # Replace spaces and underscores with hyphens
    text = re.sub(r'[\s_]+', '-', text)
    # Remove any characters that aren't alphanumeric or hyphens
    text = re.sub(r'[^a-z0-9-]', '', text)
    # Collapse multiple hyphens
    text = re.sub(r'-+', '-', text)
    # Strip leading/trailing hyphens
    text = text.strip('-')
    return text


def build_crexi_url(crexi_id: str, state: str = None, name: str = None) -> str:
    """Build a proper Crexi listing URL with slug."""
    # Build the slug: state-property-name
    parts = []
    if state:
        parts.append(slugify(state))
    if name:
        parts.append(slugify(name))
    
    slug = "-".join(parts) if parts else "property"
    
    return f"https://www.crexi.com/properties/{crexi_id}/{slug}"


def backfill_listing_urls(dry_run: bool = False):
    """Update all Crexi leads that have crexi_id but no listing_url."""
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Find Crexi leads missing listing_url but with crexi_id (include state for slug)
    cursor.execute("""
        SELECT id, name, crexi_id, listing_url, state
        FROM leads
        WHERE scrape_source = 'crexi'
          AND crexi_id IS NOT NULL
          AND crexi_id != ''
          AND (listing_url IS NULL OR listing_url = '')
    """)
    
    leads_to_update = cursor.fetchall()
    print(f"Found {len(leads_to_update)} Crexi leads needing listing_url backfill")
    
    if not leads_to_update:
        print("Nothing to do!")
        conn.close()
        return
    
    # Show sample
    print("\nSample leads to update:")
    for lead in leads_to_update[:5]:
        crexi_url = build_crexi_url(lead['crexi_id'], lead['state'], lead['name'])
        print(f"  ID {lead['id']}: {lead['name']} -> {crexi_url}")
    
    if len(leads_to_update) > 5:
        print(f"  ... and {len(leads_to_update) - 5} more")
    
    if dry_run:
        print("\n[DRY RUN] No changes made.")
        conn.close()
        return
    
    # Perform update
    print("\nUpdating database...")
    updated = 0
    for lead in leads_to_update:
        crexi_url = build_crexi_url(lead['crexi_id'], lead['state'], lead['name'])
        cursor.execute(
            "UPDATE leads SET listing_url = ? WHERE id = ?",
            (crexi_url, lead['id'])
        )
        updated += 1
    
    conn.commit()
    conn.close()
    
    print(f"Done! Updated {updated} leads with Crexi listing URLs.")


def main():
    parser = argparse.ArgumentParser(description="Backfill Crexi listing URLs")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without saving")
    args = parser.parse_args()
    
    backfill_listing_urls(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
