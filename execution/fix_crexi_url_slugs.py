"""
fix_crexi_url_slugs.py

Fixes Crexi listing URLs that are missing the slug portion.
Example bad URL: https://www.crexi.com/properties/123456
Example good URL: https://www.crexi.com/properties/123456/florida-sunny-acres-mhp

Usage:
    python execution/fix_crexi_url_slugs.py --dry-run   # Preview changes
    python execution/fix_crexi_url_slugs.py             # Apply changes
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
    text = text.lower()
    text = re.sub(r'[\s_]+', '-', text)
    text = re.sub(r'[^a-z0-9-]', '', text)
    text = re.sub(r'-+', '-', text)
    text = text.strip('-')
    return text


def is_bad_url(url: str) -> bool:
    """Check if URL is missing the slug (ends with just the ID)."""
    if not url:
        return False
    # Bad pattern: ends with /properties/{numbers} and nothing after
    # Good pattern: ends with /properties/{numbers}/{slug}
    pattern = r'^https://www\.crexi\.com/properties/\d+$'
    return bool(re.match(pattern, url))


def fix_listing_urls(dry_run: bool = False):
    """Fix all Crexi leads that have broken URLs (missing slug)."""
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Find all Crexi leads with listing URLs
    cursor.execute("""
        SELECT id, name, crexi_id, listing_url, state
        FROM leads
        WHERE scrape_source = 'crexi'
          AND crexi_id IS NOT NULL
          AND crexi_id != ''
          AND listing_url IS NOT NULL
          AND listing_url != ''
    """)
    
    all_leads = cursor.fetchall()
    print(f"Found {len(all_leads)} Crexi leads with URLs")
    
    # Filter to those with bad URLs
    leads_to_fix = [lead for lead in all_leads if is_bad_url(lead['listing_url'])]
    print(f"Found {len(leads_to_fix)} leads with broken URLs (missing slug)")
    
    if not leads_to_fix:
        print("No URLs need fixing!")
        conn.close()
        return
    
    # Show sample
    print("\nSample URLs to fix:")
    for lead in leads_to_fix[:5]:
        state_slug = slugify(lead['state']) if lead['state'] else 'florida'
        name_slug = slugify(lead['name']) if lead['name'] else 'property'
        slug = f"{state_slug}-{name_slug}" if state_slug and name_slug else (state_slug or name_slug or 'property')
        new_url = f"https://www.crexi.com/properties/{lead['crexi_id']}/{slug}"
        print(f"  OLD: {lead['listing_url']}")
        print(f"  NEW: {new_url}")
        print()
    
    if len(leads_to_fix) > 5:
        print(f"  ... and {len(leads_to_fix) - 5} more")
    
    if dry_run:
        print("\n[DRY RUN] No changes made.")
        conn.close()
        return
    
    # Perform update
    print("\nUpdating database...")
    updated = 0
    for lead in leads_to_fix:
        state_slug = slugify(lead['state']) if lead['state'] else 'florida'
        name_slug = slugify(lead['name']) if lead['name'] else 'property'
        slug = f"{state_slug}-{name_slug}" if state_slug and name_slug else (state_slug or name_slug or 'property')
        new_url = f"https://www.crexi.com/properties/{lead['crexi_id']}/{slug}"
        
        cursor.execute(
            "UPDATE leads SET listing_url = ? WHERE id = ?",
            (new_url, lead['id'])
        )
        updated += 1
    
    conn.commit()
    conn.close()
    
    print(f"Done! Fixed {updated} Crexi listing URLs.")


def main():
    parser = argparse.ArgumentParser(description="Fix Crexi listing URL slugs")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without saving")
    args = parser.parse_args()
    
    fix_listing_urls(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
