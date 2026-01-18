"""
backfill_loopnet_urls.py

Backfill missing loopnet_url fields for leads that have loopnet_id but no URL.
Constructs LoopNet URLs from address components and loopnet_id.

Usage:
    python execution/backfill_loopnet_urls.py --dry-run  # Preview changes without modifying database
    python execution/backfill_loopnet_urls.py            # Apply changes with confirmation prompt
"""

import sqlite3
import re
import sys
import shutil
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"


def slugify(text):
    """
    Convert text to URL-friendly slug.
    - Lowercase
    - Replace spaces with hyphens
    - Remove special characters except hyphens
    """
    if not text:
        return ""

    # Lowercase
    text = text.lower()

    # Replace spaces with hyphens
    text = text.replace(' ', '-')

    # Remove special characters (keep only alphanumeric and hyphens)
    text = re.sub(r'[^a-z0-9\-]', '', text)

    # Remove multiple consecutive hyphens
    text = re.sub(r'-+', '-', text)

    # Remove leading/trailing hyphens
    text = text.strip('-')

    return text


def construct_loopnet_url(lead_id, name, address, city, state, loopnet_id):
    """
    Construct LoopNet URL from address components and ID.

    Format: https://www.loopnet.com/Listing/{street-slug}-{city-slug}-{state}/{loopnet_id}/
    Fallback: https://www.loopnet.com/Listing/{loopnet_id}/
    """
    # Try to construct full URL with address
    if address and city and state and loopnet_id:
        street_slug = slugify(address)
        city_slug = slugify(city)
        state_clean = state.strip().upper()

        # Only use full URL if we have valid components
        if street_slug and city_slug and len(state_clean) == 2:
            url = f"https://www.loopnet.com/Listing/{street_slug}-{city_slug}-{state_clean}/{loopnet_id}/"
            return url, "full"

    # Fallback to minimal URL
    if loopnet_id:
        url = f"https://www.loopnet.com/Listing/{loopnet_id}/"
        return url, "minimal"

    return None, "none"


def get_leads_without_urls(conn):
    """
    Query leads that have loopnet_id but no loopnet_url.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, name, address, city, state, loopnet_id
        FROM leads
        WHERE loopnet_id IS NOT NULL
        AND loopnet_url IS NULL
        ORDER BY id
    """)

    leads = []
    for row in cursor.fetchall():
        leads.append({
            'id': row[0],
            'name': row[1],
            'address': row[2],
            'city': row[3],
            'state': row[4],
            'loopnet_id': row[5]
        })

    return leads


def create_backup(db_path):
    """
    Create a backup of the database before making changes.
    """
    backup_path = db_path.parent / f"{db_path.stem}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}{db_path.suffix}"
    shutil.copy2(db_path, backup_path)
    return backup_path


def update_lead_url(conn, lead_id, url):
    """
    Update the loopnet_url field for a specific lead.
    """
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE leads
        SET loopnet_url = ?
        WHERE id = ?
    """, (url, lead_id))


def main():
    # Check for dry-run flag
    dry_run = '--dry-run' in sys.argv

    print("=" * 80)
    print("LoopNet URL Backfill Script")
    print("=" * 80)
    print()

    if dry_run:
        print("[DRY RUN MODE] - No changes will be made to the database")
        print()

    # Connect to database
    if not DB_PATH.exists():
        print(f"[ERROR] Database not found at {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)

    # Get leads without URLs
    print("[INFO] Querying database for leads without URLs...")
    leads = get_leads_without_urls(conn)

    if not leads:
        print("[SUCCESS] All LoopNet leads already have URLs!")
        conn.close()
        sys.exit(0)

    print(f"Found {len(leads)} LoopNet leads without URLs")
    print()

    # Generate URLs for each lead
    updates = []
    full_url_count = 0
    minimal_url_count = 0
    failed_count = 0

    for lead in leads:
        url, url_type = construct_loopnet_url(
            lead['id'],
            lead['name'],
            lead['address'],
            lead['city'],
            lead['state'],
            lead['loopnet_id']
        )

        if url:
            updates.append({
                'lead_id': lead['id'],
                'name': lead['name'][:50],
                'url': url,
                'url_type': url_type
            })

            if url_type == "full":
                full_url_count += 1
            else:
                minimal_url_count += 1
        else:
            failed_count += 1
            print(f"[WARNING] Could not generate URL for Lead ID {lead['id']} ({lead['name'][:40]})")

    # Display summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total leads to update: {len(updates)}")
    print(f"  - Full URLs (with address): {full_url_count}")
    print(f"  - Minimal URLs (ID only): {minimal_url_count}")
    if failed_count > 0:
        print(f"  - Failed to generate: {failed_count}")
    print()

    # Show sample of proposed URLs
    print("Sample of proposed URLs:")
    print("-" * 80)
    for update in updates[:5]:
        print(f"Lead ID {update['lead_id']}: {update['name']}")
        print(f"  URL: {update['url']}")
        print()

    if len(updates) > 5:
        print(f"... and {len(updates) - 5} more")
        print()

    # If dry-run, exit here
    if dry_run:
        print("=" * 80)
        print("[DRY RUN COMPLETE]")
        print("Run without --dry-run flag to apply these changes")
        print("=" * 80)
        conn.close()
        sys.exit(0)

    # Confirm with user
    print("=" * 80)
    print("[CONFIRMATION REQUIRED]")
    print("=" * 80)
    print(f"This will update {len(updates)} leads in the database.")
    print("A backup will be created before making changes.")
    print()

    response = input("Proceed with update? (yes/no): ").strip().lower()

    if response not in ['yes', 'y']:
        print("[CANCELLED] Operation cancelled by user")
        conn.close()
        sys.exit(0)

    # Create backup
    print()
    print("[INFO] Creating database backup...")
    backup_path = create_backup(DB_PATH)
    print(f"[SUCCESS] Backup created: {backup_path}")
    print()

    # Apply updates
    print("[INFO] Updating database...")
    try:
        conn.execute("BEGIN TRANSACTION")

        updated_count = 0
        for update in updates:
            update_lead_url(conn, update['lead_id'], update['url'])
            updated_count += 1

            # Show progress every 10 leads
            if updated_count % 10 == 0:
                print(f"  Updated {updated_count}/{len(updates)} leads...")

        conn.commit()
        print(f"[SUCCESS] Successfully updated {updated_count} leads")

    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Error during update: {e}")
        print(f"Changes have been rolled back. Database is unchanged.")
        print(f"Backup is still available at: {backup_path}")
        conn.close()
        sys.exit(1)

    # Verify results
    print()
    print("[INFO] Verifying results...")
    cursor = conn.cursor()

    # Count total LoopNet leads
    total_loopnet = cursor.execute(
        "SELECT COUNT(*) FROM leads WHERE loopnet_id IS NOT NULL"
    ).fetchone()[0]

    # Count leads with URLs
    with_urls = cursor.execute(
        "SELECT COUNT(*) FROM leads WHERE loopnet_url IS NOT NULL"
    ).fetchone()[0]

    # Count leads still without URLs
    without_urls = cursor.execute(
        "SELECT COUNT(*) FROM leads WHERE loopnet_id IS NOT NULL AND loopnet_url IS NULL"
    ).fetchone()[0]

    print()
    print("=" * 80)
    print("[UPDATE COMPLETE]")
    print("=" * 80)
    print(f"Total LoopNet leads: {total_loopnet}")
    print(f"Leads with URLs: {with_urls}")
    print(f"Leads still without URLs: {without_urls}")
    print()
    print(f"Backup saved at: {backup_path}")
    print()
    print("Next steps:")
    print("1. Refresh your browser at localhost:8000")
    print("2. Click on LoopNet leads to verify URLs appear")
    print("3. Test that URLs redirect correctly")
    print("4. Run: python execution/verify_loopnet_urls.py")
    print("=" * 80)

    conn.close()


if __name__ == "__main__":
    main()
