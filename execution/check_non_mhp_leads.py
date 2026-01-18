"""
check_non_mhp_leads.py

Check leads that might not be MHP/RV parks and need validation.
"""

import sqlite3
import json
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get non-dead leads by source
    print("=" * 80)
    print("NON-DEAD LEADS BY SOURCE AND STATUS")
    print("=" * 80)
    cursor.execute("""
        SELECT COUNT(*) as cnt, scrape_source, status
        FROM leads
        WHERE status != 'dead'
        GROUP BY scrape_source, status
    """)
    for row in cursor.fetchall():
        source = row['scrape_source'] or 'google_places'
        print(f"{source}: status={row['status']}, count={row['cnt']}")

    # Sample some LoopNet/Crexi leads
    print("\n" + "=" * 80)
    print("SAMPLE LOOPNET/CREXI LEADS (NON-DEAD)")
    print("=" * 80)
    cursor.execute("""
        SELECT id, name, city, state, status, scrape_source,
               loopnet_url, listing_url, description, detailed_description,
               description_keywords, sub_type
        FROM leads
        WHERE scrape_source IN ('loopnet', 'crexi') AND status != 'dead'
        LIMIT 10
    """)

    for row in cursor.fetchall():
        print(f"\nID: {row['id']}")
        print(f"Name: {row['name']}")
        print(f"Location: {row['city']}, {row['state']}")
        print(f"Source: {row['scrape_source']}")
        print(f"Status: {row['status']}")
        print(f"Sub-Type: {row['sub_type']}")
        print(f"LoopNet URL: {row['loopnet_url']}")
        print(f"Crexi URL: {row['listing_url']}")

        desc = row['description'] or row['detailed_description']
        if desc:
            print(f"Description: {desc[:200]}...")

        if row['description_keywords']:
            print(f"Keywords: {row['description_keywords']}")
        print("-" * 80)

    # Check for leads with no description data
    print("\n" + "=" * 80)
    print("LEADS WITHOUT DESCRIPTION DATA")
    print("=" * 80)
    cursor.execute("""
        SELECT COUNT(*) as cnt
        FROM leads
        WHERE scrape_source IN ('loopnet', 'crexi')
        AND status != 'dead'
        AND (description IS NULL OR description = '')
        AND (detailed_description IS NULL OR detailed_description = '')
    """)
    no_desc = cursor.fetchone()['cnt']
    print(f"Leads without descriptions: {no_desc}")

    conn.close()

if __name__ == "__main__":
    main()
