"""
check_unclear_leads.py

Check the unclear leads in detail to see what information we have.
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"

UNCLEAR_IDS = [207, 286, 298, 313, 350, 351, 357]

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    print("=" * 80)
    print("UNCLEAR LEADS - DETAILED VIEW")
    print("=" * 80)

    for lead_id in UNCLEAR_IDS:
        cursor.execute("""
            SELECT id, name, address, city, state, zip,
                   scrape_source, loopnet_url, listing_url,
                   description, detailed_description, description_keywords,
                   sub_type, year_built, building_size, lot_size,
                   list_price, asking_price, cap_rate
            FROM leads
            WHERE id = ?
        """, (lead_id,))

        lead = cursor.fetchone()
        if not lead:
            continue

        print(f"\n{'='*80}")
        print(f"ID: {lead['id']}")
        print(f"Name: {lead['name']}")
        print(f"Address: {lead['address']}, {lead['city']}, {lead['state']} {lead['zip']}")
        print(f"Source: {lead['scrape_source']}")

        if lead['loopnet_url']:
            print(f"LoopNet URL: {lead['loopnet_url']}")
        if lead['listing_url']:
            print(f"Crexi URL: {lead['listing_url']}")

        print(f"\nSub-Type: {lead['sub_type']}")
        print(f"Year Built: {lead['year_built']}")
        print(f"Building Size: {lead['building_size']}")
        print(f"Lot Size: {lead['lot_size']}")
        print(f"List Price: {lead['list_price']}")
        print(f"Asking Price: {lead['asking_price']}")
        print(f"Cap Rate: {lead['cap_rate']}")

        print(f"\n--- Description ---")
        print(lead['description'] or "(None)")

        print(f"\n--- Detailed Description ---")
        print(lead['detailed_description'] or "(None)")

        print(f"\n--- Keywords ---")
        print(lead['description_keywords'] or "(None)")

    conn.close()

if __name__ == "__main__":
    main()
