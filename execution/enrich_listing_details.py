"""
enrich_listing_details.py

Scrape detailed listing pages from LoopNet and Crexi for leads that need
more information to determine if they are MHP/RV parks.

Uses web scraping to fetch full listing descriptions and property details.
"""

import os
import sys
import time
import sqlite3
import requests
from pathlib import Path
from typing import Optional, Dict
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"

def scrape_crexi_listing(url: str) -> Optional[Dict]:
    """
    Scrape a Crexi listing page for detailed information.

    Returns dict with keys: detailed_description, sub_type, highlights, etc.
    """
    if not url:
        return None

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        print(f"  Fetching: {url}")
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract description
        # Crexi usually has description in specific divs
        description_section = soup.find('div', class_='description') or \
                             soup.find('div', {'data-testid': 'description'}) or \
                             soup.find('div', class_='property-description')

        detailed_description = ""
        if description_section:
            detailed_description = description_section.get_text(strip=True, separator=' ')

        # Look for property type / subtype
        sub_type = ""
        type_elem = soup.find('span', class_='property-type') or \
                   soup.find('div', class_='property-subtype')
        if type_elem:
            sub_type = type_elem.get_text(strip=True)

        # Look for investment highlights
        highlights = ""
        highlights_section = soup.find('div', class_='investment-highlights') or \
                            soup.find('div', {'data-testid': 'highlights'})
        if highlights_section:
            highlights = highlights_section.get_text(strip=True, separator='; ')

        # Extract any h1/h2 headings that might have property type info
        headings = [h.get_text(strip=True) for h in soup.find_all(['h1', 'h2'])[:5]]

        return {
            'detailed_description': detailed_description[:5000],  # Limit length
            'sub_type': sub_type,
            'investment_highlights': highlights[:2000],
            'headings': ' | '.join(headings)
        }

    except Exception as e:
        print(f"  Error scraping Crexi: {e}")
        return None

def scrape_loopnet_listing(url: str) -> Optional[Dict]:
    """
    Scrape a LoopNet listing page for detailed information.

    Returns dict with keys: detailed_description, sub_type, highlights, etc.
    """
    if not url:
        return None

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        print(f"  Fetching: {url}")
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract description
        description_section = soup.find('div', class_='property-description') or \
                             soup.find('div', id='listing-description')

        detailed_description = ""
        if description_section:
            detailed_description = description_section.get_text(strip=True, separator=' ')

        # Property type
        sub_type = ""
        type_elem = soup.find('span', class_='property-type')
        if type_elem:
            sub_type = type_elem.get_text(strip=True)

        # Extract page title and subtitles
        headings = [h.get_text(strip=True) for h in soup.find_all(['h1', 'h2'])[:5]]

        return {
            'detailed_description': detailed_description[:5000],
            'sub_type': sub_type,
            'headings': ' | '.join(headings)
        }

    except Exception as e:
        print(f"  Error scraping LoopNet: {e}")
        return None

def update_lead_with_details(conn: sqlite3.Connection, lead_id: int, details: Dict):
    """Update lead with scraped details."""
    cursor = conn.cursor()

    update_fields = []
    values = []

    if details.get('detailed_description'):
        update_fields.append('detailed_description = ?')
        values.append(details['detailed_description'])

    if details.get('sub_type'):
        update_fields.append('sub_type = ?')
        values.append(details['sub_type'])

    if details.get('investment_highlights'):
        update_fields.append('investment_highlights = ?')
        values.append(details['investment_highlights'])

    if not update_fields:
        return

    values.append(lead_id)
    query = f"UPDATE leads SET {', '.join(update_fields)} WHERE id = ?"

    cursor.execute(query, values)

def main():
    # Get lead IDs to enrich from command line or use unclear leads
    if len(sys.argv) > 1:
        lead_ids = [int(x) for x in sys.argv[1:]]
    else:
        # Default: unclear leads from previous analysis
        lead_ids = [207, 286, 298, 313, 350, 351, 357]

    print("=" * 80)
    print("ENRICHING LISTING DETAILS")
    print("=" * 80)
    print(f"Target leads: {lead_ids}")
    print()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    enriched_count = 0
    failed_count = 0

    for lead_id in lead_ids:
        cursor.execute("""
            SELECT id, name, scrape_source, loopnet_url, listing_url
            FROM leads
            WHERE id = ?
        """, (lead_id,))

        lead = cursor.fetchone()
        if not lead:
            print(f"Lead {lead_id} not found, skipping")
            continue

        print(f"\n[{lead['id']}] {lead['name']}")
        print(f"  Source: {lead['scrape_source']}")

        details = None

        if lead['scrape_source'] == 'crexi' and lead['listing_url']:
            details = scrape_crexi_listing(lead['listing_url'])
        elif lead['scrape_source'] == 'loopnet' and lead['loopnet_url']:
            details = scrape_loopnet_listing(lead['loopnet_url'])

        if details:
            update_lead_with_details(conn, lead['id'], details)
            enriched_count += 1
            print(f"  [OK] Enriched with {len(details.get('detailed_description', ''))} chars")
        else:
            failed_count += 1
            print(f"  [FAIL] Could not enrich")

        # Be nice to the servers
        time.sleep(2)

    conn.commit()
    conn.close()

    print("\n" + "=" * 80)
    print("ENRICHMENT COMPLETE")
    print("=" * 80)
    print(f"Enriched: {enriched_count}")
    print(f"Failed: {failed_count}")

if __name__ == "__main__":
    main()
