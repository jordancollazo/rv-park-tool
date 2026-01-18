import sqlite3
import json
import os
import re
import math

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "leads.db")

def process_raw_listings():
    conn = sqlite3.connect(DB_PATH)
    # Enable accessing columns by name
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    print("Checking for unprocessed Landwatch listings...")
    
    # Select unprocessed raw listings
    cursor.execute("""
        SELECT id, landwatch_id, json_data 
        FROM landwatch_raw_listings 
        WHERE processed_at IS NULL
    """)
    
    rows = cursor.fetchall()
    
    if not rows:
        print("No new listings to process.")
        conn.close()
        return

    print(f"Found {len(rows)} listings to process.")
    
    processed_count = 0
    skipped_count = 0

    for row in rows:
        try:
            data = json.loads(row['json_data'])
            
            # --- Extract & Map Data ---
            
            # 1. Unique ID Generation
            # We prefix with 'lw_' to avoid collision with Google Place IDs
            landwatch_id = row['landwatch_id']
            place_id = f"lw_{landwatch_id}"
            
            # 2. Basic Fields
            name = data.get('title') or data.get('name') or "Unknown Landwatch Listing"
            address = data.get('address')
            city = data.get('city')
            state = data.get('state')
            zip_code = data.get('zip')
            
            price = data.get('price')
            
            # Clean price if it's a string like "$1,000,000"
            if isinstance(price, str):
                price = re.sub(r'[^\d.]', '', price)
                price = float(price) if price else None
                
            acres = data.get('acres')
            if isinstance(acres, str):
                 acres = re.sub(r'[^\d.]', '', acres)
                 acres = float(acres) if acres else None

            description = data.get('description', '')
            url = data.get('url')
            
            # Broker Info
            broker = data.get('broker', {})
            owner_name = broker.get('name')
            owner_phone = broker.get('phone')
            # You might want to store broker company too? Schema has owner_name, maybe append?
            if broker.get('company'):
                owner_name = f"{owner_name} ({broker.get('company')})" if owner_name else broker.get('company')

            # 3. Insight Extraction (Regex)
            notes = description
            
            # Extract Units/Pads
            lot_count = None
            # Look for "50 units", "50 pads", "50 sites"
            units_match = re.search(r'(\d+)\s*(?:units|pads|sites|spaces)', description, re.IGNORECASE)
            if units_match:
                lot_count = int(units_match.group(1))
            elif acres:
                # Fallback estimation? Maybe don't fill if not sure.
                # Project requirement: "Extract all insights". 
                # Let's just calculate it but store it in notes or reasons? 
                # Better to only set lot_count if explicit.
                pass
                
            # Owner Financing Signal
            owner_fatigue_score = 0
            fatigue_reasons = []
            
            if re.search(r'owner financ|seller financ|creative financ', description, re.IGNORECASE):
                owner_fatigue_score += 20
                fatigue_reasons.append("Mentions owner financing")
            
            if re.search(r'bring offer|must sell|price reduced|priced to sell', description, re.IGNORECASE):
                owner_fatigue_score += 15
                fatigue_reasons.append("Motivated seller language")

            # 4. Upsert into Leads Table
            # We use INSERT OR IGNORE or INSERT OR REPLACE. 
            # We want to keep existing manual edits, so match on place_id and only update if new info?
            # For now, let's just insert new ones and update price/status on existing.
            
            # Geocoords (might be in 'geo_info' or 'lat'/'lum')
            lat = data.get('latitude') or (data.get('geo_info', {}).get('latitude'))
            lon = data.get('longitude') or (data.get('geo_info', {}).get('longitude'))

            cursor.execute("""
                INSERT INTO leads (
                    place_id, name, address, city, state, zip, 
                    asking_price, lot_count, owner_name, owner_phone, 
                    website, notes, latitude, longitude,
                    source_query, first_scraped_at, last_scraped_at,
                    owner_fatigue_score_0_100, owner_fatigue_reasons_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'), ?, ?)
                ON CONFLICT(place_id) DO UPDATE SET
                    asking_price = excluded.asking_price,
                    last_scraped_at = datetime('now'),
                    owner_name = COALESCE(leads.owner_name, excluded.owner_name),
                    owner_phone = COALESCE(leads.owner_phone, excluded.owner_phone)
            """, (
                place_id, name, address, city, state, zip_code,
                price, lot_count, owner_name, owner_phone,
                url, notes, lat, lon,
                'Landwatch Scraper', 
                owner_fatigue_score if owner_fatigue_score > 0 else 0, # Only set initial score
                json.dumps(fatigue_reasons) if fatigue_reasons else None
            ))
            
            # Mark as processed
            cursor.execute("""
                UPDATE landwatch_raw_listings
                SET processed_at = datetime('now')
                WHERE id = ?
            """, (row['id'],))
            
            processed_count += 1
            
        except Exception as e:
            print(f"Error processing row {row['id']}: {e}")
            skipped_count += 1

    conn.commit()
    conn.close()
    print(f"Processing complete. Processed: {processed_count}, Skipped: {skipped_count}")

if __name__ == "__main__":
    process_raw_listings()
