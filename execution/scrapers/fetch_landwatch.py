import os
import time
import requests
import sqlite3
import json
import argparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

APIFY_TOKEN = os.getenv("APIFY_API_TOKEN")
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "leads.db")

def fetch_landwatch_leads(search_url, max_items=100):
    if not APIFY_TOKEN:
        print("Error: APIFY_API_TOKEN not found in environment variables.")
        return

    # 1. Start the Actor
    print(f"Starting Landwatch Scraper for URL: {search_url}")
    run_url = f"https://api.apify.com/v2/acts/memo23~landwatch-scraper/runs?token={APIFY_TOKEN}"
    
    # Input configuration for Memo23 Landwatch Scraper
    actor_input = {
        "startUrls": [{"url": search_url}],
        "maxItems": max_items,
        "proxyConfiguration": {
            "useApifyProxy": True
        }
    }

    try:
        response = requests.post(run_url, json=actor_input)
        response.raise_for_status()
        run_data = response.json()['data']
        run_id = run_data['id']
        dataset_id = run_data['defaultDatasetId']
        print(f"Actor started. Run ID: {run_id}")
    except Exception as e:
        print(f"Failed to start actor: {e}")
        return

    # 2. Poll for completion
    while True:
        status_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"
        status_response = requests.get(status_url)
        status_data = status_response.json()['data']
        status = status_data['status']
        
        print(f"Status: {status}")
        
        if status == "SUCCEEDED":
            break
        elif status in ["FAILED", "ABORTED", "TIMED-OUT"]:
            print(f"Run failed with status: {status}")
            return
        
        time.sleep(10) # Wait 10 seconds before checking again

    # 3. Fetch Results
    print("Fetching results...")
    dataset_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_TOKEN}&format=json"
    dataset_response = requests.get(dataset_url)
    items = dataset_response.json()
    print(f"Retrieved {len(items)} items.")

    # 4. Save to Database (Raw)
    save_raw_listings(items)

def save_raw_listings(items):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    new_count = 0
    updated_count = 0
    
    for item in items:
        # Assuming 'id' is a unique identifier from Landwatch (e.g. 123456)
        # If not present, we will rely on auto-increment ID but better to have dedupe key.
        # Looking at Apify sample output, usually there is an 'id' or url we can hash. 
        # For now, let's try to find an ID or use the URL.
        
        landwatch_id = item.get('id')
        if not landwatch_id:
             # Fallback if ID is missing, though typical for this scraper
             # Generate a hash or skip? Let's skip for now or try to extract from URL
             url = item.get('url', '')
             if url:
                 # Extract digits from URL like /land/123456
                 import re
                 match = re.search(r'/(\d+)', url)
                 if match:
                     landwatch_id = int(match.group(1))
        
        if landwatch_id:
            json_dump = json.dumps(item)
            try:
                cursor.execute("""
                    INSERT INTO landwatch_raw_listings (landwatch_id, json_data)
                    VALUES (?, ?)
                """, (landwatch_id, json_dump))
                new_count += 1
            except sqlite3.IntegrityError:
                # Update existing?
                cursor.execute("""
                    UPDATE landwatch_raw_listings 
                    SET json_data = ?, scraped_at = datetime('now')
                    WHERE landwatch_id = ?
                """, (json_dump, landwatch_id))
                updated_count += 1
        else:
            print(f"Skipping item without ID/URL: {item.get('address')}")

    conn.commit()
    conn.close()
    print(f"Saved {new_count} new listings, updated {updated_count} existing listings.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Landwatch leads")
    parser.add_argument("--url", help="Landwatch Search URL", default="https://www.landwatch.com/florida/land-for-sale")
    parser.add_argument("--max", type=int, help="Max items", default=50)
    
    args = parser.parse_args()
    
    fetch_landwatch_leads(args.url, args.max)
