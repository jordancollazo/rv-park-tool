"""
Fetch and inspect raw LoopNet data from the most recent Apify run.
This helps us understand the exact field structure for price and address parsing.
"""

import os
import json
from apify_client import ApifyClient
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

client = ApifyClient(os.getenv("APIFY_API_TOKEN"))

# Get most recent runs
runs = client.actor("memo23/apify-loopnet-search-cheerio").runs().list(limit=1).items

if not runs:
    print("No recent runs found.")
    exit()

run = runs[0]
dataset_id = run.get("defaultDatasetId")
print(f"Fetching from dataset: {dataset_id}")
print(f"Run status: {run.get('status')}")

items = client.dataset(dataset_id).list_items(limit=2).items

if not items:
    print("No items in dataset.")
    exit()

print(f"\nFound {len(items)} sample items.\n")

# Dump first item completely to see all available fields
print("=" * 60)
print("RAW ITEM #1 (Full JSON)")
print("=" * 60)
print(json.dumps(items[0], indent=2, default=str))

# Show key fields we care about
print("\n" + "=" * 60)
print("KEY FIELDS ANALYSIS")
print("=" * 60)

for i, item in enumerate(items):
    print(f"\n--- Item {i+1} ---")
    
    # Price-related fields
    print("\nPRICE FIELDS:")
    for k in ['price', 'askingPrice', 'salePrice', 'listPrice', 'displayPrice']:
        if k in item:
            print(f"  {k}: {repr(item[k])}")
    
    # Address-related fields  
    print("\nADDRESS FIELDS:")
    for k in ['streetAddress', 'address', 'street', 'fullAddress', 'location', 'city', 'state', 'zip', 'zipCode', 'postalCode', 'latitude', 'longitude', 'lat', 'lng', 'geo']:
        if k in item:
            print(f"  {k}: {repr(item[k])}")
    
    # Check nested structures
    if 'listingDetail' in item:
        print("\n  NESTED listingDetail:")
        detail = item['listingDetail']
        for k in ['price', 'address', 'streetAddress', 'latitude', 'longitude']:
            if k in detail:
                print(f"    {k}: {repr(detail[k])}")
    
    if 'geo' in item:
        print(f"\n  GEO: {item['geo']}")
    
    if 'location' in item:
        print(f"\n  LOCATION: {item['location']}")

# Save raw data to file for reference
out_file = Path(__file__).parent.parent / "loopnet_raw_sample.json"
with open(out_file, 'w', encoding='utf-8') as f:
    json.dump(items, f, indent=2, default=str)
print(f"\nRaw data saved to: {out_file}")
