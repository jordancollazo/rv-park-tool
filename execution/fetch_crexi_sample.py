"""Fetch sample Crexi data from recent run to see all fields."""
from apify_client import ApifyClient
import os
import json
from dotenv import load_dotenv

load_dotenv()

client = ApifyClient(os.getenv("APIFY_API_TOKEN"))

# Get recent runs
runs = list(client.actor("memo23/apify-crexi").runs().list().items)
print("Recent runs:")
for r in runs[:5]:
    print(f"  {r['id']}: {r['status']} - Dataset: {r.get('defaultDatasetId')}")

# Get the most recent successful run
for r in runs:
    if r['status'] == 'SUCCEEDED':
        ds_id = r.get('defaultDatasetId')
        if ds_id:
            print(f"\nFetching from dataset: {ds_id}")
            items = list(client.dataset(ds_id).iterate_items(limit=5))
            print(f"Got {len(items)} items")
            for i, item in enumerate(items):
                print(f"\n--- ITEM {i+1} ---")
                print(json.dumps(item, indent=2, default=str))
            break
