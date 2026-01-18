
import os
import sys
import json
from apify_client import ApifyClient
from dotenv import load_dotenv

load_dotenv()

def fetch_dataset(dataset_id):
    token = os.getenv("APIFY_API_TOKEN")
    if not token:
        print("APIFY_API_TOKEN not found.")
        return

    client = ApifyClient(token)
    
    print(f"Fetching items from dataset: {dataset_id}...")
    try:
        # Fetch items
        items = client.dataset(dataset_id).list_items().items
        
        print(f"Found {len(items)} items.")
        if items:
            # Print sample of first item to check structure
            print("\n--- Sample Item ---")
            print(json.dumps(items[0], indent=2))
            
            # Save to temp file for ingestion
            outfile = f"apify_dataset_{dataset_id}.json"
            with open(outfile, 'w', encoding='utf-8') as f:
                json.dump(items, f, indent=2)
            print(f"\nSaved full dataset to {outfile}")
            
    except Exception as e:
        print(f"Error fetching dataset: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python fetch_apify_dataset.py <dataset_id>")
    else:
        fetch_dataset(sys.argv[1])
