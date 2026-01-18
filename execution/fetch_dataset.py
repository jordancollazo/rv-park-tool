"""Fetch dataset from a completed or running Apify run."""
from apify_client import ApifyClient
import os
import json
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("APIFY_API_TOKEN")
RUN_ID = "piZXaHtCf17Cg3blL"

def fetch():
    client = ApifyClient(TOKEN)
    run = client.run(RUN_ID).get()
    
    print(f"Run Status: {run.get('status')}")
    
    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        print("No dataset ID found.")
        return
    
    print(f"Dataset ID: {dataset_id}")
    items = list(client.dataset(dataset_id).iterate_items())
    print(f"Items found: {len(items)}")
    
    if items:
        print("\nSample items:")
        for item in items[:3]:
            print(json.dumps(item, indent=2, default=str)[:500])
            print("---")

if __name__ == "__main__":
    fetch()
