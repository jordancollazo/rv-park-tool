
import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("APIFY_API_TOKEN")

def fetch_run_results(run_id):
    if not TOKEN:
        print("No token")
        return

    print(f"Fetching run info for: {run_id}")
    run_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={TOKEN}"
    try:
        resp = requests.get(run_url)
        data = resp.json()['data']
        dataset_id = data['defaultDatasetId']
        print(f"Dataset ID: {dataset_id}")
        
        items_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={TOKEN}&format=json"
        items_resp = requests.get(items_url)
        items = items_resp.json()
        print(f"Items found: {len(items)}")
        if items:
            print("First item sample:")
            print(json.dumps(items[0], indent=2))
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fetch_run_results("sYkluvqX8Q6ATKSQb")
