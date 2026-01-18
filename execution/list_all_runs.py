
import os
import requests
from datetime import datetime

# Token from .env
# We'll read it manually or rely on os.environ if loaded, but let's read file to be safe
try:
    with open('.env', 'r', encoding='utf-8') as f:
        for line in f:
            if line.startswith('APIFY_API_TOKEN='):
                token = line.split('=')[1].strip()
                break
except Exception:
    token = None

if not token:
    print("Error: Could not read APIFY_API_TOKEN from .env")
    exit(1)

URL = f"https://api.apify.com/v2/acts/runs?token={token}&desc=1&limit=50"

def list_all_runs():
    print(f"Fetching last 50 runs for token: {token[:10]}...")
    resp = requests.get(URL)
    
    if resp.status_code != 200:
        print(f"Error: {resp.status_code} - {resp.text}")
        return

    data = resp.json()
    items = data.get('data', {}).get('items', [])
    
    print(f"Found {len(items)} runs.\n")
    print(f"{'Run ID':<20} | {'Status':<10} | {'Started At':<25} | {'Dataset ID':<20} | {'Actor/Task Name'}")
    print("-" * 110)
    
    for run in items:
        run_id = run.get('id')
        status = run.get('status')
        started = run.get('startedAt')
        dataset_id = run.get('defaultDatasetId')
        
        # Try to get actor/task name (basic)
        actor_id = run.get('actId')
        
        # We won't fetch actor details to keep it fast, just print ID/Status
        print(f"{run_id:<20} | {status:<10} | {started:<25} | {dataset_id:<20} | {actor_id}")

if __name__ == "__main__":
    list_all_runs()
