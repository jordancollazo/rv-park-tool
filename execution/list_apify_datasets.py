
import os
import sys
from apify_client import ApifyClient
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def list_datasets():
    token = os.getenv("APIFY_API_TOKEN")
    if not token:
        print("APIFY_API_TOKEN not found.")
        return

    client = ApifyClient(token)
    
    print("Fetching datasets...")
    # List datasets, sort by createdAt desc
    datasets = client.datasets().list(desc=True, limit=10)
    
    print(f"{'ID':<20} | {'Created At':<25} | {'Item Count':<10} | {'Name'}")
    print("-" * 80)
    
    for d in datasets.items:
        # Get stats (item count) - requires separate call or might be in list?
        # The list endpoint returns minimal info. created_at is datetime.
        created_at = d.get('createdAt')
        
        # To get item count, we often check stats, but let's just print basic info first.
        # We can try to fetch the dataset metadata to get itemCount.
        try:
            info = client.dataset(d['id']).get()
            count = info.get('itemCount', 0)
        except:
            count = "?"
            
        print(f"{d['id']:<20} | {str(created_at):<25} | {count:<10} | {d.get('name', 'N/A')}")

if __name__ == "__main__":
    list_datasets()
