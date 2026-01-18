
from apify_client import ApifyClient
import os
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("APIFY_API_TOKEN")
RUN_ID = "SXtDLAEuRtZMZNb0f"

def check():
    client = ApifyClient(TOKEN)
    run = client.run(RUN_ID).get()
    print(f"Run Status: {run.get('status')}")
    
    dataset_id = run.get("defaultDatasetId")
    if dataset_id:
        dataset = client.dataset(dataset_id).get()
        print(f"Dataset Items: {dataset.get('itemCount')}")
    else:
        print("No dataset ID found yet.")
    
    print("\n--- Recent Log ---")
    try:
        log = client.log(RUN_ID).get()
        print(log[-1000:] if log else "No log available.")
    except Exception as e:
        print(f"Could not fetch log: {e}")

if __name__ == "__main__":
    check()
