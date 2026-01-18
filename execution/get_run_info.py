
import os
import sys
from apify_client import ApifyClient
from dotenv import load_dotenv

load_dotenv()

def get_run_info(run_id):
    token = os.getenv("APIFY_API_TOKEN")
    client = ApifyClient(token)
    
    print(f"Fetching run info: {run_id}...")
    try:
        run = client.run(run_id).get()
        if run:
            print(f"Status: {run.get('status')}")
            print(f"Dataset ID: {run.get('defaultDatasetId')}")
            print(f"Started At: {run.get('startedAt')}")
        else:
            print("Run not found.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        get_run_info(sys.argv[1])
