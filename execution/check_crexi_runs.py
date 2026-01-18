
import os
from apify_client import ApifyClient
from dotenv import load_dotenv

load_dotenv()

def check_crexi_runs():
    token = os.getenv("APIFY_API_TOKEN")
    client = ApifyClient(token)
    actor_id = "memo23/apify-crexi"
    
    print(f"Checking runs for actor: {actor_id}...")
    try:
        # Check if actor exists first (optional, but good for debugging)
        try:
            actor = client.actor(actor_id).get()
            print(f"Actor found: {actor.get('title')} ({actor.get('id')})")
        except Exception as e:
            print(f"Actor lookup warning: {e}")

        # List runs for this actor
        runs = client.actor(actor_id).runs().list(desc=True, limit=100)
        
        if not runs.items:
            print("No runs found for this actor.")
            return

        print(f"{'Run ID':<20} | {'Started At':<25} | {'Status':<10} | {'Dataset ID'}")
        print("-" * 80)
        
        for r in runs.items:
            print(f"{r['id']:<20} | {str(r.get('startedAt')):<25} | {r.get('status'):<10} | {r.get('defaultDatasetId')}")
            
    except Exception as e:
        print(f"Error fetching runs: {e}")

if __name__ == "__main__":
    check_crexi_runs()
