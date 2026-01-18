
import os
import sys
from apify_client import ApifyClient
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def list_runs():
    token = os.getenv("APIFY_API_TOKEN")
    if not token:
        print("APIFY_API_TOKEN not found.")
        return

    client = ApifyClient(token)
    
    print("Fetching recent runs...")
    # List runs
    try:
        runs = client.runs().list(desc=True, limit=500)
        
        print(f"{'Run ID':<20} | {'Started At':<25} | {'Status':<10} | {'Actor Name'}")
        print("-" * 120)
        
        actor_names = {}
        target_keywords = ['loopnet', 'crexi']
        
        found_any = False
        
        for r in runs.items:
            act_id = r.get('actId')
            if act_id not in actor_names:
                try:
                    actor_obj = client.actor(act_id).get()
                    if actor_obj:
                        actor_names[act_id] = actor_obj.get('title', actor_obj.get('name', act_id))
                    else:
                        actor_names[act_id] = act_id
                except:
                    actor_names[act_id] = "Unknown/Del"
            
            actor_name = actor_names[act_id]
            
            # Filter
            is_relevant = any(k in actor_name.lower() for k in target_keywords)
            
            if is_relevant:
                found_any = True
                print(f"FOUND MATCH: {actor_name}")
                print(f"Run ID: {r['id']}")
                print(f"Dataset ID: {r.get('defaultDatasetId')}")
                print(f"Started: {r.get('startedAt')}")
                print("-" * 40)
        
        if not found_any:
            print("No LoopNet or Crexi runs found in last 500.")
            
    except Exception as e:
        print(f"Error fetching runs: {e}")

if __name__ == "__main__":
    list_runs()
