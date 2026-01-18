import requests
import sys

def verify_api():
    try:
        # Get all leads
        r = requests.get('http://localhost:5000/api/leads')
        if r.status_code != 200:
            print(f"FAILED: /api/leads returned {r.status_code}")
            return
        
        leads = r.json()
        print(f"SUCCESS: Retrieved {len(leads)} leads.")
        
        # Debug: Check for availability of keys
        if leads:
            print("Keys in first lead:", leads[0].keys())

        # Find specific lead
        target = [l for l in leads if 'Deerfield' in l.get('name', '')]
        if target:
            print(f"Found Deerfield: {target[0]}")
        else:
            print("Deerfield not found in API response.")

        # Find enriched lead
        enriched = [l for l in leads if l.get('amenity_score') is not None]
        print(f"Enriched leads count: {len(enriched)}")


    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    verify_api()
