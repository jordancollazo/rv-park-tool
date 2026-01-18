"""Display 5 sample Crexi leads in a clean format."""
import json

with open('.tmp/sample5.json') as f:
    data = json.load(f)

print("=" * 80)
print("5 SAMPLE CREXI LEADS - FULL DATA EXTRACTION")
print("=" * 80)

for i, item in enumerate(data):
    loc = item.get('locations', [{}])
    primary_loc = loc[0] if loc else {}
    state = primary_loc.get('state', {})
    state_code = state.get('code') if isinstance(state, dict) else state
    
    print(f"\n--- LEAD {i+1} ---")
    print(f"  Name: {item.get('name')}")
    print(f"  Types: {item.get('types')}")
    print(f"  Address: {primary_loc.get('address')}")
    print(f"  City: {primary_loc.get('city')}")
    print(f"  State: {state_code}")
    print(f"  ZIP: {primary_loc.get('zip')}")
    print(f"  Full Address: {primary_loc.get('fullAddress')}")
    print(f"  Lat/Long: {primary_loc.get('latitude')}, {primary_loc.get('longitude')}")
    print(f"  ---")
    print(f"  Asking Price: ${item.get('askingPrice'):,}" if item.get('askingPrice') else "  Asking Price: N/A")
    print(f"  Lot Size: {item.get('lotSizeAcres')} acres")
    print(f"  # Units: {item.get('numberOfUnits')}")
    print(f"  Cap Rate: {item.get('capRate')}")
    print(f"  NOI: {item.get('netOperatingIncome')}")
    print(f"  ---")
    print(f"  Brokerage: {item.get('brokerageName')}")
    print(f"  URL Slug: {item.get('urlSlug')}")
