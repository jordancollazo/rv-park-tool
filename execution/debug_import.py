import sqlite3
import pandas as pd
import re

# Load xlsx
df = pd.read_excel('.tmp/500 leads test.xlsx')
print(f"Original: {len(df)} rows")

# Filter logic from import script
VALID_CATEGORIES = ['mobile home park', 'rv park', 'trailer park', 'manufactured home', 'campground', 'mobile home dealer']
INVALID_NAME_PATTERNS = [r'\bapartment', r'\bapts?\b', r'\brestaurant\b', r'\bgrill\b', r'\bbar\b', r'\bpizza\b', r'\bcafe\b', r'\bcoffee\b', r'\bhotel\b', r'\bmotel\b', r'\binn\b', r'\bself.?storage\b', r'\bstorage\b unit', r'\bwarehouse\b', r'\boffice\b', r'\bplaza\b', r'\bshopping\b', r'\bretail\b', r'\bbank\b', r'\bchurch\b', r'\bschool\b', r'\bhospital\b', r'\bclinic\b', r'\bdentist\b', r'\bdoctor\b', r'\bsalon\b', r'\bspa\b', r'\bgym\b', r'\bfitness\b']

valid_rows = []
for idx, row in df.iterrows():
    name = str(row.get('name', '')).lower()
    category = str(row.get('category', '')).lower()
    subtypes = str(row.get('subtypes', '')).lower()
    
    category_match = any(v in category or v in subtypes for v in VALID_CATEGORIES)
    name_keywords = ['mobile', 'rv', 'trailer', 'manufactured', 'mhp', 'campground']
    name_match = any(kw in name for kw in name_keywords)
    
    is_invalid = False
    for pattern in INVALID_NAME_PATTERNS:
        if re.search(pattern, name, re.IGNORECASE):
            is_invalid = True
            break
    
    if not is_invalid and (category_match or name_match):
        valid_rows.append(row)

print(f"After filter: {len(valid_rows)} valid rows")
df = pd.DataFrame(valid_rows)
print(f"DataFrame shape: {df.shape}")
print(f"Columns: {list(df.columns)}")
print(f"First row place_id: {df.iloc[0]['place_id'] if len(df) > 0 else 'N/A'}")

# Now check duplicates
conn = sqlite3.connect('data/leads.db')
cursor = conn.cursor()
cursor.execute("SELECT place_id FROM leads")
existing_ids = {row[0] for row in cursor.fetchall()}
print(f"\nExisting in DB: {len(existing_ids)}")

imported = 0
duplicates = 0
for _, row in df.iterrows():
    place_id = row.get("place_id")
    if place_id in existing_ids:
        duplicates += 1
    else:
        imported += 1
        existing_ids.add(place_id)

print(f"Would import: {imported}")
print(f"Duplicates: {duplicates}")
