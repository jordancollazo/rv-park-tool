
import sqlite3
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from crawl_website import crawl_website
from db import get_db, update_lead_fields, upsert_lead

def get_unenriched_leads(limit=50):
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM leads 
            WHERE website != '' 
            AND website IS NOT NULL 
            AND (is_enriched IS NULL OR is_enriched = 0)
            LIMIT ?
        """, (limit,)).fetchall()
    return [dict(row) for row in rows]

def enrich_lead(lead):
    url = lead.get("website")
    print(f"Enriching: {lead['name']} ({url})")
    
    # Run crawler (enhanced version)
    crawl_data = crawl_website(url)
    
    updates = {
        "is_enriched": 1
    }
    
    # Extraction map
    fields = ["social_facebook", "social_instagram", "social_linkedin", "owner_name"]
    found_any = False
    
    for field in fields:
        val = crawl_data.get(field)
        if val:
            updates[field] = val
            found_any = True
            
    # Save to DB
    update_lead_fields(lead["id"], **updates)
    
    status = "Found data" if found_any else "No extra data"
    print(f"  -> {status}")
    return lead["name"], status

def main():
    print("Starting enrichment process...")
    leads = get_unenriched_leads(limit=100) # Process batches
    
    if not leads:
        print("No leads to enrich.")
        return

    print(f"Found {len(leads)} leads to enrich.")
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(enrich_lead, lead): lead for lead in leads}
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error: {e}")

    print("\nEnrichment complete.")

if __name__ == "__main__":
    main()
