
import json
import os
import sys
from datetime import datetime, timezone

import db

def ingest_loopnet_cloud(file_path):
    print(f"Ingesting LoopNet data from {file_path}...")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        items = json.load(f)
        
    print(f"Found {len(items)} items.")
    
    leads_to_upsert = []
    
    for item in items:
        # Basic validation
        if not item.get("propertyId"):
            continue
            
        loopnet_id = str(item.get("propertyId"))
        place_id = f"loopnet:{loopnet_id}"
        
        # Map fields
        lead = {
            "place_id": place_id,
            "loopnet_id": loopnet_id,
            "scrape_source": "loopnet",
            
            # Core Info
            "name": item.get("address") or "Unknown Property", # Use address as name if name missing
            "address": item.get("address"),
            "city": item.get("city"),
            "state": item.get("state"),
            "zip": item.get("zip"),
            
            # Financials
            "asking_price": item.get("priceNumeric"),
            "cap_rate": item.get("capRate"),
            # item.get("price") is string like "$1,000,000", numeric is safer
            
            # Details
            "description": item.get("description"),
            "listing_url": item.get("listingUrl"),
            "property_type": item.get("propertyType"),
            "sq_ft": item.get("squareFootage"),
            
            # Broker
            "broker_company": item.get("brokerCompany"),
            
            # Metadata
            "last_scraped_at": datetime.now(timezone.utc).isoformat(),
            "first_scraped_at": datetime.now(timezone.utc).isoformat(), # Defaulting for recovery
            "status": "not_contacted"
        }
        
        # Add to list
        leads_to_upsert.append(lead)
        
    print(f"Prepared {len(leads_to_upsert)} leads for upsert.")
    
    if leads_to_upsert:
        inserted = 0
        updated = 0
        print(f"Upserting {len(leads_to_upsert)} leads...")
        for lead in leads_to_upsert:
            try:
                db.upsert_lead(lead)
                inserted += 1
            except Exception as e:
                print(f"Error upserting lead {lead.get('place_id')}: {e}")
                
        print(f"Success! Processed: {inserted}")
    else:
        print("No valid leads to upsert.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python execution/ingest_apify_cloud.py <path_to_json>")
    else:
        ingest_loopnet_cloud(sys.argv[1])
