
import json
import os
import sys
import sqlite3
import datetime

# Add execution dir to path to import db
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from execution import db

def ingest_recovered_data():
    conn = sqlite3.connect('data/leads.db')
    cursor = conn.cursor()
    
    print("--- Ingesting Recovered LoopNet Data ---")
    try:
        with open('loopnet_raw_sample.json', 'r', encoding='utf-8') as f:
            loopnet_data = json.load(f)
            
        count = 0
        for item in loopnet_data:
            # Map fields
            lead = {
                "loopnet_id": item.get("propertyId"),
                "name": item.get("address", "Unknown LoopNet Property"), # Use address as name if missing
                "address": item.get("address"),
                "city": item.get("city"),
                "state": item.get("state"),
                "zip": item.get("zip"),
                "asking_price": item.get("priceNumeric"),
                "cap_rate": item.get("capRate"),
                "loopnet_url": item.get("listingUrl"),
                "detailed_description": item.get("description"),
                "source_query": "LoopNet Scraper",
                "status": "not_contacted",
                "first_scraped_at": datetime.datetime.now().isoformat(),
                "last_scraped_at": datetime.datetime.now().isoformat(),
                # Defaults
                "list_price": item.get("priceNumeric"),
                "listing_status": item.get("listingType"),
                "sub_type": item.get("propertyTypeDetailed")
            }
            
            # Construct PLACE_ID (crucial for uniqueness)
            # Strategy: loopnet_{id}
            if lead["loopnet_id"]:
                lead["place_id"] = f"loopnet_{lead['loopnet_id']}"
            else:
                print("Skipping item without propertyId")
                continue

            # Upsert
            columns = ', '.join(lead.keys())
            placeholders = ', '.join(['?'] * len(lead))
            updates = ', '.join([f"{k}=excluded.{k}" for k in lead.keys()])
            
            sql = f"""
                INSERT INTO leads ({columns}) 
                VALUES ({placeholders})
                ON CONFLICT(place_id) DO UPDATE SET {updates}
            """
            
            cursor.execute(sql, list(lead.values()))
            count += 1
            
        print(f"✅ Ingested {count} LoopNet leads.")
        
    except FileNotFoundError:
        print("❌ loopnet_raw_sample.json not found.")
    except Exception as e:
        print(f"❌ Error importing LoopNet: {e}")


    print("\n--- Ingesting Recovered Crexi Data ---")
    try:
        with open('execution/tests/mock_crexi_data.json', 'r', encoding='utf-8') as f:
            crexi_data = json.load(f)
            
        count = 0
        for item in crexi_data:
            # Map fields
            lead = {
                "crexi_id": item.get("id"),
                "name": item.get("propertyName"),
                "address": item.get("address"),
                "city": item.get("city"),
                "state": item.get("state"),
                "zip": item.get("zip"),
                "latitude": item.get("latitude"),
                "longitude": item.get("longitude"),
                "asking_price": item.get("askingPrice"),
                "cap_rate": item.get("capRate"),
                "noi": item.get("noi"),
                "occupancy_rate": item.get("occupancy"),
                "lot_count": item.get("units"),
                "listing_url": item.get("url"),
                "detailed_description": item.get("description"),
                "scrape_source": "crexi",
                "status": "not_contacted",
                "first_scraped_at": datetime.datetime.now().isoformat(),
                "last_scraped_at": datetime.datetime.now().isoformat(),
                
                # Broker info
                "broker_name": item.get("broker", {}).get("name"),
                "broker_company": item.get("broker", {}).get("companyName"),
                "owner_phone": item.get("broker", {}).get("phone"), # Put broker phone in owner/contact phone for now or new col? Schema has broker_name/firm
                "owner_email": item.get("broker", {}).get("email")
            }
            
            # Construct PLACE_ID
            if lead["crexi_id"]:
                lead["place_id"] = f"crexi_{lead['crexi_id']}"
            else:
                continue

            # Upsert
            columns = ', '.join(lead.keys())
            placeholders = ', '.join(['?'] * len(lead))
            updates = ', '.join([f"{k}=excluded.{k}" for k in lead.keys()])
            
            sql = f"""
                INSERT INTO leads ({columns}) 
                VALUES ({placeholders})
                ON CONFLICT(place_id) DO UPDATE SET {updates}
            """
            
            cursor.execute(sql, list(lead.values()))
            count += 1
            
        print(f"✅ Ingested {count} Crexi leads.")

    except FileNotFoundError:
        print("❌ execution/tests/mock_crexi_data.json not found.")
    except Exception as e:
        print(f"❌ Error importing Crexi: {e}")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    ingest_recovered_data()
