
import os
import json
import argparse
import logging
from datetime import datetime
from apify_client import ApifyClient
from dotenv import load_dotenv
from pathlib import Path
from db import get_db, upsert_lead, get_lead_by_id

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment
load_dotenv()

APIFY_TOKEN = os.getenv("APIFY_API_TOKEN")

def normalize_lead(item):
    """
    Map LoopNet JSON to our database schema.
    """
    import re
    
    # Keys based on 'memo23/apify-loopnet-search-cheerio' output
    
    # ID and URL
    loopnet_id = item.get("propertyId") or item.get("id")
    loopnet_url = item.get("listingUrl") or item.get("url") or item.get("propertyUrl")

    # Address logic
    # The 'address' field often contains garbage like "1919 30th Ave N16,455 SF Specialty"
    # We need to extract just the street address portion
    raw_address = item.get("streetAddress") or item.get("address") or ""
    
    # Clean the address: remove patterns like "16,455 SF Specialty" or similar
    # Pattern: digits followed by comma/digits, then "SF" or square footage info
    street = re.sub(r'\d{1,3},?\d{3}\s*SF.*$', '', raw_address, flags=re.IGNORECASE).strip()
    # Also remove trailing property type keywords
    street = re.sub(r'\s*(Multifamily|Specialty|Industrial|Retail|Office|Land|Commercial)\s*$', '', street, flags=re.IGNORECASE).strip()
    
    # If street is still empty or too short, try extracting from URL
    # URL format: https://www.loopnet.com/Listing/1919-30th-Ave-N-Saint-Petersburg-FL/38265685/
    if (not street or len(street) < 5) and loopnet_url:
        url_match = re.search(r'/Listing/([^/]+)-([A-Z]{2})/\d+', loopnet_url)
        if url_match:
            # Extract address part from URL and convert dashes to spaces
            url_address = url_match.group(1).replace('-', ' ')
            street = url_address
    
    city = item.get("city", "")
    state = item.get("state", "")
    zip_code = item.get("zip", "")

    # If still no URL but we have an ID, construct it from address components
    if not loopnet_url and loopnet_id:
        if street and city and state:
            # Create URL-friendly slugs
            street_slug = street.lower().replace(' ', '-').replace(',', '')
            # Remove special characters from street slug
            street_slug = re.sub(r'[^a-z0-9\-]', '', street_slug)
            street_slug = re.sub(r'-+', '-', street_slug).strip('-')

            city_slug = city.lower().replace(' ', '-')
            city_slug = re.sub(r'[^a-z0-9\-]', '', city_slug)
            city_slug = re.sub(r'-+', '-', city_slug).strip('-')

            state_clean = state.strip().upper()

            # Only construct full URL if we have valid components
            if street_slug and city_slug and len(state_clean) == 2:
                loopnet_url = f"https://www.loopnet.com/Listing/{street_slug}-{city_slug}-{state_clean}/{loopnet_id}/"
                logger.info(f"Generated full LoopNet URL from address: {loopnet_url}")

        # Fallback to minimal URL if address construction failed
        if not loopnet_url:
            loopnet_url = f"https://www.loopnet.com/Listing/{loopnet_id}/"
            logger.info(f"Generated minimal LoopNet URL from ID: {loopnet_url}")

    # If street was extracted from URL, it may include the city name at the end - remove it
    if city and street.lower().endswith(' ' + city.lower()):
        street = street[:-len(city)-1].strip()
    
    full_address = f"{street}, {city}, {state} {zip_code}".strip()
    
    # Name/Title - prefer a real property name, fall back to address
    title = item.get("propertyName") or item.get("title") or full_address
    
    # Financials - USE priceNumeric first (integer), fall back to parsing price string
    price = None
    
    # First try the numeric field (best source)
    if item.get("priceNumeric"):
        try:
            price = float(item["priceNumeric"])
        except (ValueError, TypeError):
            pass
    
    # Fall back to parsing the price string
    if price is None:
        price_str = item.get("price") or item.get("askingPrice")
        if price_str:
            try:
                # handle "$1,500,000" or similar - extract just the numeric part
                clean_price = "".join(c for c in str(price_str) if c.isdigit() or c == '.')
                price = float(clean_price) if clean_price else None
            except ValueError:
                price = None


    cap_rate = item.get("capRate")
    if cap_rate:
        try:
            # handle "6.5%"
            if isinstance(cap_rate, str):
                cap_rate = float(cap_rate.replace('%', ''))
            else:
                cap_rate = float(cap_rate)
        except ValueError:
            cap_rate = None
            
    units = item.get("numberOfUnits") or item.get("units")
    
    # Broker Info - scraper seems to have top-level keys
    broker_name = item.get("brokerName")
    broker_firm = item.get("brokerCompany")
    
    # Fallback to agents array if top-level missing
    if not broker_name:
        agents = item.get("listingAgents", [])
        if agents and isinstance(agents, list) and len(agents) > 0:
            broker_name = agents[0].get("name")
            broker_firm = agents[0].get("company")
    
    # Property Type Filtering
    # Since we are using a strict URL provided by the user (/mobile-home-parks/),
    # we can trust the source more and be less strict on the 'propertyType' field
    # which often comes back generically as "Multifamily" or "Commercial".
    
    valid_keywords = ["mobile home", "manufactured", "rv park", "trailer", "campground", "resort", "multifamily", "commercial", "land"]
    
    prop_type = item.get("propertyType", "").lower()
    prop_subtype = item.get("propertySubType", "").lower()
    
    # We will log what we find but accept most things to match the user's manual count of ~68
    # unless it's clearly wrong (e.g. Office/Retail not in our list? explicit "Office"?)
    # For now, let's keep the valid_keywords broad or just rely on the search functionality.
    
    is_valid = False
    combined_type = f"{prop_type} {prop_subtype}"
    
    for kw in valid_keywords:
        if kw in combined_type:
            is_valid = True
            break
            
    # Also check title/description if type is generic
    if not is_valid:
        combined_text = f"{title} {item.get('description', '')}".lower()
        for kw in valid_keywords:
            if kw in combined_text:
                is_valid = True
                break
    
    # If still not valid, just log it but maybe KEEP it if it looks ambiguous? 
    # The user is very sure of their link. Let's filter only obvious disconnects.
    # Actually, let's just use the flexible keywords above.
    
    if not is_valid:
        # One last check: If the URL itself contains mobile-home-parks (which it defaults to in search)
        # proceed.
        pass
        
    # Detailed Extraction
    # These fields might come from top level or 'listingDetail' or 'data' sub-object depending on scraper version
    
    year_built = item.get("yearBuilt") or item.get("year_built")
    if not year_built:
         # Try nested structures if they exist
         details = item.get("listingDetail") or {}
         year_built = details.get("yearBuilt")

    building_size = item.get("buildingSize") or item.get("buildingArea") or item.get("squareFootage")
    lot_size = item.get("lotSize") or item.get("lotArea")
    num_units = item.get("numberOfUnits") or units
    
    description = item.get("description", "")
    detailed_desc = item.get("longDescription") or item.get("detailedDescription") or description
    
    # Try to find price if missing
    if not price:
        price = item.get("salePrice") or item.get("askingPrice")
        if not price and "listingDetail" in item:
            price = item["listingDetail"].get("salePrice") or item["listingDetail"].get("askingPrice")
            
    return {
        "loopnet_id": loopnet_id,
        "loopnet_url": loopnet_url,
        "name": title,
        "address": street,
        "city": city,
        "state": state,
        "zip": zip_code,
        "list_price": price,
        "cap_rate": cap_rate,
        "noi": item.get("noi") or item.get("netOperatingIncome"),
        "occupancy_rate": item.get("occupancy") or item.get("occupancyRate"),
        "broker_name": broker_name,
        "broker_firm": broker_firm,
        "description_keywords": f"{item.get('propertyType')} - {item.get('propertySubType')}", 
        "listing_status": "Active",
        "place_id": f"loopnet:{loopnet_id}" if loopnet_id else None,
        "source_query": "LoopNet Scraper",
        "area": f"{city}, {state}",
        "site_score_1_10": 5, 
        "crawl_status": "scraped_loopnet",
        # Detailed fields
        "year_built": year_built,
        "building_size": building_size,
        "lot_size": lot_size,
        "detailed_description": detailed_desc
    }

def run_scraper(state="FL", limit=20):
    if not APIFY_TOKEN:
        logger.error("APIFY_API_TOKEN not found in environment.")
        return

    client = ApifyClient(APIFY_TOKEN)
    
    # Prepare Actor Input
    # Using memo23/apify-loopnet-search-cheerioa as planned
    # Try specific MHP path provided by user
    search_urls = [
        "https://www.loopnet.com/search/mobile-home-parks/fl/for-sale/?view=map"
    ]
    
    run_input = {
        "startUrls": [{"url": url} for url in search_urls],
        "maxItems": limit,
        "proxyConfiguration": {
            "useApifyProxy": True
        },
        "includeListingDetails": True,
    }
    
    logger.info(f"Starting Apify Actor run with input: {search_urls}")
    
    # Run the actor
    # Note: user might need to subscribe to the actor. 
    # If this fails, we catch it.
    try:
        run = client.actor("memo23/apify-loopnet-search-cheerio").call(run_input=run_input)
    except Exception as e:
        logger.error(f"Failed to start actor: {e}")
        import traceback
        traceback.print_exc()
        # Also print attributes if it's an ApifyApiError
        if hasattr(e, 'message'):
            print(f"Error Message: {e.message}")
        return

    logger.info(f"Actor run finished. Run info: {run}")
    
    dataset_items = []
    try:
        # Fetch results
        dataset_id = run["defaultDatasetId"]
        logger.info(f"Fetching items from Dataset ID: {dataset_id}")
        dataset_items = client.dataset(dataset_id).list_items().items
        logger.info(f"Fetched {len(dataset_items)} items from dataset.")
        
        # --- HARDCODING / BACKUP STRATEGY ---
        if dataset_items:
            # Save raw data to backups/ directory to ensure permanence
            backup_dir = Path("backups")
            backup_dir.mkdir(exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = backup_dir / f"loopnet_raw_{timestamp}.json"
            
            logger.info(f"Writing backup to {backup_file}...")
            with open(backup_file, "w", encoding="utf-8") as f:
                json.dump(dataset_items, f, indent=2, default=str)
                f.flush()
            logger.info(f"PERMANENT BACKUP SAVED: {backup_file}")
            
            # Log sample
            logger.info(f"First item sample: {json.dumps(dataset_items[0], default=str)[:1000]}")
        else:
            logger.warning("No items found in dataset.")
        # ------------------------------------

    except Exception as e:
        logger.error(f"CRITICAL ERROR FETCHING/BACKING UP DATA: {e}")
        import traceback
        traceback.print_exc()
        # Even if fetch failed, we can't ingest (unless we continue with empty list?)
        # return # Actually let's continue so we see 0 inserted logs
        pass

    inserted = 0
    updated = 0
    
    if dataset_items:
        logger.info(f"First item keys: {list(dataset_items[0].keys())}")
        
        # --- HARDCODING / BACKUP STRATEGY ---
        # Save raw data to backups/ directory to ensure permanence
        backup_dir = Path("backups")
        backup_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = backup_dir / f"loopnet_raw_{timestamp}.json"
        
        with open(backup_file, "w", encoding="utf-8") as f:
            json.dump(dataset_items, f, indent=2, default=str)
        logger.info(f"PERMANENT BACKUP SAVED: {backup_file}")
        # ------------------------------------

        # Log a snippet of the first item to see where details are hid
        import json
        logger.info(f"First item sample: {json.dumps(dataset_items[0], default=str)[:1000]}")
    
    for item in dataset_items:
        lead_data = normalize_lead(item)
        
        if not lead_data or not lead_data.get("loopnet_id"):
            prop_type = item.get("propertyType", "N/A")
            prop_subtype = item.get("propertySubType", "N/A")
            logger.info(f"Skipping item: {item.get('title')} (Type: {prop_type} - {prop_subtype})")
            continue
            
        try:
            # We use a custom upsert logic here or rely on the db one?
            # db.upsert_lead relies on place_id.
            # We generated a temp place_id: loopnet:{id}
            # Ideally we should try to match existing by address first?
            # For now, let's just insert/update based on our generated ID.
            # Real matching would happen in a separate normalization step or complex upsert.
            
            lead_id = upsert_lead(lead_data)
            # If we want to enrich specific fields that upsert_leads doesn't cover by default (like LoopNet fields)
            # upsert_lead does accept them if we modify it or call update_lead_fields after.
            # db.upsert_lead calls update_lead_fields for enrichment keys if passed.
            # We added LoopNet fields to db.py whitelist, so passing them in lead_data should work!
            # Wait, upsert_lead logic:
            # It extracts specific fields for the UPDATE, and then checks for 'enrichment_fields'.
            # I need to make sure upsert_lead handles passing these extras.
            # Looking at db.py, it checks keys: 'owner_fatigue...', 'social_...'.
            # It does NOT automatically pass all unknown keys to update_lead_fields.
            # So I should manually call update_lead_fields after upsert.
            
            # The custom fields we added:
            extras = {k: v for k, v in lead_data.items() if k in [
                'loopnet_id', 'loopnet_url', 'listing_status', 'list_price', 
                'cap_rate', 'noi', 'occupancy_rate', 'broker_name', 
                'broker_firm', 'description_keywords',
                'year_built', 'building_size', 'lot_size', 'detailed_description'
            ]}
            
            from db import update_lead_fields
            update_lead_fields(lead_id, **extras)
            
            updated += 1 
        except Exception as e:
            logger.error(f"Error processing item {item.get('id')}: {e}")
            
    logger.info(f"Processing complete. Processed {inserted + updated} leads.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape LoopNet leads.")
    parser.add_argument("--state", default="FL", help="State to scrape (default: FL)")
    parser.add_argument("--limit", type=int, default=20, help="Max items to scrape")
    
    args = parser.parse_args()
    
    run_scraper(state=args.state, limit=args.limit)
