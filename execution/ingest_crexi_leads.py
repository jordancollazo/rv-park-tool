"""
ingest_crexi_leads.py

Orchestrates the scraping of Crexi.com via Apify and ingests the results into the leads database.
Handles identity resolution (defaulting to synthetic 'crexi:' IDs) and field mapping.

Usage:
    python execution/ingest_crexi_leads.py --search-url "https://www.crexi.com/properties?types%5B%5D=Mobile%20Home%20Park&counties%5B%5D=Florida"
    python execution/ingest_crexi_leads.py --dataset-id "your-dataset-id"
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# Try to import ApifyClient, warn if missing
try:
    from apify_client import ApifyClient
except ImportError:
    print("ERROR: apify-client not installed. Run: pip install apify-client")
    sys.exit(1)

from dotenv import load_dotenv

import db

# Load environment variables
load_dotenv()

ACTOR_ID = "memo23/apify-crexi"


def slugify(text: str) -> str:
    """Convert text to URL-friendly slug (lowercase, hyphens, no special chars)."""
    if not text:
        return ""
    # Lowercase
    text = text.lower()
    # Replace spaces and underscores with hyphens
    text = re.sub(r'[\s_]+', '-', text)
    # Remove any characters that aren't alphanumeric or hyphens
    text = re.sub(r'[^a-z0-9-]', '', text)
    # Collapse multiple hyphens
    text = re.sub(r'-+', '-', text)
    # Strip leading/trailing hyphens
    text = text.strip('-')
    return text


def build_crexi_url(crexi_id: str, state: str = None, name: str = None) -> str:
    """Build a proper Crexi listing URL with slug."""
    # Build the slug: state-property-name
    parts = []
    if state:
        parts.append(slugify(state))
    if name:
        parts.append(slugify(name))
    
    slug = "-".join(parts) if parts else "property"
    
    return f"https://www.crexi.com/properties/{crexi_id}/{slug}"


def get_apify_client() -> ApifyClient:
    """Initialize Apify client."""
    token = os.getenv("APIFY_API_TOKEN")
    if not token:
        print("ERROR: APIFY_API_TOKEN not found in environment.")
        sys.exit(1)
    return ApifyClient(token)


# Keywords indicating MHP/RV parks (positive signals)
MHP_RV_POSITIVE_KEYWORDS = [
    "mobile home", "mobile-home", "mobilehome",
    "manufactured home", "manufactured housing",
    "mhp", "mhc",  # Mobile Home Park/Community
    "rv park", "rv resort", "rv community",
    "trailer park", "trailer court",
    "camp ground", "campground",
]

# Keywords indicating NOT an MHP/RV (negative signals)  
NON_MHP_RV_KEYWORDS = [
    "retail", "restaurant", "office", "warehouse", "industrial",
    "grocery", "market", "produce", "liquor", "store", "shop",
    "hotel", "motel", "apartment", "condo", "townhouse",
    "medical", "dental", "clinic", "hospital",
    "church", "school", "daycare",
    "gas station", "car wash", "auto",
    "land only", "vacant land", "raw land",
    "self storage", "mini storage", "self-storage",
]

# Crexi type field values that indicate MHP/RV
VALID_CREXI_TYPES = [
    "mobile home park", "manufactured housing",
    "rv park", "rv resort",
    "multifamily - mobile home",
    "specialty - rv",
    "specialty - mobile",
]


def is_mhp_or_rv_park(item: dict) -> tuple[bool, str | None]:
    """
    Validate if a Crexi item is genuinely an MHP or RV park.
    
    AGGRESSIVE filter - requires explicit MHP/RV signals in the name or type.
    
    Returns:
        tuple: (is_valid: bool, category: str or None)
            - is_valid: True if this appears to be a legitimate MHP/RV park
            - category: 'Mobile Home Park' or 'RV Park' or None if invalid
    """
    # 0. Check 'types' list (New Schema)
    types_list = item.get("types")
    if types_list and isinstance(types_list, list):
        # MHP keywords
        mhp_keywords = ["mobile home", "mobile-home", "mobilehome", "mhp", "mhc", 
                        "manufactured", "trailer park", "trailer court"]
        # RV keywords
        rv_keywords = ["rv park", "rv resort", "rv community", "campground", "camp ground"]
        
        # User explicitly requested Multifamily in targeted scrape
        other_allowed = ["multifamily"]

        for t in types_list:
            t_lower = str(t).lower()
            if any(kw in t_lower for kw in mhp_keywords):
                return True, "Mobile Home Park"
            if any(kw in t_lower for kw in rv_keywords):
                return True, "RV Park"
            if any(kw in t_lower for kw in other_allowed):
                return True, "Multifamily"

    # Gather text to analyze
    prop_name = str(item.get("propertyName", "") or item.get("name", "")).lower()
    description = str(item.get("description", "")).lower()
    
    # Also check details.name if available
    details = item.get("details", {}) or {}
    details_name = str(details.get("name", "")).lower()
    
    # Combined name for checking
    full_name = f"{prop_name} {details_name}"
    
    # MHP keywords - if found in name, it's a Mobile Home Park
    mhp_keywords = ["mobile home", "mobile-home", "mobilehome", "mhp", "mhc", 
                    "manufactured", "trailer park", "trailer court"]
    # RV keywords - if found in name, it's an RV Park
    rv_keywords = ["rv park", "rv resort", "rv community", "campground", "camp ground"]
    
    # Check name for positive keywords (most reliable)
    has_mhp_signal = any(kw in full_name for kw in mhp_keywords)
    has_rv_signal = any(kw in full_name for kw in rv_keywords)
    
    if has_mhp_signal:
        return (True, "Mobile Home Park")
    if has_rv_signal:
        return (True, "RV Park")
    
    # Check description for strong signals if name doesn't have them
    strong_mhp_signals = ["mobile home park", "trailer park", "manufactured housing community"]
    strong_rv_signals = ["rv park", "rv resort", "campground"]
    
    for signal in strong_mhp_signals:
        if signal in description:
            return (True, "Mobile Home Park")
    
    for signal in strong_rv_signals:
        if signal in description:
            return (True, "RV Park")
    
    # No strong signals - reject this lead
    return (False, None)


def resolve_place_id(item: dict) -> str:
    """
    Resolve a unique place_id for the lead.
    
    Strategy:
    1. Future: Use address/coordinates to query Google Places API.
    2. Current: Generate synthetic ID from Crexi ID.
    """
    crexi_id = item.get("id")
    if not crexi_id:
        # Fallback if no ID (shouldn't happen on Crexi)
        return f"crexi:unknown:{datetime.now().timestamp()}"
    
    return f"crexi:{crexi_id}"


def normalize_crexi_item(item: dict, category: str) -> dict:
    """Map Crexi output fields to our database schema.
    
    Args:
        item: Raw Crexi data item
        category: Pre-validated category ('Mobile Home Park' or 'RV Park')
    """
    
    place_id = resolve_place_id(item)
    
    # Extract specifics
    broker = item.get("broker", {}) or {}
    
    # Extract location from locations array (Crexi returns address info here)
    locations = item.get("locations", [])
    primary_location = locations[0] if locations else {}
    
    # Get state code from nested state object
    state_obj = primary_location.get("state", {})
    state_code = state_obj.get("code") if isinstance(state_obj, dict) else state_obj
    
    # Get address/city/zip from location
    address = primary_location.get("address") or item.get("address")
    city = primary_location.get("city") or item.get("city")
    zip_code = primary_location.get("zip") or item.get("zip")
    latitude = primary_location.get("latitude") or item.get("latitude")
    longitude = primary_location.get("longitude") or item.get("longitude")
    full_address = primary_location.get("fullAddress", "")
    
    # Get property name from details if available, otherwise from item
    details = item.get("details", {}) or {}
    property_name = details.get("name") or item.get("propertyName") or item.get("name") or address or "Unknown Property"
    
    return {
        "place_id": place_id,
        "crexi_id": str(item.get("id")),
        "scrape_source": "crexi",
        
        # Basic Info
        "name": property_name,
        "address": address,
        "city": city,
        "state": state_code,
        "zip": zip_code,
        "latitude": latitude,
        "longitude": longitude,
        
        # Financials
        "asking_price": item.get("askingPrice"),
        "cap_rate": item.get("capRate"),
        "noi": item.get("noi"),
        "occupancy": item.get("occupancy"),
        # Calculate price per unit if possible
        "price_per_unit": (item.get("askingPrice") / item.get("units")) if (item.get("askingPrice") and item.get("units")) else None,
        
        # Property Details
        "lot_count": item.get("units"), # mapping units to lot_count
        # Ensure listing_url is always populated - use API url if available, else construct from crexi_id with slug
        "listing_url": item.get("url") or (build_crexi_url(str(item.get('id')), state_code, property_name) if item.get("id") else None),
        "description": item.get("description"),
        "category": category,  # Pre-validated category from is_mhp_or_rv_park()
        
        # Extended Details (Phase 2)
        "lease_type": item.get("leaseType"),
        "tenancy": item.get("tenancy"),
        "lease_expiration": item.get("leaseExpiration"),
        "sq_ft": item.get("sqFt"),
        "sub_type": item.get("subType"),
        "year_built": item.get("yearBuilt"),
        "date_listed": item.get("dateListed"),
        "days_on_market": item.get("daysOnMarket"),
        "investment_highlights": item.get("investmentHighlights") or item.get("marketingDescription"),
        
        # Broker Details
        "broker_name": broker.get("name"),
        "broker_company": broker.get("companyName"),
        "broker_phone": broker.get("phone"),
        "broker_email": broker.get("email"),
        
        # Metdata
        "last_scraped_at": datetime.now(timezone.utc).isoformat(),
        
        # Default CRM fields
        "status": "not_contacted",
    }


def ingest_crexi_leads(
    search_url: str | None = None,
    dataset_id: str | None = None,
    local_file: str | None = None,
    limit: int = 100,
    dry_run: bool = False,
    use_residential_proxy: bool = False,
    keyword: str | None = None
):
    """Run scraper or fetch existing dataset, then ingest."""
    client = None
    items = []

    if local_file:
        print(f"Loading from local file: {local_file}")
        with open(local_file, "r", encoding="utf-8") as f:
            items = json.load(f)
    elif dataset_id:
        client = get_apify_client()
        print(f"Fetching existing dataset: {dataset_id}")
        items = list(client.dataset(dataset_id).iterate_items())
    elif search_url or keyword:
        client = get_apify_client()
        
        # Build actor input based on search method
        if keyword:
            # Construct search URL from keyword
            import urllib.parse
            encoded_kw = urllib.parse.quote(keyword)
            constructed_url = f"https://www.crexi.com/properties?keyword={encoded_kw}"
            print(f"Starting Crexi scraper with keyword: {keyword}")
            print(f"  Constructed URL: {constructed_url}")
            actor_input = {
                "startUrls": [{"url": constructed_url}],
                "maxItems": limit,
                "includeListingDetails": True,
                "includeBrokerDetails": True,
            }
        else:
            print(f"Starting Crexi scraper for URL: {search_url}")
            actor_input = {
                "startUrls": [{"url": search_url}],
                "maxItems": limit,
                "includeListingDetails": True,
                "includeBrokerDetails": True,
            }
        
        # Add proxy config if requested
        if use_residential_proxy:
            actor_input["proxy"] = {
                "useApifyProxy": True,
                "apifyProxyGroups": ["RESIDENTIAL"]
            }
        try:
            print(f"Sending input to actor: {actor_input}")
            run = client.actor(ACTOR_ID).call(run_input=actor_input)
        except Exception as e:
            print(f"CRITICAL APIFY ERROR: {e}")
            import traceback
            traceback.print_exc()
            return
        
        if not run:
            print("Error: Actor run returned None.")
            return

        dataset_id = run["defaultDatasetId"]
        print(f"Scrape completed. Dataset ID: {dataset_id}")
        items = list(client.dataset(dataset_id).iterate_items())
    else:
        print("ERROR: Must provide either --search-url or --dataset-id")
        return

    print(f"Found {len(items)} items. Processing...")
    
    # --- HARDCODING / BACKUP STRATEGY ---
    # Save raw data to backups/ directory to ensure permanence
    if items:
        backup_dir = Path("backups")
        backup_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = backup_dir / f"crexi_raw_{timestamp}.json"
        
        with open(backup_file, "w", encoding="utf-8") as f:
            json.dump(items, f, indent=2, default=str)
        print(f"PERMANENT BACKUP SAVED: {backup_file}")
    # ------------------------------------
    
    leads_to_upsert = []
    skipped_count = 0
    skipped_examples = []
    
    for item in items:
        # Filter for basic validity
        if not item.get("id"):
            continue
        
        # Validate that this is actually an MHP or RV park
        is_valid, category = is_mhp_or_rv_park(item)
        if not is_valid:
            skipped_count += 1
            if len(skipped_examples) < 5:
                prop_name = item.get("propertyName") or item.get("name") or "Unknown"
                skipped_examples.append(f"{prop_name} (type: {item.get('type')})")
            continue
        
        normalized = normalize_crexi_item(item, category)
        leads_to_upsert.append(normalized)
    
    if skipped_count > 0:
        print(f"\nSkipped {skipped_count} non-MHP/RV properties:")
        for ex in skipped_examples:
            print(f"  - {ex}")
        if skipped_count > 5:
            print(f"  ... and {skipped_count - 5} more")

    if dry_run:
        print("\n[DRY RUN] Would upsert the following leads:")
        for lead in leads_to_upsert[:5]:
            print(f"  - {lead['name']} ({lead['city']}, {lead['state']}) | Price: {lead['asking_price']} | Cap: {lead['cap_rate']}%")
        print(f"  ... and {len(leads_to_upsert) - 5} more.")
        return

    # Upsert to DB
    print(f"Upserting {len(leads_to_upsert)} leads to database...")
    inserted = 0
    updated = 0
    for lead in leads_to_upsert:
        db.upsert_lead(lead)
        # upsert_lead returns ID, doesn't explicitly return inserted/updated count easily without checking logic
        # but for now we trust it works.
        inserted += 1 # Approximation
    
    print(f"Done. Processed: {inserted}")


def main():
    parser = argparse.ArgumentParser(description="Ingest Crexi leads to DB")
    parser.add_argument("--search-url", help="Crexi search result URL to scrape")
    parser.add_argument("--dataset-id", help="Existing Apify dataset ID to ingest")
    parser.add_argument("--local-file", help="Path to local JSON file to ingest (bypass Apify)")
    parser.add_argument("--limit", type=int, default=100, help="Max items to scrape")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without saving")
    parser.add_argument("--use-residential-proxy", action="store_true", help="Use Apify Residential Proxies (requires plan)")
    parser.add_argument("--keyword", help="Keyword to search on Crexi (e.g., 'Mobile Home Park Florida')")
    
    args = parser.parse_args()
    
    if not args.search_url and not args.dataset_id and not args.local_file and not args.keyword:
        print("No arguments provided. Defaulting to Florida MHP Search (User Targeted - Simplified)...")
        # User filtered URL (Map params removed to ensure List View for scraper)
        args.search_url = "https://www.crexi.com/properties?placeIds%5B%5D=ChIJvypWkWV2wYgR0E7HW9MTLvc&pageSize=60&subtypes%5B%5D=RV%20Park&types%5B%5D=Multifamily&types%5B%5D=Mobile%20Home%20Park"
        
    ingest_crexi_leads(
        search_url=args.search_url,
        dataset_id=args.dataset_id,
        local_file=args.local_file,
        limit=args.limit,
        dry_run=args.dry_run,
        use_residential_proxy=args.use_residential_proxy,
        keyword=args.keyword
    )

if __name__ == "__main__":
    main()
