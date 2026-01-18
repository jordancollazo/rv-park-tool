"""
normalize_places.py
Cleans and normalizes raw Apify place records.

Usage:
    python execution/normalize_places.py
"""

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# Paths
INPUT_PATH = Path(".tmp/raw_places.json")
OUTPUT_PATH = Path(".tmp/normalized_places.json")


def parse_address(address: str | None) -> dict:
    """
    Parse address string into components.
    Returns dict with city, state, zip.
    """
    result = {"city": "", "state": "", "zip": ""}
    
    if not address:
        return result
    
    # Try to extract ZIP code (5 digits or 5+4 format)
    zip_match = re.search(r"\b(\d{5}(?:-\d{4})?)\b", address)
    if zip_match:
        result["zip"] = zip_match.group(1)
    
    # Try to extract state (2 letter code before ZIP)
    state_match = re.search(r"\b([A-Z]{2})\s*\d{5}", address)
    if state_match:
        result["state"] = state_match.group(1)
    
    # Try to extract city (before state)
    city_match = re.search(r",\s*([^,]+),\s*[A-Z]{2}\s*\d{5}", address)
    if city_match:
        result["city"] = city_match.group(1).strip()
    
    return result


def normalize_phone(phone: str | None) -> str:
    """Normalize phone number to consistent format."""
    if not phone:
        return ""
    
    # Remove all non-digit characters
    digits = re.sub(r"\D", "", phone)
    
    # Format as (XXX) XXX-XXXX if we have 10 digits
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits[0] == "1":
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    
    return phone  # Return original if can't normalize


def normalize_place(place: dict, source_query: str, area: str) -> dict:
    """
    Normalize a single place record.
    
    Extracts: name, address, phone, website, maps_url, place_id,
              rating, review_count, lat, lng, category
    """
    # Parse address components
    full_address = place.get("address", "")
    address_parts = parse_address(full_address)
    
    # Build Google Maps URL from place ID
    place_id = place.get("placeId", "")
    maps_url = ""
    if place_id:
        maps_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
    elif place.get("url"):
        maps_url = place.get("url", "")
    
    # Get website (prefer website field, fall back to others)
    website = place.get("website", "") or place.get("webUrl", "") or ""
    
    # Get phone
    phone = place.get("phone", "") or place.get("phoneUnformatted", "") or ""
    
    # Get coordinates
    location = place.get("location", {}) or {}
    lat = location.get("lat") or place.get("latitude")
    lng = location.get("lng") or place.get("longitude")
    
    # Get category
    category = ""
    categories = place.get("categories", []) or []
    if categories:
        category = categories[0] if isinstance(categories[0], str) else ""
    
    return {
        "source_query": source_query,
        "area": area,
        "name": place.get("title", "") or place.get("name", ""),
        "address": full_address,
        "city": address_parts["city"],
        "state": address_parts["state"],
        "zip": address_parts["zip"],
        "phone": normalize_phone(phone),
        "website": website,
        "maps_url": maps_url,
        "place_id": place_id,
        "google_rating": place.get("totalScore") or place.get("rating"),
        "review_count": place.get("reviewsCount") or place.get("userRatingsTotal"),
        "latitude": lat,
        "longitude": lng,
        "category": category,
    }


def deduplicate_places(places: list[dict]) -> list[dict]:
    """Remove duplicates based on place_id."""
    seen = set()
    unique = []
    
    for place in places:
        place_id = place.get("place_id")
        if place_id and place_id in seen:
            continue
        if place_id:
            seen.add(place_id)
        unique.append(place)
    
    return unique


def normalize_places(input_path: Path = INPUT_PATH) -> list[dict]:
    """
    Load raw places and normalize them.
    
    Returns list of normalized place records.
    """
    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}")
        print("Run run_places_search.py first")
        sys.exit(1)
    
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    metadata = data.get("metadata", {})
    area = metadata.get("area", "Unknown")
    keywords = metadata.get("keywords", [])
    places = data.get("places", [])
    
    print(f"Loaded {len(places)} raw places from {input_path}")
    print(f"Area: {area}")
    print(f"Keywords: {keywords}")
    print("-" * 50)
    
    # Normalize each place
    # Note: Apify doesn't tell us which keyword matched each place,
    # so we use the first keyword as default
    default_query = keywords[0] if keywords else "unknown"
    
    normalized = []
    for place in places:
        # Try to determine source query from search string if available
        source_query = place.get("searchString", default_query)
        normalized.append(normalize_place(place, source_query, area))
    
    # Deduplicate
    unique = deduplicate_places(normalized)
    print(f"After deduplication: {len(unique)} places")
    
    return unique


def save_normalized(places: list[dict], output_path: Path = OUTPUT_PATH) -> Path:
    """Save normalized places to JSON."""
    output_path.parent.mkdir(exist_ok=True)
    
    output_data = {
        "metadata": {
            "normalized_at": datetime.now(timezone.utc).isoformat(),
            "total_records": len(places),
        },
        "places": places,
    }
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"Saved normalized places to: {output_path}")
    return output_path


def main():
    places = normalize_places()
    save_normalized(places)
    
    # Print sample
    if places:
        print("\nSample record:")
        print(json.dumps(places[0], indent=2))


if __name__ == "__main__":
    main()
