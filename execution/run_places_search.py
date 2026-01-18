"""
run_places_search.py
Calls Apify compass/crawler-google-places actor and saves raw dataset.

Usage:
    python execution/run_places_search.py --area "Broward County, FL" --limit 200
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from apify_client import ApifyClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Constants
ACTOR_ID = "compass/crawler-google-places"
DEFAULT_KEYWORDS = [
    "RV park",
    "mobile home park",
    "manufactured home community",
    "trailer park",
]
OUTPUT_DIR = Path(".tmp")


def get_apify_client() -> ApifyClient:
    """Initialize Apify client with token from environment."""
    token = os.getenv("APIFY_API_TOKEN")
    if not token:
        print("ERROR: APIFY_API_TOKEN not found in environment.")
        print("Add APIFY_API_TOKEN=your_token to .env file")
        sys.exit(1)
    return ApifyClient(token)


def build_actor_input(
    area: str,
    keywords: list[str],
    limit: int,
) -> dict:
    """
    Build input configuration for the Google Places actor.
    
    Geography is passed via locationQuery, not embedded in keywords.
    """
    # Calculate per-keyword limit to stay within total limit
    per_keyword_limit = max(1, limit // len(keywords))
    
    return {
        "searchStringsArray": keywords,
        "locationQuery": area,
        "maxCrawledPlacesPerSearch": per_keyword_limit,
        "language": "en",
        "exportPlaceUrls": True,
        "includeWebResults": False,
        "scrapeContacts": True,
        "scrapeDirections": False,
        "maxAutomaticZoomOut": 3,
    }


def run_places_search(
    area: str,
    keywords: list[str] | None = None,
    limit: int = 200,
) -> list[dict]:
    """
    Run the Apify Google Places actor and return results.
    
    Args:
        area: Geographic area to search (e.g., "Broward County, FL")
        keywords: List of search terms (uses defaults if None)
        limit: Maximum number of places to fetch
        
    Returns:
        List of place records from Apify
    """
    if keywords is None:
        keywords = DEFAULT_KEYWORDS
    
    client = get_apify_client()
    actor_input = build_actor_input(area, keywords, limit)
    
    print(f"Starting Apify actor: {ACTOR_ID}")
    print(f"Area: {area}")
    print(f"Keywords: {keywords}")
    print(f"Limit: {limit} (per keyword: {actor_input['maxCrawledPlacesPerSearch']})")
    print("-" * 50)
    
    # Run the actor and wait for completion
    run = client.actor(ACTOR_ID).call(run_input=actor_input)
    
    # Fetch results from the dataset
    dataset_id = run["defaultDatasetId"]
    print(f"Actor completed. Dataset ID: {dataset_id}")
    
    items = list(client.dataset(dataset_id).iterate_items())
    print(f"Fetched {len(items)} places")
    
    return items


def save_raw_results(
    items: list[dict],
    area: str,
    keywords: list[str],
) -> Path:
    """Save raw results to .tmp directory with metadata."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    output_data = {
        "metadata": {
            "area": area,
            "keywords": keywords,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "total_records": len(items),
        },
        "places": items,
    }
    
    output_path = OUTPUT_DIR / "raw_places.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"Saved raw results to: {output_path}")
    return output_path


def main():
    parser = argparse.ArgumentParser(
        description="Search for RV parks and mobile home parks via Apify"
    )
    parser.add_argument(
        "--area",
        required=True,
        help="Geographic area to search (e.g., 'Broward County, FL')",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum number of places to fetch (default: 200)",
    )
    parser.add_argument(
        "--keywords",
        type=str,
        default=None,
        help="Comma-separated list of search keywords (uses defaults if not specified)",
    )
    
    args = parser.parse_args()
    
    # Parse keywords if provided
    keywords = None
    if args.keywords:
        keywords = [k.strip() for k in args.keywords.split(",")]
    
    # Run the search
    items = run_places_search(
        area=args.area,
        keywords=keywords,
        limit=args.limit,
    )
    
    # Save results
    if items:
        save_raw_results(
            items=items,
            area=args.area,
            keywords=keywords or DEFAULT_KEYWORDS,
        )
    else:
        print("WARNING: No places found")
        sys.exit(1)


if __name__ == "__main__":
    main()
