"""
enrich_census_data.py

Enriches leads with Census Bureau data:
- Population density by census tract
- Median home values
- Housing affordability metrics

Uses the free Census Bureau API (no API key required).

Usage:
    # Enrich all leads with coordinates
    python execution/enrich_census_data.py --all
    
    # Enrich leads from specific area
    python execution/enrich_census_data.py --area "Orlando, FL"
    
    # Enrich specific lead IDs
    python execution/enrich_census_data.py --lead-ids 1,2,3
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime, timezone

import requests

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from execution.db import get_db, get_all_leads, get_lead_by_id


def get_census_tract(lat: float, lon: float) -> dict | None:
    """
    Get census tract FIPS code for coordinates using Census Geocoding API.
    
    Args:
        lat: Latitude
        lon: Longitude
    
    Returns:
        Dictionary with tract info or None if not found
    """
    try:
        # Census Geocoding API
        url = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
        params = {
            "x": lon,
            "y": lat,
            "benchmark": "Public_AR_Current",
            "vintage": "Current_Current",
            "format": "json"
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if "result" in data and "geographies" in data["result"]:
            tracts = data["result"]["geographies"].get("Census Tracts", [])
            if tracts and len(tracts) > 0:
                tract_info = tracts[0]
                return {
                    "state_fips": tract_info["STATE"],
                    "county_fips": tract_info["COUNTY"],
                    "tract": tract_info["TRACT"],
                    "full_fips": f"{tract_info['STATE']}{tract_info['COUNTY']}{tract_info['TRACT']}"
                }
    except Exception as e:
        print(f"    Error geocoding ({lat}, {lon}): {e}")
    
    return None


def fetch_census_data(state_fips: str, county_fips: str, tract: str) -> dict:
    """
    Fetch population and housing data from Census ACS API.
    
    Args:
        state_fips: State FIPS code
        county_fips: County FIPS code
        tract: Census tract code
    
    Returns:
        Dictionary with census data
    """
    # Census ACS 5-Year API (most recent available)
    base_url = "https://api.census.gov/data/2021/acs/acs5"
    
    # Variables to fetch:
    # B01003_001E: Total Population
    # B25077_001E: Median Home Value
    # ALAND: Land area in square meters
    variables = "B01003_001E,B25077_001E,ALAND"
    
    params = {
        "get": variables,
        "for": f"tract:{tract}",
        "in": f"state:{state_fips} county:{county_fips}"
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if len(data) < 2:
            return {}
        
        # Parse response (first row is headers, second is data)
        headers = data[0]
        values = data[1]
        result = dict(zip(headers, values))
        
        # Calculate population density (people per square mile)
        population = int(result.get("B01003_001E", 0) or 0)
        land_area_sqm = int(result.get("ALAND", 0) or 0)
        
        if land_area_sqm > 0:
            # Convert square meters to square miles
            land_area_sqmi = land_area_sqm / 2589988.11
            population_density = population / land_area_sqmi if land_area_sqmi > 0 else 0
        else:
            population_density = 0
        
        # Median home value
        median_home_value = int(result.get("B25077_001E", 0) or 0)
        
        # Calculate affordability index (0-100, higher = more expensive)
        # Using national median as baseline (~$350k)
        if median_home_value > 0:
            affordability_index = min(100, (median_home_value / 350000) * 50)
        else:
            affordability_index = None
        
        return {
            "population": population,
            "population_density": round(population_density, 2),
            "median_home_value": median_home_value if median_home_value > 0 else None,
            "housing_affordability_index": round(affordability_index, 2) if affordability_index else None
        }
    
    except Exception as e:
        print(f"    Error fetching census data: {e}")
        return {}


def enrich_lead_census(lead: dict, verbose: bool = True) -> bool:
    """
    Enrich a single lead with census data.
    
    Returns:
        True if enrichment succeeded, False otherwise
    """
    lead_id = lead["id"]
    lat = lead.get("latitude")
    lon = lead.get("longitude")
    
    if not lat or not lon:
        if verbose:
            print(f"  [{lead_id}] {lead['name']}: No coordinates")
        return False
    
    if verbose:
        print(f"  [{lead_id}] {lead['name']}: Fetching census data...", end=" ")
    
    # Get census tract
    tract_info = get_census_tract(lat, lon)
    if not tract_info:
        if verbose:
            print("ERROR: Could not geocode to census tract")
        return False
    
    # Fetch census data
    census_data = fetch_census_data(
        tract_info["state_fips"],
        tract_info["county_fips"],
        tract_info["tract"]
    )
    
    if not census_data:
        if verbose:
            print("ERROR: Could not fetch census data")
        return False
    
    # Update database
    with get_db() as conn:
        conn.execute("""
            UPDATE leads SET
                census_tract = ?,
                population_density = ?,
                median_home_value = ?,
                housing_affordability_index = ?
            WHERE id = ?
        """, (
            tract_info["full_fips"],
            census_data.get("population_density"),
            census_data.get("median_home_value"),
            census_data.get("housing_affordability_index"),
            lead_id
        ))
        conn.commit()
    
    if verbose:
        print(f"✓ (Pop density: {census_data.get('population_density', 0):.0f}/mi², "
              f"Median home: ${census_data.get('median_home_value', 0):,})")
    
    return True


def batch_enrich_census(
    area: str | None = None,
    lead_ids: list[int] | None = None,
    all_leads: bool = False,
    verbose: bool = True
) -> tuple[int, int, int]:
    """
    Batch process multiple leads for census enrichment.
    
    Returns:
        Tuple of (total_processed, successful, failed)
    """
    # Get leads to process
    if lead_ids:
        leads = [get_lead_by_id(lid) for lid in lead_ids]
        leads = [l for l in leads if l is not None]
    elif area:
        leads = get_all_leads(area=area)
    elif all_leads:
        leads = get_all_leads()
    else:
        raise ValueError("Must specify --area, --lead-ids, or --all")
    
    # Filter to leads with coordinates that haven't been enriched
    leads_to_process = [
        l for l in leads 
        if l.get("latitude") and l.get("longitude") and not l.get("census_tract")
    ]
    
    if verbose:
        print(f"\n{'='*60}")
        print(f"Census Data Enrichment")
        print(f"{'='*60}")
        print(f"Total leads: {len(leads)}")
        print(f"With coordinates: {len([l for l in leads if l.get('latitude')])}")
        print(f"Already enriched: {len([l for l in leads if l.get('census_tract')])}")
        print(f"To process: {len(leads_to_process)}")
        print(f"{'='*60}\n")
    
    if not leads_to_process:
        if verbose:
            print("No leads to process.")
        return 0, 0, 0
    
    # Process leads
    successful = 0
    failed = 0
    
    for i, lead in enumerate(leads_to_process, 1):
        if verbose:
            print(f"[{i}/{len(leads_to_process)}]", end=" ")
        
        if enrich_lead_census(lead, verbose=verbose):
            successful += 1
        else:
            failed += 1
    
    if verbose:
        print(f"\n{'='*60}")
        print(f"Enrichment Complete")
        print(f"{'='*60}")
        print(f"Processed: {len(leads_to_process)}")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        print(f"{'='*60}\n")
    
    return len(leads_to_process), successful, failed


def main():
    parser = argparse.ArgumentParser(description="Enrich leads with Census Bureau data")
    parser.add_argument("--area", help="Process leads from specific area (e.g., 'Orlando, FL')")
    parser.add_argument("--lead-ids", help="Comma-separated list of lead IDs to process")
    parser.add_argument("--all", action="store_true", help="Process all leads with coordinates")
    parser.add_argument("--quiet", action="store_true", help="Suppress progress output")
    
    args = parser.parse_args()
    
    try:
        if args.lead_ids:
            lead_ids = [int(x.strip()) for x in args.lead_ids.split(",")]
            batch_enrich_census(lead_ids=lead_ids, verbose=not args.quiet)
        elif args.area or args.all:
            batch_enrich_census(area=args.area, all_leads=args.all, verbose=not args.quiet)
        else:
            parser.print_help()
            print("\nError: Must specify --area, --lead-ids, or --all")
            sys.exit(1)
    
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nAborted by user.")
        sys.exit(1)


if __name__ == "__main__":
    main()
