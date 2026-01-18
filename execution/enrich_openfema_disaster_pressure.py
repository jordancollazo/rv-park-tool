"""
enrich_openfema_disaster_pressure.py

Enriches leads with disaster pressure scores from OpenFEMA Disaster Declarations.
Uses county-level aggregation to compute recency and frequency pressure.

Usage:
    python execution/enrich_openfema_disaster_pressure.py [--limit N]
"""

import argparse
import json
import math
import sqlite3
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

# Database path
DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"

# OpenFEMA API endpoint
OPENFEMA_URL = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"

# Florida state FIPS code
FL_STATE_FIPS = "12"

# Scoring parameters
LOOKBACK_YEARS_RECENT = 5
LOOKBACK_YEARS_HISTORICAL = 20
CACHE_TTL_DAYS = 30

# Disaster score max contribution
DISASTER_SCORE_MAX = 20.0


def get_db():
    """Get database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_florida_disasters() -> list[dict]:
    """
    Fetch all disaster declarations for Florida from OpenFEMA.
    
    Returns list of disaster declaration records.
    """
    print("Fetching Florida disaster declarations from OpenFEMA...")
    
    all_disasters = []
    skip = 0
    page_size = 1000
    
    while True:
        params = {
            "$filter": "state eq 'FL'",
            "$select": "disasterNumber,fipsStateCode,fipsCountyCode,designatedDate,incidentType,declarationTitle,incidentBeginDate,incidentEndDate",
            "$orderby": "designatedDate desc",
            "$skip": skip,
            "$top": page_size,
        }
        
        url = f"{OPENFEMA_URL}?{urlencode(params)}"
        
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "MHP-Outreach-Tool/1.0",
                "Accept": "application/json",
            })
            
            with urllib.request.urlopen(req, timeout=60) as response:
                data = json.loads(response.read().decode("utf-8"))
            
            records = data.get("DisasterDeclarationsSummaries", [])
            
            if not records:
                break
            
            all_disasters.extend(records)
            print(f"  Fetched {len(all_disasters)} records...")
            
            if len(records) < page_size:
                break
            
            skip += page_size
            time.sleep(0.5)  # Rate limiting
            
        except urllib.error.HTTPError as e:
            print(f"HTTP error {e.code}: {e.reason}")
            break
        except urllib.error.URLError as e:
            print(f"Network error: {e.reason}")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            break
    
    print(f"✓ Fetched {len(all_disasters)} total disaster declarations for Florida")
    return all_disasters


def aggregate_by_county(disasters: list[dict]) -> dict:
    """
    Aggregate disaster declarations by county FIPS code.
    
    Returns dict mapping county_fips to aggregated metrics.
    """
    print("Aggregating disasters by county...")
    
    current_year = datetime.now().year
    cutoff_5yr = current_year - LOOKBACK_YEARS_RECENT
    cutoff_20yr = current_year - LOOKBACK_YEARS_HISTORICAL
    
    county_metrics = {}
    
    for disaster in disasters:
        # Build full county FIPS (state + county)
        state_fips = disaster.get("fipsStateCode", "")
        county_code = disaster.get("fipsCountyCode", "")
        
        if not county_code or county_code == "000":
            continue  # Statewide declaration, skip
        
        county_fips = f"{state_fips.zfill(2)}{county_code.zfill(3)}"
        
        # Parse date
        date_str = disaster.get("designatedDate", "") or disaster.get("incidentBeginDate", "")
        if not date_str:
            continue
        
        try:
            # Handle various date formats
            if "T" in date_str:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            else:
                dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
            year = dt.year
        except ValueError:
            continue
        
        if year < cutoff_20yr:
            continue
        
        # Initialize county entry
        if county_fips not in county_metrics:
            county_metrics[county_fips] = {
                "declarations_5yr": [],
                "declarations_20yr": [],
                "last_disaster_date": None,
                "incident_types": set(),
            }
        
        metrics = county_metrics[county_fips]
        
        # Track declarations
        if year >= cutoff_5yr:
            metrics["declarations_5yr"].append({
                "date": date_str,
                "type": disaster.get("incidentType", ""),
                "title": disaster.get("declarationTitle", ""),
                "years_ago": current_year - year,
            })
        
        metrics["declarations_20yr"].append({
            "date": date_str,
            "type": disaster.get("incidentType", ""),
        })
        
        metrics["incident_types"].add(disaster.get("incidentType", ""))
        
        # Track most recent
        if not metrics["last_disaster_date"] or date_str > metrics["last_disaster_date"]:
            metrics["last_disaster_date"] = date_str
    
    print(f"  Found data for {len(county_metrics)} counties")
    return county_metrics


def compute_disaster_score(metrics: dict) -> float:
    """
    Compute disaster pressure score (0-20) for a county.
    
    Formula:
    - Recency: exponential decay with 5-year half-life
    - Frequency: count-based with diminishing returns
    """
    declarations_5yr = metrics.get("declarations_5yr", [])
    declarations_20yr = metrics.get("declarations_20yr", [])
    
    # Recency score: more recent = higher pressure
    recency_score = 0.0
    for decl in declarations_5yr:
        years_ago = decl.get("years_ago", 5)
        # Exponential decay: weight = 2^(-years_ago/5)
        weight = math.pow(2, -years_ago / 5)
        recency_score += weight
    
    # Cap recency contribution
    recency_score = min(recency_score * 4, 10.0)  # Max 10 points from recency
    
    # Frequency score: logarithmic scaling
    count_20yr = len(declarations_20yr)
    if count_20yr > 0:
        # Log scaling: ~5 declarations = 5 points, ~20 = 8 points, ~50 = 10 points
        frequency_score = min(math.log1p(count_20yr) * 2.5, 10.0)
    else:
        frequency_score = 0.0
    
    # Combine
    total_score = min(recency_score + frequency_score, DISASTER_SCORE_MAX)
    
    return round(total_score, 1)


def cache_county_metrics(conn: sqlite3.Connection, county_fips: str, 
                         metrics: dict, score: float):
    """Cache county disaster metrics."""
    cursor = conn.cursor()
    fetched_at = datetime.now(timezone.utc).isoformat()
    
    # Convert sets to lists for JSON serialization
    metrics_copy = {
        "declarations_5yr_count": len(metrics.get("declarations_5yr", [])),
        "declarations_20yr_count": len(metrics.get("declarations_20yr", [])),
        "incident_types": list(metrics.get("incident_types", set())),
        "last_disaster_date": metrics.get("last_disaster_date"),
    }
    
    cursor.execute("""
        INSERT OR REPLACE INTO fema_disaster_cache
        (county_fips, declaration_count_5yr, declaration_count_20yr, 
         last_disaster_date, disaster_score, metrics_json, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        county_fips,
        len(metrics.get("declarations_5yr", [])),
        len(metrics.get("declarations_20yr", [])),
        metrics.get("last_disaster_date"),
        score,
        json.dumps(metrics_copy),
        fetched_at,
    ))
    
    conn.commit()


def get_cached_disaster_score(conn: sqlite3.Connection, county_fips: str) -> dict | None:
    """Check cache for existing disaster score."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT disaster_score, declaration_count_5yr, declaration_count_20yr,
               last_disaster_date, metrics_json, fetched_at
        FROM fema_disaster_cache
        WHERE county_fips = ?
    """, (county_fips,))
    
    row = cursor.fetchone()
    if row:
        # Check if cache is still valid
        fetched_at = datetime.fromisoformat(row["fetched_at"].replace("Z", "+00:00"))
        age_days = (datetime.now(timezone.utc) - fetched_at).days
        
        if age_days < CACHE_TTL_DAYS:
            return {
                "score": row["disaster_score"],
                "count_5yr": row["declaration_count_5yr"],
                "count_20yr": row["declaration_count_20yr"],
                "last_date": row["last_disaster_date"],
            }
    
    return None


def get_county_fips_for_zip(conn: sqlite3.Connection, zip_code: str) -> str | None:
    """
    Get county FIPS for a ZIP code.
    
    Uses a static mapping for Florida ZIP prefixes to approximate county.
    This is a fallback since precise crosswalk may not be available.
    """
    if not zip_code:
        return None
    
    # Florida ZIP code prefix to approximate county FIPS mapping
    # This covers the major Florida counties by ZIP prefix
    FL_ZIP_COUNTY_MAP = {
        "320": "12031",  # Duval (Jacksonville)
        "321": "12109",  # St. Johns
        "322": "12127",  # Volusia (Daytona)
        "323": "12001",  # Alachua (Gainesville)
        "324": "12067",  # Jefferson/Leon (Tallahassee area)
        "325": "12073",  # Leon (Tallahassee)
        "326": "12083",  # Marion (Ocala)
        "327": "12105",  # Orange (Orlando)
        "328": "12105",  # Orange/Seminole (Orlando metro)
        "329": "12117",  # Brevard (Space Coast)
        "330": "12086",  # Miami-Dade
        "331": "12086",  # Miami-Dade
        "332": "12086",  # Miami-Dade
        "333": "12011",  # Broward (Fort Lauderdale)
        "334": "12099",  # Palm Beach
        "335": "12011",  # Broward
        "336": "12099",  # Palm Beach
        "337": "12057",  # Hillsborough (Tampa)
        "338": "12103",  # Pinellas (St. Pete)
        "339": "12021",  # Collier (Naples)
        "340": "12081",  # Manatee (Bradenton)
        "341": "12115",  # Sarasota
        "342": "12071",  # Lee (Fort Myers)
        "344": "12101",  # Pasco (Tampa north)
        "346": "12057",  # Hillsborough
        "347": "12115",  # Sarasota/Charlotte
        "349": "12009",  # Brevard
        "384": "12087",  # Monroe (Keys)
    }
    
    # Get first 3 digits of ZIP
    zip_prefix = zip_code[:3] if len(zip_code) >= 3 else None
    
    if zip_prefix and zip_prefix in FL_ZIP_COUNTY_MAP:
        return FL_ZIP_COUNTY_MAP[zip_prefix]
    
    # Try to check if leads table has county_fips already set elsewhere
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT county_fips FROM leads 
            WHERE zip LIKE ? AND county_fips IS NOT NULL 
            LIMIT 1
        """, (zip_code[:3] + "%",))
        row = cursor.fetchone()
        if row and row["county_fips"]:
            return row["county_fips"]
    except sqlite3.Error:
        pass
    
    return None


def get_disaster_description(count_5yr: int, count_20yr: int) -> str:
    """Get human-readable description of disaster pressure."""
    if count_5yr >= 5:
        return f"Very high disaster frequency: {count_5yr} declarations in 5 years, {count_20yr} in 20 years"
    elif count_5yr >= 3:
        return f"High disaster frequency: {count_5yr} declarations in 5 years, {count_20yr} in 20 years"
    elif count_5yr >= 1:
        return f"Some recent disaster activity: {count_5yr} declarations in 5 years"
    elif count_20yr >= 5:
        return f"Historical disaster activity: {count_20yr} declarations in 20 years"
    else:
        return "Low disaster declaration history"


def enrich_disaster_pressure(limit: int | None = None, force_refresh: bool = False):
    """
    Enrich leads with disaster pressure scores from OpenFEMA.
    
    Args:
        limit: Optional limit on number of leads to process
        force_refresh: If True, re-fetch all disaster data
    """
    print("=" * 60)
    print("OPENFEMA DISASTER PRESSURE ENRICHMENT")
    print("=" * 60)
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Fetch fresh disaster data if needed
    if force_refresh:
        disasters = fetch_florida_disasters()
        county_metrics = aggregate_by_county(disasters)
        
        # Cache all county metrics
        print("\nCaching county metrics...")
        for county_fips, metrics in county_metrics.items():
            score = compute_disaster_score(metrics)
            cache_county_metrics(conn, county_fips, metrics, score)
        print(f"✓ Cached metrics for {len(county_metrics)} counties")
    else:
        # Check if we have cached data
        cursor.execute("SELECT COUNT(*) as cnt FROM fema_disaster_cache")
        cache_count = cursor.fetchone()["cnt"]
        
        if cache_count == 0:
            print("No cached disaster data found, fetching fresh data...")
            disasters = fetch_florida_disasters()
            county_metrics = aggregate_by_county(disasters)
            
            for county_fips, metrics in county_metrics.items():
                score = compute_disaster_score(metrics)
                cache_county_metrics(conn, county_fips, metrics, score)
            print(f"✓ Cached metrics for {len(county_metrics)} counties")
    
    # Get leads to process
    if force_refresh:
        query = """
            SELECT id, name, zip, county_fips
            FROM leads
            WHERE (zip IS NOT NULL AND zip != '') OR county_fips IS NOT NULL
            ORDER BY id
        """
    else:
        query = """
            SELECT id, name, zip, county_fips
            FROM leads
            WHERE ((zip IS NOT NULL AND zip != '') OR county_fips IS NOT NULL)
              AND disaster_pressure_score IS NULL
            ORDER BY id
        """
    
    if limit:
        query += f" LIMIT {limit}"
    
    cursor.execute(query)
    leads = cursor.fetchall()
    
    print(f"\nProcessing {len(leads)} leads...")
    
    if not leads:
        print("No leads need disaster pressure enrichment.")
        conn.close()
        return
    
    # Process each lead
    enriched_count = 0
    no_county_count = 0
    no_data_count = 0
    
    for i, lead in enumerate(leads):
        lead_id = lead["id"]
        zip_code = lead["zip"]
        existing_fips = lead["county_fips"]
        
        if (i + 1) % 100 == 0:
            print(f"  Processing {i+1}/{len(leads)}...")
        
        # Get county FIPS
        county_fips = existing_fips
        if not county_fips and zip_code:
            county_fips = get_county_fips_for_zip(conn, zip_code)
        
        if not county_fips:
            # Can't determine county, use statewide default
            no_county_count += 1
            cursor.execute("""
                UPDATE leads SET disaster_pressure_score = 10.0 WHERE id = ?
            """, (lead_id,))
            continue
        
        # Update lead's county_fips if not set
        if not existing_fips and county_fips:
            cursor.execute("""
                UPDATE leads SET county_fips = ? WHERE id = ?
            """, (county_fips, lead_id))
        
        # Get disaster score from cache
        cached = get_cached_disaster_score(conn, county_fips)
        
        if cached:
            score = cached["score"]
            cursor.execute("""
                UPDATE leads SET disaster_pressure_score = ? WHERE id = ?
            """, (score, lead_id))
            enriched_count += 1
        else:
            # County not in cache (might be outside FL or data gap)
            no_data_count += 1
            cursor.execute("""
                UPDATE leads SET disaster_pressure_score = 5.0 WHERE id = ?
            """, (lead_id,))
    
    conn.commit()
    conn.close()
    
    # Summary
    print("\n" + "=" * 60)
    print("ENRICHMENT COMPLETE")
    print("=" * 60)
    print(f"Enriched: {enriched_count}")
    print(f"No county attribution: {no_county_count}")
    print(f"No disaster data: {no_data_count}")


def main():
    parser = argparse.ArgumentParser(
        description="Enrich leads with OpenFEMA disaster pressure scores"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of leads to process",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force refresh disaster data from OpenFEMA",
    )
    
    args = parser.parse_args()
    enrich_disaster_pressure(limit=args.limit, force_refresh=args.force)


if __name__ == "__main__":
    main()
