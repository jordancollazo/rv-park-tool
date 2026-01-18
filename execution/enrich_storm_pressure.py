"""
enrich_storm_pressure.py

Enriches leads with storm proximity pressure scores from the pre-computed
IBTrACS storm grid.

Usage:
    python execution/enrich_storm_pressure.py [--limit N]
"""

import argparse
import sqlite3
from pathlib import Path

# Database path
DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"

# Grid resolution (must match build_storm_pressure_grid.py)
GRID_RESOLUTION = 0.05

# Score normalization
# Based on analysis: max intensity-weighted count is ~300-500 for high-exposure areas
MAX_EXPECTED_INTENSITY = 400.0
STORM_SCORE_MAX = 30.0  # Max contribution to insurance pressure index


def get_db():
    """Get database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def snap_to_grid(lat: float, lon: float) -> tuple[float, float]:
    """Snap coordinates to nearest grid cell center."""
    grid_lat = round(round(lat / GRID_RESOLUTION) * GRID_RESOLUTION, 3)
    grid_lon = round(round(lon / GRID_RESOLUTION) * GRID_RESOLUTION, 3)
    return grid_lat, grid_lon


def get_storm_score_from_grid(conn: sqlite3.Connection, lat: float, lon: float) -> dict | None:
    """
    Look up storm exposure for coordinates from pre-computed grid.
    
    Returns dict with storm_count, intensity_weighted_count, or None if not found.
    """
    grid_lat, grid_lon = snap_to_grid(lat, lon)
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT storm_count, intensity_weighted_count
        FROM storm_grid_cache
        WHERE grid_lat = ? AND grid_lon = ?
    """, (grid_lat, grid_lon))
    
    row = cursor.fetchone()
    
    if row:
        return {
            "storm_count": row["storm_count"],
            "intensity_weighted_count": row["intensity_weighted_count"],
        }
    
    # Try nearby cells if exact match not found
    cursor.execute("""
        SELECT storm_count, intensity_weighted_count,
               ABS(grid_lat - ?) + ABS(grid_lon - ?) as dist
        FROM storm_grid_cache
        WHERE grid_lat BETWEEN ? AND ?
          AND grid_lon BETWEEN ? AND ?
        ORDER BY dist
        LIMIT 1
    """, (
        grid_lat, grid_lon,
        grid_lat - GRID_RESOLUTION * 2, grid_lat + GRID_RESOLUTION * 2,
        grid_lon - GRID_RESOLUTION * 2, grid_lon + GRID_RESOLUTION * 2,
    ))
    
    row = cursor.fetchone()
    
    if row:
        return {
            "storm_count": row["storm_count"],
            "intensity_weighted_count": row["intensity_weighted_count"],
        }
    
    return None


def normalize_storm_score(intensity_weighted: float) -> float:
    """
    Normalize intensity-weighted storm count to 0-30 score.
    
    Uses logarithmic scaling to handle wide range of values.
    """
    if intensity_weighted <= 0:
        return 0.0
    
    # Log scaling with cap
    import math
    log_score = math.log1p(intensity_weighted) / math.log1p(MAX_EXPECTED_INTENSITY)
    normalized = min(log_score * STORM_SCORE_MAX, STORM_SCORE_MAX)
    
    return round(normalized, 1)


def get_storm_description(storm_count: int, intensity_weighted: float) -> str:
    """Get human-readable description of storm exposure."""
    if intensity_weighted >= 200:
        return f"Very high hurricane exposure: ~{storm_count} storm track points within 50 miles in 30 years"
    elif intensity_weighted >= 100:
        return f"High hurricane exposure: ~{storm_count} storm track points within 50 miles in 30 years"
    elif intensity_weighted >= 50:
        return f"Moderate hurricane exposure: ~{storm_count} storm track points within 50 miles in 30 years"
    elif intensity_weighted >= 20:
        return f"Some hurricane exposure: ~{storm_count} storm track points within 50 miles in 30 years"
    else:
        return "Low hurricane exposure in recent decades"


def enrich_storm_pressure(limit: int | None = None, force_refresh: bool = False):
    """
    Enrich leads with storm proximity scores from the pre-computed grid.
    
    Args:
        limit: Optional limit on number of leads to process
        force_refresh: If True, re-compute scores for all leads
    """
    print("=" * 60)
    print("STORM PRESSURE ENRICHMENT")
    print("=" * 60)
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Check if grid exists
    cursor.execute("SELECT COUNT(*) as cnt FROM storm_grid_cache")
    grid_count = cursor.fetchone()["cnt"]
    
    if grid_count == 0:
        print("ERROR: Storm grid not found. Run build_storm_pressure_grid.py first.")
        conn.close()
        return
    
    print(f"Using storm grid with {grid_count:,} cells")
    
    # Get leads to process
    if force_refresh:
        query = """
            SELECT id, name, latitude, longitude
            FROM leads
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY id
        """
    else:
        query = """
            SELECT id, name, latitude, longitude
            FROM leads
            WHERE latitude IS NOT NULL 
              AND longitude IS NOT NULL
              AND storm_proximity_score IS NULL
            ORDER BY id
        """
    
    if limit:
        query += f" LIMIT {limit}"
    
    cursor.execute(query)
    leads = cursor.fetchall()
    
    print(f"Found {len(leads)} leads to process")
    
    if not leads:
        print("No leads need storm pressure enrichment.")
        conn.close()
        return
    
    # Process each lead
    enriched_count = 0
    no_data_count = 0
    
    for i, lead in enumerate(leads):
        lead_id = lead["id"]
        name = lead["name"]
        lat = lead["latitude"]
        lon = lead["longitude"]
        
        if (i + 1) % 50 == 0:
            print(f"Processing {i+1}/{len(leads)}...")
        
        # Look up storm exposure
        storm_data = get_storm_score_from_grid(conn, lat, lon)
        
        if storm_data:
            storm_count = storm_data["storm_count"]
            intensity_weighted = storm_data["intensity_weighted_count"]
            score = normalize_storm_score(intensity_weighted)
            
            cursor.execute("""
                UPDATE leads
                SET storm_proximity_score = ?
                WHERE id = ?
            """, (score, lead_id))
            
            enriched_count += 1
        else:
            # Location outside grid (shouldn't happen for Florida)
            cursor.execute("""
                UPDATE leads
                SET storm_proximity_score = 0
                WHERE id = ?
            """, (lead_id,))
            no_data_count += 1
    
    conn.commit()
    conn.close()
    
    # Summary
    print("\n" + "=" * 60)
    print("ENRICHMENT COMPLETE")
    print("=" * 60)
    print(f"Enriched: {enriched_count}")
    print(f"No grid data: {no_data_count}")


def main():
    parser = argparse.ArgumentParser(
        description="Enrich leads with storm proximity scores"
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
        help="Force refresh all storm scores",
    )
    
    args = parser.parse_args()
    enrich_storm_pressure(limit=args.limit, force_refresh=args.force)


if __name__ == "__main__":
    main()
