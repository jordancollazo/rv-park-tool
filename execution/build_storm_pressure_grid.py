"""
build_storm_pressure_grid.py

Downloads NOAA IBTrACS hurricane track data and builds a pre-computed
storm pressure grid for Florida. The grid enables O(1) lookup of hurricane
exposure for any coordinate.

Usage:
    python execution/build_storm_pressure_grid.py [--force]
"""

import argparse
import csv
import gzip
import io
import math
import sqlite3
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

# Paths
DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"
DATA_DIR = Path(__file__).parent.parent / "data" / "ibtracs"

# IBTrACS data source - North Atlantic basin
IBTRACS_URL = "https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r01/access/csv/ibtracs.NA.list.v04r01.csv"

# Florida bounding box (slightly expanded)
FL_LAT_MIN = 24.0
FL_LAT_MAX = 32.0
FL_LON_MIN = -88.0
FL_LON_MAX = -79.5

# Grid parameters
GRID_RESOLUTION = 0.05  # ~5.5km cells
LOOKBACK_YEARS = 30
PROXIMITY_MILES = 50  # Count storms within this radius

# Earth radius in miles for Haversine
EARTH_RADIUS_MILES = 3959


def get_db():
    """Get database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate Haversine distance between two points in miles.
    """
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = (math.sin(delta_lat / 2) ** 2 + 
         math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return EARTH_RADIUS_MILES * c


def download_ibtracs_data(force: bool = False) -> Path:
    """
    Download IBTrACS North Atlantic CSV data if not present or force refresh.
    
    Returns path to the downloaded file.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    local_path = DATA_DIR / "ibtracs_na.csv"
    
    # Check if we need to download
    if local_path.exists() and not force:
        # Check age of file
        mtime = datetime.fromtimestamp(local_path.stat().st_mtime, tz=timezone.utc)
        age_days = (datetime.now(timezone.utc) - mtime).days
        
        if age_days < 365:  # Refresh yearly
            print(f"✓ Using cached IBTrACS data ({age_days} days old)")
            return local_path
    
    print("Downloading IBTrACS North Atlantic data...")
    print(f"  URL: {IBTRACS_URL}")
    
    try:
        req = urllib.request.Request(IBTRACS_URL, headers={
            "User-Agent": "MHP-Outreach-Tool/1.0",
        })
        
        with urllib.request.urlopen(req, timeout=120) as response:
            data = response.read()
        
        # Save to local file
        with open(local_path, "wb") as f:
            f.write(data)
        
        print(f"✓ Downloaded {len(data) / (1024*1024):.1f} MB to {local_path}")
        return local_path
        
    except Exception as e:
        print(f"ERROR downloading IBTrACS: {e}")
        if local_path.exists():
            print("Using existing cached file...")
            return local_path
        raise


def parse_ibtracs_data(csv_path: Path) -> list[dict]:
    """
    Parse IBTrACS CSV and extract relevant storm track points.
    
    Returns list of track points within Florida bounding box and time range.
    """
    print(f"Parsing IBTrACS data from {csv_path}...")
    
    cutoff_year = datetime.now().year - LOOKBACK_YEARS
    
    track_points = []
    
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        # Skip the header descriptive rows (first 2 lines)
        lines = f.readlines()
        
        # Find header row
        header_idx = 0
        for i, line in enumerate(lines):
            if line.startswith("SID,"):
                header_idx = i
                break
        
        reader = csv.DictReader(lines[header_idx:])
        
        total_rows = 0
        florida_rows = 0
        
        for row in reader:
            total_rows += 1
            
            try:
                # Parse coordinates
                lat_str = row.get("LAT", "").strip()
                lon_str = row.get("LON", "").strip()
                
                if not lat_str or not lon_str:
                    continue
                
                lat = float(lat_str)
                lon = float(lon_str)
                
                # Check if within expanded Florida region
                if not (FL_LAT_MIN - 2 <= lat <= FL_LAT_MAX + 2 and 
                        FL_LON_MIN - 2 <= lon <= FL_LON_MAX + 2):
                    continue
                
                # Parse date
                iso_time = row.get("ISO_TIME", "")
                if not iso_time:
                    continue
                
                try:
                    dt = datetime.strptime(iso_time.strip()[:10], "%Y-%m-%d")
                    if dt.year < cutoff_year:
                        continue
                except ValueError:
                    continue
                
                # Parse wind speed (if available) for intensity weighting
                wind_str = row.get("USA_WIND", "") or row.get("WMO_WIND", "")
                try:
                    wind_speed = float(wind_str) if wind_str.strip() else 34  # Default tropical storm
                except ValueError:
                    wind_speed = 34
                
                florida_rows += 1
                
                track_points.append({
                    "lat": lat,
                    "lon": lon,
                    "wind_speed": wind_speed,
                    "year": dt.year,
                    "storm_id": row.get("SID", ""),
                    "name": row.get("NAME", ""),
                })
                
            except (ValueError, KeyError) as e:
                continue
        
        print(f"  Processed {total_rows:,} total track points")
        print(f"  Found {florida_rows:,} points in Florida region (last {LOOKBACK_YEARS} years)")
    
    return track_points


def build_grid() -> list[tuple[float, float]]:
    """
    Generate grid cell centers covering Florida.
    
    Returns list of (lat, lon) tuples for grid cell centers.
    """
    grid_cells = []
    
    lat = FL_LAT_MIN
    while lat <= FL_LAT_MAX:
        lon = FL_LON_MIN
        while lon <= FL_LON_MAX:
            grid_cells.append((round(lat, 3), round(lon, 3)))
            lon += GRID_RESOLUTION
        lat += GRID_RESOLUTION
    
    print(f"Generated {len(grid_cells):,} grid cells")
    return grid_cells


def compute_grid_storm_scores(grid_cells: list[tuple[float, float]], 
                               track_points: list[dict]) -> dict:
    """
    For each grid cell, compute storm exposure metrics.
    
    Uses spatial indexing for efficiency.
    """
    print(f"Computing storm scores for {len(grid_cells):,} grid cells...")
    print(f"  Using {len(track_points):,} track points")
    print(f"  Proximity radius: {PROXIMITY_MILES} miles")
    
    # Simple spatial index: bin track points by rounded lat/lon
    BIN_SIZE = 1.0  # 1 degree bins
    spatial_index = {}
    
    for point in track_points:
        bin_lat = int(point["lat"] / BIN_SIZE)
        bin_lon = int(point["lon"] / BIN_SIZE)
        key = (bin_lat, bin_lon)
        
        if key not in spatial_index:
            spatial_index[key] = []
        spatial_index[key].append(point)
    
    grid_scores = {}
    
    for i, (grid_lat, grid_lon) in enumerate(grid_cells):
        if i % 500 == 0:
            print(f"  Processing cell {i+1}/{len(grid_cells)}...")
        
        storm_count = 0
        intensity_weighted_count = 0.0
        unique_storms = set()
        
        # Check neighboring bins
        bin_lat = int(grid_lat / BIN_SIZE)
        bin_lon = int(grid_lon / BIN_SIZE)
        
        for d_lat in range(-2, 3):
            for d_lon in range(-2, 3):
                key = (bin_lat + d_lat, bin_lon + d_lon)
                if key not in spatial_index:
                    continue
                
                for point in spatial_index[key]:
                    # Calculate distance
                    dist = haversine_distance(grid_lat, grid_lon, 
                                             point["lat"], point["lon"])
                    
                    if dist <= PROXIMITY_MILES:
                        storm_count += 1
                        
                        # Intensity weight: hurricane categories add more
                        # Cat 1: 64-82 kt, Cat 2: 83-95, Cat 3: 96-112, Cat 4: 113-136, Cat 5: 137+
                        wind = point["wind_speed"]
                        if wind >= 137:
                            intensity = 5.0
                        elif wind >= 113:
                            intensity = 4.0
                        elif wind >= 96:
                            intensity = 3.0
                        elif wind >= 83:
                            intensity = 2.0
                        elif wind >= 64:
                            intensity = 1.5
                        else:
                            intensity = 1.0
                        
                        intensity_weighted_count += intensity
                        unique_storms.add(point["storm_id"])
        
        grid_scores[(grid_lat, grid_lon)] = {
            "storm_count": storm_count,
            "intensity_weighted_count": round(intensity_weighted_count, 2),
            "unique_storms": len(unique_storms),
        }
    
    return grid_scores


def save_grid_to_database(grid_scores: dict):
    """Save computed storm grid to database."""
    print("Saving storm grid to database...")
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Clear existing grid
    cursor.execute("DELETE FROM storm_grid_cache")
    
    computed_at = datetime.now(timezone.utc).isoformat()
    
    # Insert all grid cells
    for (grid_lat, grid_lon), scores in grid_scores.items():
        cursor.execute("""
            INSERT INTO storm_grid_cache 
            (grid_lat, grid_lon, storm_count, intensity_weighted_count, computed_at)
            VALUES (?, ?, ?, ?, ?)
        """, (
            grid_lat, 
            grid_lon, 
            scores["storm_count"],
            scores["intensity_weighted_count"],
            computed_at
        ))
    
    conn.commit()
    conn.close()
    
    print(f"✓ Saved {len(grid_scores):,} grid cells to database")


def is_grid_stale() -> bool:
    """Check if storm grid needs to be rebuilt."""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) as cnt, MAX(computed_at) as last FROM storm_grid_cache")
    row = cursor.fetchone()
    conn.close()
    
    if not row or row["cnt"] == 0:
        return True
    
    if row["last"]:
        try:
            last_computed = datetime.fromisoformat(row["last"].replace("Z", "+00:00"))
            age_days = (datetime.now(timezone.utc) - last_computed).days
            return age_days > 365  # Rebuild yearly
        except ValueError:
            return True
    
    return True


def ensure_storm_grid(force: bool = False):
    """
    Ensure storm grid is built and up to date.
    
    This is the main entry point for other scripts.
    """
    if not force and not is_grid_stale():
        print("✓ Storm grid is up to date")
        return
    
    print("=" * 60)
    print("BUILDING STORM PRESSURE GRID")
    print("=" * 60)
    
    # Download data
    csv_path = download_ibtracs_data(force=force)
    
    # Parse track data
    track_points = parse_ibtracs_data(csv_path)
    
    if not track_points:
        print("ERROR: No track points found!")
        return
    
    # Build grid
    grid_cells = build_grid()
    
    # Compute scores
    grid_scores = compute_grid_storm_scores(grid_cells, track_points)
    
    # Save to database
    save_grid_to_database(grid_scores)
    
    # Statistics
    counts = [s["storm_count"] for s in grid_scores.values()]
    weighted = [s["intensity_weighted_count"] for s in grid_scores.values()]
    
    print("\n" + "=" * 60)
    print("GRID BUILD COMPLETE")
    print("=" * 60)
    print(f"Grid cells: {len(grid_scores):,}")
    print(f"Track points used: {len(track_points):,}")
    print(f"Storm count range: {min(counts)} - {max(counts)}")
    print(f"Intensity-weighted range: {min(weighted):.1f} - {max(weighted):.1f}")


def main():
    parser = argparse.ArgumentParser(
        description="Build storm pressure grid from IBTrACS data"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force rebuild even if grid is up to date",
    )
    
    args = parser.parse_args()
    ensure_storm_grid(force=args.force)


if __name__ == "__main__":
    main()
