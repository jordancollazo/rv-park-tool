"""
compute_insurance_pressure.py

Computes the composite Insurance Pressure Index (0-100) by combining:
- Flood zone stress (0-50)
- Storm proximity pressure (0-30)
- Disaster declaration pressure (0-20)

Usage:
    python execution/compute_insurance_pressure.py [--limit N]
"""

import argparse
import json
import sqlite3
from pathlib import Path

# Database path
DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"

# Flood zone scoring (0-50 scale)
FLOOD_ZONE_SCORES = {
    "VE": 50,
    "V": 45,
    "AE": 40,
    "AH": 35,
    "AO": 35,
    "A": 35,
    "A99": 25,
    "AR": 25,
    "D": 20,
    "X": 5,
}


def get_db():
    """Get database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_flood_zone_score(flood_zone: str | None) -> tuple[float, str]:
    """
    Get flood zone score (0-50) and description.
    
    Returns (score, description) tuple.
    """
    if not flood_zone:
        return 0.0, None
    
    zone = flood_zone.upper().strip()
    score = FLOOD_ZONE_SCORES.get(zone, 5.0)
    
    descriptions = {
        "VE": "Coastal velocity flood zone (highest flood risk + wave action)",
        "V": "Coastal high hazard flood zone",
        "AE": "1% annual flood risk with base flood elevations",
        "AH": "Shallow flooding zone (1-3ft depth)",
        "AO": "Sheet flow flooding zone",
        "A": "1% annual flood risk zone",
        "A99": "Protected by federal levee under construction",
        "AR": "Temporarily increased flood risk during levee restoration",
        "D": "Undetermined flood hazard area",
        "X": "Minimal flood hazard",
    }
    
    description = descriptions.get(zone)
    if description:
        description = f"Flood zone {zone}: {description}"
    
    return float(score), description


def get_storm_description(score: float) -> str | None:
    """Get description for storm proximity score."""
    if score is None or score <= 0:
        return None
    
    if score >= 25:
        return "Very high hurricane exposure in past 30 years"
    elif score >= 18:
        return "High hurricane exposure in past 30 years"
    elif score >= 10:
        return "Moderate hurricane exposure in past 30 years"
    elif score >= 5:
        return "Some hurricane exposure in past 30 years"
    else:
        return "Low hurricane exposure"


def get_disaster_description(score: float) -> str | None:
    """Get description for disaster pressure score."""
    if score is None or score <= 0:
        return None
    
    if score >= 15:
        return "Very high disaster declaration frequency in county"
    elif score >= 10:
        return "High disaster declaration frequency in county"
    elif score >= 5:
        return "Moderate disaster declaration history in county"
    else:
        return "Low disaster declaration history"


def compute_confidence(has_flood: bool, has_storm: bool, has_disaster: bool) -> str:
    """Compute confidence level based on data availability."""
    components = sum([has_flood, has_storm, has_disaster])
    
    if components >= 3:
        return "high"
    elif components >= 2:
        return "medium"
    else:
        return "low"


def compute_insurance_pressure(limit: int | None = None, force_refresh: bool = False):
    """
    Compute composite Insurance Pressure Index for leads.
    
    Combines flood zone, storm proximity, and disaster pressure scores.
    
    Args:
        limit: Optional limit on number of leads to process
        force_refresh: If True, recompute for all leads
    """
    print("=" * 60)
    print("INSURANCE PRESSURE INDEX COMPUTATION")
    print("=" * 60)
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Get leads to process
    if force_refresh:
        query = """
            SELECT id, name, flood_zone, storm_proximity_score, disaster_pressure_score
            FROM leads
            ORDER BY id
        """
    else:
        query = """
            SELECT id, name, flood_zone, storm_proximity_score, disaster_pressure_score
            FROM leads
            WHERE insurance_pressure_score_0_100 IS NULL
            ORDER BY id
        """
    
    if limit:
        query += f" LIMIT {limit}"
    
    cursor.execute(query)
    leads = cursor.fetchall()
    
    print(f"Computing insurance pressure for {len(leads)} leads...")
    
    if not leads:
        print("No leads need insurance pressure computation.")
        conn.close()
        return
    
    # Score distribution tracking
    score_buckets = {"0-20": 0, "21-40": 0, "41-60": 0, "61-80": 0, "81-100": 0}
    
    for i, lead in enumerate(leads):
        lead_id = lead["id"]
        flood_zone = lead["flood_zone"]
        storm_score = lead["storm_proximity_score"]
        disaster_score = lead["disaster_pressure_score"]
        
        if (i + 1) % 100 == 0:
            print(f"  Processing {i+1}/{len(leads)}...")
        
        # Get component scores
        flood_score, flood_desc = get_flood_zone_score(flood_zone)
        storm_desc = get_storm_description(storm_score)
        disaster_desc = get_disaster_description(disaster_score)
        
        # Handle nulls
        storm_score = storm_score or 0.0
        disaster_score = disaster_score or 0.0
        
        # Compute composite score
        total_score = flood_score + storm_score + disaster_score
        
        # Clamp to 0-100
        total_score = max(0.0, min(100.0, total_score))
        
        # Compute confidence
        has_flood = flood_zone is not None and flood_zone != ""
        has_storm = lead["storm_proximity_score"] is not None
        has_disaster = lead["disaster_pressure_score"] is not None
        confidence = compute_confidence(has_flood, has_storm, has_disaster)
        
        # Build reasons list
        reasons = []
        if flood_desc:
            reasons.append(flood_desc)
        if storm_desc:
            reasons.append(storm_desc)
        if disaster_desc:
            reasons.append(disaster_desc)
        
        if not reasons:
            reasons.append("Insufficient data for detailed pressure analysis")
        
        # Build breakdown
        breakdown = {
            "flood_zone_score": round(flood_score, 1),
            "storm_proximity_score": round(storm_score, 1),
            "disaster_pressure_score": round(disaster_score, 1),
            "total": round(total_score, 1),
        }
        
        # Update database
        cursor.execute("""
            UPDATE leads
            SET insurance_pressure_score_0_100 = ?,
                insurance_pressure_confidence = ?,
                insurance_pressure_reasons_json = ?,
                insurance_pressure_breakdown_json = ?
            WHERE id = ?
        """, (
            round(total_score, 1),
            confidence,
            json.dumps(reasons),
            json.dumps(breakdown),
            lead_id,
        ))
        
        # Track distribution
        if total_score <= 20:
            score_buckets["0-20"] += 1
        elif total_score <= 40:
            score_buckets["21-40"] += 1
        elif total_score <= 60:
            score_buckets["41-60"] += 1
        elif total_score <= 80:
            score_buckets["61-80"] += 1
        else:
            score_buckets["81-100"] += 1
    
    conn.commit()
    conn.close()
    
    # Summary
    print("\n" + "=" * 60)
    print("COMPUTATION COMPLETE")
    print("=" * 60)
    print(f"Processed: {len(leads)} leads")
    print("\nScore Distribution:")
    for bucket, count in score_buckets.items():
        pct = (count / len(leads) * 100) if leads else 0
        bar = "█" * int(pct / 2)
        print(f"  {bucket:>8}: {count:>5} ({pct:>5.1f}%) {bar}")


def main():
    parser = argparse.ArgumentParser(
        description="Compute composite Insurance Pressure Index"
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
        help="Force recompute for all leads",
    )
    
    args = parser.parse_args()
    compute_insurance_pressure(limit=args.limit, force_refresh=args.force)


if __name__ == "__main__":
    main()
