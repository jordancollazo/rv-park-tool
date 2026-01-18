"""
run_insurance_pressure_enrichment.py

Orchestrates the full Insurance Pressure Index enrichment pipeline:
1. Build/refresh storm grid (if needed)
2. Enrich flood zones from FEMA NFHL
3. Enrich storm pressure from IBTrACS grid
4. Enrich disaster pressure from OpenFEMA
5. Compute composite Insurance Pressure Index

Usage:
    python execution/run_insurance_pressure_enrichment.py [--limit N] [--force]
"""

import argparse
import sys
from pathlib import Path

# Add execution directory to path
sys.path.insert(0, str(Path(__file__).parent))

from build_storm_pressure_grid import ensure_storm_grid
from enrich_nfhl_flood_zone import enrich_flood_zones
from enrich_storm_pressure import enrich_storm_pressure
from enrich_openfema_disaster_pressure import enrich_disaster_pressure
from compute_insurance_pressure import compute_insurance_pressure


def run_full_enrichment(limit: int | None = None, force: bool = False):
    """
    Run the complete Insurance Pressure enrichment pipeline.
    
    Args:
        limit: Optional limit on number of leads to process
        force: If True, force refresh all data sources
    """
    print("=" * 70)
    print("INSURANCE PRESSURE INDEX - FULL ENRICHMENT PIPELINE")
    print("=" * 70)
    print()
    
    # Step 1: Ensure storm grid is built
    print("[1/5] BUILDING/CHECKING STORM PRESSURE GRID...")
    print("-" * 50)
    ensure_storm_grid(force=force)
    print()
    
    # Step 2: Enrich flood zones
    print("[2/5] ENRICHING FEMA NFHL FLOOD ZONES...")
    print("-" * 50)
    enrich_flood_zones(limit=limit, force_refresh=force)
    print()
    
    # Step 3: Enrich storm pressure
    print("[3/5] ENRICHING STORM PROXIMITY PRESSURE...")
    print("-" * 50)
    enrich_storm_pressure(limit=limit, force_refresh=force)
    print()
    
    # Step 4: Enrich disaster pressure
    print("[4/5] ENRICHING OPENFEMA DISASTER PRESSURE...")
    print("-" * 50)
    enrich_disaster_pressure(limit=limit, force_refresh=force)
    print()
    
    # Step 5: Compute composite score
    print("[5/5] COMPUTING COMPOSITE INSURANCE PRESSURE INDEX...")
    print("-" * 50)
    compute_insurance_pressure(limit=limit, force_refresh=force)
    print()
    
    print("=" * 70)
    print("PIPELINE COMPLETE")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Run full Insurance Pressure enrichment pipeline"
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
        help="Force refresh all data sources and recompute all scores",
    )
    
    args = parser.parse_args()
    run_full_enrichment(limit=args.limit, force=args.force)


if __name__ == "__main__":
    main()
