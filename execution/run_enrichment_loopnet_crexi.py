"""
run_enrichment_loopnet_crexi.py

Runs the full data enrichment pipeline for leads from LoopNet and Crexi.
Enrichment includes:
- Flood Zone Identification (FEMA NFHL)
- Storm Pressure (IBTrACS)
- Disaster Pressure (FEMA OpenFEMA)
- Tax Shock Risk
- Insurance Pressure Index (composite)
- Amenity Score (Google Places)

Usage:
    python execution/run_enrichment_loopnet_crexi.py
"""

import subprocess
import sys
from pathlib import Path

EXECUTION_DIR = Path(__file__).parent

# Enrichment scripts in dependency order
ENRICHMENT_SCRIPTS = [
    # 1. Flood Zone (dependency for Insurance Pressure)
    "enrich_nfhl_flood_zone.py",
    # 2. Storm Pressure (dependency for Insurance Pressure)
    "enrich_storm_pressure.py",
    # 3. Disaster Pressure (dependency for Insurance Pressure)
    "enrich_openfema_disaster_pressure.py",
    # 4. Tax Shock Risk
    "compute_tax_shock.py",
    # 5. Insurance Pressure Index (composite)
    "compute_insurance_pressure.py",
    # 6. Google Places Amenity Score (optional, but valuable)
    "enrich_leads_gmaps.py",
]


def run_script(script_name: str) -> bool:
    """Run an enrichment script and return success status."""
    script_path = EXECUTION_DIR / script_name
    if not script_path.exists():
        print(f"⚠️  Script not found: {script_name}")
        return False
    
    print(f"\n{'='*60}")
    print(f"Running: {script_name}")
    print('='*60)
    
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=EXECUTION_DIR.parent,  # Run from project root
        capture_output=False
    )
    
    if result.returncode != 0:
        print(f"❌ {script_name} failed with exit code {result.returncode}")
        return False
    
    print(f"✅ {script_name} completed successfully")
    return True


def main():
    print("="*60)
    print("MHP Data Enrichment Pipeline - LoopNet/Crexi Leads")
    print("="*60)
    
    # Check lead counts first
    import sqlite3
    db_path = EXECUTION_DIR.parent / "data" / "leads.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT scrape_source, COUNT(*) 
        FROM leads 
        WHERE scrape_source IN ('loopnet', 'crexi')
        GROUP BY scrape_source
    """)
    counts = cursor.fetchall()
    conn.close()
    
    total_leads = sum(c[1] for c in counts) if counts else 0
    print(f"\nTarget Leads:")
    for source, count in counts:
        print(f"  - {source}: {count} leads")
    print(f"  Total: {total_leads} leads")
    
    if total_leads == 0:
        print("\n⚠️  No LoopNet or Crexi leads found in database!")
        print("Please run the LoopNet/Crexi scrapers first.")
        return
    
    # Run enrichment pipeline
    successful = 0
    failed = 0
    
    for script in ENRICHMENT_SCRIPTS:
        if run_script(script):
            successful += 1
        else:
            failed += 1
    
    # Summary
    print("\n" + "="*60)
    print("ENRICHMENT PIPELINE COMPLETE")
    print("="*60)
    print(f"Scripts run: {successful + failed}")
    print(f"  ✅ Successful: {successful}")
    print(f"  ❌ Failed: {failed}")


if __name__ == "__main__":
    main()
