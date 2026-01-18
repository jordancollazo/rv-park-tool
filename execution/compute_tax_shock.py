"""
compute_tax_shock.py
Calculates Tax Shock Risk Score for each lead based on county millage rates.
Higher millage = higher tax burden on property owners = higher acquisition opportunity.
"""

import sqlite3
import json
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_PATH = Path("data/leads.db")

# Florida millage statistics (2025 data)
# Statewide average is ~18, range is roughly 10-25
FL_MILLAGE_MIN = 10
FL_MILLAGE_MAX = 25
FL_MILLAGE_AVG = 18

def compute_risk():
    """Calculate Tax Shock Risk Score for all leads."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 1. Get County Metrics (most recent year)
    df_tax = pd.read_sql("""
        SELECT county_fips, total_millage 
        FROM county_tax_metrics 
        ORDER BY year DESC
    """, conn)
    
    if df_tax.empty:
        logging.error("No county tax metrics found. Run ingest_florida_millage.py first.")
        conn.close()
        return
        
    # Dedup to keep latest year per county
    df_tax = df_tax.drop_duplicates(subset=['county_fips'])
    tax_map = {row['county_fips']: row['total_millage'] for _, row in df_tax.iterrows()}
    
    # Calculate percentiles for relative scoring
    millages = list(tax_map.values())
    p25 = pd.Series(millages).quantile(0.25)
    p50 = pd.Series(millages).quantile(0.50)
    p75 = pd.Series(millages).quantile(0.75)
    p90 = pd.Series(millages).quantile(0.90)
    
    logging.info(f"Millage stats: min={min(millages):.1f}, p25={p25:.1f}, p50={p50:.1f}, p75={p75:.1f}, p90={p90:.1f}, max={max(millages):.1f}")
    
    # 2. Get Leads
    cursor.execute("SELECT id, county_fips, county_name FROM leads WHERE archived = 0 OR archived IS NULL")
    leads = cursor.fetchall()
    
    updates = []
    
    logging.info(f"Scoring {len(leads)} leads...")
    
    for lead in leads:
        score = 0
        reasons = []
        confidence = 'low'
        county_millage = None
        
        fips = lead['county_fips']
        
        if fips and fips in tax_map:
            millage = tax_map[fips]
            county_millage = millage
            confidence = 'high'
            
            # Scoring: Map millage to 0-100 score
            # Low millage (< p25) = low score (0-25)
            # Average millage (p25-p75) = medium score (25-60)
            # High millage (> p75) = high score (60-85)
            # Very high millage (> p90) = very high score (85-100)
            
            if millage >= p90:
                # Top 10% - highest tax burden
                score = 85 + (millage - p90) / (max(millages) - p90) * 15
                reasons.append(f"Very high county millage rate: {millage:.1f} mills (top 10%)")
            elif millage >= p75:
                # Top 25% - high tax burden
                score = 60 + (millage - p75) / (p90 - p75) * 25
                reasons.append(f"High county millage rate: {millage:.1f} mills (top 25%)")
            elif millage >= p50:
                # Above average
                score = 40 + (millage - p50) / (p75 - p50) * 20
                reasons.append(f"Above-average county millage: {millage:.1f} mills")
            elif millage >= p25:
                # Average
                score = 20 + (millage - p25) / (p50 - p25) * 20
            else:
                # Below average - low tax pressure
                score = (millage / p25) * 20
            
            # Clamp to 0-100
            score = max(0, min(100, score))
            
        else:
            reasons.append("County data not available")
        
        updates.append((
            round(score, 1),
            confidence,
            json.dumps(reasons),
            county_millage,
            0,  # county_taxes_levied_growth (not tracking)
            lead['id']
        ))

    # Batch Update
    cursor.executemany("""
        UPDATE leads SET 
            tax_shock_score_0_100 = ?,
            tax_shock_confidence = ?,
            tax_shock_reasons_json = ?,
            county_millage_change = ?,
            county_taxes_levied_growth = ?
        WHERE id = ?
    """, updates)
    
    conn.commit()
    
    # Report distribution
    cursor.execute("SELECT COUNT(*) FROM leads WHERE tax_shock_score_0_100 > 0")
    scored = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM leads WHERE tax_shock_score_0_100 >= 50")
    high_risk = cursor.fetchone()[0]
    
    logging.info(f"Updated scores for {len(updates)} leads. {scored} with score > 0, {high_risk} with high risk (≥50).")
    conn.close()

if __name__ == "__main__":
    compute_risk()
