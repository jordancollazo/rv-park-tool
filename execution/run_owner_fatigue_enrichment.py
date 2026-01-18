"""
run_owner_fatigue_enrichment.py
Score all leads in the database for Owner Fatigue.

This script reads leads from the database and scores them based on their
existing crawl data and metadata. It doesn't require .tmp/scored_sites.json.

Usage:
    python execution/run_owner_fatigue_enrichment.py
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.request
import urllib.error

# Import scoring functions
from score_owner_fatigue import (
    score_site_maintenance_neglect,
    score_operational_modernity_gap,
    score_listing_comms_friction,
    score_customer_friction_signals,
    determine_confidence,
)

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"

# HTTP settings for fetching robots.txt/sitemap
TIMEOUT = 10
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


def fetch_url_exists(url: str) -> bool:
    """Check if a URL exists (returns 200)."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=TIMEOUT) as response:
            return response.status == 200
    except Exception:
        return False


def score_lead_from_db(lead: dict) -> dict:
    """
    Score a lead for owner fatigue based on database fields.
    
    For leads without full crawl data, use available signals.
    """
    result = {
        "owner_fatigue_score_0_100": 0,
        "owner_fatigue_confidence": "low",
        "owner_fatigue_reasons_json": "[]",
        "owner_fatigue_breakdown_json": "{}",
    }
    
    crawl_status = lead.get("crawl_status")
    website = lead.get("website")
    
    # No website
    if not website or crawl_status == "no_website":
        result["owner_fatigue_score_0_100"] = 40
        result["owner_fatigue_confidence"] = "low"
        result["owner_fatigue_reasons_json"] = json.dumps(["No website - not investing in online presence"])
        result["owner_fatigue_breakdown_json"] = json.dumps({
            "site_maintenance_neglect": 0,
            "operational_modernity_gap": 20,
            "listing_comms_friction": 15,
            "customer_friction_signals": 5,
        })
        return result
    
    # Failed crawl
    if crawl_status == "failed":
        result["owner_fatigue_score_0_100"] = 35
        result["owner_fatigue_confidence"] = "low"
        result["owner_fatigue_reasons_json"] = json.dumps(["Website unreachable"])
        result["owner_fatigue_breakdown_json"] = json.dumps({
            "site_maintenance_neglect": 15,
            "operational_modernity_gap": 10,
            "listing_comms_friction": 10,
            "customer_friction_signals": 0,
        })
        return result
    
    # Aggregator only (Facebook, Yelp, etc.)
    if crawl_status == "aggregator":
        result["owner_fatigue_score_0_100"] = 35
        result["owner_fatigue_confidence"] = "low"
        result["owner_fatigue_reasons_json"] = json.dumps(["Aggregator listing only - no owned website"])
        result["owner_fatigue_breakdown_json"] = json.dumps({
            "site_maintenance_neglect": 10,
            "operational_modernity_gap": 10,
            "listing_comms_friction": 10,
            "customer_friction_signals": 5,
        })
        return result
    
    # For leads with successful crawl or no crawl status, score based on available data
    all_reasons = []
    score = 0
    breakdown = {
        "site_maintenance_neglect": 0,
        "operational_modernity_gap": 0,
        "listing_comms_friction": 0,
        "customer_friction_signals": 0,
    }
    
    # Check score_breakdown_json for existing website metrics
    score_breakdown = {}
    if lead.get("score_breakdown_json"):
        try:
            score_breakdown = json.loads(lead["score_breakdown_json"])
        except:
            pass
    
    # Modernity Gap signals from existing score breakdown
    if score_breakdown:
        # No HTTPS (check from technical score being low or conversion issues)
        technical = score_breakdown.get("technical", 10)
        mobile = score_breakdown.get("mobile", 10)
        conversion = score_breakdown.get("conversion", 10)
        
        if technical <= 5:
            breakdown["operational_modernity_gap"] += 5
            all_reasons.append("Poor technical implementation")
        
        if mobile <= 3:
            breakdown["operational_modernity_gap"] += 10
            all_reasons.append("Not mobile-friendly")
        
        if conversion <= 4:
            breakdown["listing_comms_friction"] += 10
            all_reasons.append("Poor conversion elements (contact info not visible)")
    
    # Site score indicates overall health
    site_score = lead.get("site_score_1_10", 5)
    if site_score <= 2:
        breakdown["site_maintenance_neglect"] += 15
        all_reasons.append("Very poor website score")
    elif site_score <= 4:
        breakdown["site_maintenance_neglect"] += 10
        all_reasons.append("Below-average website score")
    
    # Score reasons text analysis
    score_reasons = lead.get("score_reasons", "")
    if score_reasons:
        if "No HTTPS" in score_reasons:
            breakdown["operational_modernity_gap"] += 10
            all_reasons.append("No HTTPS")
        if "viewport" in score_reasons.lower():
            breakdown["operational_modernity_gap"] += 5
            all_reasons.append("No viewport meta tag")
        if "phone" in score_reasons.lower() and "not visible" in score_reasons.lower():
            breakdown["listing_comms_friction"] += 5
            all_reasons.append("Phone not visible on site")
    
    # Google reviews analysis
    google_rating = lead.get("google_rating")
    review_count = lead.get("review_count", 0)
    
    if google_rating and google_rating < 3.0:
        breakdown["customer_friction_signals"] += 10
        all_reasons.append(f"Low Google rating ({google_rating:.1f}★)")
    
    if review_count and review_count < 5:
        breakdown["site_maintenance_neglect"] += 5
        all_reasons.append("Very few reviews (low visibility)")
    
    # No crawl status but has website - likely neglected
    if not crawl_status and website:
        breakdown["site_maintenance_neglect"] += 10
        all_reasons.append("Website not recently crawled")
    
    # Calculate total
    total_score = sum(breakdown.values())
    total_score = min(100, max(0, total_score))
    
    # Determine confidence
    if crawl_status == "success" and score_breakdown:
        confidence = "high"
    elif crawl_status or score_breakdown:
        confidence = "medium"
    else:
        confidence = "low"
    
    result["owner_fatigue_score_0_100"] = total_score
    result["owner_fatigue_confidence"] = confidence
    result["owner_fatigue_reasons_json"] = json.dumps(all_reasons)
    result["owner_fatigue_breakdown_json"] = json.dumps(breakdown)
    
    return result


def enrich_all_leads():
    """Score all leads in the database for owner fatigue."""
    print(f"Opening database: {DB_PATH}")
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Get all leads
    cursor.execute("""
        SELECT id, name, website, crawl_status, site_score_1_10, 
               score_breakdown_json, score_reasons, google_rating, review_count, phone
        FROM leads
    """)
    leads = cursor.fetchall()
    
    print(f"Found {len(leads)} leads to score")
    print("-" * 50)
    
    updated = 0
    for i, row in enumerate(leads, 1):
        lead = dict(row)
        result = score_lead_from_db(lead)
        
        # Update database
        cursor.execute("""
            UPDATE leads SET
                owner_fatigue_score_0_100 = ?,
                owner_fatigue_confidence = ?,
                owner_fatigue_reasons_json = ?,
                owner_fatigue_breakdown_json = ?
            WHERE id = ?
        """, (
            result["owner_fatigue_score_0_100"],
            result["owner_fatigue_confidence"],
            result["owner_fatigue_reasons_json"],
            result["owner_fatigue_breakdown_json"],
            lead["id"]
        ))
        updated += 1
        
        if i % 50 == 0 or i == len(leads):
            print(f"[{i}/{len(leads)}] Scored {i} leads...")
            conn.commit()
    
    conn.commit()
    conn.close()
    
    print(f"\n✓ Updated {updated} leads with Owner Fatigue scores")
    
    # Print summary
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT AVG(owner_fatigue_score_0_100) FROM leads")
    avg = cursor.fetchone()[0] or 0
    
    cursor.execute("SELECT COUNT(*) FROM leads WHERE owner_fatigue_score_0_100 >= 60")
    high = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM leads WHERE owner_fatigue_score_0_100 >= 30 AND owner_fatigue_score_0_100 < 60")
    medium = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM leads WHERE owner_fatigue_score_0_100 < 30")
    low = cursor.fetchone()[0]
    
    cursor.execute("SELECT owner_fatigue_confidence, COUNT(*) FROM leads GROUP BY owner_fatigue_confidence")
    conf_breakdown = cursor.fetchall()
    
    conn.close()
    
    print(f"\nOwner Fatigue Summary:")
    print(f"  Average score: {avg:.1f}/100")
    print(f"  High fatigue (≥60): {high} ({100*high/updated:.0f}%)")
    print(f"  Medium fatigue (30-59): {medium} ({100*medium/updated:.0f}%)")
    print(f"  Low fatigue (<30): {low} ({100*low/updated:.0f}%)")
    
    print(f"\nConfidence Breakdown:")
    for conf, count in conf_breakdown:
        print(f"  {conf}: {count}")


if __name__ == "__main__":
    enrich_all_leads()
