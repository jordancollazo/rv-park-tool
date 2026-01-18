"""
batch_crawl_and_score.py
Crawl leads from south to north and update owner fatigue scores in real-time.

Usage:
    python execution/batch_crawl_and_score.py --limit 20  # First batch
    python execution/batch_crawl_and_score.py             # All remaining
"""

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

# Add parent dir to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from crawl_website import crawl_website
from score_owner_fatigue import (
    score_site_maintenance_neglect,
    score_operational_modernity_gap,
    score_listing_comms_friction,
    score_customer_friction_signals,
    determine_confidence,
)

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"


def score_crawled_lead(crawl_result: dict, lead: dict) -> dict:
    """Score a lead for owner fatigue based on fresh crawl data."""
    result = {
        "owner_fatigue_score_0_100": 0,
        "owner_fatigue_confidence": "low",
        "owner_fatigue_reasons_json": "[]",
        "owner_fatigue_breakdown_json": "{}",
    }
    
    crawl_status = crawl_result.get("crawl_status", "failed")
    pages = crawl_result.get("pages", [])
    
    # No website
    if crawl_status == "no_website":
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
    if crawl_status == "failed" or not pages:
        result["owner_fatigue_score_0_100"] = 35
        result["owner_fatigue_confidence"] = "low"
        result["owner_fatigue_reasons_json"] = json.dumps(["Website unreachable or failed to crawl"])
        result["owner_fatigue_breakdown_json"] = json.dumps({
            "site_maintenance_neglect": 15,
            "operational_modernity_gap": 10,
            "listing_comms_friction": 10,
            "customer_friction_signals": 0,
        })
        return result
    
    # Aggregator only
    if crawl_result.get("is_aggregator") or crawl_result.get("is_facebook_only"):
        result["owner_fatigue_score_0_100"] = 35
        result["owner_fatigue_confidence"] = "low"
        result["owner_fatigue_reasons_json"] = json.dumps(["Aggregator or Facebook only - no owned website"])
        result["owner_fatigue_breakdown_json"] = json.dumps({
            "site_maintenance_neglect": 10,
            "operational_modernity_gap": 10,
            "listing_comms_friction": 10,
            "customer_friction_signals": 5,
        })
        return result
    
    # Build a "place" dict that the scoring functions expect
    place = {
        **lead,  # Include lead data (phone, etc.)
        **crawl_result,  # Include crawl data (pages, etc.)
    }
    
    all_reasons = []
    
    # A) Site Maintenance Neglect
    a_score, a_reasons = score_site_maintenance_neglect(place)
    all_reasons.extend(a_reasons)
    
    # B) Operational Modernity Gap
    b_score, b_reasons = score_operational_modernity_gap(place)
    all_reasons.extend(b_reasons)
    
    # C) Listing/Comms Friction
    c_score, c_reasons = score_listing_comms_friction(place)
    all_reasons.extend(c_reasons)
    
    # D) Customer Friction Signals
    d_score, d_reasons = score_customer_friction_signals(place)
    all_reasons.extend(d_reasons)
    
    total_score = min(100, a_score + b_score + c_score + d_score)
    confidence = determine_confidence(place)
    
    result["owner_fatigue_score_0_100"] = total_score
    result["owner_fatigue_confidence"] = confidence
    result["owner_fatigue_reasons_json"] = json.dumps(all_reasons)
    result["owner_fatigue_breakdown_json"] = json.dumps({
        "site_maintenance_neglect": a_score,
        "operational_modernity_gap": b_score,
        "listing_comms_friction": c_score,
        "customer_friction_signals": d_score,
    })
    
    return result


def update_lead_in_db(conn, lead_id: int, crawl_result: dict, fatigue_result: dict):
    """Update a single lead with crawl and fatigue data."""
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE leads SET
            crawl_status = ?,
            owner_fatigue_score_0_100 = ?,
            owner_fatigue_confidence = ?,
            owner_fatigue_reasons_json = ?,
            owner_fatigue_breakdown_json = ?
        WHERE id = ?
    """, (
        crawl_result.get("crawl_status", "failed"),
        fatigue_result["owner_fatigue_score_0_100"],
        fatigue_result["owner_fatigue_confidence"],
        fatigue_result["owner_fatigue_reasons_json"],
        fatigue_result["owner_fatigue_breakdown_json"],
        lead_id
    ))
    conn.commit()


def get_leads_south_to_north(conn, limit: int = None, skip_crawled: bool = True):
    """Get leads ordered by latitude (south to north)."""
    cursor = conn.cursor()
    
    query = """
        SELECT id, name, website, latitude, longitude, phone, crawl_status
        FROM leads
        WHERE latitude IS NOT NULL
    """
    
    if skip_crawled:
        query += " AND (crawl_status IS NULL OR crawl_status = '')"
    
    query += " ORDER BY latitude ASC"
    
    if limit:
        query += f" LIMIT {limit}"
    
    cursor.execute(query)
    return cursor.fetchall()


def run_batch(limit: int = None, skip_crawled: bool = True):
    """Run batch crawl and score."""
    print(f"Opening database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    leads = get_leads_south_to_north(conn, limit, skip_crawled)
    total = len(leads)
    
    if total == 0:
        print("No leads to process!")
        conn.close()
        return
    
    print(f"Found {total} leads to crawl (south to north)")
    print("=" * 60)
    
    results_summary = []
    
    for i, row in enumerate(leads, 1):
        lead = dict(row)
        lead_id = lead["id"]
        name = lead["name"][:40] if lead["name"] else "Unknown"
        website = lead["website"]
        lat = lead["latitude"]
        
        print(f"\n[{i}/{total}] {name}")
        print(f"    Lat: {lat:.4f} | Website: {website or 'None'}")
        
        # Crawl
        if website:
            print(f"    Crawling...", end=" ", flush=True)
            try:
                crawl_result = crawl_website(website)
                status = crawl_result.get("crawl_status", "failed")
                pages_count = len(crawl_result.get("pages", []))
                print(f"{status} ({pages_count} pages)")
            except Exception as e:
                print(f"ERROR: {e}")
                crawl_result = {"crawl_status": "failed", "pages": []}
        else:
            crawl_result = {"crawl_status": "no_website", "pages": []}
            print(f"    No website to crawl")
        
        # Score
        fatigue_result = score_crawled_lead(crawl_result, lead)
        score = fatigue_result["owner_fatigue_score_0_100"]
        conf = fatigue_result["owner_fatigue_confidence"]
        reasons = json.loads(fatigue_result["owner_fatigue_reasons_json"])
        
        print(f"    Fatigue Score: {score}/100 ({conf})")
        for r in reasons[:2]:
            print(f"      - {r}")
        
        # Update DB immediately
        update_lead_in_db(conn, lead_id, crawl_result, fatigue_result)
        print(f"    [SAVED TO DB]")
        
        results_summary.append({
            "name": name,
            "lat": lat,
            "score": score,
            "confidence": conf,
        })
        
        # Small delay to be nice to servers
        time.sleep(0.5)
    
    conn.close()
    
    # Print summary
    print("\n" + "=" * 60)
    print("BATCH COMPLETE!")
    print("=" * 60)
    
    avg_score = sum(r["score"] for r in results_summary) / len(results_summary) if results_summary else 0
    high_fatigue = sum(1 for r in results_summary if r["score"] >= 40)
    
    print(f"Processed: {total} leads")
    print(f"Average Fatigue Score: {avg_score:.1f}/100")
    print(f"High Fatigue (>=40): {high_fatigue}")
    print(f"\nTop 5 by Fatigue Score:")
    
    for r in sorted(results_summary, key=lambda x: x["score"], reverse=True)[:5]:
        print(f"  {r['score']}/100 | {r['name']} (Lat: {r['lat']:.2f})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch crawl and score leads")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of leads to process")
    parser.add_argument("--include-crawled", action="store_true", help="Include already-crawled leads")
    args = parser.parse_args()
    
    run_batch(limit=args.limit, skip_crawled=not args.include_crawled)
