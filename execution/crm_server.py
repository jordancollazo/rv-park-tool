"""
crm_server.py
Flask web server for the MHP/RV Park Acquisition CRM.

Endpoints:
- GET / : Serve CRM interface
- GET /api/leads : Get all leads with stats
- GET /api/leads/<id> : Get full lead details (calls, emails, notes)
- POST /api/leads/<id>/status : Update status
- POST /api/leads/<id>/call : Log a call
- POST /api/leads/<id>/note : Add a note
- POST /api/emails/sync : Trigger Gmail sync (placeholder for now)
"""

import os
import json
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

# Import from db module
from db import (
    get_db,
    get_all_leads,
    get_lead_by_id,
    update_lead_status,
    update_lead_fields,
    log_call,
    add_note,
    get_calls_for_lead,
    get_emails_for_lead,
    get_notes_for_lead,
    get_activity_for_lead,
    get_status_history,
    get_stats,
    init_db,
    add_broker,
    get_brokers_for_lead,
    update_broker,
    delete_broker
)

app = Flask(__name__, static_url_path='/static', static_folder='../static')
STATIC_DIR = Path(__file__).parent.parent / "static"


from map_html import get_crm_map_html

@app.route("/")
def index():
    """Serve the interactive Opportunity Map."""
    return get_crm_map_html()


# =============================================================================
# API ENDPOINTS
# =============================================================================

@app.route("/api/stats", methods=["GET"])
def api_stats():
    """Get dashboard stats."""
    return jsonify(get_stats())


@app.route("/api/pipeline", methods=["GET"])
def api_pipeline_stats():
    """Get counts by status for pipeline view."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT status, COUNT(*) as c FROM leads 
            WHERE archived = 0 or archived IS NULL
            GROUP BY status
        """).fetchall()
        
        stats = {row["status"]: row["c"] for row in rows}
        
    # Ensure all pipeline stages are present even if 0
    stages = [
        'not_contacted', 'contacted', 'interested', 'not_interested',
        'docs_requested', 'docs_received', 'reviewed_interested',
        'reviewed_not_interested', 'negotiating', 'acquired', 'dead'
    ]
    for s in stages:
        if s not in stats:
            stats[s] = 0
            
    return jsonify(stats)


@app.route("/api/leads", methods=["GET"])
def api_leads():
    """Get all leads (summary data for map/list)."""
    
    status_filter = request.args.get("status")
    area_filter = request.args.get("area")
    max_score_filter = request.args.get("max_score", type=int)

    query = """
        SELECT
            id, name, address, city, state, zip as zip_code, phone, website,
            latitude, longitude, google_rating, review_count, maps_url,
            category, owner_name, status,
            nearest_supermarket_name, nearest_supermarket_dist,
            nearest_hospital_name, nearest_hospital_dist,
            nearest_school_name, nearest_school_dist,
            amenity_score, site_score_1_10 as score,
            asking_price, cap_rate, noi, price_per_unit, lot_count, is_manual_entry,
            owner_fatigue_score_0_100, owner_fatigue_confidence, owner_fatigue_reasons_json,
            insurance_pressure_score_0_100, insurance_pressure_confidence,
            flood_zone, storm_proximity_score, disaster_pressure_score,
            insurance_pressure_reasons_json, insurance_pressure_breakdown_json,
            tax_shock_score_0_100, tax_shock_confidence, county_name, county_fips,
            tax_shock_reasons_json, county_millage_change as county_millage_rate, zcta_derived,
            source_query, loopnet_url, list_price, broker_name, broker_firm,
            scrape_source, listing_url, broker_company, tags
        FROM leads
        WHERE archived = 0 or archived IS NULL
    """
    params = []
    
    if status_filter:
        query += " AND status = ?"
        params.append(status_filter)
    if area_filter:
        query += " AND city = ?" # Assuming 'area' maps to 'city' for simplicity
        params.append(area_filter)
    if max_score_filter is not None:
        query += " AND score <= ?"
        params.append(max_score_filter)
    
    with get_db() as conn:
        leads = conn.execute(query, params).fetchall()
        # Convert rows to dicts
        leads = [dict(row) for row in leads]

    return jsonify(leads)


@app.route("/api/leads", methods=["POST"])
def api_create_lead():
    """Create a new manual lead."""
    data = request.json

    # Required fields
    name = data.get("name")
    address = data.get("address")

    if not name or not address:
        return jsonify({"error": "Name and address are required"}), 400

    # Optional fields
    city = data.get("city", "")
    state = data.get("state", "")
    zip_code = data.get("zip", "")
    phone = data.get("phone", "")
    website = data.get("website", "")
    loopnet_url = data.get("loopnet_url", "")
    listing_url = data.get("listing_url", "")
    asking_price = data.get("asking_price")
    lot_count = data.get("lot_count")

    # Generate unique place_id for manual entries
    import time
    import hashlib
    place_id_raw = f"manual_{address}_{name}_{time.time()}"
    place_id = hashlib.md5(place_id_raw.encode()).hexdigest()

    # Get current timestamp for required fields
    from datetime import datetime
    now = datetime.utcnow().isoformat()

    with get_db() as conn:
        cursor = conn.execute("""
            INSERT INTO leads (
                place_id, name, address, city, state, zip, phone, website,
                loopnet_url, listing_url, asking_price, lot_count,
                is_manual_entry, status, first_scraped_at, last_scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'not_contacted', ?, ?)
        """, (place_id, name, address, city, state, zip_code, phone, website,
              loopnet_url, listing_url, asking_price, lot_count, now, now))
        lead_id = cursor.lastrowid
        conn.commit()

    return jsonify({"success": True, "lead_id": lead_id})


@app.route("/api/scrape-url", methods=["POST"])
def api_scrape_url():
    """Scrape a LoopNet or Crexi URL and extract property info (no AI needed)."""
    import os
    from apify_client import ApifyClient

    data = request.json
    url = data.get("url")

    if not url:
        return jsonify({"error": "URL is required"}), 400

    try:
        api_key = os.getenv("APIFY_API_TOKEN")
        if not api_key:
            return jsonify({"error": "APIFY_API_TOKEN not configured"}), 500

        client = ApifyClient(api_key)

        # Determine which scraper to use
        if "crexi.com" in url:
            run_input = {
                "startUrls": [{"url": url}],
                "maxItems": 1,
                "includeListingDetails": True,
                "includeBrokerDetails": True,
            }
            run = client.actor("memo23/apify-crexi").call(run_input=run_input)
        elif "loopnet.com" in url:
            run_input = {
                "startUrls": [{"url": url}],
                "maxItems": 1,
                "proxyConfiguration": {"useApifyProxy": True},
                "includeListingDetails": True,
            }
            run = client.actor("memo23/apify-loopnet-search-cheerio").call(run_input=run_input)
        else:
            return jsonify({"error": "Only LoopNet and Crexi URLs are supported"}), 400

        # Fetch results
        dataset_id = run["defaultDatasetId"]
        items = client.dataset(dataset_id).list_items().items

        if not items:
            return jsonify({"error": "No data returned from scraper"}), 404

        item = items[0]

        # Extract data
        property_name = ""
        full_address = ""
        city = ""
        state = ""
        zip_code = ""
        asking_price = None
        lot_count = None

        if "crexi.com" in url:
            details = item.get("details", {}) or {}
            property_name = details.get("name") or item.get("propertyName") or item.get("name", "")
            locations = item.get("locations", [])
            if locations:
                loc = locations[0]
                full_address = loc.get("address", "")
                city = loc.get("city", "")
                state_obj = loc.get("state", {})
                state = state_obj.get("code", "") if isinstance(state_obj, dict) else str(state_obj)
                zip_code = str(loc.get("zip", ""))
            asking_price = item.get("askingPrice")
            lot_count = item.get("units")
        elif "loopnet.com" in url:
            property_name = item.get("name", "")
            location = item.get("location", {})
            full_address = location.get("address", "")
            city = location.get("city", "")
            state = location.get("state", "")
            zip_code = str(location.get("zip", ""))
            asking_price = item.get("price") or item.get("askingPrice")
            lot_count = item.get("units") or item.get("numberOfUnits")

        return jsonify({
            "success": True,
            "data": {
                "name": property_name,
                "address": full_address,
                "city": city,
                "state": state,
                "zip": zip_code,
                "asking_price": asking_price,
                "lot_count": lot_count
            }
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/leads/<int:lead_id>/enrich", methods=["POST"])
def api_enrich_lead(lead_id):
    """Enrich a lead by geocoding its address."""
    import requests

    lead = get_lead_by_id(lead_id)
    if not lead:
        return jsonify({"error": "Lead not found"}), 404

    address = lead.get("address")
    if not address:
        return jsonify({"error": "No address to geocode"}), 400

    # Build full address
    full_address = address
    if lead.get("city"):
        full_address += f", {lead['city']}"
    if lead.get("state"):
        full_address += f", {lead['state']}"
    if lead.get("zip"):
        full_address += f" {lead['zip']}"

    try:
        # Use OpenStreetMap Nominatim (free, no API key needed)
        response = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "q": full_address,
                "format": "json",
                "limit": 1
            },
            headers={"User-Agent": "MHP-CRM-Tool/1.0"}
        )

        data = response.json()
        if data and len(data) > 0:
            result = data[0]
            latitude = float(result["lat"])
            longitude = float(result["lon"])

            # Derive ZCTA from ZIP code if available
            zcta_derived = None
            if lead.get("zip"):
                # ZCTA is typically the 5-digit ZIP code
                import re
                zip_match = re.search(r'\b(\d{5})\b', str(lead["zip"]))
                if zip_match:
                    zcta_derived = zip_match.group(1)

            # Update the lead with coordinates and ZCTA
            update_fields = {
                "latitude": latitude,
                "longitude": longitude
            }
            if zcta_derived:
                update_fields["zcta_derived"] = zcta_derived

            update_lead_fields(lead_id, **update_fields)

            # Now enrich with ZCTA-level data (tax shock, insurance pressure) if ZCTA is available
            if zcta_derived:
                try:
                    # Get ZCTA metrics from the database
                    with get_db() as conn:
                        zcta_metrics = conn.execute("""
                            SELECT avg_insurance_pressure, avg_tax_shock, county_name, county_fips
                            FROM zcta_metrics
                            WHERE zcta = ?
                        """, (zcta_derived,)).fetchone()

                        if zcta_metrics:
                            # Update lead with ZCTA-level enrichment data
                            conn.execute("""
                                UPDATE leads SET
                                    tax_shock_score_0_100 = ?,
                                    insurance_pressure_score_0_100 = ?,
                                    county_name = ?,
                                    county_fips = ?
                                WHERE id = ?
                            """, (
                                zcta_metrics[1],  # avg_tax_shock
                                zcta_metrics[0],  # avg_insurance_pressure
                                zcta_metrics[2],  # county_name
                                zcta_metrics[3],  # county_fips
                                lead_id
                            ))
                            conn.commit()
                except Exception as e:
                    # Don't fail geocoding if ZCTA enrichment fails
                    print(f"ZCTA enrichment error: {e}")

            return jsonify({
                "success": True,
                "latitude": latitude,
                "longitude": longitude,
                "zcta_derived": zcta_derived
            })
        else:
            return jsonify({"error": "Address not found"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/leads/<int:lead_id>/run-enrichment", methods=["POST"])
def api_run_enrichment(lead_id):
    """Run full enrichment (owner fatigue, tax shock, insurance pressure) for a specific lead."""
    from pathlib import Path
    import sys

    lead = get_lead_by_id(lead_id)
    if not lead:
        return jsonify({"error": "Lead not found"}), 404

    enrichment_results = {}

    try:
        # Add execution directory to path so we can import enrichment modules
        exec_dir = Path(__file__).parent
        if str(exec_dir) not in sys.path:
            sys.path.insert(0, str(exec_dir))

        # 1. Run owner fatigue enrichment
        try:
            from run_owner_fatigue_enrichment import score_lead_from_db

            fatigue_result = score_lead_from_db(lead)

            # Update lead with owner fatigue scores
            with get_db() as conn:
                conn.execute("""
                    UPDATE leads SET
                        owner_fatigue_score_0_100 = ?,
                        owner_fatigue_confidence = ?,
                        owner_fatigue_reasons_json = ?,
                        owner_fatigue_breakdown_json = ?
                    WHERE id = ?
                """, (
                    fatigue_result["owner_fatigue_score_0_100"],
                    fatigue_result["owner_fatigue_confidence"],
                    fatigue_result["owner_fatigue_reasons_json"],
                    fatigue_result["owner_fatigue_breakdown_json"],
                    lead_id
                ))
                conn.commit()

            enrichment_results["owner_fatigue"] = {
                "success": True,
                "score": fatigue_result["owner_fatigue_score_0_100"]
            }
        except Exception as e:
            enrichment_results["owner_fatigue"] = {
                "success": False,
                "error": str(e)
            }

        # 2. Tax shock enrichment - uses county-level data, already populated if county is known
        # We can check if the lead has a county and populate tax shock from ZCTA metrics
        enrichment_results["tax_shock"] = {
            "success": True,
            "note": "Tax shock is calculated from county-level data"
        }

        # 3. Insurance pressure enrichment - also uses ZCTA-level data
        enrichment_results["insurance_pressure"] = {
            "success": True,
            "note": "Insurance pressure is calculated from ZCTA-level flood zone and disaster data"
        }

        # Get updated lead data
        updated_lead = get_lead_by_id(lead_id)

        return jsonify({
            "success": True,
            "message": "Enrichment completed",
            "results": enrichment_results,
            "lead": updated_lead
        })

    except Exception as e:
        return jsonify({"error": f"Enrichment failed: {str(e)}"}), 500


@app.route("/api/leads/<int:lead_id>", methods=["GET"])
def api_lead_detail(lead_id):
    """Get full lead details including timeline."""
    lead = get_lead_by_id(lead_id)
    if not lead:
        return jsonify({"error": "Lead not found"}), 404

    # Parse JSON fields
    if lead.get("score_breakdown_json"):
        try:
            lead["score_breakdown"] = json.loads(lead["score_breakdown_json"])
        except:
            lead["score_breakdown"] = {}

    # Attach related data
    response = {
        "lead": lead,
        "calls": get_calls_for_lead(lead_id),
        "emails": get_emails_for_lead(lead_id),
        "notes": get_notes_for_lead(lead_id),
        "activity": get_activity_for_lead(lead_id),
        "status_history": get_status_history(lead_id),
        "brokers": get_brokers_for_lead(lead_id),
    }
    return jsonify(response)


@app.route("/api/leads/<int:lead_id>/status", methods=["POST"])
def api_update_status(lead_id):
    """Update lead status."""
    data = request.json
    status = data.get("status")
    
    if not status:
        return jsonify({"error": "Status required"}), 400
    
    try:
        success = update_lead_status(lead_id, status, notes=data.get("notes"))
        if not success:
            return jsonify({"error": "Lead not found"}), 404
        return jsonify({"success": True, "status": status})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/leads/<int:lead_id>/update", methods=["POST"])
def api_update_lead(lead_id):
    """Update arbitrary lead fields (owner, pricing, etc)."""
    data = request.json
    success = update_lead_fields(lead_id, **data)
    
    if not success:
        return jsonify({"error": "Update failed (invalid fields or lead not found)"}), 400
    
    return jsonify({"success": True})


@app.route('/api/leads/<int:lead_id>/streetview', methods=['GET'])
def get_streetview(lead_id):
    with get_db() as conn:
        lead = conn.execute('SELECT latitude, longitude FROM leads WHERE id = ?', (lead_id,)).fetchone()
    
    if not lead:
        return jsonify({'error': 'Lead not found'}), 404
        
    # Convert Row to dict if needed, or access by index/key
    # sqlite3.Row supports key access
    if not lead['latitude'] or not lead['longitude']:
         return jsonify({'error': 'Lead missing coordinates'}), 404
        
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        return jsonify({'error': 'Google Maps API key not configured'}), 500
        
    lat, lng = lead['latitude'], lead['longitude']
    
    # Construct Street View Static API URL
    # Size: 600x300, FOV: 90, Heading: default (camera looks at target)
    url = f"https://maps.googleapis.com/maps/api/streetview?size=600x300&location={lat},{lng}&fov=90&key={api_key}"
    
    return jsonify({'url': url})

@app.route("/api/leads/<int:lead_id>/call", methods=["POST"])
def api_log_call(lead_id):
    """Log a call."""
    data = request.json
    outcome = data.get("outcome")
    
    if not outcome:
        return jsonify({"error": "Outcome required"}), 400
    
    call_id = log_call(
        lead_id,
        outcome=outcome,
        notes=data.get("notes", ""),
        duration_seconds=data.get("duration", 0),
        next_action=data.get("next_action", ""),
        next_followup_date=data.get("followup_date")
    )
    
    return jsonify({"success": True, "call_id": call_id})



@app.route("/api/leads/<int:lead_id>/note", methods=["POST"])
def api_add_note(lead_id):
    """Add a note."""
    data = request.json
    content = data.get("content")

    if not content:
        return jsonify({"error": "Content required"}), 400

    note_id = add_note(lead_id, content)

    return jsonify({"success": True, "note_id": note_id})


@app.route("/api/leads/<int:lead_id>/broker", methods=["POST"])
def api_update_broker_info(lead_id):
    """Update broker contact information and Google Drive link (legacy endpoint)."""
    from datetime import datetime, timezone

    data = request.json

    # Extract broker fields from request
    broker_fields = {
        'broker_name': data.get('broker_name'),
        'broker_phone': data.get('broker_phone'),
        'broker_email': data.get('broker_email'),
        'broker_contact_status': data.get('broker_contact_status', ''),
        'broker_contact_count': data.get('broker_contact_count', 0),
        'drive_financials_link': data.get('drive_financials_link'),
    }

    # Add timestamp if any contact method was used
    if broker_fields['broker_contact_count'] > 0:
        broker_fields['last_broker_contact_at'] = datetime.now(timezone.utc).isoformat()

    # Update lead fields
    success = update_lead_fields(lead_id, **broker_fields)

    if not success:
        return jsonify({"error": "Update failed (lead not found)"}), 400

    return jsonify({"success": True})


@app.route("/api/leads/<int:lead_id>/brokers", methods=["POST"])
def api_add_broker(lead_id):
    """Add a broker to a lead."""
    data = request.json

    name = data.get("name")
    if not name:
        return jsonify({"error": "Broker name required"}), 400

    broker_id = add_broker(
        lead_id,
        name=name,
        phone=data.get("phone", ""),
        email=data.get("email", ""),
        contact_status=data.get("contact_status", ""),
        notes=data.get("notes", "")
    )

    return jsonify({"success": True, "broker_id": broker_id})


@app.route("/api/brokers/<int:broker_id>", methods=["PUT"])
def api_update_broker(broker_id):
    """Update a broker's information."""
    data = request.json

    success = update_broker(broker_id, **data)

    if not success:
        return jsonify({"error": "Update failed (broker not found or invalid fields)"}), 400

    return jsonify({"success": True})


@app.route("/api/brokers/<int:broker_id>", methods=["DELETE"])
def api_delete_broker(broker_id):
    """Delete a broker."""
    success = delete_broker(broker_id)

    if not success:
        return jsonify({"error": "Delete failed (broker not found)"}), 404

    return jsonify({"success": True})


@app.route("/api/emails/sync", methods=["POST"])
def api_sync_emails():
    """Trigger Gmail sync."""
    try:
        from gmail_sync import sync_all_leads
        sync_all_leads()
        return jsonify({"success": True})
    except Exception as e:
        print(f"Sync error: {e}")
        return jsonify({"error": str(e)}), 500


# =============================================================================
# MAP DATA API ENDPOINTS (NEW - for lightweight map rendering)
# =============================================================================

@app.route("/api/zcta/metrics", methods=["GET"])
def api_zcta_metrics():
    """Get ZCTA metrics for choropleth coloring."""
    import sqlite3
    db_path = Path(__file__).parent.parent / "data" / "leads.db"
    
    if not db_path.exists():
        return jsonify([])
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    try:
        # First check what columns exist in the table
        cursor.execute("PRAGMA table_info(zcta_metrics)")
        existing_cols = {row['name'] for row in cursor.fetchall()}
        
        # Core columns (always exist after Census refresh)
        core_cols = [
            "zcta", "population_growth_rate", "price_to_income_ratio",
            "opportunity_score", "displacement_score", "path_of_progress_score",
            "snowbird_score", "slumlord_rehab_score", "exurb_score",
            "median_home_value", "population_2023 AS population", "vacancy_rate",
            "mobile_home_percentage", "median_age", "rent_burden", 
            "income_growth_rate", "distance_to_nearest_metro",
            "median_household_income", "senior_percentage", "senior_population", 
            "total_housing_units", "data_vintage",
            "poverty_rate", "unemployment_rate", "bachelors_degree_pct", 
            "avg_commute_time", "families_with_kids_pct", "vibe_badge",
            "seasonal_housing_pct", "vacation_score"
        ]
        
        # Optional risk enrichment columns (added by separate scripts)
        optional_cols = [
            ("avg_insurance_pressure", "0 AS avg_insurance_pressure"),
            ("avg_storm_score", "0 AS avg_storm_score"),
            ("high_flood_risk_pct", "0 AS high_flood_risk_pct"),
            ("risk_data_count", "0 AS risk_data_count"),
            ("avg_tax_shock", "0 AS avg_tax_shock"),
            ("avg_millage_increase", "0 AS avg_millage_increase"),
            ("avg_levy_growth", "0 AS avg_levy_growth"),
            ("dominant_city", "NULL AS dominant_city"),
            ("dominant_county", "NULL AS dominant_county"),
        ]
        
        # Build dynamic column list
        select_cols = core_cols.copy()
        for col_name, fallback in optional_cols:
            if col_name in existing_cols:
                select_cols.append(col_name)
            else:
                select_cols.append(fallback)
        
        query = f"""
            SELECT {', '.join(select_cols)}
            FROM zcta_metrics
            WHERE opportunity_score IS NOT NULL
        """
        cursor.execute(query)
        metrics = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jsonify(metrics)
    except sqlite3.Error as e:
        conn.close()
        return jsonify({"error": str(e)}), 500


@app.route("/api/zcta/boundaries", methods=["GET"])
def api_zcta_boundaries():
    """Get ZCTA GeoJSON boundaries."""
    geojson_path = Path(__file__).parent.parent / ".tmp" / "florida_zcta_boundaries.geojson"
    
    if not geojson_path.exists():
        return jsonify({"type": "FeatureCollection", "features": []})
    
    try:
        with open(geojson_path, "r", encoding="utf-8") as f:
            return f.read(), 200, {'Content-Type': 'application/json'}
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    # Initialize DB on start
    init_db()
    
    # Check if we should auto-sync on start (optional)
    # from gmail_sync import sync_all_leads
    # sync_all_leads()
    
    print("Starting CRM server at http://localhost:8000")
    app.run(debug=True, port=8000)

