"""
db.py
Database access layer for the MHP/RV Park CRM.

Provides functions for:
- Managing leads (CRUD, upsert on re-scrape)
- Logging calls
- Syncing emails
- Recording notes
- Activity timeline

Usage:
    from db import get_db, get_all_leads, upsert_lead, log_call
"""

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Database path
DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"
SCHEMA_PATH = Path(__file__).parent.parent / "data" / "schema.sql"


def init_db():
    """Initialize the database with schema if it doesn't exist."""
    DB_PATH.parent.mkdir(exist_ok=True)
    
    with get_db() as conn:
        # Read and execute schema
        with open(SCHEMA_PATH, "r") as f:
            conn.executescript(f.read())
        conn.commit()
    
    print(f"Database initialized at: {DB_PATH}")


@contextmanager
def get_db():
    """Get a database connection with row factory."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()


def row_to_dict(row: sqlite3.Row | None) -> dict | None:
    """Convert a sqlite3.Row to a dictionary."""
    if row is None:
        return None
    return dict(row)


def rows_to_list(rows: list[sqlite3.Row]) -> list[dict]:
    """Convert a list of sqlite3.Row to a list of dictionaries."""
    return [dict(row) for row in rows]


# =============================================================================
# LEADS
# =============================================================================

def get_all_leads(
    status: str | None = None,
    area: str | None = None,
    max_score: int | None = None,
    limit: int | None = None
) -> list[dict]:
    """
    Get all leads with optional filtering.
    
    Args:
        status: Filter by status (e.g., 'not_contacted')
        area: Filter by area (e.g., 'Jacksonville, FL')
        max_score: Filter by max website score
        limit: Limit number of results
    
    Returns:
        List of lead dictionaries
    """
    query = "SELECT * FROM leads WHERE 1=1"
    params = []
    
    if status:
        query += " AND status = ?"
        params.append(status)
    
    if area:
        query += " AND area = ?"
        params.append(area)
    
    if max_score is not None:
        query += " AND site_score_1_10 <= ?"
        params.append(max_score)
    
    query += " ORDER BY site_score_1_10 ASC, last_scraped_at DESC"
    
    if limit:
        query += " LIMIT ?"
        params.append(limit)
    
    with get_db() as conn:
        rows = conn.execute(query, params).fetchall()
    
    return rows_to_list(rows)


def get_lead_by_id(lead_id: int) -> dict | None:
    """Get a single lead by ID."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM leads WHERE id = ?", (lead_id,)
        ).fetchone()
    return row_to_dict(row)


def get_lead_by_place_id(place_id: str) -> dict | None:
    """Get a single lead by Google Place ID."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM leads WHERE place_id = ?", (place_id,)
        ).fetchone()
    return row_to_dict(row)


def upsert_lead(lead: dict) -> int:
    """
    Insert a new lead or update if place_id already exists.
    Preserves existing CRM data (status, calls, etc.) on update.
    
    Returns the lead ID.
    """
    now = datetime.now(timezone.utc).isoformat()
    place_id = lead.get("place_id")
    
    if not place_id:
        raise ValueError("Lead must have a place_id")
    
    # Check if lead exists
    existing = get_lead_by_place_id(place_id)
    
    def safe_json(val):
        if val is None:
            return None # or "{}" depending on schema, let's say None is fine for TEXT cols
        if isinstance(val, (dict, list)):
            return json.dumps(val)
        return str(val)

    with get_db() as conn:
        if existing:
            # Update - preserve CRM fields, update scrape data
            conn.execute("""
                UPDATE leads SET
                    name = ?,
                    address = ?,
                    city = ?,
                    state = ?,
                    zip = ?,
                    phone = ?,
                    website = ?,
                    maps_url = ?,
                    latitude = ?,
                    longitude = ?,
                    google_rating = ?,
                    review_count = ?,
                    category = ?,
                    site_score_1_10 = ?,
                    score_breakdown_json = ?,
                    score_reasons = ?,
                    crawl_status = ?,
                    source_query = ?,
                    area = ?,
                    last_scraped_at = ?
                WHERE place_id = ?
            """, (
                lead.get("name"),
                lead.get("address"),
                lead.get("city"),
                lead.get("state"),
                lead.get("zip"),
                lead.get("phone"),
                lead.get("website"),
                lead.get("maps_url"),
                lead.get("latitude"),
                lead.get("longitude"),
                lead.get("google_rating"),
                lead.get("review_count"),
                lead.get("category"),
                lead.get("site_score_1_10", 0),
                safe_json(lead.get("score_breakdown_json", {})),
                lead.get("score_reasons"),
                lead.get("crawl_status"),
                lead.get("source_query"),
                lead.get("area"),
                now,
                place_id,
            ))
            
            # Update enrichment fields if provided
            enrichment_fields = {}
            for k in ['social_facebook', 'social_instagram', 'social_linkedin', 'owner_name', 'is_enriched',
                      'owner_fatigue_score_0_100', 'owner_fatigue_confidence', 
                      'owner_fatigue_reasons_json', 'owner_fatigue_breakdown_json',
                      'cap_rate', 'noi', 'occupancy', 'price_per_unit',
                      'broker_name', 'broker_company', 'broker_phone', 'broker_email',
                      'listing_url', 'description', 'scrape_source', 'crexi_id',
                      'asking_price']:
                if k in lead:
                    enrichment_fields[k] = lead[k]
                    
            if enrichment_fields:
                update_lead_fields(existing["id"], conn=conn, **enrichment_fields)
                
            conn.commit()
            return existing["id"]
        else:
            # Insert new lead
            cursor = conn.execute("""
                INSERT INTO leads (
                    place_id, name, address, city, state, zip, phone, website,
                    maps_url, latitude, longitude, google_rating, review_count,
                    category, site_score_1_10, score_breakdown_json, score_reasons,
                    crawl_status, source_query, area, first_scraped_at, last_scraped_at,
                    social_facebook, social_instagram, social_linkedin, owner_name, is_enriched
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                place_id,
                lead.get("name"),
                lead.get("address"),
                lead.get("city"),
                lead.get("state"),
                lead.get("zip"),
                lead.get("phone"),
                lead.get("website"),
                lead.get("maps_url"),
                lead.get("latitude"),
                lead.get("longitude"),
                lead.get("google_rating"),
                lead.get("review_count"),
                lead.get("category"),
                lead.get("site_score_1_10", 0),
                safe_json(lead.get("score_breakdown_json", {})),
                lead.get("score_reasons"),
                lead.get("crawl_status"),
                lead.get("source_query"),
                lead.get("area"),
                now,
                now,
                lead.get("social_facebook"),
                lead.get("social_instagram"),
                lead.get("social_linkedin"),
                lead.get("owner_name"),
                lead.get("is_enriched", 0)
            ))
            conn.commit()
            lead_id = cursor.lastrowid
            
            # Update enrichment/Crexi fields
            update_lead_fields(lead_id, conn=conn, **lead)
            
            # Log activity
            _log_activity(conn, lead_id, "scraped", f"Lead scraped from {lead.get('area')}")
            conn.commit()
            
            return lead_id


def update_lead_status(lead_id: int, status: str, notes: str | None = None) -> bool:
    """Update lead status and log to history."""
    valid_statuses = [
        'not_contacted', 'contacted', 'interested', 'not_interested',
        'docs_requested', 'docs_received', 'reviewed_interested',
        'reviewed_not_interested', 'negotiating', 'acquired', 'dead'
    ]
    if status not in valid_statuses:
        raise ValueError(f"Invalid status: {status}")
    
    with get_db() as conn:
        # Get current status
        current = conn.execute(
            "SELECT status FROM leads WHERE id = ?", (lead_id,)
        ).fetchone()
        
        if not current:
            return False
        
        old_status = current["status"]
        
        conn.execute(
            "UPDATE leads SET status = ? WHERE id = ?",
            (status, lead_id)
        )
        
        # Log to specific status history table
        conn.execute("""
            INSERT INTO status_history (lead_id, old_status, new_status, notes)
            VALUES (?, ?, ?, ?)
        """, (lead_id, old_status, status, notes))
        
        # Keep logging to unified activity log for now as well
        _log_activity(
            conn, lead_id, "status_change",
            f"Status changed from '{old_status}' to '{status}'",
            {"notes": notes}
        )
        conn.commit()
    
    return True


def get_status_history(lead_id: int) -> list[dict]:
    """Get status history for a lead."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM status_history WHERE lead_id = ?
            ORDER BY changed_at DESC
        """, (lead_id,)).fetchall()
    return rows_to_list(rows)


def update_lead_fields(lead_id: int, conn: sqlite3.Connection | None = None, **fields) -> bool:
    """Update arbitrary lead fields."""
    allowed_fields = {
        'owner_name', 'owner_email', 'owner_phone',
        'asking_price', 'lot_count', 'next_followup',
        'latitude', 'longitude', 'website', 'maps_url',
        'social_facebook', 'social_instagram', 'social_linkedin',
        'is_enriched',
        'registered_agent_name', 'registered_agent_address',
        'utilities_status', 'rent_info',
        # Owner Fatigue Score fields
        'owner_fatigue_score_0_100', 'owner_fatigue_confidence',
        'owner_fatigue_reasons_json', 'owner_fatigue_breakdown_json',
        # LoopNet Fields
        'loopnet_id', 'loopnet_url', 'listing_status',
        'list_price', 'cap_rate', 'noi', 'occupancy_rate',
        'broker_name', 'broker_firm', 'description_keywords',
        # Detailed LoopNet Fields
        'year_built', 'building_size', 'lot_size', 'detailed_description',
        # Crexi fields
        'cap_rate', 'noi', 'occupancy', 'price_per_unit',
        'broker_name', 'broker_company', 'broker_phone', 'broker_email',
        'listing_url', 'description', 'scrape_source', 'crexi_id',
        'lease_type', 'tenancy', 'lease_expiration',
        'sq_ft', 'year_built', 'sub_type',
        'date_listed', 'days_on_market', 'investment_highlights',
        # Tags
        'tags',
        # Broker Contact Tracking
        'broker_contact_status', 'broker_contact_count', 'drive_financials_link',
        'last_broker_contact_at',
    }
    
    import json

    def safe_serialize(val):
        if val is None:
            return None
        if isinstance(val, (dict, list)):
            return json.dumps(val)
        return val

    update_fields = {k: safe_serialize(v) for k, v in fields.items() if k in allowed_fields}
    if not update_fields:
        return False
    
    set_clause = ", ".join(f"{k} = ?" for k in update_fields.keys())
    values = list(update_fields.values()) + [lead_id]
    
    if conn:
        conn.execute(
            f"UPDATE leads SET {set_clause} WHERE id = ?",
            values
        )
        # Caller handles commit
    else:
        with get_db() as local_conn:
            local_conn.execute(
                f"UPDATE leads SET {set_clause} WHERE id = ?",
                values
            )
            local_conn.commit()
    
    return True


# =============================================================================
# CALLS
# =============================================================================

def log_call(
    lead_id: int,
    outcome: str,
    notes: str = "",
    duration_seconds: int | None = None,
    next_action: str = "",
    next_followup_date: str | None = None
) -> int:
    """
    Log a call to a lead.
    
    Returns the call ID.
    """
    now = datetime.now(timezone.utc).isoformat()
    
    with get_db() as conn:
        cursor = conn.execute("""
            INSERT INTO calls (
                lead_id, called_at, outcome, duration_seconds,
                notes, next_action, next_followup_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            lead_id, now, outcome, duration_seconds,
            notes, next_action, next_followup_date
        ))
        call_id = cursor.lastrowid
        
        # Update lead aggregates
        conn.execute("""
            UPDATE leads SET
                call_count = call_count + 1,
                last_called_at = ?,
                last_call_outcome = ?,
                next_followup = COALESCE(?, next_followup)
            WHERE id = ?
        """, (now, outcome, next_followup_date, lead_id))
        
        # Update status if first call
        lead = conn.execute(
            "SELECT status FROM leads WHERE id = ?", (lead_id,)
        ).fetchone()
        if lead and lead["status"] == "not_contacted":
            conn.execute(
                "UPDATE leads SET status = 'contacted' WHERE id = ?",
                (lead_id,)
            )
            _log_activity(conn, lead_id, "status_change", 
                         "Status changed from 'not_contacted' to 'contacted'")
        
        _log_activity(conn, lead_id, "call", f"Call logged: {outcome}")
        conn.commit()
    
    return call_id


def get_calls_for_lead(lead_id: int) -> list[dict]:
    """Get all calls for a lead, most recent first."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM calls WHERE lead_id = ?
            ORDER BY called_at DESC
        """, (lead_id,)).fetchall()
    return rows_to_list(rows)


# =============================================================================
# EMAILS
# =============================================================================

def sync_email(
    lead_id: int,
    gmail_thread_id: str,
    gmail_message_id: str,
    direction: str,
    subject: str,
    snippet: str,
    from_address: str,
    to_address: str,
    email_date: str,
    labels: list[str] | None = None
) -> int | None:
    """
    Sync an email from Gmail. Returns email ID or None if duplicate.
    """
    with get_db() as conn:
        # Check for existing
        existing = conn.execute(
            "SELECT id FROM emails WHERE gmail_message_id = ?",
            (gmail_message_id,)
        ).fetchone()
        
        if existing:
            return None
        
        cursor = conn.execute("""
            INSERT INTO emails (
                lead_id, gmail_thread_id, gmail_message_id, direction,
                subject, snippet, from_address, to_address, email_date, labels
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            lead_id, gmail_thread_id, gmail_message_id, direction,
            subject, snippet, from_address, to_address, email_date,
            json.dumps(labels or [])
        ))
        email_id = cursor.lastrowid
        
        # Update lead aggregates
        conn.execute("""
            UPDATE leads SET
                email_count = email_count + 1,
                last_email_at = ?
            WHERE id = ?
        """, (email_date, lead_id))
        
        activity_type = "email_sent" if direction == "sent" else "email_received"
        _log_activity(conn, lead_id, activity_type, f"Email: {subject[:50]}")
        conn.commit()
    
    return email_id


def get_emails_for_lead(lead_id: int) -> list[dict]:
    """Get all emails for a lead, most recent first."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM emails WHERE lead_id = ?
            ORDER BY email_date DESC
        """, (lead_id,)).fetchall()
    return rows_to_list(rows)


# =============================================================================
# NOTES
# =============================================================================

def add_note(lead_id: int, content: str) -> int:
    """Add a note to a lead."""
    with get_db() as conn:
        cursor = conn.execute(
            "INSERT INTO notes (lead_id, content) VALUES (?, ?)",
            (lead_id, content)
        )
        note_id = cursor.lastrowid
        
        _log_activity(conn, lead_id, "note_added", 
                     f"Note added: {content[:50]}...")
        conn.commit()
    
    return note_id


def get_notes_for_lead(lead_id: int) -> list[dict]:
    """Get all notes for a lead, most recent first."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM notes WHERE lead_id = ?
            ORDER BY created_at DESC
        """, (lead_id,)).fetchall()
    return rows_to_list(rows)


# =============================================================================
# ACTIVITY LOG
# =============================================================================

def _log_activity(
    conn: sqlite3.Connection,
    lead_id: int,
    activity_type: str,
    description: str,
    metadata: dict | None = None
):
    """Internal function to log activity (requires existing connection)."""
    conn.execute("""
        INSERT INTO activity_log (lead_id, activity_type, description, metadata_json)
        VALUES (?, ?, ?, ?)
    """, (lead_id, activity_type, description, json.dumps(metadata or {})))


def get_activity_for_lead(lead_id: int, limit: int = 50) -> list[dict]:
    """Get activity timeline for a lead."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM activity_log WHERE lead_id = ?
            ORDER BY created_at DESC
            LIMIT ?
        """, (lead_id, limit)).fetchall()
    return rows_to_list(rows)


def get_recent_activity(limit: int = 50) -> list[dict]:
    """Get recent activity across all leads."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT a.*, l.name as lead_name
            FROM activity_log a
            JOIN leads l ON a.lead_id = l.id
            ORDER BY a.created_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
    return rows_to_list(rows)


# =============================================================================
# STATS
# =============================================================================

def get_stats() -> dict:
    """Get dashboard statistics."""
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) as c FROM leads").fetchone()["c"]
        
        by_status = {}
        rows = conn.execute("""
            SELECT status, COUNT(*) as c FROM leads GROUP BY status
        """).fetchall()
        for row in rows:
            by_status[row["status"]] = row["c"]
        
        by_area = {}
        rows = conn.execute("""
            SELECT area, COUNT(*) as c FROM leads GROUP BY area
        """).fetchall()
        for row in rows:
            by_area[row["area"]] = row["c"]
        
        hot_leads = conn.execute("""
            SELECT COUNT(*) as c FROM leads WHERE site_score_1_10 <= 3
        """).fetchone()["c"]
        
        calls_today = conn.execute("""
            SELECT COUNT(*) as c FROM calls 
            WHERE date(called_at) = date('now')
        """).fetchone()["c"]
    
    return {
        "total_leads": total,
        "by_status": by_status,
        "by_area": by_area,
        "hot_leads": hot_leads,
        "calls_today": calls_today,
    }


# =============================================================================
# BULK OPERATIONS
# =============================================================================

def bulk_upsert_leads(leads: list[dict]) -> tuple[int, int]:
    """
    Bulk upsert leads from pipeline.

    Returns (inserted_count, updated_count)
    """
    inserted = 0
    updated = 0

    for lead in leads:
        place_id = lead.get("place_id")
        if not place_id:
            continue

        existing = get_lead_by_place_id(place_id)
        upsert_lead(lead)

        if existing:
            updated += 1
        else:
            inserted += 1

    return inserted, updated


# =============================================================================
# BROKERS
# =============================================================================

def add_broker(lead_id: int, name: str, phone: str = "", email: str = "",
               contact_status: str = "", notes: str = "") -> int:
    """
    Add a broker to a lead.

    Returns the broker ID.
    """
    now = datetime.now(timezone.utc).isoformat()

    # Calculate contact count from status
    contact_count = 0
    if contact_status:
        contact_count = len([x for x in contact_status.split('+') if x.strip()])

    with get_db() as conn:
        cursor = conn.execute("""
            INSERT INTO brokers (
                lead_id, name, phone, email, contact_status,
                contact_count, last_contact_at, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            lead_id, name, phone, email, contact_status,
            contact_count, now if contact_count > 0 else None, notes
        ))
        broker_id = cursor.lastrowid

        _log_activity(conn, lead_id, "note_added",
                     f"Added broker: {name}")
        conn.commit()

    return broker_id


def get_brokers_for_lead(lead_id: int) -> list[dict]:
    """Get all brokers for a lead."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM brokers WHERE lead_id = ?
            ORDER BY created_at ASC
        """, (lead_id,)).fetchall()
    return rows_to_list(rows)


def update_broker(broker_id: int, **fields) -> bool:
    """Update broker fields."""
    allowed_fields = {
        'name', 'phone', 'email', 'contact_status',
        'contact_count', 'last_contact_at', 'notes'
    }

    update_fields = {k: v for k, v in fields.items() if k in allowed_fields}
    if not update_fields:
        return False

    # Calculate contact count if status provided
    if 'contact_status' in update_fields:
        status = update_fields['contact_status']
        update_fields['contact_count'] = len([x for x in status.split('+') if x.strip()])

        # Update last contact timestamp if there's a contact
        if update_fields['contact_count'] > 0 and 'last_contact_at' not in update_fields:
            update_fields['last_contact_at'] = datetime.now(timezone.utc).isoformat()

    update_fields['updated_at'] = datetime.now(timezone.utc).isoformat()

    set_clause = ", ".join(f"{k} = ?" for k in update_fields.keys())
    values = list(update_fields.values()) + [broker_id]

    with get_db() as conn:
        conn.execute(
            f"UPDATE brokers SET {set_clause} WHERE id = ?",
            values
        )
        conn.commit()

    return True


def delete_broker(broker_id: int) -> bool:
    """Delete a broker."""
    with get_db() as conn:
        # Get lead_id before deleting for activity log
        broker = conn.execute(
            "SELECT lead_id, name FROM brokers WHERE id = ?", (broker_id,)
        ).fetchone()

        if not broker:
            return False

        conn.execute("DELETE FROM brokers WHERE id = ?", (broker_id,))

        _log_activity(conn, broker["lead_id"], "note_added",
                     f"Removed broker: {broker['name']}")
        conn.commit()

    return True


if __name__ == "__main__":
    # Initialize database when run directly
    init_db()
    
    # Print stats
    stats = get_stats()
    print(f"Total leads: {stats['total_leads']}")
    print(f"By status: {stats['by_status']}")
    print(f"By area: {stats['by_area']}")
