"""
Migration: Create brokers table for multiple brokers per deal

Creates a separate table to track multiple brokers associated with each listing.
"""

import sqlite3
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "execution"))

from db import DB_PATH, get_db


def migrate():
    """Create brokers table and migrate existing broker data."""

    print(f"Running migration on: {DB_PATH}")

    with get_db() as conn:
        # Create brokers table
        print("Creating brokers table...")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS brokers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL,
                name TEXT,
                phone TEXT,
                email TEXT,
                contact_status TEXT DEFAULT '',
                contact_count INTEGER DEFAULT 0,
                last_contact_at TEXT,
                notes TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
            )
        """)
        print("[OK] Created brokers table")

        # Create index for fast lookups
        print("Creating index on brokers.lead_id...")
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_brokers_lead_id ON brokers(lead_id)
        """)
        print("[OK] Created index")

        # Migrate existing broker data from leads table to brokers table
        print("Migrating existing broker data...")
        cursor = conn.execute("""
            SELECT id, broker_name, broker_phone, broker_email,
                   broker_contact_status, broker_contact_count,
                   last_broker_contact_at
            FROM leads
            WHERE broker_name IS NOT NULL AND broker_name != ''
        """)

        existing_brokers = cursor.fetchall()
        migrated_count = 0

        for row in existing_brokers:
            lead_id = row[0]
            broker_name = row[1]
            broker_phone = row[2]
            broker_email = row[3]
            contact_status = row[4] or ''
            contact_count = row[5] or 0
            last_contact = row[6]

            # Insert into brokers table
            conn.execute("""
                INSERT INTO brokers
                (lead_id, name, phone, email, contact_status, contact_count, last_contact_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (lead_id, broker_name, broker_phone, broker_email,
                  contact_status, contact_count, last_contact))

            migrated_count += 1

        print(f"[OK] Migrated {migrated_count} existing broker records")

        conn.commit()

    print("\n[SUCCESS] Migration complete!")
    print(f"  - Created brokers table")
    print(f"  - Migrated {migrated_count} existing broker entries")
    print(f"  - Old broker fields in leads table remain for backwards compatibility")


if __name__ == "__main__":
    migrate()
