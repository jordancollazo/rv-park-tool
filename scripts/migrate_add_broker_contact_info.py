"""
Migration: Add broker contact information fields

Adds fields to track:
- Broker contact outreach status (called, texted, emailed)
- Google Drive financial documents link
"""

import sqlite3
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "execution"))

from db import DB_PATH, get_db


def migrate():
    """Add broker contact tracking and Google Drive link fields."""

    print(f"Running migration on: {DB_PATH}")

    with get_db() as conn:
        # Check if columns already exist
        cursor = conn.execute("PRAGMA table_info(leads)")
        columns = {row[1] for row in cursor.fetchall()}

        # Add broker contact status field
        if 'broker_contact_status' not in columns:
            print("Adding broker_contact_status column...")
            conn.execute("""
                ALTER TABLE leads
                ADD COLUMN broker_contact_status TEXT DEFAULT ''
            """)
            print("[OK] Added broker_contact_status")
        else:
            print("[SKIP] broker_contact_status already exists")

        # Add broker outreach count field
        if 'broker_contact_count' not in columns:
            print("Adding broker_contact_count column...")
            conn.execute("""
                ALTER TABLE leads
                ADD COLUMN broker_contact_count INTEGER DEFAULT 0
            """)
            print("[OK] Added broker_contact_count")
        else:
            print("[SKIP] broker_contact_count already exists")

        # Add Google Drive financials link
        if 'drive_financials_link' not in columns:
            print("Adding drive_financials_link column...")
            conn.execute("""
                ALTER TABLE leads
                ADD COLUMN drive_financials_link TEXT
            """)
            print("[OK] Added drive_financials_link")
        else:
            print("[SKIP] drive_financials_link already exists")

        # Add last broker contact timestamp
        if 'last_broker_contact_at' not in columns:
            print("Adding last_broker_contact_at column...")
            conn.execute("""
                ALTER TABLE leads
                ADD COLUMN last_broker_contact_at TEXT
            """)
            print("[OK] Added last_broker_contact_at")
        else:
            print("[SKIP] last_broker_contact_at already exists")

        conn.commit()

    print("\n[SUCCESS] Migration complete!")


if __name__ == "__main__":
    migrate()
