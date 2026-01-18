"""
Identify non-MHP/RV park properties and move them to a validation pipeline.

This script scans the leads database for properties that don't match MHP/RV criteria
and flags them for manual review and potential deletion.
"""

import sqlite3
import re
from typing import List, Dict, Any
from datetime import datetime

# Database path
DB_PATH = '../data/leads.db'

# Valid MHP/RV park indicators
VALID_CATEGORIES = [
    'mobile home park',
    'manufactured home community',
    'trailer park',
    'rv park',
    'mobile home park',  # case variations
    'Mobile Home Park',
    'RV park',
]

# Invalid property type indicators - must appear in category, sub_type, or description
# NOT in name or address (too many false positives)
INVALID_KEYWORDS = [
    'multifamily',
    'apartment',
    'condo',
    'office',
    'retail',
    'warehouse',
    'industrial',
    'hotel',
    'motel',
    'self storage',
    'self-storage',
    'storage facility',
    'shopping center',
    'strip mall',
]

# Indicators of vacant land (only check in description/detailed_description)
LAND_KEYWORDS = [
    'vacant land',
    'development site',
    'acre lot',
    'acres zoned',
    'land for sale',
    'raw land',
    'assemblage',
]


def normalize_category(category: str) -> str:
    """Normalize category string for comparison."""
    if not category:
        return ""
    return category.lower().strip()


def is_valid_mhp_rv(name: str, category: str, sub_type: str, description: str, detailed_description: str) -> tuple[bool, str]:
    """
    Determine if a property is a valid MHP/RV park.

    Returns:
        (is_valid, reason)
    """
    # Normalize inputs
    name = (name or "").lower()
    category = normalize_category(category)
    sub_type = (sub_type or "").lower()
    description = (description or "").lower()
    detailed_description = (detailed_description or "").lower()

    # Check for invalid keywords in category/sub_type/descriptions ONLY
    # (not in name/address to avoid false positives like "Highland Park")
    searchable_text = f"{category} {sub_type} {description} {detailed_description}"

    for keyword in INVALID_KEYWORDS:
        if keyword.lower() in searchable_text:
            return False, f"Contains invalid keyword: '{keyword}'"

    # Check for land/development indicators in descriptions only
    for keyword in LAND_KEYWORDS:
        if keyword.lower() in searchable_text:
            return False, f"Appears to be vacant land/development: '{keyword}'"

    # If category is Multifamily, it's likely not MHP
    if 'multifamily' in category:
        return False, "Category is 'Multifamily'"

    # Check if category is explicitly valid
    if category and any(valid.lower() in category for valid in VALID_CATEGORIES):
        return True, "Valid category"

    # Check if name suggests it's MHP/RV
    mhp_indicators = ['mobile home', 'manufactured home', 'trailer park', 'rv park', 'mhp', 'mhc']
    if any(indicator in name for indicator in mhp_indicators):
        return True, "Valid name indicator"

    # If we don't have enough information, flag for review
    if not category and not any(indicator in name for indicator in mhp_indicators):
        return False, "Insufficient information - needs manual review"

    # Default to valid if nothing flagged it
    return True, "Passed validation"


def identify_invalid_leads() -> List[Dict[str, Any]]:
    """Scan database and identify invalid leads."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get all leads
    cursor.execute("""
        SELECT
            id, name, category, sub_type, description, detailed_description,
            status, archived, address, city, state
        FROM leads
        WHERE archived != 1 OR archived IS NULL
    """)

    leads = cursor.fetchall()
    invalid_leads = []

    print(f"Analyzing {len(leads)} leads...\n")

    for lead in leads:
        is_valid, reason = is_valid_mhp_rv(
            lead['name'],
            lead['category'],
            lead['sub_type'],
            lead['description'],
            lead['detailed_description']
        )

        if not is_valid:
            invalid_leads.append({
                'id': lead['id'],
                'name': lead['name'],
                'category': lead['category'],
                'sub_type': lead['sub_type'],
                'address': f"{lead['address'] or ''}, {lead['city'] or ''}, {lead['state'] or ''}".strip(', '),
                'status': lead['status'],
                'reason': reason
            })

    conn.close()
    return invalid_leads


def move_to_validation_pipeline(invalid_leads: List[Dict[str, Any]]) -> None:
    """Move invalid leads to 'dead' status for validation/deletion."""
    if not invalid_leads:
        print("No invalid leads found!")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Update status for invalid leads to 'dead' and add validation note
    invalid_ids = [lead['id'] for lead in invalid_leads]

    placeholders = ','.join(['?'] * len(invalid_ids))
    cursor.execute(f"""
        UPDATE leads
        SET status = 'dead'
        WHERE id IN ({placeholders})
    """, invalid_ids)

    conn.commit()

    # Log the changes with reason for flagging
    timestamp = datetime.now().isoformat()
    for lead in invalid_leads:
        cursor.execute("""
            INSERT INTO activity_log (lead_id, activity_type, description, created_at)
            VALUES (?, 'status_change', ?, ?)
        """, (lead['id'], f"[INVALID LEAD - VALIDATE & DELETE] {lead['reason']}", timestamp))

    conn.commit()
    conn.close()

    print(f"\nOK Moved {len(invalid_leads)} leads to 'dead' status for validation/deletion\n")


def main():
    """Main execution."""
    print("=" * 80)
    print("INVALID LEAD IDENTIFICATION")
    print("=" * 80)
    print()

    # Identify invalid leads
    invalid_leads = identify_invalid_leads()

    if not invalid_leads:
        print("OK All leads appear to be valid MHP/RV parks!")
        return

    # Display results
    print(f"Found {len(invalid_leads)} potentially invalid leads:\n")
    print("-" * 80)

    for i, lead in enumerate(invalid_leads, 1):
        print(f"{i}. ID {lead['id']}: {lead['name']}")
        print(f"   Category: {lead['category'] or 'N/A'}")
        print(f"   Location: {lead['address']}")
        print(f"   Reason: {lead['reason']}")
        print(f"   Current Status: {lead['status'] or 'New'}")
        print()

    print("-" * 80)
    print()

    # Move to validation pipeline
    move_to_validation_pipeline(invalid_leads)

    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"• {len(invalid_leads)} leads flagged for review")
    print(f"• Status changed to: 'dead' (ready for validation/deletion)")
    print(f"• Check the CRM tool to validate and delete these leads")
    print(f"• Filter by status='dead' to see all flagged leads")
    print()


if __name__ == '__main__':
    main()
