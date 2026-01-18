"""
identify_non_mhp_properties.py

Analyzes LoopNet and Crexi leads to identify properties that are likely NOT
MHP/RV parks based on their descriptions and metadata.

Flags suspected non-MHP properties with a custom status for user review.
"""

import sqlite3
import re
from pathlib import Path
from typing import Dict, List, Tuple

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"

# MHP/RV Park positive indicators
MHP_RV_KEYWORDS = [
    # Direct mentions
    r'\bmobile\s+home\s+park\b',
    r'\bmhp\b',
    r'\brv\s+park\b',
    r'\btrailer\s+park\b',
    r'\bmanufactured\s+home\b',
    r'\bmanufactured\s+housing\b',
    r'\bmobile\s+home\s+community\b',
    r'\brecreational\s+vehicle\b',
    r'\bcampground\b',
    r'\brv\s+resort\b',
    r'\bmotorhome\b',
    r'\b\d+\s+pads?\b',  # "50 pads" etc
    r'\b\d+\s+spaces?\b',  # "50 spaces" etc
    r'\b\d+\s+lots?\b',  # "50 lots" etc (but weaker signal)
    r'\b\d+\s+sites?\b',  # "50 sites" etc
    r'\bpad\s+rent\b',
    r'\bloc\s+rent\b',
    r'\bmh\s+park\b',
    r'\bpads\s+with\b',
    r'\bmanufactured\s+housing\s+community\b',
]

# Non-MHP/RV property type indicators (strong signals this is NOT an MHP)
NON_MHP_INDICATORS = [
    # Commercial property types
    r'\bretail\s+center\b',
    r'\bshopping\s+center\b',
    r'\boffice\s+building\b',
    r'\bwarehouse\b',
    r'\bindustrial\s+building\b',
    r'\bapartment\s+complex\b',
    r'\bapartment\s+building\b',
    r'\bmultifamily\s+apartment\b',
    r'\bcondominium\b',
    r'\bcondo\s+building\b',
    r'\bself\s+storage\b',
    r'\bstorage\s+facility\b',
    r'\bhotel\b',
    r'\bmotel\b',
    r'\brestaurant\b',
    r'\bgas\s+station\b',
    r'\bconvenience\s+store\b',
    r'\bstrip\s+mall\b',
    r'\bmixed\s+use\s+development\b',

    # Residential (non-MHP)
    r'\bsingle\s+family\s+home\b',
    r'\btownhouse\b',
    r'\bduplex\s+units?\b',
    r'\btriplex\b',
    r'\bfourplex\b',

    # Land types (raw land, not developed MHP)
    r'\bvacant\s+land\b',
    r'\bundeveloped\s+land\b',
    r'\braw\s+land\b',
    r'\bacres\s+of\s+land\b',
    r'\bbuilding\s+lots?\b',
    r'\b\d+\s+ac\b',  # "50 ac" etc - raw land listings

    # Specific uses that are definitely not MHP
    r'\bfarm\b',
    r'\branch\b',
    r'\bchurch\b',
    r'\bschool\s+building\b',
    r'\bmedical\s+office\b',
    r'\bhealth\s+care\s+facility\b',
]

def compile_patterns(patterns: List[str]) -> List[re.Pattern]:
    """Compile regex patterns for efficient matching."""
    return [re.compile(p, re.IGNORECASE) for p in patterns]

MHP_PATTERNS = compile_patterns(MHP_RV_KEYWORDS)
NON_MHP_PATTERNS = compile_patterns(NON_MHP_INDICATORS)

def analyze_property(
    name: str,
    description: str,
    detailed_description: str,
    keywords: str,
    sub_type: str
) -> Tuple[bool, str, int, int]:
    """
    Analyze property to determine if it's likely an MHP/RV park.

    Returns:
        (is_likely_mhp, reason, mhp_score, non_mhp_score)
    """
    # Combine all text fields
    full_text = " ".join(filter(None, [
        name or "",
        description or "",
        detailed_description or "",
        keywords or "",
        sub_type or ""
    ]))

    if not full_text.strip():
        return (None, "No description data available", 0, 0)

    # Count matches
    mhp_matches = []
    non_mhp_matches = []

    for pattern in MHP_PATTERNS:
        matches = pattern.findall(full_text)
        if matches:
            mhp_matches.extend(matches)

    for pattern in NON_MHP_PATTERNS:
        matches = pattern.findall(full_text)
        if matches:
            non_mhp_matches.extend(matches)

    mhp_score = len(mhp_matches)
    non_mhp_score = len(non_mhp_matches)

    # Decision logic
    if non_mhp_score > 0 and mhp_score == 0:
        return (
            False,
            f"Strong non-MHP indicators: {', '.join(set(non_mhp_matches[:5]))}",
            mhp_score,
            non_mhp_score
        )

    if mhp_score > 0:
        return (
            True,
            f"MHP/RV keywords found: {', '.join(set(mhp_matches[:5]))}",
            mhp_score,
            non_mhp_score
        )

    if non_mhp_score > 0:
        return (
            False,
            f"Possible non-MHP (mixed signals): {', '.join(set(non_mhp_matches[:3]))}",
            mhp_score,
            non_mhp_score
        )

    # No clear indicators either way
    return (
        None,
        "Unclear - no strong MHP or non-MHP indicators found",
        mhp_score,
        non_mhp_score
    )

def flag_for_validation(conn: sqlite3.Connection, lead_id: int, reason: str):
    """Add a note to the lead indicating it needs validation."""
    cursor = conn.cursor()

    # Check if there's already a validation note
    cursor.execute("""
        SELECT id FROM notes
        WHERE lead_id = ? AND content LIKE '%needs validation%'
    """, (lead_id,))

    if cursor.fetchone():
        return  # Already flagged

    # Add note
    cursor.execute("""
        INSERT INTO notes (lead_id, content)
        VALUES (?, ?)
    """, (lead_id, f"[FLAG] NEEDS VALIDATION: {reason}"))

    # Log activity
    cursor.execute("""
        INSERT INTO activity_log (lead_id, activity_type, description, metadata_json)
        VALUES (?, 'note_added', ?, '{}')
    """, (lead_id, f"Flagged for validation: {reason}"))

def main():
    print("=" * 80)
    print("NON-MHP PROPERTY IDENTIFICATION")
    print("=" * 80)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get all non-dead LoopNet/Crexi leads
    cursor.execute("""
        SELECT id, name, city, state, status, scrape_source,
               loopnet_url, listing_url, description, detailed_description,
               description_keywords, sub_type
        FROM leads
        WHERE scrape_source IN ('loopnet', 'crexi')
        AND status NOT IN ('dead', 'acquired')
        ORDER BY id
    """)

    leads = cursor.fetchall()
    print(f"\nAnalyzing {len(leads)} LoopNet/Crexi leads...\n")

    # Analysis results
    confirmed_mhp = []
    suspected_non_mhp = []
    unclear = []

    for lead in leads:
        is_mhp, reason, mhp_score, non_mhp_score = analyze_property(
            lead['name'],
            lead['description'],
            lead['detailed_description'],
            lead['description_keywords'],
            lead['sub_type']
        )

        result = {
            'id': lead['id'],
            'name': lead['name'],
            'city': lead['city'],
            'state': lead['state'],
            'source': lead['scrape_source'],
            'status': lead['status'],
            'loopnet_url': lead['loopnet_url'],
            'crexi_url': lead['listing_url'],
            'reason': reason,
            'mhp_score': mhp_score,
            'non_mhp_score': non_mhp_score
        }

        if is_mhp is True:
            confirmed_mhp.append(result)
        elif is_mhp is False:
            suspected_non_mhp.append(result)
            # Flag for validation
            flag_for_validation(conn, lead['id'], reason)
        else:
            unclear.append(result)

    conn.commit()

    # Print results
    print("=" * 80)
    print("ANALYSIS RESULTS")
    print("=" * 80)
    print(f"\n[OK] Confirmed MHP/RV Parks: {len(confirmed_mhp)}")
    print(f"[!!] Suspected Non-MHP Properties: {len(suspected_non_mhp)}")
    print(f"[??] Unclear/Need Manual Review: {len(unclear)}")

    # Show suspected non-MHP properties
    if suspected_non_mhp:
        print("\n" + "=" * 80)
        print("SUSPECTED NON-MHP PROPERTIES (FLAGGED FOR REVIEW)")
        print("=" * 80)
        for prop in suspected_non_mhp:
            print(f"\n[FLAG] ID: {prop['id']}")
            print(f"   Name: {prop['name']}")
            print(f"   Location: {prop['city']}, {prop['state']}")
            print(f"   Source: {prop['source']}")
            print(f"   Current Status: {prop['status']}")
            print(f"   Reason: {prop['reason']}")
            print(f"   Scores: MHP={prop['mhp_score']}, Non-MHP={prop['non_mhp_score']}")
            if prop['loopnet_url']:
                print(f"   URL: {prop['loopnet_url']}")
            if prop['crexi_url']:
                print(f"   URL: {prop['crexi_url']}")

    # Show unclear cases
    if unclear:
        print("\n" + "=" * 80)
        print("UNCLEAR CASES (MAY NEED BETTER DESCRIPTION DATA)")
        print("=" * 80)
        for prop in unclear[:10]:  # Show first 10
            print(f"\n[?] ID: {prop['id']}")
            print(f"   Name: {prop['name']}")
            print(f"   Location: {prop['city']}, {prop['state']}")
            print(f"   Reason: {prop['reason']}")

        if len(unclear) > 10:
            print(f"\n   ... and {len(unclear) - 10} more unclear cases")

    # Export to CSV for review
    output_path = Path(__file__).parent.parent / ".tmp" / "suspected_non_mhp.csv"
    output_path.parent.mkdir(exist_ok=True)

    import csv
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'id', 'name', 'city', 'state', 'source', 'status',
            'reason', 'mhp_score', 'non_mhp_score', 'loopnet_url', 'crexi_url'
        ])
        writer.writeheader()
        for prop in suspected_non_mhp:
            writer.writerow(prop)

    print(f"\n" + "=" * 80)
    print(f"Exported suspected non-MHP properties to: {output_path}")
    print("=" * 80)

    conn.close()

if __name__ == "__main__":
    main()
