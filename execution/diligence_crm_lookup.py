"""
Diligence Agent - CRM Integration
Connects deal diligence with existing CRM lead data
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List

# Add parent directory to path for db imports
sys.path.insert(0, str(Path(__file__).parent))

# Import CRM database functions
try:
    from db import (
        get_lead_by_id,
        update_lead_status,
        update_lead_fields,
        add_note
    )
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False
    print("Warning: db.py not available - CRM integration disabled")

# Import diligence utilities
from diligence_utils import (
    validate_deal_id,
    load_json,
    save_json
)


def link_deal_to_lead(deal_id: str, lead_id: int) -> bool:
    """
    Link diligence deal to CRM lead record

    Args:
        deal_id: Deal identifier
        lead_id: CRM lead ID

    Returns:
        True if successful
    """
    if not validate_deal_id(deal_id):
        print(f"Invalid deal_id: {deal_id}")
        return False

    if not DB_AVAILABLE:
        print("CRM database not available")
        return False

    try:
        # Verify lead exists
        lead = get_lead_by_id(lead_id)
        if not lead:
            print(f"Lead {lead_id} not found")
            return False

        # Update deal metadata with lead_id
        from diligence_document_processor import get_deal_metadata, update_deal_metadata
        metadata = get_deal_metadata(deal_id)
        metadata["lead_id"] = lead_id
        metadata["lead_name"] = lead.get("name")
        metadata["linked_at"] = datetime.now().isoformat()
        metadata["updated_at"] = datetime.now().isoformat()

        if not update_deal_metadata(deal_id, metadata):
            return False

        # Add note to CRM lead
        note_content = f"Deal diligence started: {metadata.get('name')} (Deal ID: {deal_id})"
        add_note(lead_id, note_content)

        print(f"Linked deal {deal_id} to lead {lead_id}")
        return True

    except Exception as e:
        print(f"Error linking deal to lead: {e}")
        return False


def get_lead_data_for_deal(deal_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch CRM lead data for a deal

    Args:
        deal_id: Deal identifier

    Returns:
        Lead data dict or None
    """
    if not validate_deal_id(deal_id):
        return None

    if not DB_AVAILABLE:
        return None

    try:
        from diligence_document_processor import get_deal_metadata
        metadata = get_deal_metadata(deal_id)

        lead_id = metadata.get("lead_id")
        if not lead_id:
            return None

        lead = get_lead_by_id(lead_id)
        return dict(lead) if lead else None

    except Exception as e:
        print(f"Error fetching lead data: {e}")
        return None


def update_lead_with_diligence(lead_id: int, deal_id: str, summary: Dict[str, Any]) -> bool:
    """
    Update CRM lead with diligence summary

    Args:
        lead_id: CRM lead ID
        deal_id: Deal identifier
        summary: Diligence summary dict

    Returns:
        True if successful
    """
    if not DB_AVAILABLE:
        return False

    try:
        # Update lead status based on analysis
        confidence = summary.get("confidence_score", 0)

        if confidence >= 70:
            # High confidence analysis - mark as reviewed interested
            new_status = "reviewed_interested"
            status_note = f"Diligence analysis complete (confidence: {confidence}/100). Deal looks promising."
        elif confidence >= 40:
            # Medium confidence - docs received
            new_status = "docs_received"
            status_note = f"Diligence analysis complete (confidence: {confidence}/100). Needs more data."
        else:
            # Low confidence - keep current status
            new_status = None
            status_note = f"Diligence analysis incomplete (confidence: {confidence}/100). Missing critical data."

        # Update status if applicable
        if new_status:
            update_lead_status(lead_id, new_status, notes=status_note)

        # Add detailed note with summary
        exec_summary = summary.get("executive_summary", "No summary available")
        note_content = f"""Deal Diligence Summary (Deal ID: {deal_id})

{exec_summary}

Confidence Score: {confidence}/100

Key Findings:
"""

        # Add red flags if any
        red_flags = summary.get("red_flags", [])
        if red_flags:
            note_content += "\nRed Flags:\n"
            for flag in red_flags[:3]:
                note_content += f"- {flag}\n"

        # Add value-add opportunities
        opportunities = summary.get("value_add_opportunities", [])
        if opportunities:
            note_content += "\nValue-Add Opportunities:\n"
            for opp in opportunities[:3]:
                note_content += f"- {opp.get('description', 'N/A')} ({opp.get('estimated_upside', 'TBD')})\n"

        add_note(lead_id, note_content)

        # Update lead fields with financial data if available
        prop = summary.get("property_overview", {})
        fin = summary.get("financial_analysis", {})

        updates = {}
        if prop.get("purchase_price"):
            updates["asking_price"] = prop["purchase_price"]
        if prop.get("units"):
            updates["lot_count"] = prop["units"]
        if fin.get("noi"):
            updates["noi"] = fin["noi"]
        if fin.get("cap_rate"):
            updates["cap_rate"] = fin["cap_rate"]

        if updates:
            update_lead_fields(lead_id, **updates)

        print(f"Updated lead {lead_id} with diligence summary")
        return True

    except Exception as e:
        print(f"Error updating lead with diligence: {e}")
        return False


def get_deals_for_lead(lead_id: int) -> List[Dict[str, Any]]:
    """
    List all diligence deals linked to a CRM lead

    Args:
        lead_id: CRM lead ID

    Returns:
        List of deal dicts
    """
    deals = []

    from diligence_utils import get_all_deals
    all_deals = get_all_deals()

    for deal in all_deals:
        if deal.get("lead_id") == lead_id:
            deals.append(deal)

    return deals


def sync_all_deal_links() -> Dict[str, int]:
    """
    Sync all existing deals with CRM leads
    Useful for batch updates

    Returns:
        Dict with sync stats
    """
    stats = {
        "total_deals": 0,
        "linked_deals": 0,
        "unlinked_deals": 0,
        "errors": 0
    }

    if not DB_AVAILABLE:
        return stats

    from diligence_utils import get_all_deals

    all_deals = get_all_deals()
    stats["total_deals"] = len(all_deals)

    for deal in all_deals:
        deal_id = deal.get("deal_id")
        lead_id = deal.get("lead_id")

        if lead_id:
            stats["linked_deals"] += 1

            # Verify link is still valid
            try:
                lead = get_lead_by_id(lead_id)
                if not lead:
                    print(f"Warning: Deal {deal_id} linked to non-existent lead {lead_id}")
                    stats["errors"] += 1
            except Exception as e:
                print(f"Error checking lead {lead_id}: {e}")
                stats["errors"] += 1
        else:
            stats["unlinked_deals"] += 1

    return stats


if __name__ == "__main__":
    # Test CRM integration
    print("Testing diligence_crm_lookup...")

    if DB_AVAILABLE:
        print("CRM database is available")

        # Test sync
        stats = sync_all_deal_links()
        print(f"Sync stats: {stats}")
    else:
        print("CRM database not available - integration tests skipped")

    print("CRM integration module loaded successfully!")
