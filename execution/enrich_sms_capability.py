"""
enrich_sms_capability.py

Validates phone numbers and identifies SMS-capable numbers using Twilio Lookup API.

Usage:
    # Test with a single phone number
    python execution/enrich_sms_capability.py --test-phone "+14075551234"
    
    # Batch process all leads with phone numbers
    python execution/enrich_sms_capability.py --all
    
    # Process leads from a specific area
    python execution/enrich_sms_capability.py --area "Orlando, FL"
    
    # Process specific lead IDs
    python execution/enrich_sms_capability.py --lead-ids 1,2,3

Environment Variables Required:
    TWILIO_ACCOUNT_SID - Your Twilio Account SID
    TWILIO_AUTH_TOKEN - Your Twilio Auth Token
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import phonenumbers
from dotenv import load_dotenv
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from execution.db import get_db, get_all_leads, get_lead_by_id

# Load environment variables
load_dotenv()


def get_twilio_client():
    """Get authenticated Twilio client."""
    account_sid = os.getenv("TWILIO_ACCOUNT_SID")
    auth_token = os.getenv("TWILIO_AUTH_TOKEN")
    
    if not account_sid or not auth_token:
        raise ValueError(
            "Missing Twilio credentials. Please set TWILIO_ACCOUNT_SID and "
            "TWILIO_AUTH_TOKEN in your .env file."
        )
    
    return Client(account_sid, auth_token)


def normalize_phone_number(phone: str) -> str | None:
    """
    Normalize phone number to E.164 format.
    
    Args:
        phone: Raw phone number string
    
    Returns:
        E.164 formatted number (e.g., +14075551234) or None if invalid
    """
    if not phone:
        return None
    
    try:
        # Try parsing as US number first
        parsed = phonenumbers.parse(phone, "US")
        if phonenumbers.is_valid_number(parsed):
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except phonenumbers.NumberParseException:
        pass
    
    # Try without country code
    try:
        parsed = phonenumbers.parse(f"+1{phone}", None)
        if phonenumbers.is_valid_number(parsed):
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except phonenumbers.NumberParseException:
        pass
    
    return None


def validate_phone_number(client: Client, phone: str) -> dict:
    """
    Validate phone number using Twilio Lookup API.
    
    Args:
        client: Twilio client
        phone: E.164 formatted phone number
    
    Returns:
        Dictionary with validation results:
        {
            'valid': bool,
            'sms_capable': bool,
            'carrier_type': str,  # 'mobile', 'landline', 'voip', or 'unknown'
            'carrier_name': str,
            'error': str or None
        }
    """
    try:
        # Lookup phone number with carrier info
        phone_number = client.lookups.v2.phone_numbers(phone).fetch(
            fields="line_type_intelligence"
        )
        
        # Extract carrier info
        carrier_info = phone_number.line_type_intelligence or {}
        carrier_type = carrier_info.get("type", "unknown").lower()
        carrier_name = carrier_info.get("carrier_name", "Unknown")
        
        # Determine SMS capability
        # Mobile and VoIP can typically receive SMS, landlines cannot
        sms_capable = carrier_type in ["mobile", "voip"]
        
        return {
            "valid": True,
            "sms_capable": sms_capable,
            "carrier_type": carrier_type,
            "carrier_name": carrier_name,
            "error": None
        }
    
    except TwilioRestException as e:
        # Handle Twilio API errors
        return {
            "valid": False,
            "sms_capable": False,
            "carrier_type": "unknown",
            "carrier_name": "Unknown",
            "error": str(e)
        }
    except Exception as e:
        return {
            "valid": False,
            "sms_capable": False,
            "carrier_type": "unknown",
            "carrier_name": "Unknown",
            "error": f"Unexpected error: {str(e)}"
        }


def update_lead_sms_status(lead_id: int, sms_data: dict):
    """
    Update lead with SMS capability data.
    
    Args:
        lead_id: Lead ID
        sms_data: Dictionary from validate_phone_number()
    """
    now = datetime.now(timezone.utc).isoformat()
    
    with get_db() as conn:
        conn.execute("""
            UPDATE leads SET
                sms_capable = ?,
                carrier_type = ?,
                carrier_name = ?,
                phone_validated_at = ?
            WHERE id = ?
        """, (
            sms_data["sms_capable"],
            sms_data["carrier_type"],
            sms_data["carrier_name"],
            now,
            lead_id
        ))
        conn.commit()


def enrich_lead(client: Client, lead: dict, verbose: bool = True) -> bool:
    """
    Enrich a single lead with SMS capability data.
    
    Returns:
        True if enrichment succeeded, False otherwise
    """
    lead_id = lead["id"]
    phone = lead.get("phone")
    
    if not phone:
        if verbose:
            print(f"  [{lead_id}] {lead['name']}: No phone number")
        return False
    
    # Normalize phone number
    normalized_phone = normalize_phone_number(phone)
    if not normalized_phone:
        if verbose:
            print(f"  [{lead_id}] {lead['name']}: Invalid phone format: {phone}")
        return False
    
    # Validate with Twilio
    if verbose:
        print(f"  [{lead_id}] {lead['name']}: Validating {normalized_phone}...", end=" ")
    
    sms_data = validate_phone_number(client, normalized_phone)
    
    if sms_data["error"]:
        if verbose:
            print(f"ERROR: {sms_data['error']}")
        return False
    
    # Update database
    update_lead_sms_status(lead_id, sms_data)
    
    if verbose:
        sms_status = "✓ SMS" if sms_data["sms_capable"] else "✗ No SMS"
        print(f"{sms_status} ({sms_data['carrier_type']}, {sms_data['carrier_name']})")
    
    return True


def batch_enrich_sms(
    area: str | None = None,
    lead_ids: list[int] | None = None,
    all_leads: bool = False,
    verbose: bool = True
) -> tuple[int, int, int]:
    """
    Batch process multiple leads for SMS enrichment.
    
    Args:
        area: Filter by area (e.g., "Orlando, FL")
        lead_ids: List of specific lead IDs to process
        all_leads: Process all leads with phone numbers
        verbose: Print progress
    
    Returns:
        Tuple of (total_processed, successful, failed)
    """
    client = get_twilio_client()
    
    # Get leads to process
    if lead_ids:
        leads = [get_lead_by_id(lid) for lid in lead_ids]
        leads = [l for l in leads if l is not None]
    elif area:
        leads = get_all_leads(area=area)
    elif all_leads:
        leads = get_all_leads()
    else:
        raise ValueError("Must specify --area, --lead-ids, or --all")
    
    # Filter to leads with phone numbers that haven't been validated
    leads_to_process = [
        l for l in leads 
        if l.get("phone") and not l.get("phone_validated_at")
    ]
    
    if verbose:
        print(f"\n{'='*60}")
        print(f"SMS Capability Enrichment")
        print(f"{'='*60}")
        print(f"Total leads: {len(leads)}")
        print(f"With phone numbers: {len([l for l in leads if l.get('phone')])}")
        print(f"Already validated: {len([l for l in leads if l.get('phone_validated_at')])}")
        print(f"To process: {len(leads_to_process)}")
        print(f"{'='*60}\n")
    
    if not leads_to_process:
        if verbose:
            print("No leads to process.")
        return 0, 0, 0
    
    # Estimate cost
    cost_per_lookup = 0.005  # $0.005 per lookup
    estimated_cost = len(leads_to_process) * cost_per_lookup
    
    if verbose:
        print(f"Estimated cost: ${estimated_cost:.2f} ({len(leads_to_process)} lookups × ${cost_per_lookup})")
        response = input("\nProceed? (y/n): ")
        if response.lower() != 'y':
            print("Aborted.")
            return 0, 0, 0
        print()
    
    # Process leads
    successful = 0
    failed = 0
    
    for i, lead in enumerate(leads_to_process, 1):
        if verbose:
            print(f"[{i}/{len(leads_to_process)}]", end=" ")
        
        if enrich_lead(client, lead, verbose=verbose):
            successful += 1
        else:
            failed += 1
    
    if verbose:
        print(f"\n{'='*60}")
        print(f"Enrichment Complete")
        print(f"{'='*60}")
        print(f"Processed: {len(leads_to_process)}")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        print(f"Actual cost: ${successful * cost_per_lookup:.2f}")
        print(f"{'='*60}\n")
    
    return len(leads_to_process), successful, failed


def test_phone(phone: str):
    """Test SMS validation with a single phone number."""
    client = get_twilio_client()
    
    print(f"\nTesting phone number: {phone}")
    print("="*60)
    
    # Normalize
    normalized = normalize_phone_number(phone)
    if not normalized:
        print(f"ERROR: Invalid phone number format")
        return
    
    print(f"Normalized: {normalized}")
    
    # Validate
    print("Validating with Twilio...", end=" ")
    result = validate_phone_number(client, normalized)
    
    if result["error"]:
        print(f"\nERROR: {result['error']}")
        return
    
    print("✓")
    print(f"\nResults:")
    print(f"  Valid: {result['valid']}")
    print(f"  SMS Capable: {result['sms_capable']}")
    print(f"  Carrier Type: {result['carrier_type']}")
    print(f"  Carrier Name: {result['carrier_name']}")
    print()


def main():
    parser = argparse.ArgumentParser(description="Enrich leads with SMS capability data")
    parser.add_argument("--test-phone", help="Test with a single phone number")
    parser.add_argument("--area", help="Process leads from specific area (e.g., 'Orlando, FL')")
    parser.add_argument("--lead-ids", help="Comma-separated list of lead IDs to process")
    parser.add_argument("--all", action="store_true", help="Process all leads with phone numbers")
    parser.add_argument("--quiet", action="store_true", help="Suppress progress output")
    
    args = parser.parse_args()
    
    try:
        if args.test_phone:
            test_phone(args.test_phone)
        elif args.lead_ids:
            lead_ids = [int(x.strip()) for x in args.lead_ids.split(",")]
            batch_enrich_sms(lead_ids=lead_ids, verbose=not args.quiet)
        elif args.area or args.all:
            batch_enrich_sms(area=args.area, all_leads=args.all, verbose=not args.quiet)
        else:
            parser.print_help()
            print("\nError: Must specify --test-phone, --area, --lead-ids, or --all")
            sys.exit(1)
    
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nAborted by user.")
        sys.exit(1)


if __name__ == "__main__":
    main()
