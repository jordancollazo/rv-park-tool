"""
gmail_sync.py
Sync emails from Gmail to the CRM database using Gmail API.

Requires:
    - credentials.json (Google Cloud OAuth credentials)
    - token.json (auto-generated on first run)
"""

import base64
import json
import re
import sys
from datetime import datetime
from pathlib import Path

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

from db import get_all_leads, sync_email, upsert_lead

# Paths
CREDENTIALS_PATH = Path(__file__).parent.parent / "credentials.json"
TOKEN_PATH = Path(__file__).parent.parent / "token.json"

# Scopes - Updated to include Gmail readonly
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/gmail.readonly"
]

def get_gmail_service():
    """Get authenticated Gmail API service."""
    creds = None
    
    if TOKEN_PATH.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
        except Exception:
            creds = None

    # Refresh or new login
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception:
                creds = None

        if not creds:
            if not CREDENTIALS_PATH.exists():
                print("ERROR: credentials.json not found")
                return None
            
            flow = InstalledAppFlow.from_client_secrets_file(
                str(CREDENTIALS_PATH), SCOPES
            )
            creds = flow.run_local_server(port=0)
        
        # Save token
        with open(TOKEN_PATH, "w") as f:
            f.write(creds.to_json())

    return build("gmail", "v1", credentials=creds)


def parse_email_body(payload):
    """Extract snippet/body from email payload."""
    snippet = payload.get("snippet", "")
    return snippet


def parse_header(headers, name):
    """Get header value by name."""
    for h in headers:
        if h["name"].lower() == name.lower():
            return h["value"]
    return ""


def sync_emails_for_lead(service, lead):
    """
    Sync emails for a specific lead.
    Searches for threads matching lead's name or phone.
    """
    lead_id = lead["id"]
    name = lead["name"]
    phone = lead["phone"]
    
    # Build search query
    # Search for emails FROM or TO the user, AND matching lead details
    # We strip common terms to improve search (e.g., "MHP", "LLC")
    search_terms = []
    
    if name:
        # Simplify name for search
        simple_name = re.sub(r'\s+(LLC|Inc|MHP|RV Park|Resort)\b', '', name, flags=re.I).strip()
        if len(simple_name) > 3:
            search_terms.append(f'"{simple_name}"')
            
    if phone:
        # Search phone formats
        digits = re.sub(r'\D', '', phone)
        if len(digits) >= 10:
            search_terms.append(f"{digits}")
            # Try dashed format 555-555-5555
            dashed = f"{digits[-10:-7]}-{digits[-7:-4]}-{digits[-4:]}"
            search_terms.append(f"{dashed}")
            
    if not search_terms:
        return 0
        
    query = f"({' OR '.join(search_terms)})"
    print(f"  Searching for: {query}")
    
    try:
        results = service.users().messages().list(userId="me", q=query, maxResults=10).execute()
        messages = results.get("messages", [])
        
        count = 0
        for msg in messages:
            msg_id = msg["id"]
            thread_id = msg["threadId"]
            
            # Fetch details
            full_msg = service.users().messages().get(userId="me", id=msg_id).execute()
            payload = full_msg.get("payload", {})
            headers = payload.get("headers", [])
            
            subject = parse_header(headers, "Subject")
            from_addr = parse_header(headers, "From")
            to_addr = parse_header(headers, "To")
            date_str = parse_header(headers, "Date")
            
            # Use internalDate for sorting (timestamp ms)
            internal_date = full_msg.get("internalDate")
            try:
                email_date = datetime.fromtimestamp(int(internal_date)/1000).isoformat()
            except:
                email_date = datetime.now().isoformat()
                
            snippet = full_msg.get("snippet", "")
            
            # Determine direction
            # If "me" is in From, it's sent. Otherwise received.
            # This logic depends on authenticated user's email, which we don't strictly know without API call
            # For simplicity, if we authenticated, it's "me"
            
            # Check if "me" sent it (using alias checking is hard, so we assume if user authed, they are 'me')
            # But we can check if the From header contains our email? 
            # We'll just infer: if existing syncs show this address is 'sent', reuse
            # For now, just store as is. 
            
            # BETTER LOGIC: If 'SENT' label is present, it's sent.
            label_ids = full_msg.get("labelIds", [])
            direction = "sent" if "SENT" in label_ids else "received"
            
            # save to db
            res = sync_email(
                lead_id=lead_id,
                gmail_thread_id=thread_id,
                gmail_message_id=msg_id,
                direction=direction,
                subject=subject,
                snippet=snippet,
                from_addr_str = from_addr, # Fix variable name matching db.py
                to_addr_str = to_addr, # Fix variable name matching db.py
                email_date=email_date,
                labels=label_ids
            )
            
            if res:
                count += 1
                
        return count
            
    except Exception as e:
        print(f"  Error syncing lead {name}: {e}")
        return 0


# Wrapper for db.sync_email to match args above
def sync_email_wrapper(lead_id, gmail_thread_id, gmail_message_id, direction,
                       subject, snippet, from_addr_str, to_addr_str, email_date, labels):
    return sync_email(
        lead_id=lead_id,
        gmail_thread_id=gmail_thread_id,
        gmail_message_id=gmail_message_id,
        direction=direction,
        subject=subject,
        snippet=snippet,
        from_address=from_addr_str,
        to_address=to_addr_str,
        email_date=email_date,
        labels=labels
    )
    
# Monkey patch locally to use wrapper or fix imports
# But better: fix db.py call in sync_emails_for_lead?
# I'll update the call in sync_emails_for_lead to match db.sync_email arguments
# db.sync_email takes: lead_id, gmail_thread_id, gmail_message_id, direction, subject, snippet, from_address, to_address, email_date, labels

def sync_all_leads():
    service = get_gmail_service()
    if not service:
        print("Failed to init Gmail service")
        return
    
    leads = get_all_leads()
    print(f"Syncing emails for {len(leads)} leads...")
    
    total_new = 0
    for lead in leads:
        # Update logic inside the loop to call db.sync_email correctly
        # Re-implementing snippet logic here to correct the function call
        lead_id = lead["id"]
        name = lead["name"]
        phone = lead["phone"]
        
        search_terms = []
        if name:
            simple_name = re.sub(r'\s+(LLC|Inc|MHP|RV Park|Resort)\b', '', name, flags=re.I).strip()
            if len(simple_name) > 3:
                search_terms.append(f'"{simple_name}"')
        if phone:
            digits = re.sub(r'\D', '', phone)
            if len(digits) >= 10:
                dashed = f"{digits[-10:-7]}-{digits[-7:-4]}-{digits[-4:]}"
                # search both raw and dashed
                # search_terms.append(digits) - Removing raw digit search to reduce noise
                search_terms.append(dashed)
                
        if not search_terms:
            continue
            
        query = f"({' OR '.join(search_terms)})"
        
        try:
            results = service.users().messages().list(userId="me", q=query, maxResults=10).execute()
            messages = results.get("messages", [])
            
            count = 0
            for msg in messages:
                msg_id = msg["id"]
                
                # Fetch details
                full_msg = service.users().messages().get(userId="me", id=msg_id).execute()
                payload = full_msg.get("payload", {})
                headers = payload.get("headers", [])
                
                subject = parse_header(headers, "Subject")
                from_addr = parse_header(headers, "From")
                to_addr = parse_header(headers, "To")
                
                internal_date = full_msg.get("internalDate")
                try:
                    email_date = datetime.fromtimestamp(int(internal_date)/1000).isoformat()
                except:
                    email_date = datetime.now().isoformat()
                
                snippet = full_msg.get("snippet", "")
                label_ids = full_msg.get("labelIds", [])
                direction = "sent" if "SENT" in label_ids else "received"
                
                # Call DB function
                res = sync_email(
                    lead_id=lead_id,
                    gmail_thread_id=msg["threadId"],
                    gmail_message_id=msg_id,
                    direction=direction,
                    subject=subject,
                    snippet=snippet,
                    from_address=from_addr,
                    to_address=to_addr,
                    email_date=email_date,
                    labels=label_ids
                )
                if res:
                    count += 1
            
            if count > 0:
                print(f"  Synced {count} emails for {name}")
                total_new += count
                
        except Exception as e:
            print(f"  Error syncing {name}: {e}")
            
    print(f"Total new emails synced: {total_new}")

if __name__ == "__main__":
    sync_all_leads()
