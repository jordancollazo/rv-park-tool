"""
export_to_sheets.py
Export lead data to Google Sheets.

Usage:
    python execution/export_to_sheets.py
    
Requires:
    - credentials.json (Google Cloud OAuth credentials)
    - token.json (auto-generated on first run)
"""

import json
import sys
from datetime import datetime
from pathlib import Path

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Paths
INPUT_PATH = Path(".tmp/scored_sites.json")
CREDENTIALS_PATH = Path("credentials.json")
TOKEN_PATH = Path("token.json")

# Google Sheets API scope
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def get_google_credentials() -> Credentials:
    """Get or refresh Google API credentials."""
    creds = None
    
    # Load existing token
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    
    # Refresh or get new credentials
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not CREDENTIALS_PATH.exists():
                print("ERROR: credentials.json not found")
                print("Download OAuth credentials from Google Cloud Console")
                print("and save as credentials.json in project root")
                sys.exit(1)
            
            flow = InstalledAppFlow.from_client_secrets_file(
                str(CREDENTIALS_PATH), SCOPES
            )
            creds = flow.run_local_server(port=0)
        
        # Save token
        with open(TOKEN_PATH, "w") as f:
            f.write(creds.to_json())
    
    return creds


def create_spreadsheet(service, title: str) -> str:
    """Create a new Google Spreadsheet and return its ID."""
    spreadsheet = {
        "properties": {"title": title},
        "sheets": [{"properties": {"title": "Leads"}}],
    }
    
    result = service.spreadsheets().create(body=spreadsheet).execute()
    spreadsheet_id = result["spreadsheetId"]
    
    print(f"Created spreadsheet: {title}")
    print(f"ID: {spreadsheet_id}")
    print(f"URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
    
    return spreadsheet_id


def format_header_row(service, spreadsheet_id: str):
    """Apply formatting to header row."""
    requests = [
        {
            "repeatCell": {
                "range": {
                    "sheetId": 0,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2},
                        "textFormat": {
                            "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                            "bold": True,
                        },
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat)",
            }
        },
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": 0,
                    "gridProperties": {"frozenRowCount": 1},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        },
    ]
    
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": requests},
    ).execute()


def export_to_sheets(
    results: list[dict],
    area: str,
    spreadsheet_id: str | None = None,
) -> str:
    """
    Export results to Google Sheets.
    
    Args:
        results: List of scored place records
        area: Geographic area (for naming)
        spreadsheet_id: Existing spreadsheet ID, or None to create new
        
    Returns:
        Spreadsheet ID
    """
    creds = get_google_credentials()
    service = build("sheets", "v4", credentials=creds)
    
    # Create new spreadsheet if needed
    if not spreadsheet_id:
        date_str = datetime.now().strftime("%Y-%m-%d")
        title = f"RV/MHP Leads - {area} - {date_str}"
        spreadsheet_id = create_spreadsheet(service, title)
    
    # Define columns
    columns = [
        "Name",
        "Score",
        "Phone",
        "Website",
        "Address",
        "City",
        "State",
        "ZIP",
        "Google Rating",
        "Reviews",
        "Score Reasons",
        "Crawl Status",
        "Maps URL",
        "Place ID",
    ]
    
    # Build rows
    rows = [columns]  # Header
    
    for place in results:
        row = [
            place.get("name", ""),
            place.get("site_score_1_10", ""),
            place.get("phone", ""),
            place.get("website", ""),
            place.get("address", ""),
            place.get("city", ""),
            place.get("state", ""),
            place.get("zip", ""),
            place.get("google_rating", ""),
            place.get("review_count", ""),
            place.get("score_reasons", ""),
            place.get("crawl_status", ""),
            place.get("maps_url", ""),
            place.get("place_id", ""),
        ]
        rows.append(row)
    
    # Write to sheet
    body = {"values": rows}
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range="Leads!A1",
        valueInputOption="RAW",
        body=body,
    ).execute()
    
    print(f"Wrote {len(rows) - 1} leads to spreadsheet")
    
    # Format header
    format_header_row(service, spreadsheet_id)
    
    return spreadsheet_id


def load_and_export(input_path: Path = INPUT_PATH, area: str = "Unknown") -> str:
    """Load scored results and export to Google Sheets."""
    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}")
        print("Run the pipeline first")
        sys.exit(1)
    
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    results = data.get("places", [])
    print(f"Loaded {len(results)} places to export")
    
    return export_to_sheets(results, area)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Export leads to Google Sheets")
    parser.add_argument(
        "--area",
        default="Unknown",
        help="Area name (for spreadsheet title)",
    )
    parser.add_argument(
        "--spreadsheet-id",
        default=None,
        help="Existing spreadsheet ID to update (creates new if not specified)",
    )
    
    args = parser.parse_args()
    
    spreadsheet_id = load_and_export(area=args.area)
    print(f"\nSpreadsheet URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")


if __name__ == "__main__":
    main()
