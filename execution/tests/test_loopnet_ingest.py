
import pytest
from unittest.mock import MagicMock, patch
import json
from scrape_loopnet_apify import normalize_lead, run_scraper
import sys
from pathlib import Path

# Add execution dir to path to import db
sys.path.append(str(Path(__file__).parent.parent))

def test_normalize_lead():
    raw_item = {
        "id": "123456",
        "url": "https://loopnet.com/listing/123",
        "title": "Sunny Park",
        "streetAddress": "123 Main St",
        "city": "Tampa",
        "state": "FL",
        "zip": "33602",
        "price": "$1,500,000",
        "capRate": "6.5%",
        "noi": 100000,
        "occupancy": 95,
        "numberOfUnits": 50,
        "listingAgents": [
            {"name": "John Doe", "company": "Top Brokerage"}
        ],
        "description": "Great value add opportunity."
    }
    
    normalized = normalize_lead(raw_item)
    
    assert normalized["loopnet_id"] == "123456"
    assert normalized["list_price"] == 1500000.0
    assert normalized["cap_rate"] == 6.5
    assert normalized["broker_name"] == "John Doe"
    assert normalized["broker_firm"] == "Top Brokerage"
    assert normalized["place_id"] == "loopnet:123456"
    assert normalized["address"] == "123 Main St"
    assert normalized["area"] == "Tampa, FL"

@patch('scrape_loopnet_apify.ApifyClient')
@patch('scrape_loopnet_apify.upsert_lead')
@patch('db.update_lead_fields') 
def test_run_scraper_mocks(mock_update_fields, mock_upsert, mock_client_cls):
    """
    Test the run_scraper function with mocked Apify and DB.
    """
    # Setup Mock Client
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    
    # Mock Actor Call
    mock_run = {"defaultDatasetId": "dataset_123"}
    mock_client.actor.return_value.call.return_value = mock_run
    
    # Mock Dataset Items
    mock_items = [
        {
            "id": "1001",
            "streetAddress": "Test Park",
            "city": "Orlando",
            "state": "FL",
            "price": "$2,000,000"
        }
    ]
    mock_client.dataset.return_value.list_items.return_value.items = mock_items
    
    # Mock upsert return value (lead_id)
    mock_upsert.return_value = 55
    
    # Run Scraper
    run_scraper(state="FL", limit=1)
    
    # Assertions
    mock_client.actor.assert_called_with("memo23/apify-loopnet-search-cheerioa")
    mock_upsert.assert_called_once()
    
    # Verify update_lead_fields called with extra fields
    mock_update_fields.assert_called_once()
    args, kwargs = mock_update_fields.call_args
    assert args[0] == 55 # lead_id
    assert kwargs["loopnet_id"] == "1001"
    assert kwargs["list_price"] == 2000000.0
