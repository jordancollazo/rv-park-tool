"""
Financial Calculator - Web Scraping Module
Extract property listing data directly from URLs (LoopNet, Crexi, etc.)
"""

import os
import json
import sys
from datetime import datetime
from typing import Dict, Any, Optional
from urllib.parse import urlparse

try:
    from anthropic import Anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False
    print("Warning: anthropic not installed")

try:
    from bs4 import BeautifulSoup
    SCRAPING_AVAILABLE = True
except ImportError:
    SCRAPING_AVAILABLE = False
    print("Warning: beautifulsoup4 not installed. Install with: pip install beautifulsoup4")

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("Warning: playwright not installed. Install with: pip install playwright && playwright install chromium")

try:
    import undetected_chromedriver as uc
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("Warning: undetected-chromedriver not installed. Install with: pip install undetected-chromedriver selenium")

try:
    from apify_client import ApifyClient
    APIFY_AVAILABLE = True
except ImportError:
    APIFY_AVAILABLE = False
    print("Warning: apify-client not installed. Install with: pip install apify-client")


def fetch_with_apify_loopnet(url: str) -> str:
    """
    Fetch LoopNet listing using Apify actor (best for LoopNet)

    Args:
        url: Full URL to LoopNet property listing

    Returns:
        HTML content as string
    """
    if not APIFY_AVAILABLE:
        raise RuntimeError("apify-client not installed")

    api_key = os.getenv("APIFY_API_TOKEN")
    if not api_key:
        raise RuntimeError("APIFY_API_TOKEN not found in environment")

    print("Loading page with Apify LoopNet scraper...")

    try:
        client = ApifyClient(api_key)

        # Use memo23/apify-loopnet-search-cheerio actor
        run_input = {
            "startUrls": [{"url": url}],
            "maxItems": 1,
            "proxyConfiguration": {"useApifyProxy": True},
            "includeListingDetails": True,
        }

        # Run the actor
        run = client.actor("memo23/apify-loopnet-search-cheerio").call(run_input=run_input)

        # Fetch results
        dataset_id = run["defaultDatasetId"]
        items = client.dataset(dataset_id).list_items().items

        if not items:
            raise Exception("No data returned from Apify LoopNet scraper")

        # Convert first item to text format for LLM parsing
        item = items[0]

        # Build a text representation of the listing
        text_parts = []
        if item.get("propertyName"):
            text_parts.append(f"Property Name: {item['propertyName']}")
        if item.get("address"):
            text_parts.append(f"Address: {item['address']}")
        if item.get("city") and item.get("state"):
            text_parts.append(f"Location: {item['city']}, {item['state']} {item.get('zip', '')}")
        if item.get("price") or item.get("priceNumeric"):
            price = item.get("priceNumeric") or item.get("price")
            text_parts.append(f"Price: ${price:,}" if isinstance(price, (int, float)) else f"Price: {price}")
        if item.get("numberOfUnits"):
            text_parts.append(f"Number of Units: {item['numberOfUnits']}")
        if item.get("capRate"):
            text_parts.append(f"Cap Rate: {item['capRate']}")
        if item.get("noi"):
            text_parts.append(f"NOI: ${item['noi']:,}" if isinstance(item['noi'], (int, float)) else f"NOI: {item['noi']}")
        if item.get("description"):
            text_parts.append(f"\nDescription: {item['description']}")
        if item.get("propertyType"):
            text_parts.append(f"Property Type: {item['propertyType']}")
        if item.get("buildingSize"):
            text_parts.append(f"Building Size: {item['buildingSize']}")
        if item.get("lotSize"):
            text_parts.append(f"Lot Size: {item['lotSize']}")

        text_content = "\n".join(text_parts)

        print(f"Extracted {len(text_content)} characters from Apify LoopNet")

        return text_content

    except Exception as e:
        raise Exception(f"Apify LoopNet error: {str(e)}")


def fetch_with_apify_crexi(url: str) -> str:
    """
    Fetch Crexi listing using Apify actor (best for Crexi)

    Args:
        url: Full URL to Crexi property listing

    Returns:
        HTML content as string
    """
    if not APIFY_AVAILABLE:
        raise RuntimeError("apify-client not installed")

    api_key = os.getenv("APIFY_API_TOKEN")
    if not api_key:
        raise RuntimeError("APIFY_API_TOKEN not found in environment")

    print("Loading page with Apify Crexi scraper...")

    try:
        client = ApifyClient(api_key)

        # Use memo23/apify-crexi actor
        run_input = {
            "startUrls": [{"url": url}],
            "maxItems": 1,
            "includeListingDetails": True,
            "includeBrokerDetails": True,
        }

        # Run the actor
        run = client.actor("memo23/apify-crexi").call(run_input=run_input)

        # Fetch results
        dataset_id = run["defaultDatasetId"]
        items = client.dataset(dataset_id).list_items().items

        if not items:
            raise Exception("No data returned from Apify Crexi scraper")

        # Convert first item to text format for LLM parsing
        item = items[0]

        # Build a text representation of the listing
        text_parts = []

        # Get name from details if available
        details = item.get("details", {}) or {}
        property_name = details.get("name") or item.get("propertyName") or item.get("name")

        if property_name:
            text_parts.append(f"Property Name: {property_name}")

        # Extract location
        locations = item.get("locations", [])
        if locations:
            loc = locations[0]
            if loc.get("address"):
                text_parts.append(f"Address: {loc['address']}")
            if loc.get("city") and loc.get("state"):
                state_code = loc['state'].get('code') if isinstance(loc['state'], dict) else loc['state']
                text_parts.append(f"Location: {loc['city']}, {state_code} {loc.get('zip', '')}")

        if item.get("askingPrice"):
            text_parts.append(f"Asking Price: ${item['askingPrice']:,}" if isinstance(item['askingPrice'], (int, float)) else f"Asking Price: {item['askingPrice']}")
        if item.get("units"):
            text_parts.append(f"Number of Units: {item['units']}")
        if item.get("capRate"):
            text_parts.append(f"Cap Rate: {item['capRate']}")
        if item.get("noi"):
            text_parts.append(f"NOI: ${item['noi']:,}" if isinstance(item['noi'], (int, float)) else f"NOI: {item['noi']}")
        if item.get("description"):
            text_parts.append(f"\nDescription: {item['description']}")
        if item.get("type"):
            text_parts.append(f"Property Type: {item['type']}")
        if item.get("sqFt"):
            text_parts.append(f"Square Feet: {item['sqFt']}")
        if item.get("yearBuilt"):
            text_parts.append(f"Year Built: {item['yearBuilt']}")

        # Broker info
        broker = item.get("broker", {})
        if broker.get("name"):
            text_parts.append(f"Broker: {broker['name']}")
        if broker.get("companyName"):
            text_parts.append(f"Brokerage: {broker['companyName']}")

        text_content = "\n".join(text_parts)

        print(f"Extracted {len(text_content)} characters from Apify Crexi")

        return text_content

    except Exception as e:
        raise Exception(f"Apify Crexi error: {str(e)}")


def fetch_webpage_selenium(url: str) -> str:
    """
    Fetch HTML using undetected-chromedriver (best for anti-bot sites)

    Args:
        url: Full URL to property listing

    Returns:
        HTML content as string
    """
    if not SELENIUM_AVAILABLE:
        raise RuntimeError("undetected-chromedriver not installed. Install with: pip install undetected-chromedriver selenium")

    print("Loading page with undetected Chrome...")

    options = uc.ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--window-size=1920,1080')

    driver = None
    try:
        # Use undetected chromedriver
        driver = uc.Chrome(options=options, version_main=None)

        # Navigate to URL
        driver.get(url)

        # Wait for page to load - use multiple strategies
        WebDriverWait(driver, 20).until(
            lambda d: d.execute_script('return document.readyState') == 'complete'
        )

        # Additional wait for dynamic content
        import time
        time.sleep(5)

        # Get page source
        html = driver.page_source

        print(f"Extracted {len(html)} characters with Selenium")

        return html

    except Exception as e:
        raise Exception(f"Selenium error: {str(e)}")
    finally:
        if driver:
            driver.quit()


def fetch_webpage_playwright(url: str) -> str:
    """
    Fetch HTML using Playwright (fallback method)

    Args:
        url: Full URL to property listing

    Returns:
        HTML content as string
    """
    if not PLAYWRIGHT_AVAILABLE:
        raise RuntimeError("playwright not installed")

    print("Loading page with Playwright...")

    try:
        with sync_playwright() as p:
            browser = p.firefox.launch(
                headless=True,
                firefox_user_prefs={
                    "dom.webdriver.enabled": False,
                    "useAutomationExtension": False
                }
            )

            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
                locale='en-US',
                extra_http_headers={
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'DNT': '1',
                    'Upgrade-Insecure-Requests': '1'
                }
            )

            page = context.new_page()
            page.goto(url, wait_until='networkidle', timeout=90000)
            page.wait_for_timeout(10000)
            html = page.content()
            browser.close()

            print(f"Extracted {len(html)} characters with Playwright")

            return html

    except Exception as e:
        raise Exception(f"Playwright error: {str(e)}")


def fetch_webpage(url: str) -> str:
    """
    Fetch HTML content from a URL using best available method
    Tries Apify scrapers first (best for LoopNet/Crexi), falls back to browser automation

    Args:
        url: Full URL to property listing

    Returns:
        HTML content as string
    """
    errors = []

    # Detect which platform from URL
    url_lower = url.lower()
    is_loopnet = "loopnet.com" in url_lower
    is_crexi = "crexi.com" in url_lower

    # Method 1: Try Apify scrapers (best for LoopNet/Crexi)
    if APIFY_AVAILABLE and is_loopnet:
        try:
            return fetch_with_apify_loopnet(url)
        except Exception as e:
            errors.append(f"Apify LoopNet: {str(e)}")
            print(f"Apify LoopNet failed: {e}")

    if APIFY_AVAILABLE and is_crexi:
        try:
            return fetch_with_apify_crexi(url)
        except Exception as e:
            errors.append(f"Apify Crexi: {str(e)}")
            print(f"Apify Crexi failed: {e}")

    # Method 2: Try undetected-chromedriver (fallback for any site)
    if SELENIUM_AVAILABLE:
        try:
            return fetch_webpage_selenium(url)
        except Exception as e:
            errors.append(f"Selenium: {str(e)}")
            print(f"Selenium failed: {e}")

    # Method 3: Try Playwright as final fallback
    if PLAYWRIGHT_AVAILABLE:
        try:
            return fetch_webpage_playwright(url)
        except Exception as e:
            errors.append(f"Playwright: {str(e)}")
            print(f"Playwright failed: {e}")

    # All methods failed
    raise Exception(f"All scraping methods failed. Errors: {'; '.join(errors)}")


def extract_text_from_html(html: str) -> str:
    """
    Extract clean text from HTML

    Args:
        html: Raw HTML content

    Returns:
        Cleaned text content
    """
    if not SCRAPING_AVAILABLE:
        raise RuntimeError("beautifulsoup4 not installed")

    soup = BeautifulSoup(html, 'html.parser')

    # Remove script and style elements
    for script in soup(["script", "style", "nav", "header", "footer"]):
        script.decompose()

    # Get text
    text = soup.get_text(separator='\n')

    # Clean up whitespace
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = '\n'.join(chunk for chunk in chunks if chunk)

    return text


def parse_listing_with_llm(text_content: str, source_url: str, api_key: Optional[str] = None) -> Dict[str, Any]:
    """
    Parse scraped text using Claude to extract structured listing data

    Args:
        text_content: Cleaned text from webpage
        source_url: Original URL
        api_key: Anthropic API key

    Returns:
        Dict with extracted fields
    """
    if not ANTHROPIC_AVAILABLE:
        raise RuntimeError("anthropic library not installed")

    if not api_key:
        api_key = os.getenv("ANTHROPIC_API_KEY")

    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not found in environment")

    client = Anthropic(api_key=api_key)

    # Truncate if too long (max ~100k chars for API)
    if len(text_content) > 100000:
        text_content = text_content[:100000] + "\n...[truncated]"

    prompt = f"""You are a real estate data extraction specialist. Parse the following web content from a property listing and extract structured data.

Source URL: {source_url}

Web Content:
{text_content}

Extract the following fields (use null for missing numeric fields, empty string for missing text):
- property_name: Name of the property
- description: Brief description (2-3 sentences max)
- purchase_price: Dollar amount (numeric, no commas) or null
- unit_count: Number of units/lots/spaces (numeric) or null
- location: City, State or full address
- noi: Net Operating Income if mentioned (numeric) or null
- cap_rate: Cap rate if mentioned (numeric, as percentage like 7.5) or null
- additional_metrics: Any other financial metrics mentioned (as dict)

Respond ONLY with valid JSON, no markdown formatting:
{{
  "property_name": "...",
  "description": "...",
  "purchase_price": 1234567 or null,
  "unit_count": 123 or null,
  "location": "...",
  "noi": 123456 or null,
  "cap_rate": 7.5 or null,
  "additional_metrics": {{}},
  "confidence": "high|medium|low"
}}"""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=2000,
            temperature=0,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )

        response_text = response.content[0].text.strip()

        # Strip markdown code blocks if present
        if response_text.startswith("```json"):
            response_text = response_text[7:]  # Remove ```json
        if response_text.startswith("```"):
            response_text = response_text[3:]  # Remove ```
        if response_text.endswith("```"):
            response_text = response_text[:-3]  # Remove trailing ```
        response_text = response_text.strip()

        # Parse JSON response
        parsed = json.loads(response_text)

        return parsed

    except json.JSONDecodeError as e:
        print(f"Failed to parse LLM response as JSON: {e}")
        print(f"Response: {response_text}")
        # Return fallback structure
        return {
            "property_name": "Parse Error",
            "description": "Failed to parse listing",
            "purchase_price": None,
            "unit_count": None,
            "location": "Unknown",
            "noi": None,
            "cap_rate": None,
            "additional_metrics": {},
            "confidence": "low",
            "error": str(e)
        }

    except Exception as e:
        print(f"LLM API Error: {e}")
        raise


def process_listing_url(url: str, output_dir: str = ".tmp/calculator_option_1/leads") -> Dict[str, Any]:
    """
    Full pipeline: Fetch URL → Extract text → LLM parsing → JSON output

    Args:
        url: URL to property listing
        output_dir: Directory to save extracted JSON

    Returns:
        Extracted listing data dict
    """
    print(f"Processing URL: {url}")

    # Generate lead_id
    lead_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Step 1: Fetch webpage
    print("Fetching webpage...")
    try:
        html = fetch_webpage(url)
    except Exception as e:
        print(f"Failed to fetch URL: {e}")
        # Return error structure
        parsed_data = {
            "property_name": "URL Fetch Failed",
            "description": f"Could not fetch URL: {str(e)}",
            "purchase_price": None,
            "unit_count": None,
            "location": "",
            "noi": None,
            "cap_rate": None,
            "additional_metrics": {},
            "confidence": "low",
            "error": f"URL fetch failed: {str(e)}",
            "source_url": url,
            "processed_at": datetime.now().isoformat(),
            "lead_id": lead_id
        }

        # Save error lead
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{lead_id}.json")
        with open(output_path, 'w') as f:
            json.dump(parsed_data, f, indent=2)

        return parsed_data

    # Step 2: Extract text
    print("Extracting text from HTML...")
    text_content = extract_text_from_html(html)
    print(f"Extracted {len(text_content)} characters")

    # Step 3: LLM Parsing
    print("Parsing with Claude API...")
    parsed_data = parse_listing_with_llm(text_content, url)

    # Step 4: Add metadata
    parsed_data["source_url"] = url
    parsed_data["processed_at"] = datetime.now().isoformat()
    parsed_data["lead_id"] = lead_id
    parsed_data["raw_text"] = text_content[:5000]  # Save first 5000 chars for reference

    # Step 5: Save to JSON
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{lead_id}.json")

    with open(output_path, 'w') as f:
        json.dump(parsed_data, f, indent=2)

    print(f"Saved to: {output_path}")
    print(f"Confidence: {parsed_data.get('confidence', 'unknown')}")

    return parsed_data


def main():
    """CLI entry point"""
    if len(sys.argv) < 2:
        print("Usage: python financial_calc_webscrape.py <listing_url>")
        print("Example: python financial_calc_webscrape.py https://www.loopnet.com/Listing/...")
        sys.exit(1)

    url = sys.argv[1]

    # Validate URL
    parsed_url = urlparse(url)
    if not parsed_url.scheme or not parsed_url.netloc:
        print(f"Error: Invalid URL: {url}")
        sys.exit(1)

    try:
        result = process_listing_url(url)

        # Print summary
        print("\n" + "="*60)
        print("EXTRACTION SUMMARY")
        print("="*60)
        print(f"Property: {result.get('property_name', 'N/A')}")
        print(f"Location: {result.get('location', 'N/A')}")
        print(f"Price: ${result.get('purchase_price', 0):,}" if result.get('purchase_price') else "Price: Unspecified")
        print(f"Units: {result.get('unit_count', 'N/A')}")
        print(f"NOI: ${result.get('noi', 0):,}" if result.get('noi') else "NOI: N/A")
        print(f"Cap Rate: {result.get('cap_rate', 'N/A')}%" if result.get('cap_rate') else "Cap Rate: N/A")
        print(f"Confidence: {result.get('confidence', 'unknown')}")
        print("="*60)

    except Exception as e:
        print(f"Error processing URL: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
