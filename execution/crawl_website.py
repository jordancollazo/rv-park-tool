"""
crawl_website.py
Fetches website HTML and metadata for scoring.

Usage:
    python execution/crawl_website.py
"""

import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# Paths
INPUT_PATH = Path(".tmp/normalized_places.json")
OUTPUT_PATH = Path(".tmp/crawled_sites.json")

# Crawl settings
TIMEOUT = 15  # seconds
MAX_WORKERS = 5  # concurrent requests
MAX_INTERNAL_LINKS = 3  # internal pages to crawl per site
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# Aggregator/social domains to flag
AGGREGATOR_DOMAINS = {
    "facebook.com",
    "fb.com",
    "yelp.com",
    "yellowpages.com",
    "mhvillage.com",
    "rvparkreviews.com",
    "tripadvisor.com",
    "google.com",
    "wix.com",  # Site builder but not actual site
}


def is_aggregator_url(url: str) -> bool:
    """Check if URL is a social/aggregator site."""
    if not url:
        return False
    try:
        domain = urlparse(url).netloc.lower()
        for agg_domain in AGGREGATOR_DOMAINS:
            if agg_domain in domain:
                return True
    except Exception:
        pass
    return False


def is_facebook_only(url: str) -> bool:
    """Check if URL is Facebook."""
    if not url:
        return False
    domain = urlparse(url).netloc.lower()
    return "facebook.com" in domain or "fb.com" in domain


def fetch_page(url: str) -> dict:
    """
    Fetch a single page and extract metadata.
    
    Returns dict with HTML, status, metadata.
    """
    result = {
        "url": url,
        "status": "failed",
        "error": None,
        "html": None,
        "title": None,
        "meta_description": None,
        "h1": None,
        "has_https": url.startswith("https://") if url else False,
        "has_viewport": False,
        "phone_visible": False,
        "email_visible": False,
        "has_contact_page": False,
        "page_size_bytes": 0,
        "load_time_ms": 0,
        "social_facebook": None,
        "social_instagram": None,
        "social_linkedin": None,
        "owner_name": None,
    }
    
    if not url:
        result["error"] = "No URL provided"
        return result
    
    try:
        start_time = datetime.now()
        
        response = requests.get(
            url,
            timeout=TIMEOUT,
            headers={"User-Agent": USER_AGENT},
            allow_redirects=True,
        )
        
        load_time = (datetime.now() - start_time).total_seconds() * 1000
        result["load_time_ms"] = round(load_time)
        result["page_size_bytes"] = len(response.content)
        
        # Check final URL for HTTPS (after redirects)
        result["has_https"] = response.url.startswith("https://")
        
        if response.status_code != 200:
            result["error"] = f"HTTP {response.status_code}"
            return result
        
        result["status"] = "success"
        result["html"] = response.text
        
        # Parse HTML
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Title
        title_tag = soup.find("title")
        result["title"] = title_tag.get_text(strip=True) if title_tag else None
        
        # Meta description
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            result["meta_description"] = meta_desc.get("content", "")
        
        # H1
        h1_tag = soup.find("h1")
        result["h1"] = h1_tag.get_text(strip=True) if h1_tag else None
        
        # Viewport
        viewport = soup.find("meta", attrs={"name": "viewport"})
        result["has_viewport"] = viewport is not None
        
        # Phone detection (look for tel: links or phone patterns)
        page_text = soup.get_text()
        tel_links = soup.find_all("a", href=re.compile(r"^tel:"))
        phone_pattern = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", page_text)
        result["phone_visible"] = bool(tel_links or phone_pattern)
        
        # Email detection
        mailto_links = soup.find_all("a", href=re.compile(r"^mailto:"))
        email_pattern = re.search(r"[\w.-]+@[\w.-]+\.\w+", page_text)
        result["email_visible"] = bool(mailto_links or email_pattern)
        
        # Contact page detection
        contact_links = soup.find_all("a", href=re.compile(r"contact", re.I))
        result["has_contact_page"] = bool(contact_links)

        # Social Media Extraction
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            if "facebook.com" in href and not "sharer" in href:
                result["social_facebook"] = a["href"]
            elif "instagram.com" in href:
                result["social_instagram"] = a["href"]
            elif "linkedin.com" in href:
                result["social_linkedin"] = a["href"]

        # Owner / LLC Detection (Copyright heuristics)
        footer_text = soup.get_text()
        # Look for Copyright patterns
        # Matches: © 2023 Something LLC | Copyright 2023 Something Inc
        copyright_match = re.search(r"(?:©|Copyright)\s*(?:20\d{2})?,?\s*([A-Z][\w\s.,&]+(?:LLC|Inc|Corp|Properties|Communities|Investments|Group|Holdings|Partners))", footer_text, re.I)
        if copyright_match:
            result["owner_name"] = copyright_match.group(1).strip()
        else:
             # Fallback: Look for "Managed by X"
            managed_match = re.search(r"Managed by\s+([A-Z][\w\s.,&]+)", footer_text, re.I)
            if managed_match:
                 result["owner_name"] = managed_match.group(1).strip()

        # Utility Infrastructure Detection
        page_text_lower = page_text.lower()
        utilities = []
        
        # Positive signals (City/Municipal)
        if re.search(r'\b(city water|municipal water|public water)\b', page_text_lower):
            utilities.append("City Water")
        if re.search(r'\b(city sewer|municipal sewer|public sewer)\b', page_text_lower):
            utilities.append("City Sewer")
        
        # Negative signals (Private/Septic)
        if re.search(r'\b(septic|septic tank|septic system)\b', page_text_lower):
            utilities.append("Septic")
        if re.search(r'\b(well water|private well)\b', page_text_lower):
            utilities.append("Well Water")
        
        if utilities:
            result["utilities_status"] = " / ".join(utilities)
        else:
            result["utilities_status"] = None

        # Rent/Rates Extraction
        # Look for dollar amounts near keywords like "rent", "rate", "monthly", "daily"
        rent_patterns = [
            r'\$\s*(\d{1,4})\s*(?:/|per)?\s*(month|mo|monthly)',  # $500/month
            r'(?:lot rent|site rent|monthly rent)[\s:]*\$\s*(\d{1,4})',  # Lot Rent: $500
            r'\$\s*(\d{1,4})\s*(?:/|per)?\s*(day|daily|night)',  # $50/day
            r'(?:daily rate|nightly rate)[\s:]*\$\s*(\d{1,4})',  # Daily Rate: $50
        ]
        
        rent_info = []
        for pattern in rent_patterns:
            matches = re.findall(pattern, page_text_lower)
            if matches:
                for match in matches[:2]:  # Limit to first 2 matches
                    if isinstance(match, tuple):
                        amount, period = match
                        rent_info.append(f"${amount}/{period}")
                    else:
                        rent_info.append(f"${match}")
        
        if rent_info:
            result["rent_info"] = ", ".join(set(rent_info))  # Remove duplicates
        else:
            result["rent_info"] = None

        
    except requests.Timeout:
        result["error"] = "Timeout"
    except requests.ConnectionError:
        result["error"] = "Connection failed"
    except Exception as e:
        result["error"] = str(e)[:100]
    
    return result


def get_internal_links(html: str, base_url: str, limit: int = MAX_INTERNAL_LINKS) -> list[str]:
    """Extract internal links from HTML."""
    if not html:
        return []
    
    try:
        soup = BeautifulSoup(html, "html.parser")
        base_domain = urlparse(base_url).netloc.lower()
        
        internal_links = []
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            
            # Skip anchors and javascript
            if href.startswith("#") or href.startswith("javascript:"):
                continue
            
            # Make absolute URL
            full_url = urljoin(base_url, href)
            parsed = urlparse(full_url)
            
            # Check if same domain
            if parsed.netloc.lower() == base_domain:
                # Prefer important pages
                href_lower = href.lower()
                priority = 0
                if "contact" in href_lower:
                    priority = 3
                elif "about" in href_lower:
                    priority = 2
                elif "rate" in href_lower or "price" in href_lower:
                    priority = 1
                
                internal_links.append((priority, full_url))
        
        # Sort by priority and dedupe
        internal_links.sort(key=lambda x: -x[0])
        seen = {base_url}
        result = []
        for _, url in internal_links:
            if url not in seen:
                seen.add(url)
                result.append(url)
                if len(result) >= limit:
                    break
        
        return result
    except Exception:
        return []


def crawl_website(url: str) -> dict:
    """
    Crawl a website (homepage + internal links).
    
    Returns comprehensive crawl data for scoring.
    """
    result = {
        "url": url,
        "crawl_status": "no_website",
        "crawl_notes": "",
        "is_aggregator": False,
        "is_facebook_only": False,
        "pages": [],
        "crawled_at": datetime.now(timezone.utc).isoformat(),
    }
    
    if not url:
        return result
    
    # Check for aggregator/social
    if is_aggregator_url(url):
        result["is_aggregator"] = True
        result["crawl_status"] = "aggregator"
        result["crawl_notes"] = f"Detected aggregator/social site: {urlparse(url).netloc}"
        if is_facebook_only(url):
            result["is_facebook_only"] = True
            result["crawl_notes"] = "Facebook page only"
        return result
    
    # Crawl homepage
    homepage = fetch_page(url)
    result["pages"].append(homepage)
    
    if homepage["status"] != "success":
        result["crawl_status"] = "failed"
        result["crawl_notes"] = homepage["error"] or "Unknown error"
        return result
    
    # Crawl internal links
    internal_links = get_internal_links(homepage["html"], url)
    for link in internal_links:
        page_data = fetch_page(link)
        # Don't store full HTML for internal pages
        page_data["html"] = None
        result["pages"].append(page_data)
    
    result["crawl_status"] = "success"
    result["crawl_notes"] = f"Crawled {len(result['pages'])} page(s)"
    
    return result


def crawl_all_websites(input_path: Path = INPUT_PATH) -> list[dict]:
    """
    Crawl websites for all normalized places.
    
    Returns list of crawl results matched with place data.
    """
    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}")
        print("Run normalize_places.py first")
        sys.exit(1)
    
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    places = data.get("places", [])
    print(f"Loaded {len(places)} places to crawl")
    print("-" * 50)
    
    results = []
    
    # Use thread pool for concurrent crawling
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all crawl jobs
        future_to_place = {
            executor.submit(crawl_website, place.get("website")): place
            for place in places
        }
        
        # Collect results as they complete
        for i, future in enumerate(as_completed(future_to_place), 1):
            place = future_to_place[future]
            try:
                crawl_data = future.result()
            except Exception as e:
                crawl_data = {
                    "url": place.get("website"),
                    "crawl_status": "failed",
                    "crawl_notes": str(e)[:100],
                    "pages": [],
                }
            
            # Combine place data with crawl data
            combined = {**place, **crawl_data}
            results.append(combined)
            
            # Progress
            status = crawl_data.get("crawl_status", "unknown")
            name = place.get("name", "Unknown")[:30]
            print(f"[{i}/{len(places)}] {name}: {status}")
    
    return results


def save_crawled(results: list[dict], output_path: Path = OUTPUT_PATH) -> Path:
    """Save crawl results to JSON."""
    output_path.parent.mkdir(exist_ok=True)
    
    # Remove raw HTML to save space
    for result in results:
        for page in result.get("pages", []):
            page["html"] = None
    
    output_data = {
        "metadata": {
            "crawled_at": datetime.now(timezone.utc).isoformat(),
            "total_records": len(results),
        },
        "places": results,
    }
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"\nSaved crawl results to: {output_path}")
    return output_path


def main():
    results = crawl_all_websites()
    save_crawled(results)
    
    # Summary
    success = sum(1 for r in results if r.get("crawl_status") == "success")
    failed = sum(1 for r in results if r.get("crawl_status") == "failed")
    no_site = sum(1 for r in results if r.get("crawl_status") == "no_website")
    aggregator = sum(1 for r in results if r.get("crawl_status") == "aggregator")
    
    print(f"\nSummary:")
    print(f"  Success: {success}")
    print(f"  Failed: {failed}")
    print(f"  No website: {no_site}")
    print(f"  Aggregator: {aggregator}")


if __name__ == "__main__":
    main()
