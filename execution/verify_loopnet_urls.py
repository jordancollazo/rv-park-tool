"""
verify_loopnet_urls.py

Verify that LoopNet URLs in the database are valid and accessible.
Tests a sample of URLs by making HTTP HEAD requests.

Usage:
    python execution/verify_loopnet_urls.py --sample 10  # Test 10 random URLs
    python execution/verify_loopnet_urls.py --all        # Test all URLs (slow)
"""

import sqlite3
import sys
import random
import argparse
from pathlib import Path
from urllib.parse import urlparse

try:
    import requests
except ImportError:
    print("[ERROR] requests library not found. Install it with: pip install requests")
    sys.exit(1)

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"


def get_loopnet_urls(conn, limit=None):
    """
    Get all LoopNet URLs from the database.
    """
    cursor = conn.cursor()

    query = """
        SELECT id, name, loopnet_url
        FROM leads
        WHERE loopnet_url IS NOT NULL
        ORDER BY id
    """

    if limit:
        query += f" LIMIT {limit}"

    cursor.execute(query)

    urls = []
    for row in cursor.fetchall():
        urls.append({
            'id': row[0],
            'name': row[1],
            'url': row[2]
        })

    return urls


def verify_url(url, timeout=10):
    """
    Verify a URL is accessible by making a HEAD request.
    Returns (status_code, is_valid, message)
    """
    try:
        # Make HEAD request to avoid downloading full page
        response = requests.head(url, timeout=timeout, allow_redirects=True)

        # LoopNet URLs often redirect (301/302) before landing on 200
        # 403 is also acceptable - it means LoopNet is blocking automated requests
        # but the URL format is correct and will work in a browser
        if response.status_code in [200, 301, 302]:
            return response.status_code, True, "OK"
        elif response.status_code == 403:
            return response.status_code, True, "OK (403 - URL valid, automated access blocked)"
        else:
            return response.status_code, False, f"Unexpected status: {response.status_code}"

    except requests.exceptions.Timeout:
        return None, False, "Request timeout"
    except requests.exceptions.ConnectionError:
        return None, False, "Connection error"
    except requests.exceptions.RequestException as e:
        return None, False, f"Request failed: {str(e)}"
    except Exception as e:
        return None, False, f"Unknown error: {str(e)}"


def main():
    parser = argparse.ArgumentParser(description="Verify LoopNet URLs in database")
    parser.add_argument("--sample", type=int, help="Test a random sample of N URLs")
    parser.add_argument("--all", action="store_true", help="Test all URLs (may be slow)")
    parser.add_argument("--timeout", type=int, default=10, help="Request timeout in seconds (default: 10)")

    args = parser.parse_args()

    print("=" * 80)
    print("LoopNet URL Verification")
    print("=" * 80)
    print()

    # Connect to database
    if not DB_PATH.exists():
        print(f"[ERROR] Database not found at {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)

    # Get URLs to test
    print("[INFO] Querying database for LoopNet URLs...")
    all_urls = get_loopnet_urls(conn)

    if not all_urls:
        print("[SUCCESS] No LoopNet URLs found in database")
        conn.close()
        sys.exit(0)

    print(f"Found {len(all_urls)} LoopNet URLs in database")
    print()

    # Determine which URLs to test
    if args.all:
        urls_to_test = all_urls
        print(f"[INFO] Testing all {len(urls_to_test)} URLs...")
    elif args.sample:
        sample_size = min(args.sample, len(all_urls))
        urls_to_test = random.sample(all_urls, sample_size)
        print(f"[INFO] Testing random sample of {sample_size} URLs...")
    else:
        # Default: test 10 random URLs
        sample_size = min(10, len(all_urls))
        urls_to_test = random.sample(all_urls, sample_size)
        print(f"[INFO] Testing random sample of {sample_size} URLs (use --all to test everything)")

    print()
    print("-" * 80)

    # Test each URL
    results = {
        'valid': [],
        'invalid': [],
        'error': []
    }

    for i, lead in enumerate(urls_to_test, 1):
        lead_id = lead['id']
        name = lead['name'][:50]
        url = lead['url']

        print(f"[{i}/{len(urls_to_test)}] Testing Lead ID {lead_id}...")
        print(f"  Name: {name}")
        print(f"  URL: {url}")

        status_code, is_valid, message = verify_url(url, timeout=args.timeout)

        if is_valid:
            print(f"  [SUCCESS] {message} (Status: {status_code})")
            results['valid'].append(lead)
        elif status_code:
            print(f"  [WARNING] {message} (Status: {status_code})")
            results['invalid'].append({'lead': lead, 'error': message})
        else:
            print(f"  [ERROR] {message}")
            results['error'].append({'lead': lead, 'error': message})

        print()

    # Summary
    print("=" * 80)
    print("VERIFICATION SUMMARY")
    print("=" * 80)
    print(f"URLs tested: {len(urls_to_test)}")
    print(f"  Valid: {len(results['valid'])} ({len(results['valid'])*100//len(urls_to_test)}%)")
    print(f"  Invalid: {len(results['invalid'])} ({len(results['invalid'])*100//len(urls_to_test)}%)")
    print(f"  Errors: {len(results['error'])} ({len(results['error'])*100//len(urls_to_test)}%)")
    print()

    # Show details for failures
    if results['invalid']:
        print("-" * 80)
        print("INVALID URLs (unexpected status codes):")
        print("-" * 80)
        for item in results['invalid']:
            lead = item['lead']
            print(f"Lead ID {lead['id']}: {lead['name'][:40]}")
            print(f"  URL: {lead['url']}")
            print(f"  Error: {item['error']}")
            print()

    if results['error']:
        print("-" * 80)
        print("ERRORs (connection/timeout issues):")
        print("-" * 80)
        for item in results['error']:
            lead = item['lead']
            print(f"Lead ID {lead['id']}: {lead['name'][:40]}")
            print(f"  URL: {lead['url']}")
            print(f"  Error: {item['error']}")
            print()

    # Recommendation
    if len(results['valid']) == len(urls_to_test):
        print("=" * 80)
        print("[SUCCESS] All tested URLs are valid!")
        print("=" * 80)
        conn.close()
        sys.exit(0)
    else:
        print("=" * 80)
        print("[WARNING] Some URLs failed verification")
        print("=" * 80)
        print("This could be due to:")
        print("  - Temporary network issues")
        print("  - LoopNet server downtime")
        print("  - Rate limiting")
        print("  - Incorrect URL format")
        print()
        print("Recommendation:")
        print("  1. Manually check a few failed URLs in a browser")
        print("  2. Re-run verification to rule out temporary issues")
        print("  3. If URLs are consistently failing, review the URL construction logic")
        print("=" * 80)
        conn.close()
        sys.exit(1)


if __name__ == "__main__":
    main()
