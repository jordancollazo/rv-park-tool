import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
project_root = Path(__file__).parent.parent
env_path = project_root / '.env'
load_dotenv(env_path)

print(f"Loading .env from: {env_path}")
print(f".env exists: {env_path.exists()}")
print(f"APIFY_API_TOKEN loaded: {'Yes' if os.getenv('APIFY_API_TOKEN') else 'No'}")
print(f"API Token (first 20 chars): {os.getenv('APIFY_API_TOKEN')[:20] if os.getenv('APIFY_API_TOKEN') else 'None'}")
print("=" * 80)

from financial_calc_webscrape import fetch_with_apify_loopnet

url = 'https://www.loopnet.com/Listing/465-Chaffee-Rd-N-Jacksonville-FL/38774729/'
print(f'Testing Apify LoopNet scraper...')
print(f'URL: {url}')
print('=' * 80)

try:
    result = fetch_with_apify_loopnet(url)
    print('SUCCESS! Extracted data:')
    print('=' * 80)
    print(result[:2000])  # Print first 2000 chars
    print('=' * 80)
    print(f'Total length: {len(result)} characters')
except Exception as e:
    print(f'ERROR: {e}')
    import traceback
    traceback.print_exc()
