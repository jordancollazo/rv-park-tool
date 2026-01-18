
import os
import subprocess
import sys

SCRAPER_DIR = os.path.join(os.path.dirname(__file__), 'scrapers')
FETCH_SCRIPT = os.path.join(SCRAPER_DIR, 'fetch_landwatch.py')
PROCESS_SCRIPT = os.path.join(SCRAPER_DIR, 'process_landwatch.py')

def run_pipeline():
    print("=== Starting Landwatch Data Pipeline ===")
    
    # Step 1: Fetch
    print(f"\n[1/2] Running Fetch Script: {FETCH_SCRIPT}")
    try:
        subprocess.check_call([sys.executable, FETCH_SCRIPT])
    except subprocess.CalledProcessError as e:
        print(f"Fetch failed with error code {e.returncode}. Stopping pipeline.")
        return

    # Step 2: Process
    print(f"\n[2/2] Running Process Script: {PROCESS_SCRIPT}")
    try:
        subprocess.check_call([sys.executable, PROCESS_SCRIPT])
    except subprocess.CalledProcessError as e:
        print(f"Processing failed with error code {e.returncode}.")
        return

    print("\n=== Pipeline Completed Successfully ===")

if __name__ == "__main__":
    run_pipeline()
