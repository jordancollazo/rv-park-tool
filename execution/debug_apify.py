
import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("APIFY_API_TOKEN")

def check_hello_world():
    print("Checking access to 'apify/hello-world'...")
    run_url = f"https://api.apify.com/v2/acts/apify~hello-world/runs?token={TOKEN}"
    try:
        # Start run
        resp = requests.post(run_url)
        if resp.status_code == 201:
            print("Run check: SUCCESS (201 Created)")
            data = resp.json()['data']
            print(f"Run ID: {data['id']}")
        else:
             print(f"Run check: {resp.status_code} {resp.reason}")
             print(resp.text)
    except Exception as e:
        print(f"Run check error: {e}")

if __name__ == "__main__":
    check_hello_world()
