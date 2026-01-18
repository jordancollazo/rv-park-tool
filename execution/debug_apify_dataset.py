
import os
from apify_client import ApifyClient
from dotenv import load_dotenv

load_dotenv()

client = ApifyClient(os.getenv("APIFY_API_TOKEN"))

dataset_id = "hI1grTphDL8L8e8YH4" # From logs

items = client.dataset(dataset_id).list_items(limit=1).items
if items:
    print("Keys found in first item:")
    print(list(items[0].keys()))
    print("Sample Item:", items[0])
else:
    print("No items found.")
