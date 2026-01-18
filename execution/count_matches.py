
try:
    with open("apify_runs_acct2.txt", "r", encoding="utf-16") as f:
        content = f.read()
        count = content.count("FOUND MATCH")
        print(f"Matches found: {count}")
        lines = content.splitlines()
        dataset_ids = set()
        for line in lines:
            if "Dataset ID:" in line:
                # Format: Dataset ID: pC0h9tZABa6gIr5kd
                parts = line.split(":")
                if len(parts) > 1:
                    dataset_ids.add(parts[1].strip())
        
        print("\n--- Unique Datasets Found ---")
        for ds_id in dataset_ids:
            print(ds_id)
except Exception as e:
    print(f"Error: {e}")
