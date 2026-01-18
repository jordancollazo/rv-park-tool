from db import get_db

# Add a test LLC name to the first lead
with get_db() as conn:
    # Get first lead
    lead = conn.execute("SELECT id, name FROM leads LIMIT 1").fetchone()
    
    if lead:
        # Use a real Florida LLC for testing: "Sun Communities Operating Limited Partnership"
        # This is a major MHP operator in Florida
        test_llc = "Sun Communities Operating Limited Partnership"
        
        conn.execute("""
            UPDATE leads 
            SET owner_name = ?, registered_agent_name = NULL, registered_agent_address = NULL
            WHERE id = ?
        """, (test_llc, lead['id']))
        
        conn.commit()
        
        print(f"✓ Updated lead '{lead['name']}' (ID: {lead['id']})")
        print(f"  Set owner_name to: {test_llc}")
        print(f"  Cleared registered_agent fields for testing")
    else:
        print("No leads found in database")
