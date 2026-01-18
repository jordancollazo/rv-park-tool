from db import get_db

# Update to a simpler, more common LLC name for testing
with get_db() as conn:
    # Use a simpler LLC that's definitely in Sunbiz
    test_llc = "Equity LifeStyle Properties Inc"  # Major MHP operator, definitely in FL registry
    
    conn.execute("""
        UPDATE leads 
        SET owner_name = ?, registered_agent_name = NULL, registered_agent_address = NULL
        WHERE id = 1
    """, (test_llc,))
    
    conn.commit()
    
    print(f"✓ Updated lead ID 1")
    print(f"  Set owner_name to: {test_llc}")
