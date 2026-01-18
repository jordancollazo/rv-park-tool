
import sqlite3

def update_dummy_lead():
    try:
        conn = sqlite3.connect('data/leads.db')
        # Update ID 2 (Test Park)
        conn.execute("""
            UPDATE leads 
            SET latitude = 27.7, longitude = -81.5, status = 'not_contacted', city='Test City', state='FL'
            WHERE id = 2
        """)
        conn.commit()
        print("Updated dummy lead coordinates.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    update_dummy_lead()
