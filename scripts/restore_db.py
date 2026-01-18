
import shutil
import os

def restore():
    src = "data/leads.db.bak"
    dst = "data/leads.db"
    if not os.path.exists(src):
        print(f"Backup {src} not found!")
        return
    
    try:
        shutil.copy2(src, dst)
        print(f"Restored {src} to {dst}")
    except Exception as e:
        print(f"Restore failed: {e}")

if __name__ == "__main__":
    restore()
