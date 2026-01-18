from db import get_db
import json

rows = list(get_db().execute('''
    SELECT id, name, owner_name 
    FROM leads 
    WHERE owner_name IS NOT NULL 
    AND owner_name != "" 
    AND (registered_agent_name IS NULL OR registered_agent_name = "")
    LIMIT 5
''').fetchall())

print(json.dumps([dict(r) for r in rows], indent=2))
