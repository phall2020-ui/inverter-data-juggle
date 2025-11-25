import sqlite3
import json
from plant_store import PlantStore

store = PlantStore('plant_registry.sqlite')
plant = store.load('Blachford UK')
plant_uid = plant['plant_uid']

conn = sqlite3.connect(store.db_path)
try:
    cur = conn.execute(
        """
        SELECT ts, emig_id, payload FROM readings
        WHERE plant_uid = ? AND emig_id LIKE 'POA:%'
        LIMIT 5
        """,
        (plant_uid,)
    )
    
    print("Sample POA readings:")
    for ts, emig_id, payload_json in cur.fetchall():
        payload = json.loads(payload_json)
        print(f"\nTimestamp: {ts}")
        print(f"Device: {emig_id}")
        print(f"Payload keys: {list(payload.keys())}")
        print(f"Payload: {payload}")
        break  # Just show first one
    
finally:
    conn.close()
