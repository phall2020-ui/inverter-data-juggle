import sqlite3
import json
import pandas as pd
from plant_store import PlantStore

store = PlantStore('plant_registry.sqlite')

# Test with Blachford
plant = store.load('Blachford UK')
plant_uid = plant['plant_uid']

# Get weighted POA device
device_ids = store.list_emig_ids(plant_uid)
print(f"\nAll POA devices:")
for dev in device_ids:
    if 'POA' in dev:
        print(f"  - {dev}")

weighted = [d for d in device_ids if 'WEIGHTED' in d][0]
orientation = [d for d in device_ids if 'POA:SOLARGIS:AZ' in d][0]

print(f"\nChecking WEIGHTED device: {weighted}")
print(f"Checking orientation device: {orientation}")

# Query first 10 readings
conn = sqlite3.connect(store.db_path)
try:
    # Check both devices
    for device in [weighted, orientation]:
        print(f"\n{'='*60}")
        print(f"Device: {device}")
        print('='*60)
        
        # Get some daytime readings
        cur = conn.execute(
            """
            SELECT ts, payload FROM readings
            WHERE plant_uid = ? AND emig_id = ?
            AND ts LIKE '%T12:%'
            ORDER BY ts
            LIMIT 5
            """,
            (plant_uid, device)
        )
        rows = cur.fetchall()
        
        print(f"Sample daytime (12:xx) readings:")
        for ts, payload_json in rows:
            payload = json.loads(payload_json)
            print(f"  {ts}: POA={payload.get('poa', 0):.1f} W/m²")
    
finally:
    conn.close()
