import sqlite3
import json
from plant_store import PlantStore

store = PlantStore('plant_registry.sqlite')
plant = store.load('Blachford UK')
plant_uid = plant['plant_uid']

conn = sqlite3.connect(store.db_path)
try:
    # Find any non-zero POA values
    cur = conn.execute(
        """
        SELECT ts, emig_id, payload FROM readings
        WHERE plant_uid = ? AND emig_id LIKE 'POA:%'
        """,
        (plant_uid,)
    )
    
    non_zero_count = 0
    zero_count = 0
    max_poa = 0
    max_ts = None
    max_device = None
    
    for ts, emig_id, payload_json in cur.fetchall():
        payload = json.loads(payload_json)
        poa = payload.get('poa', 0)
        
        if poa > 0:
            non_zero_count += 1
            if poa > max_poa:
                max_poa = poa
                max_ts = ts
                max_device = emig_id
        else:
            zero_count += 1
    
    print(f"Total POA readings: {non_zero_count + zero_count}")
    print(f"Zero values: {zero_count}")
    print(f"Non-zero values: {non_zero_count}")
    
    if non_zero_count > 0:
        print(f"\nMax POA: {max_poa:.1f} W/m²")
        print(f"  At: {max_ts}")
        print(f"  Device: {max_device}")
        
        # Show a sample of non-zero values
        cur = conn.execute(
            """
            SELECT ts, emig_id, payload FROM readings
            WHERE plant_uid = ? AND emig_id = ?
            ORDER BY ts
            LIMIT 2000
            """,
            (plant_uid, max_device)
        )
        
        print(f"\nSample readings from {max_device}:")
        count = 0
        for ts, emig_id, payload_json in cur.fetchall():
            payload = json.loads(payload_json)
            poa = payload.get('poa', 0)
            if poa > 0 and count < 10:
                print(f"  {ts}: {poa:.1f} W/m²")
                count += 1
    
finally:
    conn.close()
