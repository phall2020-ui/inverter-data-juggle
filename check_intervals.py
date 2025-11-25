import sqlite3
import json
from plant_store import PlantStore
import pandas as pd

store = PlantStore('plant_registry.sqlite')
plant = store.load('Blachford UK')
plant_uid = plant['plant_uid']

# Get weighted POA device
device_ids = store.list_emig_ids(plant_uid)
weighted = [d for d in device_ids if 'WEIGHTED' in d][0]

conn = sqlite3.connect(store.db_path)
try:
    # Check the time intervals in the database
    cur = conn.execute(
        """
        SELECT ts FROM readings
        WHERE plant_uid = ? AND emig_id = ?
        ORDER BY ts
        LIMIT 20
        """,
        (plant_uid, weighted)
    )
    
    timestamps = [row[0] for row in cur.fetchall()]
    
    print("First 20 timestamps in database:")
    for i, ts in enumerate(timestamps):
        if i > 0:
            prev_ts = pd.to_datetime(timestamps[i-1])
            curr_ts = pd.to_datetime(ts)
            diff = (curr_ts - prev_ts).total_seconds() / 60
            print(f"  {ts} (interval: {diff:.0f} min)")
        else:
            print(f"  {ts}")
    
    # Check one orientation device to see original CSV interval
    orientation = [d for d in device_ids if 'AZ167:SL6' in d][0]
    
    cur = conn.execute(
        """
        SELECT ts FROM readings
        WHERE plant_uid = ? AND emig_id = ?
        ORDER BY ts
        LIMIT 20
        """,
        (plant_uid, orientation)
    )
    
    timestamps = [row[0] for row in cur.fetchall()]
    
    print(f"\n\nOrientation {orientation} timestamps:")
    for i, ts in enumerate(timestamps[:10]):
        if i > 0:
            prev_ts = pd.to_datetime(timestamps[i-1])
            curr_ts = pd.to_datetime(ts)
            diff = (curr_ts - prev_ts).total_seconds() / 60
            print(f"  {ts} (interval: {diff:.0f} min)")
        else:
            print(f"  {ts}")
    
    # Calculate correct daily total for June 1
    print("\n\nJune 1st calculation:")
    cur = conn.execute(
        """
        SELECT ts, payload FROM readings
        WHERE plant_uid = ? AND emig_id = ?
        AND ts BETWEEN '2025-06-01T00:00:00' AND '2025-06-01T23:59:59'
        ORDER BY ts
        """,
        (plant_uid, weighted)
    )
    
    rows = cur.fetchall()
    print(f"Number of readings on June 1st: {len(rows)}")
    print(f"Expected for 30-min intervals: 48")
    print(f"Expected for 15-min intervals: 96")
    
    total_wrong = 0  # Using 0.5 hours
    total_correct_30min = 0  # If truly 30-min
    total_correct_15min = 0  # If truly 15-min
    
    for ts, payload_json in rows:
        payload = json.loads(payload_json)
        poa_irr = payload.get('poaIrradiance', {})
        poa_w = poa_irr.get('value', 0)
        
        total_wrong += poa_w * 0.5 / 1000.0
        total_correct_30min += poa_w * 0.5 / 1000.0
        total_correct_15min += poa_w * 0.25 / 1000.0  # 15 min = 0.25 hours
    
    print(f"\nJune 1st totals:")
    print(f"  Current calculation (0.5 hrs): {total_wrong:.3f} kWh/m²")
    print(f"  If 30-min intervals (0.5 hrs): {total_correct_30min:.3f} kWh/m²")
    print(f"  If 15-min intervals (0.25 hrs): {total_correct_15min:.3f} kWh/m²")
    
finally:
    conn.close()
