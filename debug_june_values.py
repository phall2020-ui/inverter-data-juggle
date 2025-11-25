import sqlite3
import json
from plant_store import PlantStore
import pandas as pd

store = PlantStore('plant_registry.sqlite')
plant = store.load('Blachford UK')
plant_uid = plant['plant_uid']

# Get both weighted and one orientation device
device_ids = store.list_emig_ids(plant_uid)
weighted = [d for d in device_ids if 'WEIGHTED' in d][0]
orientation = [d for d in device_ids if 'AZ167:SL6' in d][0]

conn = sqlite3.connect(store.db_path)
try:
    print("Checking POA values for June 2025...")
    print("\nWeighted POA (first 10 daytime readings):")
    
    cur = conn.execute(
        """
        SELECT ts, payload FROM readings
        WHERE plant_uid = ? AND emig_id = ?
        AND ts BETWEEN '2025-06-01T10:00:00' AND '2025-06-01T16:00:00'
        ORDER BY ts
        LIMIT 10
        """,
        (plant_uid, weighted)
    )
    
    for ts, payload_json in cur.fetchall():
        payload = json.loads(payload_json)
        poa_irr = payload.get('poaIrradiance', {})
        poa = poa_irr.get('value', 0)
        print(f"  {ts}: {poa:.2f} W/m²")
    
    # Calculate June total with detailed breakdown
    print("\n\nDetailed June 2025 calculation:")
    cur = conn.execute(
        """
        SELECT ts, payload FROM readings
        WHERE plant_uid = ? AND emig_id = ?
        AND ts BETWEEN '2025-06-01' AND '2025-06-30T23:59:59'
        ORDER BY ts
        """,
        (plant_uid, weighted)
    )
    
    total_kwh = 0
    day_totals = {}
    
    for ts, payload_json in cur.fetchall():
        payload = json.loads(payload_json)
        poa_irr = payload.get('poaIrradiance', {})
        poa_w = poa_irr.get('value', 0)  # W/m²
        
        # Convert to kWh/m²: W/m² × 0.5 hours / 1000
        poa_kwh = poa_w * 0.5 / 1000.0
        total_kwh += poa_kwh
        
        # Track daily totals
        date = ts[:10]
        if date not in day_totals:
            day_totals[date] = 0
        day_totals[date] += poa_kwh
    
    print(f"\nFirst 5 days:")
    for i, (date, daily_total) in enumerate(sorted(day_totals.items())[:5]):
        print(f"  {date}: {daily_total:.3f} kWh/m²")
    
    print(f"\nJune 2025 Monthly Total: {total_kwh:.3f} kWh/m²")
    print(f"Expected from reference: ~183.5 kWh/m² (if this is Cromwell)")
    print(f"Expected from reference: ~62.3 kWh/m² (if this is Blachford)")
    
    # Check orientation-specific
    print("\n\nChecking orientation AZ167:SL6 (10 daytime readings):")
    cur = conn.execute(
        """
        SELECT ts, payload FROM readings
        WHERE plant_uid = ? AND emig_id = ?
        AND ts BETWEEN '2025-06-01T10:00:00' AND '2025-06-01T16:00:00'
        ORDER BY ts
        LIMIT 10
        """,
        (plant_uid, orientation)
    )
    
    for ts, payload_json in cur.fetchall():
        payload = json.loads(payload_json)
        poa_irr = payload.get('poaIrradiance', {})
        poa = poa_irr.get('value', 0)
        print(f"  {ts}: {poa:.2f} W/m²")
    
finally:
    conn.close()
