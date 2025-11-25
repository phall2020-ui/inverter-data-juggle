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
    cur = conn.execute(
        """
        SELECT ts FROM readings
        WHERE plant_uid = ? AND emig_id = ?
        ORDER BY ts
        """,
        (plant_uid, weighted)
    )
    
    timestamps = [pd.to_datetime(row[0]) for row in cur.fetchall()]
    
    print(f"Total readings: {len(timestamps)}")
    print(f"Date range: {timestamps[0]} to {timestamps[-1]}")
    print(f"\nReadings per month:")
    
    df = pd.DataFrame({'ts': timestamps})
    df['month'] = df['ts'].dt.to_period('M')
    monthly_counts = df.groupby('month').size()
    
    for month, count in monthly_counts.items():
        # Expected: 30 days × 48 readings/day (30-min intervals) = ~1440 readings
        # Or 31 days × 48 = ~1488 readings
        expected = 48 * pd.Period(month, freq='M').days_in_month
        pct = (count / expected) * 100
        print(f"  {month}: {count} readings ({pct:.1f}% of expected {expected})")
    
finally:
    conn.close()
