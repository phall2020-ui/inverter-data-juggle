import pandas as pd
import sqlite3
import json
from plant_store import PlantStore

# Read CSV directly and calculate
csv_path = r"C:\Users\PeterHall\OneDrive - AMPYR IDEA UK Ltd\Monthly Excom\Monthly SolarGIS data\June 2025\Blachford.csv"

print("="*80)
print("COMPARING CSV vs DATABASE")
print("="*80)

# Read CSV
df = pd.read_csv(csv_path)
print(f"\nCSV file: {csv_path}")
print(f"Total rows: {len(df)}")
print(f"Columns: {list(df.columns)}")

# Parse time column
df['time'] = pd.to_datetime(df['time'])
df = df.sort_values('time')

# Group by orientation
orientations = df.groupby(['azimuth', 'slope'])

print(f"\nOrientations found: {len(orientations)}")

for (azimuth, slope), group in orientations:
    print(f"\n  Azimuth={azimuth}°, Slope={slope}°:")
    print(f"    Capacity: {group['array_capacity'].iloc[0]} kW")
    print(f"    Readings: {len(group)}")
    print(f"    Date range: {group['time'].min()} to {group['time'].max()}")
    
    # Calculate June total from CSV (15-min data)
    gti_values = group['gti'].values
    # Each 15-min reading represents 0.25 hours
    june_total_csv = sum(gti_values * 0.25 / 1000.0)
    print(f"    June total (CSV 15-min): {june_total_csv:.3f} kWh/m²")
    
    # Now resample to 30-min like the import does
    group_indexed = group.set_index('time')
    gti_resampled = group_indexed['gti'].resample('30min').mean()
    
    # Calculate using resampled data
    june_total_resampled = sum(gti_resampled.values * 0.5 / 1000.0)
    print(f"    June total (30-min resampled): {june_total_resampled:.3f} kWh/m²")
    
    # Check database
    store = PlantStore('plant_registry.sqlite')
    plant = store.load('Blachford UK')
    plant_uid = plant['plant_uid']
    
    emig_id = f"POA:SOLARGIS:AZ{int(azimuth)}:SL{int(slope)}"
    
    conn = sqlite3.connect(store.db_path)
    try:
        cur = conn.execute(
            """
            SELECT ts, payload FROM readings
            WHERE plant_uid = ? AND emig_id = ?
            AND ts BETWEEN '2025-06-01' AND '2025-06-30T23:59:59'
            ORDER BY ts
            """,
            (plant_uid, emig_id)
        )
        
        db_total = 0
        db_count = 0
        for ts, payload_json in cur.fetchall():
            payload = json.loads(payload_json)
            poa_irr = payload.get('poaIrradiance', {})
            poa_w = poa_irr.get('value', 0)
            db_total += poa_w * 0.5 / 1000.0
            db_count += 1
        
        print(f"    Database readings: {db_count}")
        print(f"    June total (Database): {db_total:.3f} kWh/m²")
        
        if abs(db_total - june_total_resampled) > 0.01:
            print(f"    ⚠️  MISMATCH! Difference: {abs(db_total - june_total_resampled):.3f}")
        else:
            print(f"    ✓ Database matches resampled CSV")
            
    finally:
        conn.close()

# Calculate capacity-weighted total
print("\n" + "="*80)
print("CAPACITY-WEIGHTED CALCULATION")
print("="*80)

total_capacity = 0
weighted_sum = 0

for (azimuth, slope), group in orientations:
    capacity = group['array_capacity'].iloc[0]
    gti_values = group['gti'].values
    june_total = sum(gti_values * 0.25 / 1000.0)
    
    total_capacity += capacity
    weighted_sum += june_total * capacity
    print(f"  {capacity:.1f} kW × {june_total:.3f} kWh/m² = {june_total * capacity:.3f}")

if total_capacity > 0:
    weighted_avg = weighted_sum / total_capacity
    print(f"\nWeighted average: {weighted_sum:.3f} / {total_capacity:.1f} = {weighted_avg:.3f} kWh/m²")
    print(f"\nExpected from reference: ~62.3 kWh/m²")
    print(f"Calculated from CSV: {weighted_avg:.3f} kWh/m²")
    print(f"Difference: {abs(62.3 - weighted_avg):.3f} kWh/m² ({abs(62.3 - weighted_avg)/62.3*100:.1f}%)")
