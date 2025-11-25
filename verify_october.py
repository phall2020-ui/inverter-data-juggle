"""Verify POA import results against CSV for October"""
import os
import pandas as pd
import sqlite3
import json

def verify_plant_october(plant_name, plant_uid, csv_filename):
    """Compare database value with direct CSV calculation"""
    
    # Read CSV directly
    csv_path = os.path.expanduser(
        f"~/OneDrive - AMPYR IDEA UK Ltd/Monthly Excom/Monthly SolarGIS data/October 2025/{csv_filename}"
    )
    
    if not os.path.exists(csv_path):
        return None, None, f"CSV not found: {csv_filename}"
    
    df = pd.read_csv(csv_path)
    
    # Group by unique arrays
    df['ArrayKey'] = (
        df['name'].astype(str).str.strip() + "|" +
        df['array_capacity'].astype(str) + "|" +
        df['azimuth'].astype(str).str.strip() + "|" +
        df['slope'].astype(str).str.strip()
    )
    
    # Sum GTI per array
    array_sums = df.groupby('ArrayKey').agg({
        'gti': 'sum',
        'array_capacity': 'first'
    })
    
    # Calculate weighted average
    total_cap = array_sums['array_capacity'].sum()
    weighted_sum = (array_sums['gti'] * array_sums['array_capacity']).sum()
    csv_value = weighted_sum / total_cap
    
    # Get database value
    conn = sqlite3.connect('plant_registry.sqlite')
    cur = conn.cursor()
    cur.execute("""
        SELECT ts, payload FROM readings
        WHERE plant_uid = ?
          AND emig_id = 'POA:SOLARGIS:WEIGHTED'
          AND ts >= '2025-10-01'
          AND ts < '2025-11-01'
    """, (plant_uid,))
    
    db_value = sum(json.loads(p).get('poaIrradiance', {}).get('value', 0) 
                   for _, p in cur.fetchall())
    conn.close()
    
    return csv_value, db_value, None

# Test plants
test_cases = [
    ('Blachford UK', 'AMP:00024', 'Blachford.csv', 45.104189),
    ('Cromwell Tools', 'AMP:00001', 'Cromwell_Tools.csv', 41.5345),
    ('Man City', 'AMP:00019', 'City_Football_Group_Phase_1.csv', 37.737),
    ('Finlay', 'AMP:00031', 'Finlay_Beverages.csv', 44.242),
    ('Metrocentre', 'AMP:00027', 'Metro_Centre.csv', 47.667),
]

print("October POA Verification: CSV vs Database vs Reference")
print("="*80)

for name, uid, csv_file, ref in test_cases:
    csv_val, db_val, error = verify_plant_october(name, uid, csv_file)
    
    if error:
        print(f"{name:20s}: {error}")
    else:
        csv_db_diff = ((db_val - csv_val) / csv_val * 100) if csv_val > 0 else 0
        csv_ref_diff = ((csv_val - ref) / ref * 100) if ref > 0 else 0
        
        status = "PASS" if abs(csv_db_diff) < 0.5 else "FAIL"
        print(f"{name:20s}: CSV={csv_val:6.2f}, DB={db_val:6.2f}, Ref={ref:6.2f}")
        print(f"{'':20s}  DB vs CSV: {csv_db_diff:+5.1f}%, CSV vs Ref: {csv_ref_diff:+5.1f}% [{status}]")
