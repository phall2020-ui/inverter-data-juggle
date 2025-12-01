"""
Deep dive into timestamp matching and PR calculation issues
"""
import sqlite3
import pandas as pd
import numpy as np

conn = sqlite3.connect('plant_registry.sqlite')

print("DEEP DIVE: Timestamp Matching Analysis")
print("=" * 80)

# Get raw POA data for one day
poa_df = pd.read_sql_query("""
    SELECT ts, json_extract(payload, '$.poaIrradiance.value') as poa
    FROM readings 
    WHERE plant_uid = 'AMP:00001' 
    AND emig_id = 'POA:SOLARGIS:WEIGHTED'
    AND ts LIKE '2025-06-21%'
""", conn)
poa_df['ts'] = pd.to_datetime(poa_df['ts'])
print(f"POA raw ts dtype: {poa_df['ts'].dtype}")
print(f"POA has timezone: {poa_df['ts'].dt.tz}")
print(f"POA sample: {poa_df['ts'].iloc[0]}")

# Get raw inverter data
inv_df = pd.read_sql_query("""
    SELECT ts, SUM(CAST(json_extract(payload, '$.apparentPower.value') AS REAL)) as power
    FROM readings 
    WHERE plant_uid = 'AMP:00001' 
    AND emig_id LIKE 'INVERT:%'
    AND ts LIKE '2025-06-21%'
    GROUP BY ts
""", conn)
inv_df['ts'] = pd.to_datetime(inv_df['ts'])
print(f"Inv raw ts dtype: {inv_df['ts'].dtype}")
print(f"Inv has timezone: {inv_df['ts'].dt.tz}")
print(f"Inv sample: {inv_df['ts'].iloc[0]}")

# Check if timestamps can match
print()
print("Direct comparison:")
poa_times = set(poa_df['ts'])
inv_times = set(inv_df['ts'])
common = poa_times & inv_times
print(f"POA unique times: {len(poa_times)}")
print(f"Inv unique times: {len(inv_times)}")
print(f"Common times (before TZ removal): {len(common)}")

# Try removing timezone
inv_df['ts_naive'] = inv_df['ts'].dt.tz_localize(None)
inv_times_naive = set(inv_df['ts_naive'])
common_naive = poa_times & inv_times_naive
print(f"Common after removing TZ: {len(common_naive)}")

# Sample the matched data
if common_naive:
    print()
    print("Sample matched timestamps:")
    for ts in sorted(list(common_naive))[:10]:
        poa_val = poa_df[poa_df['ts'] == ts]['poa'].values[0]
        inv_val = inv_df[inv_df['ts_naive'] == ts]['power'].values[0]
        poa_wm2 = float(poa_val) * 2000
        power_kw = inv_val / 1000
        dc_kw = 999.8
        pr = power_kw / (dc_kw * poa_wm2 / 1000) if poa_wm2 > 0 else 0
        print(f"  {ts}: POA={poa_wm2:.0f} W/m2, Power={power_kw:.1f} kW, PR={pr:.0%}")

# Now check if the issue is with multiple POA records
print()
print("=" * 80)
print("CHECKING FOR DUPLICATE/MULTIPLE POA RECORDS")
print("=" * 80)

# Count POA records per plant_uid per timestamp
poa_counts = pd.read_sql_query("""
    SELECT plant_uid, ts, COUNT(*) as cnt
    FROM readings 
    WHERE emig_id = 'POA:SOLARGIS:WEIGHTED'
    GROUP BY plant_uid, ts
    HAVING cnt > 1
    LIMIT 20
""", conn)
print(f"Timestamps with multiple POA records: {len(poa_counts)}")
if len(poa_counts) > 0:
    print(poa_counts.head(10))

# Check which plants have POA data
print()
print("Plants with POA WEIGHTED data:")
poa_plants = pd.read_sql_query("""
    SELECT DISTINCT plant_uid FROM readings 
    WHERE emig_id = 'POA:SOLARGIS:WEIGHTED'
""", conn)
print(poa_plants)

# Now recalculate PR with proper timestamp handling
print()
print("=" * 80)
print("RECALCULATING PR WITH PROPER TIMESTAMP HANDLING")
print("=" * 80)

# For Cromwell Tools - properly calculate daily PR
print()
print("Cromwell Tools (AMP:00001) - Daily PR for June 2025:")
print("-" * 60)

daily_results = []

for day in range(1, 31):
    date_str = f"2025-06-{day:02d}"
    
    # Get POA sum for day
    poa_query = pd.read_sql_query(f"""
        SELECT SUM(CAST(json_extract(payload, '$.poaIrradiance.value') AS REAL)) as poa_sum
        FROM readings 
        WHERE plant_uid = 'AMP:00001' 
        AND emig_id = 'POA:SOLARGIS:WEIGHTED'
        AND ts LIKE '{date_str}%'
    """, conn)
    poa_sum_kwhm2 = poa_query['poa_sum'].iloc[0] or 0
    
    # Get power sum for day (already in Wh since each reading is 0.5h * power_W)
    # Actually apparentPower is instantaneous W, so sum of HH readings / 2 = kWh
    inv_query = pd.read_sql_query(f"""
        SELECT SUM(CAST(json_extract(payload, '$.apparentPower.value') AS REAL)) as power_sum,
               COUNT(*) as readings
        FROM readings 
        WHERE plant_uid = 'AMP:00001' 
        AND emig_id LIKE 'INVERT:%'
        AND ts LIKE '{date_str}%'
        AND json_extract(payload, '$.apparentPower.value') IS NOT NULL
    """, conn)
    power_sum_w = inv_query['power_sum'].iloc[0] or 0
    readings = inv_query['readings'].iloc[0] or 0
    
    # Energy = power * time = W * 0.5h = Wh * 0.5
    # But we're summing all inverters at all timestamps
    # So energy_kwh = sum(power_w) * 0.5 / 1000
    energy_kwh = power_sum_w * 0.5 / 1000
    
    # POA energy = sum(kWh/m2 per HH) = kWh/m2 for day
    poa_kwhm2 = poa_sum_kwhm2
    
    # Expected energy = DC * POA_kWh/m2 (reference is 1 kWh/m2 per kWp)
    dc_kw = 999.8
    expected_kwh = dc_kw * poa_kwhm2
    
    if expected_kwh > 0 and poa_kwhm2 > 0.5:  # At least 0.5 kWh/m2
        pr = energy_kwh / expected_kwh
        daily_results.append({'date': date_str, 'energy_kwh': energy_kwh, 
                             'poa_kwhm2': poa_kwhm2, 'expected_kwh': expected_kwh, 
                             'pr': pr, 'readings': readings})
        print(f"  {date_str}: Energy={energy_kwh:.1f} kWh, POA={poa_kwhm2:.2f} kWh/m2, Expected={expected_kwh:.1f} kWh, PR={pr:.0%}")

if daily_results:
    avg_pr = np.mean([r['pr'] for r in daily_results])
    print(f"\nAverage daily PR: {avg_pr:.0%}")

conn.close()
