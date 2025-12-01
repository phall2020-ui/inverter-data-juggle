"""
PR Analysis Summary and Recommendations
Based on comprehensive analysis of DC capacity, inverter output, and POA data
"""
import sqlite3
import pandas as pd
import json

conn = sqlite3.connect('plant_registry.sqlite')
cur = conn.cursor()

print("=" * 100)
print("PR ANALYSIS SUMMARY AND RECOMMENDATIONS")
print("=" * 100)

# Gather all data
results = []

cur.execute('SELECT alias, plant_uid, dc_size_kw FROM plants ORDER BY alias')
for row in cur.fetchall():
    alias, uid, dc_kw = row
    dc_kw = dc_kw or 0
    
    # Count inverters with data
    cur.execute('''
        SELECT COUNT(DISTINCT emig_id) FROM readings 
        WHERE plant_uid = ? AND emig_id LIKE 'INVERT:%'
    ''', (uid,))
    inv_count = cur.fetchone()[0]
    
    # Get peak power sum
    cur.execute('''
        SELECT SUM(peak) FROM (
            SELECT MAX(CAST(json_extract(payload, '$.apparentPower.value') AS REAL)) as peak
            FROM readings 
            WHERE plant_uid = ? AND emig_id LIKE 'INVERT:%'
            GROUP BY emig_id
        )
    ''', (uid,))
    peak_kw = (cur.fetchone()[0] or 0) / 1000
    
    # Check for POA data
    cur.execute('''
        SELECT COUNT(*) FROM readings 
        WHERE plant_uid = ? AND emig_id = 'POA:SOLARGIS:WEIGHTED'
    ''', (uid,))
    poa_count = cur.fetchone()[0]
    
    # Calculate ratios
    peak_ratio = peak_kw / dc_kw if dc_kw > 0 else 0
    
    # Implied DC if we assume peak should be 80% of DC
    implied_dc = peak_kw / 0.8 if peak_kw > 0 else 0
    
    results.append({
        'Plant': alias,
        'UID': uid,
        'DC_kWp': dc_kw,
        'Inverters': inv_count,
        'Peak_kW': peak_kw,
        'Peak_Ratio': peak_ratio,
        'Implied_DC': implied_dc,
        'POA_Records': poa_count,
        'DC_Adjustment': implied_dc - dc_kw
    })

df = pd.DataFrame(results)

print()
print("CURRENT STATE:")
print("-" * 100)
print(df[['Plant', 'DC_kWp', 'Inverters', 'Peak_kW', 'Peak_Ratio']].to_string(index=False))

print()
print("=" * 100)
print("ANALYSIS BY CATEGORY")
print("=" * 100)

# Category 1: Low peak ratio (possible DC overstatement)
low_ratio = df[(df['Peak_Ratio'] < 0.5) & (df['Peak_kW'] > 0)]
if len(low_ratio) > 0:
    print()
    print("⚠️ SITES WITH LOW PEAK RATIO (<50%) - Possible DC Capacity Issues:")
    print("-" * 80)
    for _, row in low_ratio.iterrows():
        print(f"  {row['Plant']}")
        print(f"    Current DC: {row['DC_kWp']:.0f} kWp")
        print(f"    Peak Power: {row['Peak_kW']:.1f} kW ({row['Peak_Ratio']:.0%} of DC)")
        print(f"    Implied DC (assuming 80% peak): {row['Implied_DC']:.0f} kWp")
        print(f"    Suggested adjustment: {row['DC_Adjustment']:+.0f} kWp")
        print()

# Category 2: High peak ratio (possible data error or DC understatement)
high_ratio = df[(df['Peak_Ratio'] > 1.0) & (df['Peak_kW'] > 0)]
if len(high_ratio) > 0:
    print()
    print("⚠️ SITES WITH HIGH PEAK RATIO (>100%) - Possible Data Issues:")
    print("-" * 80)
    for _, row in high_ratio.iterrows():
        print(f"  {row['Plant']}")
        print(f"    Current DC: {row['DC_kWp']:.0f} kWp")
        print(f"    Peak Power: {row['Peak_kW']:.1f} kW ({row['Peak_Ratio']:.0%} of DC)")
        print()

# Category 3: No inverter data
no_inv = df[df['Inverters'] == 0]
if len(no_inv) > 0:
    print()
    print("⚠️ SITES WITH NO INVERTER DATA:")
    print("-" * 80)
    for _, row in no_inv.iterrows():
        print(f"  {row['Plant']} ({row['UID']})")

# Category 4: No POA data
no_poa = df[df['POA_Records'] == 0]
if len(no_poa) > 0:
    print()
    print("⚠️ SITES WITH NO POA DATA:")
    print("-" * 80)
    for _, row in no_poa.iterrows():
        print(f"  {row['Plant']} ({row['UID']})")

# Category 5: Good ratio sites
good_ratio = df[(df['Peak_Ratio'] >= 0.5) & (df['Peak_Ratio'] <= 1.0) & (df['Peak_kW'] > 0)]
if len(good_ratio) > 0:
    print()
    print("✅ SITES WITH REASONABLE PEAK RATIO (50-100%):")
    print("-" * 80)
    for _, row in good_ratio.iterrows():
        print(f"  {row['Plant']}: {row['Peak_kW']:.1f} kW / {row['DC_kWp']:.0f} kWp = {row['Peak_Ratio']:.0%}")

print()
print("=" * 100)
print("RECOMMENDATIONS")
print("=" * 100)
print("""
1. DC CAPACITY VERIFICATION NEEDED:
   Most sites show peak power at only 20-35% of registered DC capacity.
   This is abnormally low and suggests:
   a) DC capacity values may be AC export capacity, not DC peak
   b) Some inverters are not being monitored
   c) Significant clipping/limiting is occurring

2. SUGGESTED ACTIONS:
   a) Review plant documentation to confirm actual DC capacity
   b) Check if any inverters are not included in monitoring
   c) Consider using 'Implied DC' values (based on peak power / 0.8)
   
3. FOR PR CALCULATIONS:
   Option A: Use current DC values (will show low PRs ~25-35%)
   Option B: Use sum of inverter peaks as capacity reference
   Option C: Update DC values based on actual site specifications

4. SPECIFIC ISSUES:
   - Newfold Farm: Peak power shows 1.3 million kW - data corruption
   - FloPlast/Sheldons Bakery: No inverter power data available
   - Multiple sites missing POA data - need SolarGIS import
""")

# Generate updated CSV with implied DC values
print()
print("=" * 100)
print("UPDATED CSV EXPORTED: dc_capacity_implied.csv")
print("=" * 100)

df_export = df[['UID', 'Plant', 'DC_kWp', 'Peak_kW', 'Peak_Ratio', 'Implied_DC', 'Inverters']].copy()
df_export.columns = ['plant_uid', 'alias', 'current_dc_kw', 'peak_power_kw', 'peak_ratio', 'implied_dc_kw', 'inverter_count']
df_export['suggested_dc_kw'] = df_export['implied_dc_kw'].round(0)
df_export.to_csv('dc_capacity_implied.csv', index=False)
print("Saved to dc_capacity_implied.csv")

conn.close()
