"""
Check DC Capacity vs Sum of Inverter Peaks for all sites
This helps validate whether the registered DC capacity matches the actual inverter data
"""
import sqlite3
import json

conn = sqlite3.connect('plant_registry.sqlite')
cur = conn.cursor()

print("=" * 90)
print("DC CAPACITY VS ACTUAL INVERTER OUTPUT ANALYSIS")
print("=" * 90)
print()
print(f"{'Plant':<35} {'DC_kW':>10} {'Peak_kW':>12} {'Ratio':>8} {'Inverters':>10}")
print("-" * 90)

cur.execute('SELECT alias, plant_uid, dc_size_kw FROM plants ORDER BY alias')
plants = cur.fetchall()

results = []
for row in plants:
    alias, uid, dc_kw = row
    dc_kw = dc_kw or 0
    
    # Get sum of inverter peaks
    cur.execute("""
        SELECT COUNT(DISTINCT emig_id),
               SUM(peak) FROM (
            SELECT emig_id, MAX(CAST(json_extract(payload, '$.apparentPower.value') AS REAL)) as peak
            FROM readings 
            WHERE plant_uid = ? AND emig_id LIKE 'INVERT:%'
            GROUP BY emig_id
        )
    """, (uid,))
    inv_data = cur.fetchone()
    inv_count = inv_data[0] or 0
    inv_peak = (inv_data[1] or 0) / 1000
    
    ratio = (inv_peak / dc_kw * 100) if dc_kw > 0 else 0
    
    print(f"{alias[:34]:<35} {dc_kw:>10.1f} {inv_peak:>12.1f} {ratio:>7.0f}% {inv_count:>10}")
    results.append({'alias': alias, 'dc_kw': dc_kw, 'inv_peak': inv_peak, 'ratio': ratio})

print("-" * 90)
print()

# Summary
ratios = [r['ratio'] for r in results if r['dc_kw'] > 0 and r['inv_peak'] > 0]
if ratios:
    print(f"Average ratio (peak/DC): {sum(ratios)/len(ratios):.0f}%")
    print(f"Min ratio: {min(ratios):.0f}%")
    print(f"Max ratio: {max(ratios):.0f}%")
    
print()
print("INTERPRETATION:")
print("  - Ratio ~100%: DC capacity matches inverter output (likely AC capacity entered as DC)")
print("  - Ratio ~50-80%: Normal DC/AC ratio (DC capacity is correct)")
print("  - Ratio <50%: Possible data issue or missing inverters")
print("  - Ratio >100%: Possible data issue or inverters not registered")

conn.close()
