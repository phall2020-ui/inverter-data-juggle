"""Generate monthly POA summary for all plants"""
import sqlite3
import json
from datetime import datetime
import pandas as pd

conn = sqlite3.connect('plant_registry.sqlite')

# Get all plants with POA data
cur = conn.cursor()
cur.execute("""
    SELECT DISTINCT p.plant_uid, p.alias as name, p.dc_size_kw as dc_capacity
    FROM plants p
    WHERE p.plant_uid IN (
        SELECT DISTINCT plant_uid 
        FROM readings 
        WHERE emig_id = 'POA:SOLARGIS:WEIGHTED'
    )
    ORDER BY p.alias
""")

plants_with_poa = cur.fetchall()

print(f"Monthly POA Irradiance Summary")
print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*120)
print(f"\nFound {len(plants_with_poa)} plants with POA data\n")

# Month mapping
months = {
    '2025-06': 'June 2025',
    '2025-07': 'July 2025',
    '2025-08': 'August 2025',
    '2025-09': 'September 2025',
    '2025-10': 'October 2025',
    '2025-11': 'November 2025',
}

# Collect data for summary table
summary_data = []

for plant_uid, plant_name, dc_capacity in plants_with_poa:
    if not plant_name:
        plant_name = plant_uid
    
    print(f"\n{plant_name} (DC: {dc_capacity} kW)")
    print("-" * 120)
    
    # Get monthly totals
    cur.execute("""
        SELECT 
            strftime('%Y-%m', ts) as month,
            COUNT(*) as readings,
            SUM(json_extract(payload, '$.poaIrradiance.value')) as total_poa,
            AVG(json_extract(payload, '$.poaIrradiance.value')) as avg_poa,
            MIN(json_extract(payload, '$.poaIrradiance.value')) as min_poa,
            MAX(json_extract(payload, '$.poaIrradiance.value')) as max_poa
        FROM readings
        WHERE plant_uid = ?
          AND emig_id = 'POA:SOLARGIS:WEIGHTED'
        GROUP BY strftime('%Y-%m', ts)
        ORDER BY month
    """, (plant_uid,))
    
    monthly_results = cur.fetchall()
    
    print(f"{'Month':<15} {'Total POA':>12} {'Avg POA':>12} {'Min POA':>12} {'Max POA':>12} {'Readings':>10}")
    print(f"{'':15} {'(kWh/m²)':>12} {'(kWh/m²)':>12} {'(kWh/m²)':>12} {'(kWh/m²)':>12} {'(count)':>10}")
    print("-" * 120)
    
    for month, readings, total, avg, min_val, max_val in monthly_results:
        month_name = months.get(month, month)
        print(f"{month_name:<15} {total:>12.2f} {avg:>12.6f} {min_val:>12.6f} {max_val:>12.6f} {readings:>10}")
        
        summary_data.append({
            'Plant': plant_name,
            'DC Capacity (kW)': dc_capacity,
            'Month': month_name,
            'Total POA (kWh/m²)': round(total, 2),
            'Avg POA (kWh/m²)': round(avg, 6),
            'Readings': readings
        })

# Create summary comparison table
print("\n\n" + "="*120)
print("SUMMARY: Total POA by Plant and Month")
print("="*120)

# Pivot table showing total POA
df = pd.DataFrame(summary_data)
if not df.empty:
    pivot = df.pivot_table(
        values='Total POA (kWh/m²)',
        index='Plant',
        columns='Month',
        aggfunc='first'
    )
    
    # Reorder columns chronologically
    month_order = list(months.values())
    pivot = pivot[[col for col in month_order if col in pivot.columns]]
    
    # Add DC capacity column
    dc_map = {row['Plant']: row['DC Capacity (kW)'] for _, row in df.iterrows()}
    pivot.insert(0, 'DC (kW)', pivot.index.map(dc_map))
    
    print(pivot.to_string())
    
    # Save to CSV
    csv_file = 'poa_monthly_summary.csv'
    df.to_csv(csv_file, index=False)
    print(f"\n\nDetailed data saved to: {csv_file}")

conn.close()

print("\n" + "="*120)
print("Summary complete!")
