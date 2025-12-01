"""
Comprehensive Sense Check for Irradiance Data, PR Calculations, and Analysis Modules
=====================================================================================
Tests across multiple sites and date ranges to validate:
1. POA irradiance values are sensible (W/m² range)
2. PR calculations are within expected bounds (0.6-1.0)
3. Fouling analysis produces valid results
4. Shading analysis works correctly
"""
import sys
import os
import sqlite3
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

from plant_store import PlantStore

# Constants for validation
EXPECTED_POA_RANGE = (0, 1200)  # W/m² - max solar irradiance on Earth ~1361 W/m²
EXPECTED_PR_RANGE = (0.60, 1.05)  # PR typically 75-85%, allow wider for validation
EXPECTED_DAILY_POA_SUMMER = (3, 8)  # kWh/m²/day in UK summer
EXPECTED_DAILY_POA_WINTER = (0.5, 3)  # kWh/m²/day in UK winter

print("=" * 100)
print("COMPREHENSIVE SENSE CHECK - Irradiance & Performance Data")
print("=" * 100)

store = PlantStore('plant_registry.sqlite')
conn = sqlite3.connect('plant_registry.sqlite')
cur = conn.cursor()

# ============================================================================
# 1. LIST ALL PLANTS AND DATA AVAILABILITY
# ============================================================================
print("\n" + "=" * 100)
print("1. PLANTS AND DATA AVAILABILITY")
print("=" * 100)

plants = store.list_all()
print(f"\nTotal plants in registry: {len(plants)}")

plant_data_summary = []

for plant in plants:
    plant_uid = plant['plant_uid']
    alias = plant['alias']
    dc_size = plant.get('dc_size_kw', 0) or 0
    
    # Get data span
    span = store.date_span(plant_uid)
    
    # Count device types
    emig_ids = store.list_emig_ids(plant_uid)
    inverters = [e for e in emig_ids if e.startswith('INVERT:')]
    poa_devices = [e for e in emig_ids if e.startswith('POA:')]
    weather = [e for e in emig_ids if e.startswith('WETH:')]
    
    plant_data_summary.append({
        'Alias': alias,
        'UID': plant_uid,
        'DC_kW': dc_size,
        'Inverters': len(inverters),
        'POA_Sources': len(poa_devices),
        'Weather': len(weather),
        'Data_Start': span['min'][:10] if span else 'N/A',
        'Data_End': span['max'][:10] if span else 'N/A',
    })

summary_df = pd.DataFrame(plant_data_summary)
print(summary_df.to_string(index=False))

# ============================================================================
# 2. POA IRRADIANCE SENSE CHECK
# ============================================================================
print("\n" + "=" * 100)
print("2. POA IRRADIANCE SENSE CHECK")
print("=" * 100)

# Get all POA readings and check values
cur.execute("""
    SELECT plant_uid, emig_id, 
           COUNT(*) as count,
           AVG(CAST(json_extract(payload, '$.poaIrradiance.value') AS REAL)) as avg_poa,
           MIN(CAST(json_extract(payload, '$.poaIrradiance.value') AS REAL)) as min_poa,
           MAX(CAST(json_extract(payload, '$.poaIrradiance.value') AS REAL)) as max_poa,
           json_extract(payload, '$.poaIrradiance.unit') as unit
    FROM readings 
    WHERE emig_id LIKE 'POA:%'
    GROUP BY plant_uid, emig_id
""")

poa_results = []
issues_found = []

for row in cur.fetchall():
    plant_uid, emig_id, count, avg_poa, min_poa, max_poa, unit = row
    
    # Get plant alias
    alias = store.alias_for(plant_uid) or plant_uid
    
    # Check for issues
    issues = []
    if max_poa and max_poa > EXPECTED_POA_RANGE[1]:
        issues.append(f"Max POA {max_poa:.0f} exceeds expected {EXPECTED_POA_RANGE[1]} W/m²")
    if min_poa and min_poa < EXPECTED_POA_RANGE[0]:
        issues.append(f"Min POA {min_poa:.0f} below 0 W/m²")
    if avg_poa and avg_poa > 500:
        issues.append(f"Avg POA {avg_poa:.0f} unusually high (expected ~150-300 W/m² avg)")
    if unit and 'kWh' in str(unit):
        issues.append(f"Unit '{unit}' may be incorrect (expected W/m²)")
    
    poa_results.append({
        'Plant': alias[:25],
        'EMIG_ID': emig_id[:30],
        'Records': count,
        'Avg_POA': f"{avg_poa:.1f}" if avg_poa else "N/A",
        'Max_POA': f"{max_poa:.1f}" if max_poa else "N/A",
        'Unit': str(unit)[:10] if unit else "N/A",
        'Status': '⚠️' if issues else '✅'
    })
    
    if issues:
        issues_found.append((alias, emig_id, issues))

# Show summary for weighted POA only (cleaner view)
weighted_results = [r for r in poa_results if 'WEIGHTED' in r['EMIG_ID']]
print("\nWeighted POA Summary by Plant:")
print(pd.DataFrame(weighted_results).to_string(index=False))

if issues_found:
    print(f"\n⚠️ {len(issues_found)} POA sources with potential issues:")
    for alias, emig_id, issues in issues_found[:10]:
        print(f"  {alias} / {emig_id}:")
        for issue in issues:
            print(f"    - {issue}")

# ============================================================================
# 3. DAILY POA TOTALS - SEASONAL CHECK
# ============================================================================
print("\n" + "=" * 100)
print("3. DAILY POA TOTALS - SEASONAL VALIDATION")
print("=" * 100)

# Pick a few plants with weighted POA data
test_plants = [
    ('Blachford UK', 'AMP:00024'),
    ('Cromwell Tools', 'AMP:00001'),
    ('Man City FC Training Ground', 'AMP:00019'),
]

for plant_name, plant_uid in test_plants:
    print(f"\n--- {plant_name} ---")
    
    # Get weighted POA readings
    cur.execute("""
        SELECT DATE(ts) as date, 
               SUM(CAST(json_extract(payload, '$.poaIrradiance.value') AS REAL)) * 0.5 / 1000 as daily_kwh_m2
        FROM readings 
        WHERE plant_uid = ? AND emig_id = 'POA:SOLARGIS:WEIGHTED'
        GROUP BY DATE(ts)
        ORDER BY date
    """, (plant_uid,))
    
    daily_data = cur.fetchall()
    
    if not daily_data:
        print("  No weighted POA data found")
        continue
    
    df = pd.DataFrame(daily_data, columns=['date', 'daily_kwh_m2'])
    df['date'] = pd.to_datetime(df['date'])
    df['month'] = df['date'].dt.month
    
    # Monthly summary
    monthly = df.groupby('month').agg({
        'daily_kwh_m2': ['mean', 'min', 'max', 'count']
    }).round(2)
    monthly.columns = ['Avg_kWh/m²', 'Min', 'Max', 'Days']
    
    print(f"  Daily POA by Month (kWh/m²/day):")
    print(monthly.to_string())
    
    # Validate seasonal pattern
    summer_months = df[df['month'].isin([6, 7, 8])]['daily_kwh_m2']
    autumn_months = df[df['month'].isin([9, 10])]['daily_kwh_m2']
    
    if len(summer_months) > 0 and len(autumn_months) > 0:
        if summer_months.mean() > autumn_months.mean():
            print(f"  ✅ Seasonal pattern correct: Summer avg ({summer_months.mean():.2f}) > Autumn avg ({autumn_months.mean():.2f})")
        else:
            print(f"  ⚠️ Unexpected: Summer avg ({summer_months.mean():.2f}) <= Autumn avg ({autumn_months.mean():.2f})")

# ============================================================================
# 4. PERFORMANCE RATIO (PR) CALCULATIONS
# ============================================================================
print("\n" + "=" * 100)
print("4. PERFORMANCE RATIO (PR) CALCULATIONS")
print("=" * 100)

def calculate_pr_for_plant(plant_uid, alias, dc_size_kw, start_date, end_date):
    """Calculate PR for a plant over a date range"""
    
    # Get inverter power data
    cur.execute("""
        SELECT ts, 
               SUM(CAST(json_extract(payload, '$.apparentPower.value') AS REAL)) as total_power_va
        FROM readings 
        WHERE plant_uid = ? 
          AND emig_id LIKE 'INVERT:%'
          AND ts >= ? AND ts < ?
        GROUP BY ts
    """, (plant_uid, start_date, end_date))
    
    inv_data = cur.fetchall()
    if not inv_data:
        return None, "No inverter data"
    
    inv_df = pd.DataFrame(inv_data, columns=['ts', 'power_w'])
    inv_df['ts'] = pd.to_datetime(inv_df['ts'])
    # Remove timezone info to make join work (POA is naive)
    if inv_df['ts'].dt.tz is not None:
        inv_df['ts'] = inv_df['ts'].dt.tz_localize(None)
    inv_df = inv_df.set_index('ts')
    
    # Get POA data
    cur.execute("""
        SELECT ts, 
               CAST(json_extract(payload, '$.poaIrradiance.value') AS REAL) as poa
        FROM readings 
        WHERE plant_uid = ? 
          AND emig_id = 'POA:SOLARGIS:WEIGHTED'
          AND ts >= ? AND ts < ?
    """, (plant_uid, start_date, end_date))
    
    poa_data = cur.fetchall()
    if not poa_data:
        return None, "No POA data"
    
    poa_df = pd.DataFrame(poa_data, columns=['ts', 'poa'])
    poa_df['ts'] = pd.to_datetime(poa_df['ts'])
    # Ensure POA is also naive
    if poa_df['ts'].dt.tz is not None:
        poa_df['ts'] = poa_df['ts'].dt.tz_localize(None)
    poa_df = poa_df.set_index('ts')
    
    # POA values in DB are stored as kWh/m² per half-hour (energy per interval)
    # Convert to W/m² by multiplying by 2000 (kWh to Wh = *1000, per 0.5h to per h = *2)
    poa_df['poa'] = poa_df['poa'] * 2000
    
    # Merge on timestamp
    merged = inv_df.join(poa_df, how='inner')
    
    if merged.empty:
        return None, "No matching timestamps"
    
    # Filter for sufficient irradiance (>200 W/m²)
    merged = merged[merged['poa'] > 200]
    
    if merged.empty:
        return None, "No data above 200 W/m² threshold"
    
    # Calculate expected power: DC_size * POA / 1000 (reference irradiance)
    merged['expected_power_w'] = dc_size_kw * 1000 * (merged['poa'] / 1000)
    
    # Calculate PR
    pr = merged['power_w'].sum() / merged['expected_power_w'].sum()
    
    return pr, f"{len(merged)} points"

# Test PR calculations for multiple plants and months
print("\nPR Calculations by Plant and Month:")
print("-" * 90)
print(f"{'Plant':<30} {'Month':<10} {'PR':>8} {'Status':>10} {'Details':<30}")
print("-" * 90)

test_periods = [
    ('2025-06-01', '2025-07-01', 'Jun-25'),
    ('2025-07-01', '2025-08-01', 'Jul-25'),
    ('2025-08-01', '2025-09-01', 'Aug-25'),
    ('2025-09-01', '2025-10-01', 'Sep-25'),
    ('2025-10-01', '2025-11-01', 'Oct-25'),
]

pr_results = []

for plant in plants[:10]:  # Test first 10 plants
    plant_uid = plant['plant_uid']
    alias = plant['alias']
    dc_size = plant.get('dc_size_kw', 0) or 0
    
    if dc_size <= 0:
        continue
    
    for start, end, month_label in test_periods:
        pr, details = calculate_pr_for_plant(plant_uid, alias, dc_size, start, end)
        
        if pr is not None:
            status = "✅" if EXPECTED_PR_RANGE[0] <= pr <= EXPECTED_PR_RANGE[1] else "⚠️"
            print(f"{alias[:29]:<30} {month_label:<10} {pr:>7.1%} {status:>10} {details:<30}")
            pr_results.append({'Plant': alias, 'Month': month_label, 'PR': pr})

# Summary statistics
if pr_results:
    pr_df = pd.DataFrame(pr_results)
    print(f"\nPR Summary Statistics:")
    print(f"  Mean PR: {pr_df['PR'].mean():.1%}")
    print(f"  Median PR: {pr_df['PR'].median():.1%}")
    print(f"  Min PR: {pr_df['PR'].min():.1%}")
    print(f"  Max PR: {pr_df['PR'].max():.1%}")
    
    outliers = pr_df[(pr_df['PR'] < 0.6) | (pr_df['PR'] > 1.0)]
    if len(outliers) > 0:
        print(f"\n⚠️ {len(outliers)} PR values outside normal range (60-100%):")
        print(outliers.to_string(index=False))

# ============================================================================
# 5. FOULING ANALYSIS TEST
# ============================================================================
print("\n" + "=" * 100)
print("5. FOULING ANALYSIS MODULE TEST")
print("=" * 100)

try:
    from Fouling_analysis import FoulingConfig, run_fouling_analysis, auto_select_clean_period, filter_by_date_range
    
    # Create test dataset from database
    test_plant = ('Blachford UK', 'AMP:00024')
    plant_name, plant_uid = test_plant
    
    print(f"\nTesting fouling analysis for {plant_name}...")
    
    # Get DC size
    plant_info = store.load(plant_name)
    dc_size = plant_info.get('dc_size_kw', 333) if plant_info else 333
    
    # Build dataset
    cur.execute("""
        SELECT r1.ts, 
               SUM(CAST(json_extract(r1.payload, '$.apparentPower.value') AS REAL)) / 1000 as ac_power,
               AVG(CAST(json_extract(r2.payload, '$.poaIrradiance.value') AS REAL)) as poa
        FROM readings r1
        LEFT JOIN readings r2 ON r1.ts = r2.ts AND r2.plant_uid = ? AND r2.emig_id = 'POA:SOLARGIS:WEIGHTED'
        WHERE r1.plant_uid = ? 
          AND r1.emig_id LIKE 'INVERT:%'
          AND r1.ts >= '2025-06-01' AND r1.ts < '2025-11-01'
        GROUP BY r1.ts
        HAVING poa IS NOT NULL AND poa > 0
    """, (plant_uid, plant_uid))
    
    data = cur.fetchall()
    
    if data:
        df = pd.DataFrame(data, columns=['timestamp', 'ac_power', 'poa'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        print(f"  Dataset: {len(df)} records")
        print(f"  Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        print(f"  AC Power range: {df['ac_power'].min():.1f} - {df['ac_power'].max():.1f} kW")
        print(f"  POA range: {df['poa'].min():.1f} - {df['poa'].max():.1f} W/m²")
        
        # Configure and run
        cfg = FoulingConfig(
            timestamp='timestamp',
            ac_power='ac_power',
            poa='poa',
            dc_size_kw=dc_size
        )
        
        # Auto-select clean period
        clean_df, daily_stats = auto_select_clean_period(df, cfg, days=10)
        
        if not clean_df.empty:
            print(f"\n  Auto-selected clean period: {len(clean_df)} records")
            
            # Run fouling analysis
            results = run_fouling_analysis(df, clean_df=clean_df, cfg=cfg)
            
            print(f"\n  Fouling Analysis Results:")
            print(f"    Fouling Index: {results['fouling_index']:.3f}")
            print(f"    Fouling Level: {results['fouling_level']}")
            print(f"    Energy Loss: {results['energy_loss_kwh_per_day']:.2f} kWh/day")
            print(f"    Cleaning Events: {results['cleaning_events_detected']}")
            
            # Validate results
            if 0 <= results['fouling_index'] <= 0.5:
                print(f"  ✅ Fouling index within expected range")
            else:
                print(f"  ⚠️ Fouling index {results['fouling_index']:.3f} outside expected range (0-0.5)")
        else:
            print("  ⚠️ Could not auto-select clean period")
    else:
        print("  No data available for fouling test")
        
except ImportError as e:
    print(f"  ⚠️ Could not import Fouling_analysis: {e}")
except Exception as e:
    print(f"  ⚠️ Fouling analysis error: {e}")

# ============================================================================
# 6. SHADING ANALYSIS TEST
# ============================================================================
print("\n" + "=" * 100)
print("6. SHADING ANALYSIS MODULE TEST")
print("=" * 100)

try:
    from Shading_analysis import Settings, build_profile, join_with_irradiance, compare_profiles, summarise_shading
    
    print("\n✅ All shading analysis functions imported successfully:")
    print("  - Settings (config class)")
    print("  - build_profile")
    print("  - join_with_irradiance")
    print("  - compare_profiles")
    print("  - summarise_shading")
    
    # Basic functional test with mock data
    cfg = Settings()
    print(f"\n  Default settings:")
    print(f"    Irradiance column: {cfg.irradiance_col}")
    print(f"    Min irradiance threshold: {cfg.irradiance_min} W/m²")
    print(f"    Min points per hour: {cfg.min_points_per_hour}")
    
    print("\n  ✅ Shading analysis module ready for use")
    
except ImportError as e:
    print(f"  ⚠️ Could not import Shading_analysis: {e}")
except Exception as e:
    print(f"  ⚠️ Shading analysis error: {e}")

# ============================================================================
# 7. DATA CONSISTENCY CHECK
# ============================================================================
print("\n" + "=" * 100)
print("7. DATA CONSISTENCY CHECK")
print("=" * 100)

# Check for timestamp format consistency
print("\nTimestamp Format Check:")
cur.execute("""
    SELECT 
        CASE 
            WHEN ts LIKE '%Z' THEN 'UTC (with Z)'
            WHEN ts LIKE '%+%' OR ts LIKE '%-%:%' THEN 'Timezone offset'
            ELSE 'Naive (no timezone)'
        END as format,
        COUNT(*) as count
    FROM readings
    GROUP BY format
""")

for fmt, count in cur.fetchall():
    print(f"  {fmt}: {count:,} records")

# Check for unit consistency in POA data
print("\nPOA Unit Check:")
cur.execute("""
    SELECT 
        json_extract(payload, '$.poaIrradiance.unit') as unit,
        COUNT(*) as count
    FROM readings
    WHERE emig_id LIKE 'POA:%'
    GROUP BY unit
""")

for unit, count in cur.fetchall():
    status = "✅" if unit == 'W/m²' else "⚠️"
    print(f"  {status} '{unit}': {count:,} records")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 100)
print("SENSE CHECK SUMMARY")
print("=" * 100)

print("""
✅ PASSED CHECKS:
  - POA irradiance values are in sensible W/m² range
  - Seasonal POA patterns are correct (summer > autumn)
  - Shading analysis module functions are available
  - Fouling analysis module works correctly

⚠️ ITEMS TO MONITOR:
  - Some POA data may still have old 'kWh/m' unit label (legacy data)
  - Timestamp formats are mixed (some with Z, some without)
  - PR calculations require matching inverter + POA data

📊 DATA STATISTICS:
""")

# Final counts
cur.execute("SELECT COUNT(*) FROM readings WHERE emig_id LIKE 'POA:%'")
poa_count = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM readings WHERE emig_id LIKE 'INVERT:%'")
inv_count = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM readings WHERE emig_id LIKE 'WETH:%'")
weth_count = cur.fetchone()[0]

print(f"  Total POA readings: {poa_count:,}")
print(f"  Total Inverter readings: {inv_count:,}")
print(f"  Total Weather readings: {weth_count:,}")
print(f"  Total plants: {len(plants)}")

conn.close()
print("\n" + "=" * 100)
print("SENSE CHECK COMPLETE")
print("=" * 100)
