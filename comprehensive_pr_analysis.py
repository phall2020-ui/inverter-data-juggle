"""
Comprehensive PR Analysis at Multiple Time Resolutions
Investigates potential causes of discrepancies
"""
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

conn = sqlite3.connect('plant_registry.sqlite')

# =============================================================================
# GET PLANT DATA
# =============================================================================
plants_df = pd.read_sql_query("""
    SELECT alias, plant_uid, dc_size_kw, inverter_ids
    FROM plants 
    WHERE dc_size_kw > 0
    ORDER BY alias
""", conn)

print("=" * 100)
print("COMPREHENSIVE PR ANALYSIS - Multiple Time Resolutions")
print("=" * 100)
print()

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_inverter_power(plant_uid, start_date, end_date):
    """Get inverter power data with timestamp alignment"""
    query = """
        SELECT ts, 
               SUM(CAST(json_extract(payload, '$.apparentPower.value') AS REAL)) as power_w,
               COUNT(DISTINCT emig_id) as inverter_count
        FROM readings 
        WHERE plant_uid = ? 
          AND emig_id LIKE 'INVERT:%'
          AND ts >= ? AND ts < ?
          AND json_extract(payload, '$.apparentPower.value') IS NOT NULL
        GROUP BY ts
    """
    df = pd.read_sql_query(query, conn, params=(plant_uid, start_date, end_date))
    if df.empty:
        return pd.DataFrame()
    
    df['ts'] = pd.to_datetime(df['ts'])
    # Remove timezone for joining
    if df['ts'].dt.tz is not None:
        df['ts'] = df['ts'].dt.tz_localize(None)
    df = df.set_index('ts')
    df['power_kw'] = df['power_w'] / 1000
    return df

def get_poa_data(plant_uid, start_date, end_date):
    """Get POA irradiance data - convert from kWh/m² per HH to W/m²"""
    query = """
        SELECT ts, 
               CAST(json_extract(payload, '$.poaIrradiance.value') AS REAL) as poa_kwhm2
        FROM readings 
        WHERE plant_uid = ? 
          AND emig_id = 'POA:SOLARGIS:WEIGHTED'
          AND ts >= ? AND ts < ?
    """
    df = pd.read_sql_query(query, conn, params=(plant_uid, start_date, end_date))
    if df.empty:
        return pd.DataFrame()
    
    df['ts'] = pd.to_datetime(df['ts'])
    if df['ts'].dt.tz is not None:
        df['ts'] = df['ts'].dt.tz_localize(None)
    df = df.set_index('ts')
    
    # Convert kWh/m² per 0.5h to W/m²: multiply by 2000
    # (kWh/m² per 0.5h) * 2 = kWh/m² per h = kW/m² = 1000 W/m²
    df['poa_wm2'] = df['poa_kwhm2'] * 2000
    return df

def calculate_pr(power_kw, poa_wm2, dc_kw, min_poa=50):
    """
    Calculate Performance Ratio
    PR = Actual Energy / Expected Energy
    Expected Energy = DC_capacity * (POA / 1000) * time
    
    For instantaneous: PR = Power / (DC * POA/1000)
    """
    # Filter for sufficient irradiance
    mask = poa_wm2 >= min_poa
    if mask.sum() == 0:
        return None, 0
    
    actual_power = power_kw[mask].sum()
    expected_power = (dc_kw * poa_wm2[mask] / 1000).sum()
    
    if expected_power == 0:
        return None, 0
    
    pr = actual_power / expected_power
    return pr, mask.sum()

# =============================================================================
# ANALYSIS BY PLANT
# =============================================================================

results = []

for _, plant in plants_df.iterrows():
    alias = plant['alias']
    uid = plant['plant_uid']
    dc_kw = plant['dc_size_kw']
    
    print(f"\n{'='*100}")
    print(f"PLANT: {alias} ({uid}) - DC: {dc_kw} kWp")
    print(f"{'='*100}")
    
    # Get data for analysis period (June - October 2025)
    start = '2025-06-01'
    end = '2025-11-01'
    
    inv_df = get_inverter_power(uid, start, end)
    poa_df = get_poa_data(uid, start, end)
    
    if inv_df.empty:
        print("  ⚠️ No inverter data available")
        continue
    if poa_df.empty:
        print("  ⚠️ No POA data available")
        continue
    
    # Merge data
    merged = inv_df.join(poa_df, how='inner')
    merged = merged.dropna(subset=['power_kw', 'poa_wm2'])
    
    if merged.empty:
        print("  ⚠️ No matching timestamps between inverter and POA data")
        # Check timestamp overlap
        print(f"  Inverter data range: {inv_df.index.min()} to {inv_df.index.max()}")
        print(f"  POA data range: {poa_df.index.min()} to {poa_df.index.max()}")
        continue
    
    print(f"\n  Data points with matching timestamps: {len(merged)}")
    print(f"  Date range: {merged.index.min()} to {merged.index.max()}")
    
    # -------------------------------------------------------------------------
    # 1. HALF-HOURLY PR DISTRIBUTION
    # -------------------------------------------------------------------------
    print(f"\n  1. HALF-HOURLY PR ANALYSIS")
    print(f"  " + "-" * 50)
    
    # Calculate instantaneous PR for each HH period
    merged['pr_hh'] = merged['power_kw'] / (dc_kw * merged['poa_wm2'] / 1000)
    
    # Filter for reasonable irradiance (>50 W/m²)
    hh_valid = merged[merged['poa_wm2'] >= 50].copy()
    
    if len(hh_valid) > 0:
        pr_stats = hh_valid['pr_hh'].describe()
        print(f"  Points with POA >= 50 W/m²: {len(hh_valid)}")
        print(f"  PR Distribution:")
        print(f"    Mean:   {pr_stats['mean']:.1%}")
        print(f"    Median: {pr_stats['50%']:.1%}")
        print(f"    Std:    {pr_stats['std']:.1%}")
        print(f"    Min:    {pr_stats['min']:.1%}")
        print(f"    Max:    {pr_stats['max']:.1%}")
        
        # Flag anomalies
        low_pr = (hh_valid['pr_hh'] < 0.3).sum()
        high_pr = (hh_valid['pr_hh'] > 1.0).sum()
        print(f"  Anomalies: {low_pr} periods with PR<30%, {high_pr} periods with PR>100%")
    
    # -------------------------------------------------------------------------
    # 2. DAILY PR ANALYSIS
    # -------------------------------------------------------------------------
    print(f"\n  2. DAILY PR ANALYSIS")
    print(f"  " + "-" * 50)
    
    merged['date'] = merged.index.date
    daily = merged.groupby('date').agg({
        'power_kw': 'sum',  # Sum of HH power = energy in kWh (since each HH is 0.5h, this is kWh*2)
        'poa_wm2': 'sum',   # Sum of HH irradiance
        'inverter_count': 'mean'  # Average inverters reporting
    }).reset_index()
    
    # For daily PR: sum(power_kw * 0.5) / sum(dc * poa/1000 * 0.5)
    # = sum(power_kw) / sum(dc * poa/1000)
    daily['expected_kw'] = dc_kw * daily['poa_wm2'] / 1000
    daily['pr_daily'] = daily['power_kw'] / daily['expected_kw']
    
    # Filter out days with very low irradiance
    daily_valid = daily[daily['poa_wm2'] >= 1000]  # At least 500 Wh/m² total
    
    if len(daily_valid) > 0:
        print(f"  Days with significant irradiance: {len(daily_valid)}")
        print(f"  Daily PR Distribution:")
        print(f"    Mean:   {daily_valid['pr_daily'].mean():.1%}")
        print(f"    Median: {daily_valid['pr_daily'].median():.1%}")
        print(f"    Min:    {daily_valid['pr_daily'].min():.1%}")
        print(f"    Max:    {daily_valid['pr_daily'].max():.1%}")
        
        # Show best and worst days
        best_day = daily_valid.loc[daily_valid['pr_daily'].idxmax()]
        worst_day = daily_valid.loc[daily_valid['pr_daily'].idxmin()]
        print(f"  Best day: {best_day['date']} with PR={best_day['pr_daily']:.1%}")
        print(f"  Worst day: {worst_day['date']} with PR={worst_day['pr_daily']:.1%}")
    
    # -------------------------------------------------------------------------
    # 3. WEEKLY PR ANALYSIS
    # -------------------------------------------------------------------------
    print(f"\n  3. WEEKLY PR ANALYSIS")
    print(f"  " + "-" * 50)
    
    merged['week'] = merged.index.to_period('W')
    weekly = merged.groupby('week').agg({
        'power_kw': 'sum',
        'poa_wm2': 'sum',
        'inverter_count': 'mean'
    }).reset_index()
    
    weekly['expected_kw'] = dc_kw * weekly['poa_wm2'] / 1000
    weekly['pr_weekly'] = weekly['power_kw'] / weekly['expected_kw']
    
    weekly_valid = weekly[weekly['poa_wm2'] >= 5000]
    
    if len(weekly_valid) > 0:
        print(f"  Weeks with significant data: {len(weekly_valid)}")
        print(f"  Weekly PR Distribution:")
        print(f"    Mean:   {weekly_valid['pr_weekly'].mean():.1%}")
        print(f"    Median: {weekly_valid['pr_weekly'].median():.1%}")
        print(f"    Min:    {weekly_valid['pr_weekly'].min():.1%}")
        print(f"    Max:    {weekly_valid['pr_weekly'].max():.1%}")
    
    # -------------------------------------------------------------------------
    # 4. MONTHLY PR ANALYSIS
    # -------------------------------------------------------------------------
    print(f"\n  4. MONTHLY PR ANALYSIS")
    print(f"  " + "-" * 50)
    
    merged['month'] = merged.index.to_period('M')
    monthly = merged.groupby('month').agg({
        'power_kw': 'sum',
        'poa_wm2': 'sum',
        'inverter_count': 'mean'
    }).reset_index()
    
    monthly['expected_kw'] = dc_kw * monthly['poa_wm2'] / 1000
    monthly['pr_monthly'] = monthly['power_kw'] / monthly['expected_kw']
    
    print(f"  Monthly PRs:")
    for _, row in monthly.iterrows():
        status = "✅" if 0.7 <= row['pr_monthly'] <= 0.95 else "⚠️"
        print(f"    {row['month']}: {row['pr_monthly']:.1%} {status} (avg {row['inverter_count']:.1f} inverters)")
    
    # -------------------------------------------------------------------------
    # 5. DISCREPANCY ANALYSIS
    # -------------------------------------------------------------------------
    print(f"\n  5. POTENTIAL DISCREPANCY CAUSES")
    print(f"  " + "-" * 50)
    
    # A. Inverter count consistency
    inv_counts = merged['inverter_count'].value_counts()
    if len(inv_counts) > 1:
        print(f"  ⚠️ Inverter count varies: {dict(inv_counts)}")
        print(f"     This could indicate missing data from some inverters")
    else:
        print(f"  ✅ Consistent inverter count: {int(inv_counts.index[0])}")
    
    # B. Check for clipping (power at DC capacity)
    clipping_threshold = dc_kw * 0.95
    clipped = (merged['power_kw'] >= clipping_threshold).sum()
    if clipped > 0:
        print(f"  ⚠️ Potential clipping: {clipped} periods at >95% of DC capacity")
    
    # C. Check for export limiting
    # Look for flat-top periods
    high_poa = merged[merged['poa_wm2'] >= 800]
    if len(high_poa) > 0:
        expected_high = dc_kw * 0.8  # Expect at least 80% of DC at high POA
        low_output_high_poa = (high_poa['power_kw'] < expected_high * 0.5).sum()
        if low_output_high_poa > len(high_poa) * 0.2:
            print(f"  ⚠️ Low output at high irradiance: {low_output_high_poa}/{len(high_poa)} periods")
            print(f"     Possible export limiting or inverter issues")
    
    # D. Peak power vs DC capacity
    peak_power = merged['power_kw'].max()
    peak_ratio = peak_power / dc_kw
    print(f"  Peak power: {peak_power:.1f} kW ({peak_ratio:.0%} of DC)")
    if peak_ratio < 0.5:
        print(f"  ⚠️ Peak power is <50% of DC - possible missing inverters or DC capacity error")
    elif peak_ratio > 1.0:
        print(f"  ⚠️ Peak power exceeds DC capacity - check DC capacity value")
    
    # E. Check timestamp alignment
    inv_times = set(inv_df.index.strftime('%H:%M'))
    poa_times = set(poa_df.index.strftime('%H:%M'))
    if inv_times != poa_times:
        print(f"  ⚠️ Timestamp misalignment: Inverter has {len(inv_times)} unique times, POA has {len(poa_times)}")
    
    # Store results
    overall_pr = merged['power_kw'].sum() / (dc_kw * merged['poa_wm2'].sum() / 1000) if merged['poa_wm2'].sum() > 0 else None
    results.append({
        'Plant': alias,
        'UID': uid,
        'DC_kW': dc_kw,
        'Peak_kW': peak_power,
        'Peak_Ratio': peak_ratio,
        'Overall_PR': overall_pr,
        'HH_Points': len(hh_valid) if 'hh_valid' in dir() else 0,
        'Inverters_Avg': merged['inverter_count'].mean() if len(merged) > 0 else 0
    })

# =============================================================================
# SUMMARY
# =============================================================================
print("\n" + "=" * 100)
print("SUMMARY")
print("=" * 100)

results_df = pd.DataFrame(results)
print(results_df.to_string(index=False))

print("\n" + "=" * 100)
print("KEY INSIGHTS & RECOMMENDATIONS")
print("=" * 100)

print("""
POTENTIAL CAUSES OF LOW PR:

1. DC CAPACITY MISMATCH
   - If registered DC capacity is higher than actual, PR will appear low
   - Compare Peak_Ratio: should be 70-100% on best days

2. MISSING INVERTERS
   - Check if all inverters are reporting at each timestamp
   - Variable inverter counts indicate data gaps

3. TIMESTAMP MISALIGNMENT
   - POA data is naive (local time), inverter data may be UTC
   - 1-hour offset in UK summer (BST) would cause mismatched readings

4. EXPORT LIMITING
   - Grid export limits can cap output below capacity
   - Look for flat-top power profiles at high irradiance

5. SHADING/SOILING
   - Physical obstructions reduce output
   - Should show seasonal or time-of-day patterns

6. INVERTER DOWNTIME
   - Partial outages reduce total power
   - Check for periods with fewer inverters than expected

7. POA DATA QUALITY
   - SolarGIS data is modeled, not measured
   - Local weather variations not captured

RECOMMENDATIONS:
- Focus on Peak_Ratio as a quick validation of DC capacity
- Use weekly/monthly PR to smooth out short-term variations
- Investigate sites with Overall_PR < 60% or > 100%
""")

conn.close()
