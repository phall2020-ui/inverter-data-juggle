#!/usr/bin/env python3
"""
Check monthly irradiance totals for all sites with weighted POA data
"""

import pandas as pd
import sqlite3
import json
from plant_store import PlantStore

def calculate_monthly_poa(store, plant_uid, plant_name):
    """Calculate monthly POA totals from database for a plant"""
    
    # Get weighted POA device
    device_ids = store.list_emig_ids(plant_uid)
    weighted_poa = [d for d in device_ids if 'WEIGHTED' in d]
    
    if not weighted_poa:
        return None
    
    # Query database directly to get timestamp and payload
    conn = sqlite3.connect(store.db_path)
    try:
        cur = conn.execute(
            """
            SELECT ts, payload FROM readings
            WHERE plant_uid = ? AND emig_id = ?
            ORDER BY ts
            """,
            (plant_uid, weighted_poa[0])
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    
    if not rows:
        return None
    
    # Convert to DataFrame
    data = []
    for ts, payload_json in rows:
        payload = json.loads(payload_json)
        timestamp = pd.to_datetime(ts)
        # Extract POA from nested structure
        poa_irr = payload.get('poaIrradiance', {})
        # Values are already stored as kWh/m² per 30-min interval
        poa_kwh = poa_irr.get('value', 0) if isinstance(poa_irr, dict) else 0
        data.append({'timestamp': timestamp, 'poa_kwh': poa_kwh})
    
    if not data:
        return None
    
    df = pd.DataFrame(data)
    df.set_index('timestamp', inplace=True)
    df.sort_index(inplace=True)
    
    # Calculate monthly sums
    monthly = df['poa_kwh'].resample('ME').sum()
    
    # Convert to month-year format
    monthly_dict = {}
    for date, value in monthly.items():
        month_str = date.strftime('%b-%y')
        monthly_dict[month_str] = value
    
    return monthly_dict

def main():
    print("="*80)
    print("MONTHLY POA IRRADIANCE VERIFICATION")
    print("="*80)
    print("Checking all sites with weighted POA data")
    print("Available data: June-October 2025")
    print("="*80)
    
    store = PlantStore('plant_registry.sqlite')
    plants = store.list_all()
    
    results = []
    
    for plant in sorted(plants, key=lambda x: x['alias']):
        plant_name = plant['alias']
        plant_uid = plant['plant_uid']
        
        # Check if plant has weighted POA
        device_ids = store.list_emig_ids(plant_uid)
        if not any('WEIGHTED' in d for d in device_ids):
            continue
        
        print(f"  Processing {plant_name}...", end='', flush=True)
        
        monthly = calculate_monthly_poa(store, plant_uid, plant_name)
        
        if not monthly:
            print(" no data")
            continue
        
        print(f" OK ({len(monthly)} months)")
        results.append((plant_name, monthly))
    
    # Display results
    print(f"\nFound {len(results)} site(s) with weighted POA data:\n")
    
    # Get all unique months across all sites
    all_months = set()
    for _, monthly in results:
        all_months.update(monthly.keys())
    
    sorted_months = sorted(all_months, key=lambda x: pd.to_datetime(x, format='%b-%y'))
    
    # Print header
    print(f"{'Site':<35}", end='')
    for month in sorted_months:
        print(f"{month:>12}", end='')
    print(f"{'Total':>12}")
    print("-"*80)
    
    # Print each site
    for plant_name, monthly in results:
        print(f"{plant_name:<35}", end='')
        site_total = 0
        for month in sorted_months:
            value = monthly.get(month, 0)
            if value > 0:
                print(f"{value:>11.1f} ", end='')
                site_total += value
            else:
                print(f"{'—':>12}", end='')
        print(f"{site_total:>11.1f}")
    
    print("\n" + "="*80)
    print("VALUES: kWh/m² (kilowatt-hours per square meter)")
    print("="*80)
    
    # Show detailed breakdown for first site as example
    if results:
        example_name, example_monthly = results[0]
        print(f"\nExample detailed breakdown for '{example_name}':")
        print("-"*60)
        for month in sorted_months:
            value = example_monthly.get(month, 0)
            if value > 0:
                print(f"  {month}: {value:.3f} kWh/m²")
        print(f"\n  Note: Values calculated from 30-minute POA readings")
        print(f"        Formula: Sum of POA kWh/m² values (already stored as kWh/m² per 30-min)")

if __name__ == "__main__":
    main()
