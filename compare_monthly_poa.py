#!/usr/bin/env python3
"""
Compare monthly POA irradiance against reference data from spreadsheet
"""

import pandas as pd
import sqlite3
import json
from plant_store import PlantStore

# Reference data extracted from the spreadsheet (Actual Irr (kWh/m2) column)
# Organized by site and month
REFERENCE_DATA = {
    'Apr-25': {
        'Blachford UK': 62.1,
        'Cromwell Tools': 115.5,
        'Hibernian Stadium': 130.1,
        'Hibernian Training Ground': 172.0,
        'Merry Hill Shopping Centre': 123.6,
        'Man City FC Training Ground': 165.1,
        'Metrocentre': 183.8,
        'Parfetts Birmingham': 179.8,
        'Sheldons Bakery': 195.4,
    },
    'May-25': {
        'Blachford UK': 57.7,
        'Cromwell Tools': 150.0,
        'Hibernian Stadium': 168.7,
        'Hibernian Training Ground': 165.9,
        'Merry Hill Shopping Centre': 166.9,
        'Man City FC Training Ground': 177.0,
        'Metrocentre': 170.1,
        'Parfetts Birmingham': 137.9,
        'Sheldons Bakery': 163.3,
    },
    'Jun-25': {
        'Blachford UK': 62.3,
        'Cromwell Tools': 183.5,
        'Hibernian Stadium': 146.5,
        'Hibernian Training Ground': 172.4,
        'Merry Hill Shopping Centre': 165.9,
        'Man City FC Training Ground': 163.3,
        'Metrocentre': 184.5,
        'Parfetts Birmingham': 172.5,
        'Sheldons Bakery': 186.7,
    },
    'Jul-25': {
        'Blachford UK': None,  # n/a in spreadsheet
        'Cromwell Tools': None,
        'Hibernian Stadium': None,
        'Hibernian Training Ground': None,
        'Merry Hill Shopping Centre': None,
        'Man City FC Training Ground': None,
        'Metrocentre': None,
        'Parfetts Birmingham': None,
        'Sheldons Bakery': 155.0,
    },
    'Aug-25': {
        'Blachford UK': 138.9,
        'Cromwell Tools': 123.5,
        'Hibernian Stadium': 119.8,
        'Hibernian Training Ground': 127.1,
        'Merry Hill Shopping Centre': 148.9,
        'Man City FC Training Ground': 130.9,
        'Metrocentre': 138.7,
        'Parfetts Birmingham': 135.8,
        'Sheldons Bakery': 135.8,
    },
    'Sep-25': {
        'Blachford UK': 97.1,
        'Cromwell Tools': 90.0,
        'Hibernian Stadium': 79.1,
        'Hibernian Training Ground': 89.6,
        'Merry Hill Shopping Centre': 83.6,
        'Man City FC Training Ground': 79.4,
        'Metrocentre': 93.6,
        'Parfetts Birmingham': 95.5,
        'Sheldons Bakery': 89.2,
    },
    'Oct-25': {
        'Blachford UK': 98.7,
        'Cromwell Tools': 116.2,
        'Hibernian Stadium': 99.6,
        'Hibernian Training Ground': None,  # n/a
        'Merry Hill Shopping Centre': None,
        'Man City FC Training Ground': None,
        'Metrocentre': None,
        'Parfetts Birmingham': None,
        'Sheldons Bakery': None,
    },
}

def calculate_monthly_poa(store, plant_uid):
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
        poa = poa_irr.get('value', 0) if isinstance(poa_irr, dict) else 0
        # Convert to kWh/m²: W/m² × 0.5 hours (30-min interval) / 1000
        poa_kwh = poa * 0.5 / 1000.0
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
    print("="*90)
    print("MONTHLY POA IRRADIANCE COMPARISON - DATABASE vs REFERENCE")
    print("="*90)
    print("Comparing each month within 1% tolerance")
    print("="*90)
    
    store = PlantStore('plant_registry.sqlite')
    plants = store.list_all()
    
    # Get all months that have reference data
    all_months = sorted(REFERENCE_DATA.keys(), key=lambda x: pd.to_datetime(x, format='%b-%y'))
    
    results = []
    
    print("\nScanning plants...")
    for plant in sorted(plants, key=lambda x: x['alias']):
        plant_name = plant['alias']
        plant_uid = plant['plant_uid']
        
        # Check if plant has weighted POA
        device_ids = store.list_emig_ids(plant_uid)
        if not any('WEIGHTED' in d for d in device_ids):
            continue
        
        # Check if plant is in reference data
        has_reference = any(plant_name in REFERENCE_DATA[month] for month in all_months)
        if not has_reference:
            continue
        
        print(f"  Processing {plant_name}...", end='', flush=True)
        
        monthly_db = calculate_monthly_poa(store, plant_uid)
        
        if not monthly_db:
            print(" no data")
            continue
        
        print(" OK")
        
        # Compare each month
        plant_results = {'plant': plant_name, 'months': {}}
        
        for month in all_months:
            ref_value = REFERENCE_DATA[month].get(plant_name)
            db_value = monthly_db.get(month, 0)
            
            if ref_value is None:
                continue
            
            if db_value > 0 and ref_value > 0:
                diff = abs(db_value - ref_value)
                diff_pct = (diff / ref_value) * 100
                status = '✓' if diff_pct <= 1.0 else '✗'
                
                plant_results['months'][month] = {
                    'db': db_value,
                    'ref': ref_value,
                    'diff_pct': diff_pct,
                    'status': status
                }
        
        if plant_results['months']:
            results.append(plant_results)
    
    # Display results
    print(f"\nFound {len(results)} site(s) with reference data:\n")
    
    total_comparisons = 0
    passed_comparisons = 0
    failed_comparisons = 0
    
    for plant_result in results:
        plant_name = plant_result['plant']
        months = plant_result['months']
        
        print(f"\n{'='*90}")
        print(f"{plant_name}")
        print(f"{'='*90}")
        print(f"{'Month':<12} {'Database':<15} {'Reference':<15} {'Diff':<12} {'Status':<8}")
        print(f"{'-'*90}")
        
        for month in all_months:
            if month not in months:
                continue
            
            data = months[month]
            db_val = data['db']
            ref_val = data['ref']
            diff_pct = data['diff_pct']
            status = data['status']
            
            print(f"{month:<12} {db_val:<14.2f}  {ref_val:<14.2f}  {diff_pct:<10.2f}%  {status:<8}")
            
            total_comparisons += 1
            if status == '✓':
                passed_comparisons += 1
            else:
                failed_comparisons += 1
    
    # Summary
    print(f"\n{'='*90}")
    print(f"SUMMARY")
    print(f"{'='*90}")
    print(f"Total comparisons: {total_comparisons}")
    print(f"✓ Within 1% tolerance: {passed_comparisons}")
    print(f"✗ Outside 1% tolerance: {failed_comparisons}")
    
    if failed_comparisons == 0:
        print(f"\n✅ SUCCESS: All monthly POA values match within 1% tolerance!")
    else:
        print(f"\n⚠️  WARNING: {failed_comparisons} month(s) are outside 1% tolerance")
        print(f"\nNote: Differences may be due to:")
        print(f"  - Different time periods in reference vs database")
        print(f"  - Data gaps or quality issues")
        print(f"  - Rounding differences")

if __name__ == "__main__":
    main()
