#!/usr/bin/env python3
"""
Verify monthly POA totals match reference data
"""

import pandas as pd
from datetime import datetime
from plant_store import PlantStore

# Reference monthly POA data (kWh/m²) from spreadsheet
REFERENCE_POA = {
    'Apr-25': 62.296,
    'May-25': 69.048,
    'Jun-25': 26.519,
    'Jul-25': 3.488,
    'Aug-25': 91.429,
    'Sep-25': 29.723,
}

def calculate_monthly_poa_from_db(store, plant_uid, plant_name):
    """Calculate monthly POA totals from database"""
    
    # Get weighted POA device
    device_ids = store.list_emig_ids(plant_uid)
    weighted_poa = [d for d in device_ids if 'WEIGHTED' in d]
    
    if not weighted_poa:
        print(f"  ⚠️  No capacity-weighted POA found")
        return None
    
    # Load POA readings
    readings = store.load_readings(
        plant_uid, 
        weighted_poa[0],
        "2025-04-01T00:00:00",
        "2025-09-30T23:59:59"
    )
    
    if not readings:
        print(f"  ⚠️  No POA readings found")
        return None
    
    # Convert to DataFrame
    data = []
    for r in readings:
        ts = r.get('ts')
        poa_value = r.get('poaIrradiance', {})
        if isinstance(poa_value, dict):
            poa = poa_value.get('value', 0)
        else:
            poa = poa_value
        
        if ts and poa is not None:
            data.append({'timestamp': ts, 'poa': poa})
    
    if not data:
        print(f"  ⚠️  No valid POA data")
        return None
    
    df = pd.DataFrame(data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.set_index('timestamp')
    
    # Calculate monthly sums
    # POA is in W/m², need to convert to kWh/m²
    # For 30-minute intervals: kWh = W × 0.5 hours / 1000
    df['poa_kwh'] = df['poa'] * 0.5 / 1000
    
    monthly = df['poa_kwh'].resample('M').sum()
    
    # Format as month strings
    result = {}
    for date, value in monthly.items():
        month_str = date.strftime('%b-%y')
        result[month_str] = value
    
    return result

def main():
    print("=" * 80)
    print("MONTHLY POA VERIFICATION - SCANNING ALL PLANTS")
    print("=" * 80)
    
    store = PlantStore('plant_registry.sqlite')
    plants = store.list_all()
    
    print(f"\nReference POA data (kWh/m²):")
    for month, value in sorted(REFERENCE_POA.items()):
        print(f"  {month}: {value:.3f}")
    
    print("\n" + "=" * 80)
    print("Scanning all plants for matching POA data...")
    print("=" * 80)
    
    best_match = None
    best_match_score = float('inf')
    
    for plant in plants:
        plant_name = plant['alias']
        plant_uid = plant['plant_uid']
        
        # Check if plant has weighted POA
        device_ids = store.list_emig_ids(plant_uid)
        if not any('WEIGHTED' in d for d in device_ids):
            continue
        
        print(f"\n{plant_name}:")
        
        # Calculate from database
        db_monthly = calculate_monthly_poa_from_db(store, plant_uid, plant_name)
        
        if not db_monthly:
            print("  ⚠️  Could not calculate monthly POA")
            continue
        
        # Calculate match score
        total_diff = 0
        count = 0
        
        for month in REFERENCE_POA.keys():
            ref_value = REFERENCE_POA[month]
            db_value = db_monthly.get(month, 0)
            
            if db_value > 0:
                diff_pct = abs(db_value - ref_value) / ref_value * 100
                total_diff += diff_pct
                count += 1
        
        if count > 0:
            avg_diff = total_diff / count
            print(f"  Average difference: {avg_diff:.1f}%")
            
            if avg_diff < best_match_score:
                best_match_score = avg_diff
                best_match = (plant, db_monthly)
            
            if avg_diff < 10.0:
                print(f"  ✓ Potential match!")
    
    if best_match:
        plant, db_monthly = best_match
        print("\n" + "=" * 80)
        print(f"BEST MATCH: {plant['alias']} (avg diff: {best_match_score:.1f}%)")
        print("=" * 80)
        print(f"\n{'Month':<15} {'Reference':<15} {'Database':<15} {'Diff %':<15}")
        print("-" * 60)
        
        for month in sorted(REFERENCE_POA.keys()):
            ref_value = REFERENCE_POA[month]
            db_value = db_monthly.get(month, 0)
            
            if db_value > 0:
                diff_pct = abs(db_value - ref_value) / ref_value * 100
            else:
                diff_pct = 100.0
            
            status = "✓" if diff_pct < 5.0 else "⚠" if diff_pct < 10.0 else "✗"
            print(f"{month:<15} {ref_value:<15.3f} {db_value:<15.3f} {diff_pct:<10.1f}% {status}")
        
        if best_match_score < 5.0:
            print("\n✅ EXCELLENT MATCH: Monthly POA values within 5%")
        elif best_match_score < 10.0:
            print("\n✓ GOOD MATCH: Monthly POA values within 10%")
        else:
            print("\n⚠️  MODERATE MATCH: Some discrepancies present")
    else:
        print("\n❌ No matching plant found")
    
    print("\n" + "=" * 80)

if __name__ == "__main__":
    main()
