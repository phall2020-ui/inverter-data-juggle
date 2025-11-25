#!/usr/bin/env python3
"""
Quick fix for the 3 plants with wrong capacities
"""

import sys
import os
sys.path.insert(0, r'c:\Users\PeterHall\OneDrive - AMPYR IDEA UK Ltd\Python scripts\Inverter data - Juggle')

from solargis_poa_import import import_poa_for_plant_multi_folder
from plant_store import PlantStore

def main():
    # Fix only these 3 plants
    plants_to_fix = [
        ('Finlay Beverages', 'Faltec_Europe_Ltd', 675),
        ('Merry Hill Shopping Centre', 'Merry_Hill', 996),
        ("Smithy's Mushrooms", 'Smithys_Mushrooms', 224),
    ]
    
    store = PlantStore('plant_registry.sqlite')
    solargis_base = os.path.expanduser('~/OneDrive - AMPYR IDEA UK Ltd/Monthly Excom/Monthly SolarGIS data/')
    
    for plant_alias, csv_base, expected_capacity in plants_to_fix:
        print(f"\n{'='*70}")
        print(f"Fixing: {plant_alias}")
        print(f"CSV: {csv_base}.csv (Expected: {expected_capacity} kW)")
        print(f"{'='*70}")
        
        # Find plant
        plants = [p for p in store.list_all() if p['alias'] == plant_alias]
        if not plants:
            print(f"  ERROR: Plant '{plant_alias}' not found in database")
            continue
        
        plant = plants[0]
        plant_uid = plant['plant_uid']
        
        # Load and store POA
        try:
            import_poa_for_plant_multi_folder(
                plant_uid=plant_uid,
                plant_name=plant_alias,
                csv_pattern=csv_base,
                solargis_base=solargis_base
            )
            
            # Verify capacity
            devices = store.list_emig_ids(plant_uid)
            weighted_devices = [d for d in devices if 'WEIGHTED' in d]
            
            if weighted_devices:
                readings = store.get_readings(plant_uid, weighted_devices[0], limit=1)
                if readings:
                    actual_capacity = readings[0]['payload'].get('dc_capacity_kw', 0)
                    diff = abs(actual_capacity - expected_capacity)
                    diff_pct = (diff / expected_capacity * 100) if expected_capacity > 0 else 0
                    
                    print(f"\n  DC Capacity: {actual_capacity:.1f} kW")
                    print(f"  Expected: {expected_capacity} kW")
                    print(f"  Difference: {diff:.1f} kW ({diff_pct:.2f}%)")
                    
                    if diff_pct < 1.0:
                        print(f"  ✓ PASS: Within 1% tolerance")
                    else:
                        print(f"  ✗ FAIL: Outside 1% tolerance")
            
        except Exception as e:
            print(f"  ERROR: {e}")

if __name__ == "__main__":
    main()
