#!/usr/bin/env python3
"""
Re-import POA data to test capacity fixes
"""

import os
from plant_store import PlantStore
from solargis_poa_import import import_poa_for_plant_multi_folder, store_poa_in_db

def main():
    print("=" * 70)
    print("POA DATA RE-IMPORT (Testing Capacity Fixes)")
    print("=" * 70)
    
    # Setup
    store = PlantStore('plant_registry.sqlite')
    base_dir = os.path.expanduser("~/OneDrive - AMPYR IDEA UK Ltd/Monthly Excom/Monthly SolarGIS data")
    
    # Get all month folders
    if not os.path.exists(base_dir):
        print(f"ERROR: SolarGIS folder not found: {base_dir}")
        return
    
    all_items = os.listdir(base_dir)
    solargis_folders = [
        os.path.join(base_dir, item) 
        for item in all_items 
        if os.path.isdir(os.path.join(base_dir, item))
    ]
    
    print(f"\nFound {len(solargis_folders)} SolarGIS data folder(s)")
    
    # Test with Blachford UK first
    test_plants = [
        {'alias': 'Blachford UK', 'plant_uid': 'AMP:00024', 'expected_capacity': 333.0},
        {'alias': 'Finlay Beverages', 'plant_uid': 'AMP:00031', 'expected_capacity': 1837.2},
    ]
    
    for plant_info in test_plants:
        print("\n" + "=" * 70)
        print(f"Testing: {plant_info['alias']}")
        print("=" * 70)
        
        # Delete old POA data
        print("\n🗑️  Deleting old POA data...")
        store.delete_devices_by_pattern(plant_info['plant_uid'], 'POA:%')
        
        # Import new POA data
        print("\n📥 Importing POA data...")
        result = import_poa_for_plant_multi_folder(
            plant_name=plant_info['alias'],
            plant_uid=plant_info['plant_uid'],
            solargis_folders=solargis_folders,
            start_date="20250601",
            end_date="20251130",
            store=store,
            fuzzy_threshold=0.5
        )
        
        if result is not None and not result.empty:
            # Store POA data in database
            print("\n💾 Storing POA data in database...")
            store_poa_in_db(store, plant_info['plant_uid'], result)
            
            # Check capacity
            saved_plant = store.load(plant_info['alias'])
            actual_capacity = saved_plant.get('dc_size_kw', 0)
            expected_capacity = plant_info['expected_capacity']
            
            print("\n" + "=" * 70)
            print("VERIFICATION")
            print("=" * 70)
            print(f"Expected Capacity: {expected_capacity:.1f} kW")
            print(f"Actual Capacity:   {actual_capacity:.1f} kW")
            
            diff = abs(actual_capacity - expected_capacity)
            if diff < 1.0:
                print("✓ PASS: Capacity is correct!")
            else:
                print(f"✗ FAIL: Capacity differs by {diff:.1f} kW")
            
            # Check for weighted POA
            device_ids = store.list_emig_ids(plant_info['plant_uid'])
            poa_devices = [d for d in device_ids if d.startswith('POA:')]
            orientations = [d for d in poa_devices if 'WEIGHTED' not in d]
            has_weighted = any('WEIGHTED' in d for d in poa_devices)
            
            print(f"\nOrientations found: {len(orientations)}")
            for d in orientations:
                print(f"  - {d}")
            
            if len(orientations) > 1:
                if has_weighted:
                    print("✓ PASS: Capacity-weighted POA exists")
                else:
                    print("✗ FAIL: Missing capacity-weighted POA!")
            else:
                print("ℹ Single orientation (weighted not needed)")
                
        else:
            print("\n✗ FAIL: No POA data imported")
    
    print("\n" + "=" * 70)
    print("RE-IMPORT COMPLETE")
    print("=" * 70)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n✗ ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
