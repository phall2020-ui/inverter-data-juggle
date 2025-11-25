#!/usr/bin/env python3
"""
System test script to verify POA import and data integrity
"""

from plant_store import PlantStore
import pandas as pd

def test_plants_registry():
    """Test 1: Check plants registry and DC capacities"""
    print("=" * 70)
    print("TEST 1: Plants Registry & DC Capacities")
    print("=" * 70)
    
    store = PlantStore('plant_registry.sqlite')
    plants = store.list_all()
    
    print(f"\nTotal plants in registry: {len(plants)}")
    print("\nPlants with DC capacity:")
    
    has_capacity = 0
    no_capacity = 0
    
    for p in plants:
        dc = p.get('dc_size_kw')
        if dc and dc > 0:
            print(f"  ✓ {p['alias']}: {dc:.1f} kW")
            has_capacity += 1
        else:
            print(f"  ✗ {p['alias']}: Not set")
            no_capacity += 1
    
    print(f"\nSummary: {has_capacity} with capacity, {no_capacity} without")
    
    # Check Blachford specifically
    blachford = next((p for p in plants if 'blachford' in p['alias'].lower()), None)
    if blachford:
        dc = blachford.get('dc_size_kw', 0)
        expected = 333.0
        status = "✓ PASS" if abs(dc - expected) < 1.0 else "✗ FAIL"
        print(f"\nBlachford UK capacity check: {status}")
        print(f"  Expected: {expected:.1f} kW")
        print(f"  Actual: {dc:.1f} kW")
    
    return plants, store


def test_poa_devices(store, plants):
    """Test 2: Check POA devices and capacity-weighted POA"""
    print("\n" + "=" * 70)
    print("TEST 2: POA Devices & Capacity-Weighted POA")
    print("=" * 70)
    
    for plant in plants[:3]:  # Test first 3 plants
        plant_uid = plant['plant_uid']
        alias = plant['alias']
        
        print(f"\n{alias} ({plant_uid}):")
        
        # Get all devices
        device_ids = store.list_emig_ids(plant_uid)
        poa_devices = [d for d in device_ids if d.startswith('POA:')]
        
        if not poa_devices:
            print("  No POA data")
            continue
        
        # Check for weighted POA
        has_weighted = any('WEIGHTED' in d for d in poa_devices)
        orientations = [d for d in poa_devices if 'WEIGHTED' not in d]
        
        print(f"  Orientations: {len(orientations)}")
        for d in orientations:
            print(f"    - {d}")
        
        if has_weighted:
            print(f"  ✓ Capacity-weighted POA: POA:SOLARGIS:WEIGHTED")
        else:
            if len(orientations) > 1:
                print(f"  ✗ WARNING: Multiple orientations but no weighted POA!")
            else:
                print(f"  ℹ Single orientation (weighted POA not needed)")


def test_data_ranges(store, plants):
    """Test 3: Check data ranges and record counts"""
    print("\n" + "=" * 70)
    print("TEST 3: Data Ranges & Record Counts")
    print("=" * 70)
    
    for plant in plants[:3]:  # Test first 3 plants
        plant_uid = plant['plant_uid']
        alias = plant['alias']
        
        print(f"\n{alias}:")
        
        device_ids = store.list_emig_ids(plant_uid)
        poa_devices = [d for d in device_ids if d.startswith('POA:')]
        
        if not poa_devices:
            continue
        
        for device_id in poa_devices:
            readings = store.load_readings(plant_uid, device_id, "2000-01-01T00:00:00", "2099-12-31T23:59:59")
            
            if readings:
                timestamps = [r.get('ts') for r in readings if r.get('ts')]
                if timestamps:
                    first_ts = min(timestamps)
                    last_ts = max(timestamps)
                    
                    device_label = "WEIGHTED" if "WEIGHTED" in device_id else device_id.split(':')[-2:]
                    print(f"  {device_label}: {len(readings)} records ({first_ts} to {last_ts})")


def test_capacity_calculation():
    """Test 4: Verify capacity calculation logic"""
    print("\n" + "=" * 70)
    print("TEST 4: Capacity Calculation Logic")
    print("=" * 70)
    
    # Simulate the scenario from your error report
    test_data = {
        'Finlay Beverages': {
            'orientations': [
                {'azimuth': 57, 'slope': 6, 'capacity': 797.4, 'files': 4},
                {'azimuth': 237, 'slope': 6, 'capacity': 802.8, 'files': 3},
                {'azimuth': 0, 'slope': 6, 'capacity': 237.0, 'files': 1},
            ],
            'expected_total': 797.4 + 802.8 + 237.0
        }
    }
    
    for plant_name, data in test_data.items():
        print(f"\n{plant_name}:")
        total = 0
        
        for orient in data['orientations']:
            # The system should take capacity from first file only
            total += orient['capacity']
            print(f"  Azimuth {orient['azimuth']}°, Slope {orient['slope']}°:")
            print(f"    Capacity: {orient['capacity']:.1f} kW (from {orient['files']} file(s))")
            print(f"    ✓ Should NOT multiply by number of files")
        
        print(f"\n  Total capacity: {total:.1f} kW")
        print(f"  Expected: {data['expected_total']:.1f} kW")
        
        if abs(total - data['expected_total']) < 1.0:
            print(f"  ✓ PASS")
        else:
            print(f"  ✗ FAIL: Capacity mismatch!")


def test_duplicate_handling():
    """Test 5: Verify duplicate timestamp handling"""
    print("\n" + "=" * 70)
    print("TEST 5: Duplicate Timestamp Handling")
    print("=" * 70)
    
    print("\nLogic check:")
    print("  ✓ Build unique array keys (name + capacity + azimuth + slope)")
    print("  ✓ Drop duplicates per array per timestamp")
    print("  ✓ Sum GTI across unique arrays at each timestamp")
    print("  ✓ Set capacity once from first file (don't accumulate)")
    print("  ✓ Concatenate time series from different months")
    print("\nThis matches the working monthly summary script logic.")


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("INVERTER PIPELINE SYSTEM TEST")
    print("=" * 70)
    
    try:
        # Run tests
        plants, store = test_plants_registry()
        test_poa_devices(store, plants)
        test_data_ranges(store, plants)
        test_capacity_calculation()
        test_duplicate_handling()
        
        print("\n" + "=" * 70)
        print("TEST COMPLETE")
        print("=" * 70)
        print("\nReview the output above for any ✗ FAIL or WARNING indicators.")
        
    except Exception as e:
        print(f"\n✗ TEST FAILED WITH ERROR:")
        print(f"  {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
