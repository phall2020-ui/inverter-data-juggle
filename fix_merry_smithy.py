#!/usr/bin/env python3
"""
Fix Merry Hill and Smithy's Mushrooms capacities
"""

import os
from plant_store import PlantStore
from solargis_poa_import import load_solargis_csv, calculate_capacity_weighted_poa, store_poa_in_db

def process_plant(store, plant_alias, csv_base, expected_capacity):
    """Process a single plant"""
    print(f"\n{'='*80}")
    print(f"Processing: {plant_alias}")
    print(f"CSV: {csv_base}.csv (Expected: {expected_capacity} kW)")
    print(f"{'='*80}")
    
    # Find plant in database
    plants = [p for p in store.list_all() if p['alias'] == plant_alias]
    if not plants:
        print(f"  ERROR: Plant not found in database")
        return False
    
    plant = plants[0]
    plant_uid = plant['plant_uid']
    
    # Find CSV files
    base_dir = os.path.expanduser("~/OneDrive - AMPYR IDEA UK Ltd/Monthly Excom/Monthly SolarGIS data")
    all_items = os.listdir(base_dir)
    folders = [os.path.join(base_dir, item) for item in all_items if os.path.isdir(os.path.join(base_dir, item))]
    
    matching_files = []
    for folder in folders:
        for filename in os.listdir(folder):
            if not filename.lower().endswith('.csv'):
                continue
            file_base = os.path.splitext(filename)[0]
            if file_base.lower() == csv_base.lower():
                matching_files.append((os.path.join(folder, filename), filename))
    
    if not matching_files:
        print(f"  ERROR: No CSV files found matching '{csv_base}'")
        return False
    
    print(f"  Found {len(matching_files)} CSV file(s)")
    
    # Delete old POA data
    print(f"  Deleting old POA data...")
    deleted = store.delete_devices_by_pattern(plant_uid, 'POA:%')
    print(f"  Deleted {deleted} records")
    
    # Load all CSVs
    print(f"  Loading CSV files...")
    dfs_and_mappings = []
    for filepath, filename in matching_files:
        try:
            df, mapping = load_solargis_csv(filepath)
            dfs_and_mappings.append((df, mapping))
            print(f"    ✓ {filename}")
        except Exception as e:
            print(f"    ✗ {filename}: {e}")
    
    if not dfs_and_mappings:
        print(f"  ERROR: No data loaded")
        return False
    
    # Calculate capacity-weighted POA
    print(f"  Processing POA data...")
    poa_df = calculate_capacity_weighted_poa(
        dfs_and_mappings,
        start_date="20250601",
        end_date="20251130"
    )
    
    if poa_df is None or poa_df.empty:
        print(f"  ERROR: No POA data generated")
        return False
    
    # Store in database
    print(f"  Storing in database...")
    store_poa_in_db(store, plant_uid, poa_df)
    
    # Verify capacity
    updated_plant = store.load(plant_alias)
    actual_capacity = updated_plant.get('dc_size_kw', 0)
    diff = abs(actual_capacity - expected_capacity)
    diff_pct = (diff / expected_capacity * 100) if expected_capacity > 0 else 0
    
    print(f"\n  Verification:")
    print(f"    Expected: {expected_capacity} kW")
    print(f"    Actual: {actual_capacity:.1f} kW")
    print(f"    Difference: {diff:.1f} kW ({diff_pct:.2f}%)")
    
    if diff_pct < 1.0:
        print(f"    ✓ PASS")
        return True
    else:
        print(f"    ✗ FAIL")
        return False

def main():
    store = PlantStore('plant_registry.sqlite')
    
    plants_to_fix = [
        ('Merry Hill Shopping Centre', 'Merry_Hill', 996),
        ("Smithy's Mushrooms", 'Smithys_Mushrooms', 224),
    ]
    
    results = []
    for plant_alias, csv_base, expected_capacity in plants_to_fix:
        success = process_plant(store, plant_alias, csv_base, expected_capacity)
        results.append((plant_alias, success))
    
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    for plant_alias, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"  {plant_alias}: {status}")

if __name__ == "__main__":
    main()


def main():
    store = PlantStore('plant_registry.sqlite')
    
    plants_to_fix = [
        ('Merry Hill Shopping Centre', 'Merry_Hill', 996),
        ("Smithy's Mushrooms", 'Smithys_Mushrooms', 224),
    ]
    
    results = []
    for plant_alias, csv_base, expected_capacity in plants_to_fix:
        success = process_plant(store, plant_alias, csv_base, expected_capacity)
        results.append((plant_alias, success))
    
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    for plant_alias, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"  {plant_alias}: {status}")

if __name__ == "__main__":
    main()
