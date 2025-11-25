#!/usr/bin/env python3
"""
Targeted re-import with exact CSV file matching
"""

import os
from plant_store import PlantStore
from solargis_poa_import import load_solargis_csv, calculate_capacity_weighted_poa, store_poa_in_db
from plant_csv_mapping import PLANT_FILE_MAPPING

def find_exact_csv_files(solargis_folders, csv_base_name):
    """Find all CSV files matching the exact base name across all folders"""
    matching_files = []
    
    for folder in solargis_folders:
        if not os.path.exists(folder):
            continue
        
        for filename in os.listdir(folder):
            if not filename.lower().endswith('.csv'):
                continue
            
            # Extract base name without extension
            file_base = os.path.splitext(filename)[0]
            
            # Exact match (case insensitive)
            if file_base.lower() == csv_base_name.lower():
                filepath = os.path.join(folder, filename)
                matching_files.append((filepath, filename))
    
    return matching_files

def main():
    print("=" * 80)
    print("TARGETED POA RE-IMPORT WITH EXACT MATCHING")
    print("=" * 80)
    
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
    
    # Get all plants
    all_plants = store.list_all()
    
    imported_count = 0
    skipped_count = 0
    error_count = 0
    
    for plant_rec in all_plants:
        plant_alias = plant_rec['alias']
        plant_uid = plant_rec['plant_uid']
        
        # Get exact CSV mapping
        csv_base, expected_capacity = PLANT_FILE_MAPPING.get(plant_alias, (None, None))
        
        if not csv_base:
            print(f"\n⊘ {plant_alias}: No CSV file mapping (skipping)")
            skipped_count += 1
            continue
        
        print("\n" + "=" * 80)
        print(f"Plant: {plant_alias} ({plant_uid})")
        print(f"Looking for: {csv_base}.csv (Expected: {expected_capacity} kW)")
        print("=" * 80)
        
        try:
            # Find exact matching CSV files
            matching_files = find_exact_csv_files(solargis_folders, csv_base)
            
            if not matching_files:
                print(f"  ✗ No matching CSV files found for '{csv_base}'")
                error_count += 1
                continue
            
            print(f"  ✓ Found {len(matching_files)} matching file(s):")
            for filepath, filename in matching_files:
                folder_name = os.path.basename(os.path.dirname(filepath))
                print(f"    - [{folder_name}] {filename}")
            
            # Delete old POA data
            print("\n  🗑️  Deleting old POA data...")
            poa_deleted = store.delete_devices_by_pattern(plant_uid, 'POA:%')
            if poa_deleted > 0:
                print(f"     Deleted {poa_deleted} POA records")
            
            # Load all matching files
            print("\n  📥 Loading CSV files...")
            dfs_and_mappings = []
            for filepath, filename in matching_files:
                try:
                    df, mapping = load_solargis_csv(filepath)
                    dfs_and_mappings.append((df, mapping))
                    print(f"     ✓ Loaded {filename}")
                except Exception as e:
                    print(f"     ✗ Failed to load {filename}: {e}")
            
            if not dfs_and_mappings:
                print(f"  ✗ Could not load any files")
                error_count += 1
                continue
            
            # Calculate capacity-weighted POA
            print("\n  ⚙️  Processing POA data...")
            poa_df = calculate_capacity_weighted_poa(
                dfs_and_mappings,
                start_date="20250601",
                end_date="20251130"
            )
            
            if poa_df is not None and not poa_df.empty:
                # Store in database
                print("\n  💾 Storing in database...")
                store_poa_in_db(store, plant_uid, poa_df)
                
                # Verify capacity
                updated_plant = store.load(plant_alias)
                actual_capacity = updated_plant.get('dc_size_kw', 0)
                diff = abs(actual_capacity - expected_capacity)
                diff_pct = (diff / expected_capacity * 100) if expected_capacity > 0 else 0
                
                print(f"\n  📊 Verification:")
                print(f"     Expected: {expected_capacity:.1f} kW")
                print(f"     Actual:   {actual_capacity:.1f} kW")
                print(f"     Diff:     {diff:.1f} kW ({diff_pct:.1f}%)")
                
                if diff_pct <= 1.0:
                    print(f"     ✅ PASS (within 1%)")
                    imported_count += 1
                else:
                    print(f"     ⚠️  MISMATCH (>{diff_pct:.1f}%)")
                    error_count += 1
            else:
                print(f"\n  ✗ No POA data generated")
                error_count += 1
                
        except Exception as e:
            print(f"\n  ✗ ERROR: {type(e).__name__}: {e}")
            error_count += 1
    
    print("\n" + "=" * 80)
    print("RE-IMPORT COMPLETE")
    print("=" * 80)
    print(f"✅ Success: {imported_count} plants")
    print(f"⚠️  Errors: {error_count} plants")
    print(f"⊘  Skipped: {skipped_count} plants (no CSV)")
    
    # Run verification
    if imported_count > 0:
        print("\n" + "=" * 80)
        print("Running final capacity verification...")
        print("=" * 80)
        
        import subprocess
        subprocess.run(['python', 'verify_capacities.py'])

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nImport interrupted by user")
    except Exception as e:
        print(f"\n✗ FATAL ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
