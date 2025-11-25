"""Delete all POA data and reimport for all plants"""
import os
import sys
import io
from plant_store import PlantStore

store = PlantStore('plant_registry.sqlite')

# Get all plants
all_plants = store.list_all()
print(f"Found {len(all_plants)} plants in database")

# Count and delete all POA records
total_deleted = 0
for plant in all_plants:
    plant_uid = plant.get('plant_uid')
    plant_name = plant.get('name', 'Unknown')
    
    deleted = store.delete_devices_by_pattern(plant_uid, 'POA:%')
    if deleted > 0:
        total_deleted += deleted
        print(f"  {plant_name}: deleted {deleted} POA records")

print(f"\nTotal POA records deleted: {total_deleted}")
print("="*80)

# Now reimport all POA data
from solargis_poa_import import import_poa_for_plant_multi_folder, store_poa_in_db

base_dir = os.path.expanduser("~/OneDrive - AMPYR IDEA UK Ltd/Monthly Excom/Monthly SolarGIS data")
all_items = os.listdir(base_dir)
solargis_folders = [os.path.join(base_dir, item) for item in all_items if os.path.isdir(os.path.join(base_dir, item))]

print(f"\nReimporting POA data for all plants...")
print(f"Date range: June 2025 - November 2025")
print("="*80)

# Track progress
imported_count = 0
failed_count = 0

for plant in all_plants:
    plant_name = plant.get('name', 'Unknown')
    plant_uid = plant.get('plant_uid')
    
    print(f"\n[{imported_count + failed_count + 1}/{len(all_plants)}] {plant_name}...")
    
    # Suppress verbose output
    old_stdout = sys.stdout
    sys.stdout = io.StringIO()
    
    try:
        poa_df = import_poa_for_plant_multi_folder(
            plant_name=plant_name,
            plant_uid=plant_uid,
            solargis_folders=solargis_folders,
            start_date="20250601",
            end_date="20251130",
            store=store,
            fuzzy_threshold=0.5
        )
        
        if poa_df is not None and not poa_df.empty:
            store_poa_in_db(store, plant_uid, poa_df)
            sys.stdout = old_stdout
            
            # Get updated capacity
            updated_plant = store.load(plant_name)
            dc = updated_plant.get('dc_size_kw', 0)
            orientations = len(poa_df[['azimuth', 'slope']].drop_duplicates())
            print(f"  ✓ Imported {len(poa_df)} records, {orientations} orientations, DC: {dc:.1f} kW")
            imported_count += 1
        else:
            sys.stdout = old_stdout
            print(f"  ⚠ No POA data found")
            failed_count += 1
            
    except Exception as e:
        sys.stdout = old_stdout
        print(f"  ✗ Error: {str(e)}")
        failed_count += 1

print("\n" + "="*80)
print(f"Import complete: {imported_count} succeeded, {failed_count} failed/no data")
