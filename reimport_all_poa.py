"""Reimport all POA data for all plants"""
import os
import sys
import io
from plant_store import PlantStore
from solargis_poa_import import import_poa_for_plant_multi_folder, store_poa_in_db

store = PlantStore('plant_registry.sqlite')
base_dir = os.path.expanduser("~/OneDrive - AMPYR IDEA UK Ltd/Monthly Excom/Monthly SolarGIS data")

all_items = os.listdir(base_dir)
solargis_folders = [os.path.join(base_dir, item) for item in all_items if os.path.isdir(os.path.join(base_dir, item))]

# Manual list of plants with known names
plants = [
    ('Blachford UK', 'AMP:00024'),
    ('Cromwell Tools', 'AMP:00001'),
    ('Man City FC Training Ground', 'AMP:00019'),
    ('Finlay Beverages', 'AMP:00031'),
    ('Metrocentre', 'AMP:00027'),
    ('Merry Hill', 'AMP:00005'),
    ('Smithy Mushrooms', 'AMP:00009'),
    ('Smithy Mushrooms Phase 2', 'AMP:00010'),
    ('Hibernian Stadium', 'AMP:00032'),
    ('Hibernian Training Ground', 'AMP:00033'),
    ('Oasis Media City', 'AMP:00036'),
    ('Sheldons Bakery', 'AMP:00013'),
    ('BAE Fylde', 'AMP:00002'),
]

print(f"Reimporting POA data for {len(plants)} plants...")
print(f"Date range: June 2025 - November 2025")
print("="*80)

imported_count = 0
failed_count = 0

for plant_name, plant_uid in plants:
    print(f"\n[{imported_count + failed_count + 1}/{len(plants)}] {plant_name}...")
    
    # Delete old POA data
    deleted = store.delete_devices_by_pattern(plant_uid, 'POA:%')
    if deleted > 0:
        print(f"  Deleted {deleted} old POA records")
    
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
        
        sys.stdout = old_stdout
        
        if poa_df is not None and not poa_df.empty:
            # Store in database
            old_stdout = sys.stdout
            sys.stdout = io.StringIO()
            store_poa_in_db(store, plant_uid, poa_df)
            sys.stdout = old_stdout
            
            # Get updated capacity
            updated_plant = store.load(plant_name)
            dc = updated_plant.get('dc_size_kw', 0)
            orientations = len(poa_df[['azimuth', 'slope']].drop_duplicates())
            print(f"  ✓ Imported {len(poa_df)} records, {orientations} orientations, DC: {dc:.1f} kW")
            imported_count += 1
        else:
            print(f"  ⚠ No POA data found")
            failed_count += 1
            
    except Exception as e:
        sys.stdout = old_stdout
        print(f"  ✗ Error: {str(e)}")
        failed_count += 1

print("\n" + "="*80)
print(f"Import complete: {imported_count} succeeded, {failed_count} failed/no data")
