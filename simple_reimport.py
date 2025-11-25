"""Simple POA re-import without unicode"""
import os
from plant_store import PlantStore
from solargis_poa_import import import_poa_for_plant_multi_folder, store_poa_in_db

print("="*80)
print("POA RE-IMPORT")
print("="*80)

store = PlantStore('plant_registry.sqlite')
base_dir = os.path.expanduser("~/OneDrive - AMPYR IDEA UK Ltd/Monthly Excom/Monthly SolarGIS data")

all_items = os.listdir(base_dir)
solargis_folders = [os.path.join(base_dir, item) for item in all_items if os.path.isdir(os.path.join(base_dir, item))]

print(f"\nFound {len(solargis_folders)} folders")

all_plants = store.list_all()
print(f"Processing {len(all_plants)} plants\n")

for plant_rec in all_plants:
    plant_alias = plant_rec['alias']
    plant_uid = plant_rec['plant_uid']
    
    print(f"\n{plant_alias} ({plant_uid}):")
    
    try:
        # Delete old POA
        poa_deleted = store.delete_devices_by_pattern(plant_uid, 'POA:%')
        if poa_deleted > 0:
            print(f"  Deleted {poa_deleted} records")
        
        # Import
        poa_df = import_poa_for_plant_multi_folder(
            plant_name=plant_alias,
            plant_uid=plant_uid,
            solargis_folders=solargis_folders,
            start_date="20250601",
            end_date="20251130",
            store=store,
            fuzzy_threshold=0.5
        )
        
        if poa_df is not None and not poa_df.empty:
            store_poa_in_db(store, plant_uid, poa_df)
            updated_plant = store.load(plant_alias)
            dc = updated_plant.get('dc_size_kw', 0)
            print(f"  SUCCESS - DC: {dc:.1f} kW")
        else:
            print(f"  No data found")
            
    except Exception as e:
        print(f"  ERROR: {e}")

print("\n"+"="*80)
print("COMPLETE")
