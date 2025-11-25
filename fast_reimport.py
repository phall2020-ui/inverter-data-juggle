"""Fast POA re-import for all plants"""
import os
import sys
import time
from plant_store import PlantStore
from solargis_poa_import import import_poa_for_plant_multi_folder, store_poa_in_db

def main():
    print("="*80)
    print("FAST POA RE-IMPORT")
    print("="*80)
    
    start_time = time.time()
    
    store = PlantStore('plant_registry.sqlite')
    base_dir = os.path.expanduser("~/OneDrive - AMPYR IDEA UK Ltd/Monthly Excom/Monthly SolarGIS data")
    
    all_items = os.listdir(base_dir)
    solargis_folders = [os.path.join(base_dir, item) for item in all_items if os.path.isdir(os.path.join(base_dir, item))]
    
    print(f"\nFound {len(solargis_folders)} SolarGIS folders")
    
    all_plants = store.list_all()
    print(f"Processing {len(all_plants)} plants\n")
    
    success = []
    failed = []
    no_data = []
    
    for i, plant_rec in enumerate(all_plants, 1):
        plant_alias = plant_rec['alias']
        plant_uid = plant_rec['plant_uid']
        
        print(f"\n[{i}/{len(all_plants)}] {plant_alias}...", end=" ", flush=True)
        
        try:
            # Delete old POA
            deleted = store.delete_devices_by_pattern(plant_uid, 'POA:%')
            
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
                print(f"OK ({dc:.1f} kW)")
                success.append(plant_alias)
            else:
                print("No data")
                no_data.append(plant_alias)
                
        except Exception as e:
            print(f"ERROR: {e}")
            failed.append((plant_alias, str(e)))
    
    elapsed = time.time() - start_time
    
    print("\n"+"="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Success: {len(success)}")
    print(f"No data: {len(no_data)}")
    print(f"Failed: {len(failed)}")
    print(f"Time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    
    if failed:
        print("\nFailed plants:")
        for name, error in failed:
            print(f"  - {name}: {error}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
