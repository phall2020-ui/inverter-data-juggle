"""Silent POA reimport"""
import os
import sys
import io
from plant_store import PlantStore
from solargis_poa_import import import_poa_for_plant_multi_folder, store_poa_in_db

# Suppress verbose output
old_stdout = sys.stdout
sys.stdout = io.StringIO()

try:
    store = PlantStore('plant_registry.sqlite')
    base_dir = os.path.expanduser("~/OneDrive - AMPYR IDEA UK Ltd/Monthly Excom/Monthly SolarGIS data")
    
    all_items = os.listdir(base_dir)
    solargis_folders = [os.path.join(base_dir, item) for item in all_items if os.path.isdir(os.path.join(base_dir, item))]
    
    all_plants = store.list_all()
    
    for plant_rec in all_plants:
        plant_alias = plant_rec['alias']
        plant_uid = plant_rec['plant_uid']
        
        store.delete_devices_by_pattern(plant_uid, 'POA:%')
        
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

finally:
    sys.stdout = old_stdout

print("Import complete")
