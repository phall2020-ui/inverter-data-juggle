"""Test single plant import with verbose output"""
import os
from plant_store import PlantStore
from solargis_poa_import import import_poa_for_plant_multi_folder, store_poa_in_db

store = PlantStore('plant_registry.sqlite')
base_dir = os.path.expanduser("~/OneDrive - AMPYR IDEA UK Ltd/Monthly Excom/Monthly SolarGIS data")

all_items = os.listdir(base_dir)
solargis_folders = [os.path.join(base_dir, item) for item in all_items if os.path.isdir(os.path.join(base_dir, item))]

print(f"Testing with Blachford UK...")
print(f"SolarGIS folders: {solargis_folders}")
print("="*80)

try:
    poa_df = import_poa_for_plant_multi_folder(
        plant_name='Blachford UK',
        plant_uid='AMP:00024',
        solargis_folders=solargis_folders,
        start_date="20250601",
        end_date="20251130",
        store=store,
        fuzzy_threshold=0.5
    )
    
    print(f"\nReturned dataframe:")
    if poa_df is None:
        print("  None")
    elif poa_df.empty:
        print("  Empty dataframe")
    else:
        print(f"  Rows: {len(poa_df)}")
        print(f"  Columns: {list(poa_df.columns)}")
        print(f"  Head:")
        print(poa_df.head())
        
except Exception as e:
    print(f"\nError: {e}")
    import traceback
    traceback.print_exc()
