"""Debug Man City import with verbose output"""
import os
import sys
from plant_store import PlantStore
from solargis_poa_import import import_poa_for_plant_multi_folder, store_poa_in_db

store = PlantStore('plant_registry.sqlite')
base_dir = os.path.expanduser("~/OneDrive - AMPYR IDEA UK Ltd/Monthly Excom/Monthly SolarGIS data")

all_items = os.listdir(base_dir)
solargis_folders = [os.path.join(base_dir, item) for item in all_items if os.path.isdir(os.path.join(base_dir, item))]

plant_alias = 'Man City FC Training Ground'
plant_uid = 'AMP:00019'

print(f"Importing {plant_alias} with VERBOSE output...")
print("="*80)

# Delete old data
deleted = store.delete_devices_by_pattern(plant_uid, 'POA:%')
print(f"Deleted {deleted} old POA records\n")

# Import with verbose output (don't suppress)
poa_df = import_poa_for_plant_multi_folder(
    plant_name=plant_alias,
    plant_uid=plant_uid,
    solargis_folders=solargis_folders,
    start_date="20251001",
    end_date="20251031",  # October only
    store=store,
    fuzzy_threshold=0.5
)

print(f"\n>>>>>> poa_df type: {type(poa_df)}")
print(f">>>>>> poa_df is None: {poa_df is None}")
if poa_df is not None:
    print(f">>>>>> poa_df.empty: {poa_df.empty}")
    print(f">>>>>> poa_df.shape: {poa_df.shape}")

if poa_df is not None and not poa_df.empty:
    print("\n" + "="*80)
    print("RETURNED DATAFRAME INSPECTION:")
    print("="*80)
    print(f"Shape: {poa_df.shape}")
    print(f"Columns: {poa_df.columns.tolist()}")
    print(f"\nFirst 10 rows:")
    print(poa_df.head(10))
    print(f"\nPOA statistics:")
    print(poa_df['poa'].describe())
    print(f"\nPOA sum by orientation:")
    if 'azimuth' in poa_df.columns:
        for (az, sl), group in poa_df.groupby(['azimuth', 'slope']):
            print(f"  AZ{int(az):3d}:SL{int(sl):2d}: sum={group['poa'].sum():.2f}, records={len(group)}")
    print("="*80)
    
    print("\nStoring in database...")
    store_poa_in_db(store, plant_uid, poa_df)
    print("\nDone!")
else:
    print("ERROR: No data imported!")
