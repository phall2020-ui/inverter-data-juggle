from solargis_poa_import import import_poa_for_plant_multi_folder, store_poa_in_db
from plant_store import PlantStore, DEFAULT_DB

# Configure folders and dates
base = r"C:\Users\PeterHall\OneDrive - AMPYR IDEA UK Ltd\Monthly Excom\Monthly SolarGIS data"
folders = [
    r"C:\Users\PeterHall\OneDrive - AMPYR IDEA UK Ltd\Monthly Excom\Monthly SolarGIS data\June 2025",
    r"C:\Users\PeterHall\OneDrive - AMPYR IDEA UK Ltd\Monthly Excom\Monthly SolarGIS data\July 2025",
    r"C:\Users\PeterHall\OneDrive - AMPYR IDEA UK Ltd\Monthly Excom\Monthly SolarGIS data\August 2025",
    r"C:\Users\PeterHall\OneDrive - AMPYR IDEA UK Ltd\Monthly Excom\Monthly SolarGIS data\September 2025",
    r"C:\Users\PeterHall\OneDrive - AMPYR IDEA UK Ltd\Monthly Excom\Monthly SolarGIS data\October 2025",
]
start_date = '20250601'
end_date = '20251101'

store = PlantStore(DEFAULT_DB)

# Find Newfold alias and UID
plants = store.list_all()
newfold = None
for p in plants:
    if p['alias'].lower().find('newfold') != -1:
        newfold = p
        break

if not newfold:
    print('Newfold Farm not found in registry. Aborting.')
    raise SystemExit(1)

alias = newfold['alias']
uid = newfold['plant_uid']
print(f"Importing POA for {alias} ({uid}) from folders:\n  " + '\n  '.join(folders))

poa_df = import_poa_for_plant_multi_folder(alias, uid, folders, start_date, end_date, store, fuzzy_threshold=0.5)

if poa_df is None or poa_df.empty:
    print('No POA imported (no matching files or no data in date range).')
else:
    print(f'Imported {len(poa_df)} POA rows; storing in DB...')
    store_poa_in_db(store, uid, poa_df)
    print('Store complete.')
