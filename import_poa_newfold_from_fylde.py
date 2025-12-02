from solargis_poa_import import import_poa_for_plant_multi_folder, store_poa_in_db
from plant_store import PlantStore, DEFAULT_DB

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
# Newfold Farm UID discovered earlier is ERS:00001
plant_uid = 'ERS:00001'
plant_alias = store.alias_for(plant_uid) or 'Newfold Farm'
print(f"Storing POA into plant: {plant_alias} ({plant_uid}) using source name 'Fylde'")

poa_df = import_poa_for_plant_multi_folder('Fylde', plant_uid, folders, start_date, end_date, store, fuzzy_threshold=0.5)

if poa_df is None or poa_df.empty:
    print('No POA imported (no matching files or no data in date range).')
else:
    print(f'Imported {len(poa_df)} POA rows; storing in DB...')
    store_poa_in_db(store, plant_uid, poa_df)
    print('Store complete.')
