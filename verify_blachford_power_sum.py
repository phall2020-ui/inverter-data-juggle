"""Confirm that exported ac_power equals the sum of all Blachford inverter feeds."""
import pandas as pd

from plant_store import PlantStore
from inverter_pipeline import load_db_dataframe

store = PlantStore()
ids = ['INVERT:001122', 'INVERT:001123', 'INVERT:001124']

inv_df = load_db_dataframe(store, 'Blachford UK', '20250601', '20250602', ids)
inv_df['timestamp'] = pd.to_datetime(inv_df['ts'], utc=True)
inv_df['ac_kw'] = inv_df['importActivePower'] / 1000.0
by_ts = inv_df.groupby('timestamp', as_index=False)['ac_kw'].sum()
by_ts = by_ts.rename(columns={'ac_kw': 'sum_kw'})

combo = pd.read_csv('blachford_fouling_dataset.csv', parse_dates=['timestamp'])
mask = (combo['timestamp'] >= '2025-06-01T00:00:00Z') & (combo['timestamp'] < '2025-06-02T00:00:00Z')
combo = combo.loc[mask, ['timestamp', 'ac_power']]
combo = combo.rename(columns={'ac_power': 'dataset_kw'})
combo['timestamp'] = combo['timestamp'].dt.tz_convert('UTC')

merged = combo.merge(by_ts, on='timestamp', how='inner')
merged['diff'] = merged['dataset_kw'] - merged['sum_kw']
print(merged.head())
print('Rows compared:', len(merged))
print('Max absolute diff (kW):', merged['diff'].abs().max())
