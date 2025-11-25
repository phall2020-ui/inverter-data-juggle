"""Compare instantaneous power vs energy-derived average power."""
import pandas as pd
from plant_store import PlantStore
from inverter_pipeline import load_db_dataframe

store = PlantStore()
df = load_db_dataframe(store, 'Blachford UK', '20250708', '20250709', 
                      ['INVERT:001122', 'INVERT:001123', 'INVERT:001124'])

df['timestamp'] = pd.to_datetime(df['ts'], utc=True)
df = df.sort_values(['emigId', 'timestamp'])

print("Comparison at peak time (2025-07-08 11:30:00):")
print("="*80)

total_instantaneous = 0
total_energy_derived = 0

for inv in ['INVERT:001122', 'INVERT:001123', 'INVERT:001124']:
    inv_df = df[df['emigId'] == inv].copy()
    inv_df['energy_delta'] = inv_df['importEnergy'].diff()
    inv_df['avg_power_kw'] = inv_df['energy_delta'] / 1000 / 0.5  # Wh to kW over 0.5h
    
    peak = inv_df.loc[inv_df['timestamp'] == '2025-07-08 11:30:00+00:00']
    if not peak.empty:
        inst_power = peak['importActivePower'].values[0] / 1000
        energy_delta = peak['energy_delta'].values[0]
        avg_power = peak['avg_power_kw'].values[0]
        
        print(f"\n{inv}:")
        print(f"  importActivePower (instantaneous): {inst_power:.3f} kW")
        print(f"  Energy delta: {energy_delta:.0f} Wh")
        print(f"  Average power from energy: {avg_power:.3f} kW")
        print(f"  Ratio (energy/instantaneous): {avg_power/inst_power:.3f}")
        
        total_instantaneous += inst_power
        total_energy_derived += avg_power

print("\n" + "="*80)
print(f"\nTotal using importActivePower: {total_instantaneous:.3f} kW")
print(f"Total using energy deltas: {total_energy_derived:.3f} kW")
print(f"Factor difference: {total_energy_derived / total_instantaneous:.3f}x")
print(f"\nDataset peak AC power: 76.566 kW")
print(f"Matches instantaneous: {abs(total_instantaneous - 76.566) < 0.01}")
print(f"Matches energy-derived: {abs(total_energy_derived - 76.566) < 0.01}")
