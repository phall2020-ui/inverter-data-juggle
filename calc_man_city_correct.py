"""Calculate correct October weighted POA from CSV"""
import pandas as pd

csv_path = 'C:/Users/PeterHall/OneDrive - AMPYR IDEA UK Ltd/Monthly Excom/Monthly SolarGIS data/October 2025/City_Football_Group_Phase_1.csv'
df = pd.read_csv(csv_path)

# Group by orientation
orientations = df.groupby(['azimuth', 'slope']).agg({
    'gti': 'sum',
    'array_capacity': 'first'
})

print("October Man City - CSV Calculation:")
print("="*80)
print(f"{'Orientation':<20} {'Capacity':>10} {'GTI Sum':>12} {'Weighted':>12}")
print("-"*80)

total_capacity = orientations['array_capacity'].sum()
weighted_sum = 0

for (az, sl), row in orientations.iterrows():
    capacity = row['array_capacity']
    gti_sum = row['gti']
    weighted = gti_sum * capacity
    weighted_sum += weighted
    print(f"AZ{int(az):3d}:SL{int(sl):2d} {capacity:>10.1f} kWp {gti_sum:>12.2f} {weighted:>12.1f}")

weighted_avg = weighted_sum / total_capacity

print("-"*80)
print(f"{'Total':<20} {total_capacity:>10.1f} kWp {'':>12} {weighted_sum:>12.1f}")
print(f"\nWeighted Average POA: {weighted_avg:.2f} kWh/m²")
print(f"Reference value: 37.74 kWh/m²")
print(f"Difference: {(weighted_avg - 37.74):.2f} kWh/m² ({(weighted_avg - 37.74)/37.74*100:+.1f}%)")
