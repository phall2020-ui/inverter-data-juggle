"""Check Man City October CSV"""
import pandas as pd
import glob

csv = glob.glob('C:/Users/PeterHall/OneDrive - AMPYR IDEA UK Ltd/Monthly Excom/Monthly SolarGIS data/October 2025/City_Football_Group_Phase_1.csv')[0]
df = pd.read_csv(csv)

print(f'File: {csv}')
print(f'Rows: {len(df)}')
print(f'GTI sum: {df["gti"].sum():.2f}')
print(f'\nArrays:')
for name in df['name'].unique():
    subset = df[df['name'] == name]
    capacity = subset['array_capacity'].iloc[0]
    gti_sum = subset['gti'].sum()
    print(f'  {name}: {capacity} kWp, GTI sum = {gti_sum:.2f} kWh/m²')

# Calculate weighted average
total_cap = df.groupby('name')['array_capacity'].first().sum()
weighted_sum = 0
for name in df['name'].unique():
    subset = df[df['name'] == name]
    capacity = subset['array_capacity'].iloc[0]
    gti_sum = subset['gti'].sum()
    weighted_sum += gti_sum * capacity

weighted_avg = weighted_sum / total_cap
print(f'\nTotal capacity: {total_cap} kWp')
print(f'Weighted average: {weighted_avg:.2f} kWh/m²')
