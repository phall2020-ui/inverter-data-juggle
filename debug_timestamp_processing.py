"""Debug timestamp processing"""
import pandas as pd

csv_path = 'C:/Users/PeterHall/OneDrive - AMPYR IDEA UK Ltd/Monthly Excom/Monthly SolarGIS data/October 2025/City_Football_Group_Phase_1.csv'
df = pd.read_csv(csv_path)

print("Step-by-step debug of timestamp processing:")
print("="*80)

# Filter to first array only
first_array = df[df['azimuth'] == 350.0].copy()
print(f"\n1. Raw CSV data (first array, AZ350):")
print(f"   Rows: {len(first_array)}")
print(f"   First 3 timestamps:")
print(first_array['time'].head(3))
print(f"   First 3 GTI values:")
print(first_array['gti'].head(3))

# Parse timestamps
first_array['ts'] = pd.to_datetime(first_array['time'], utc=True)
print(f"\n2. After parsing to datetime:")
print(f"   First 3 timestamps:")
print(first_array['ts'].head(3))

# Filter to October
start_dt = pd.to_datetime('20251001', format='%Y%m%d', utc=True)
end_dt = pd.to_datetime('20251031', format='%Y%m%d', utc=True)
mask = (first_array['ts'] >= start_dt) & (first_array['ts'] <= end_dt)
filtered = first_array[mask].copy()

print(f"\n3. After filtering to October 1-31:")
print(f"   Rows: {len(filtered)}")
print(f"   Date range: {filtered['ts'].min()} to {filtered['ts'].max()}")
print(f"   GTI sum before groupby: {filtered['gti'].sum():.2f}")

# Group by timestamp
gti_by_ts = filtered.groupby('ts')['gti'].mean()
print(f"\n4. After groupby('ts').mean():")
print(f"   Unique timestamps: {len(gti_by_ts)}")
print(f"   First 5 values:")
print(gti_by_ts.head())
print(f"   Sum: {gti_by_ts.sum():.2f}")

# Normalize timestamps
gti_by_ts.index = gti_by_ts.index.floor('min')
print(f"\n5. After floor('min'):")
print(f"   First 5 index values:")
print(gti_by_ts.index[:5])
print(f"   First 5 values:")
print(gti_by_ts.head())
print(f"   Sum: {gti_by_ts.sum():.2f}")

# Resample
resampled = gti_by_ts.resample('30min').sum()
print(f"\n6. After resample('30min').sum():")
print(f"   Records: {len(resampled)}")
print(f"   First 10 values:")
print(resampled.head(10))
print(f"   Sum: {resampled.sum():.2f}")
print(f"   Non-zero count: {(resampled > 0).sum()}")
