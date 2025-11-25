"""Diagnose Man City data issues"""
import pandas as pd

csv_path = 'C:/Users/PeterHall/OneDrive - AMPYR IDEA UK Ltd/Monthly Excom/Monthly SolarGIS data/October 2025/City_Football_Group_Phase_1.csv'
df = pd.read_csv(csv_path)

print("Man City October CSV Diagnosis")
print("="*80)

# Check unique arrays
df['ArrayKey'] = (
    df['name'].astype(str).str.strip() + "|" +
    df['array_capacity'].astype(str) + "|" +
    df['azimuth'].astype(str) + "|" +
    df['slope'].astype(str)
)

print(f"\nTotal rows: {len(df)}")
print(f"Unique arrays: {df['ArrayKey'].nunique()}")
print(f"\nArray details:")
for key in df['ArrayKey'].unique():
    subset = df[df['ArrayKey'] == key]
    cap = subset['array_capacity'].iloc[0]
    az = subset['azimuth'].iloc[0]
    sl = subset['slope'].iloc[0]
    gti_sum = subset['gti'].sum()
    gti_nonzero = (subset['gti'] > 0).sum()
    rows = len(subset)
    
    print(f"\n  {key}")
    print(f"    Capacity: {cap} kWp")
    print(f"    Azimuth: {az}°, Slope: {sl}°")
    print(f"    Rows: {rows}")
    print(f"    GTI sum: {gti_sum:.2f} kWh/m²")
    print(f"    Non-zero GTI values: {gti_nonzero} / {rows} ({gti_nonzero/rows*100:.1f}%)")
    
    # Check date range
    df['time'] = pd.to_datetime(df['time'], utc=True)
    subset_time = df[df['ArrayKey'] == key]
    print(f"    Date range: {subset_time['time'].min()} to {subset_time['time'].max()}")
    
    # Check for NaN values
    nan_count = subset['gti'].isna().sum()
    if nan_count > 0:
        print(f"    WARNING: {nan_count} NaN values in GTI!")

print("\n" + "="*80)
print("\nChecking time series continuity...")
df['time'] = pd.to_datetime(df['time'], utc=True)

# Check first array
first_key = df['ArrayKey'].iloc[0]
first_array = df[df['ArrayKey'] == first_key].copy()
first_array = first_array.sort_values('time')

# Check for gaps
time_diffs = first_array['time'].diff()
expected_diff = pd.Timedelta(minutes=15)
gaps = time_diffs[time_diffs != expected_diff].dropna()

if len(gaps) > 0:
    print(f"  WARNING: Found {len(gaps)} time gaps in first array!")
    for idx, gap in gaps.items():
        print(f"    Gap at {first_array.loc[idx, 'time']}: {gap}")
else:
    print(f"  ✓ No gaps in time series for first array")

print(f"\nExpected timesteps: 31 days × 96 = 2976")
print(f"Actual timesteps per array: {len(first_array)}")
