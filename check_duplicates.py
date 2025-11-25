"""Check for duplicate timestamps after floor"""
import pandas as pd

csv_path = 'C:/Users/PeterHall/OneDrive - AMPYR IDEA UK Ltd/Monthly Excom/Monthly SolarGIS data/October 2025/City_Football_Group_Phase_1.csv'
df = pd.read_csv(csv_path)

# Filter to first array
first_array = df[df['azimuth'] == 350.0].copy()
first_array['ts'] = pd.to_datetime(first_array['time'], utc=True)

# Filter to October (with +1 day fix)
start_dt = pd.to_datetime('20251001', format='%Y%m%d', utc=True)
end_dt = pd.to_datetime('20251031', format='%Y%m%d', utc=True) + pd.Timedelta(days=1)
mask = (first_array['ts'] >= start_dt) & (first_array['ts'] < end_dt)
filtered = first_array[mask].copy()

print(f"After filtering with +1 day fix:")
print(f"  Rows: {len(filtered)}")
print(f"  Date range: {filtered['ts'].min()} to {filtered['ts'].max()}")
print(f"  GTI sum: {filtered['gti'].sum():.2f}")

# Group by timestamp
gti_by_ts = filtered.groupby('ts')['gti'].mean()
print(f"\nAfter groupby:")
print(f"  Unique timestamps: {len(gti_by_ts)}")
print(f"  GTI sum: {gti_by_ts.sum():.2f}")

# Floor timestamps
gti_by_ts.index = gti_by_ts.index.floor('min')
print(f"\nAfter floor('min'):")
print(f"  Unique timestamps: {len(gti_by_ts)}")
print(f"  GTI sum: {gti_by_ts.sum():.2f}")

# Check for duplicates
duplicates = gti_by_ts.index.duplicated()
print(f"  Duplicate timestamps: {duplicates.sum()}")

if duplicates.sum() > 0:
    print(f"\n  WARNING: floor('min') created {duplicates.sum()} duplicate timestamps!")
    print(f"  Example duplicates:")
    dup_times = gti_by_ts.index[duplicates][:10]
    for t in dup_times:
        vals = gti_by_ts[gti_by_ts.index == t]
        print(f"    {t}: {len(vals)} values = {vals.tolist()}")
