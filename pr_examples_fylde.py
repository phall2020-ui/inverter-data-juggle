import sqlite3
import pandas as pd
from plant_store import PlantStore, DEFAULT_DB

# Find plant alias matching 'Fylde' (case-insensitive)
store = PlantStore(DEFAULT_DB)
plants = store.list_all()
fylde = None
for p in plants:
    if p['alias'].lower().find('fylde') != -1:
        fylde = p
        break

if not fylde:
    print("Plant 'Fylde' not found in registry. Available plants:")
    for p in plants:
        print(f" - {p['alias']}")
    raise SystemExit(1)

alias = fylde['alias']
plant_uid = fylde['plant_uid']
dc_kw = fylde.get('dc_size_kw') or 0.0

print(f"Selected plant: {alias} ({plant_uid}) - DC {dc_kw} kW")

conn = sqlite3.connect(DEFAULT_DB)

# Helper: get inverter aggregated power per timestamp (sum apparentPower.value)
def get_inverter_power(plant_uid, start_date, end_date):
    query = '''
        SELECT ts,
               SUM(CAST(json_extract(payload, '$.apparentPower.value') AS REAL)) as power_w,
               COUNT(DISTINCT emig_id) as inverter_count
        FROM readings
        WHERE plant_uid = ?
          AND emig_id LIKE 'INVERT:%'
          AND ts >= ? AND ts < ?
          AND json_extract(payload, '$.apparentPower.value') IS NOT NULL
        GROUP BY ts
        ORDER BY ts
    '''
    df = pd.read_sql_query(query, conn, params=(plant_uid, start_date, end_date))
    if df.empty:
        return df
    df['ts'] = pd.to_datetime(df['ts'])
    if df['ts'].dt.tz is not None:
        df['ts'] = df['ts'].dt.tz_localize(None)
    df = df.set_index('ts')
    df['power_kw'] = df['power_w'] / 1000.0
    return df

# Helper: get POA (poaIrradiance) and convert kWh/m2 per 0.5h to W/m2
def get_poa_data(plant_uid, start_date, end_date):
    query = '''
        SELECT ts,
               CAST(json_extract(payload, '$.poaIrradiance.value') AS REAL) as poa_kwhm2
        FROM readings
        WHERE plant_uid = ?
          AND emig_id = 'POA:SOLARGIS:WEIGHTED'
          AND ts >= ? AND ts < ?
        ORDER BY ts
    '''
    df = pd.read_sql_query(query, conn, params=(plant_uid, start_date, end_date))
    if df.empty:
        return df
    df['ts'] = pd.to_datetime(df['ts'])
    if df['ts'].dt.tz is not None:
        df['ts'] = df['ts'].dt.tz_localize(None)
    df = df.set_index('ts')
    df['poa_wm2'] = df['poa_kwhm2'] * 2000.0
    return df

# Select analysis window based on available data span
span = store.date_span(plant_uid)
if not span:
    print('No readings available for this plant in DB.')
    raise SystemExit(1)

start = span['min'][:10]
end = span['max'][:10]
print(f"Data range in DB: {start} to {end}")

# For speed, limit to a recent window (30 days ending at end)
end_date = pd.to_datetime(end)
start_date = (end_date - pd.Timedelta(days=30)).strftime('%Y-%m-%d')
end_date_str = (end_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
print(f"Using sample window: {start_date} to {end_date_str} (30 days)")

inv_df = get_inverter_power(plant_uid, start_date, end_date_str)
poa_df = get_poa_data(plant_uid, start_date, end_date_str)

if inv_df.empty:
    print('No inverter power data in window.'); raise SystemExit(1)
if poa_df.empty:
    print('No POA data in window.'); raise SystemExit(1)

# Merge on timestamps
merged = inv_df.join(poa_df, how='inner')
merged = merged.dropna(subset=['power_kw', 'poa_wm2'])
print(f"Merged rows (matching timestamps): {len(merged)}")

# HALF-HOURLY PR: pr_hh = power_kw / (dc_kw * poa_wm2 / 1000)
merged['pr_hh'] = merged['power_kw'] / (dc_kw * merged['poa_wm2'] / 1000.0)

# Show few example HH rows
print('\n=== Half-hour examples ===')
examples_hh = merged[merged['poa_wm2'] >= 50].head(6)
for idx, row in examples_hh.iterrows():
    ts = idx
    power_kw = row['power_kw']
    poa = row['poa_wm2']
    expected_kw = dc_kw * poa / 1000.0
    pr = row['pr_hh']
    print(f"{ts} | AC {power_kw:.3f} kW | POA {poa:.1f} W/m2 | Expected {expected_kw:.3f} kW | PR_hh {pr:.2%}")

# DAILY PR: aggregate
merged['date'] = merged.index.date
daily = merged.groupby('date').agg({'power_kw':'sum','poa_wm2':'sum','inverter_count':'mean'}).reset_index()
# Expected kw per day = dc_kw * sum(poa_wm2)/1000
daily['expected_kw'] = dc_kw * daily['poa_wm2'] / 1000.0
daily['pr_daily'] = daily['power_kw'] / daily['expected_kw']

print('\n=== Daily examples (select days with poa sum >= 1000 W/m2 total) ===')
daily_valid = daily[daily['poa_wm2'] >= 1000]
for _, r in daily_valid.head(5).iterrows():
    date = r['date']
    sum_power = r['power_kw']
    sum_poa = r['poa_wm2']
    expected = r['expected_kw']
    pr = r['pr_daily']
    print(f"{date} | Sum AC {sum_power:.3f} kWh | Sum POA {sum_poa:.1f} W/m2 | Expected {expected:.3f} kWh | PR_daily {pr:.2%}")

# MONTHLY PR
merged['month'] = merged.index.to_period('M')
monthly = merged.groupby('month').agg({'power_kw':'sum','poa_wm2':'sum','inverter_count':'mean'}).reset_index()
monthly['expected_kw'] = dc_kw * monthly['poa_wm2'] / 1000.0
monthly['pr_monthly'] = monthly['power_kw'] / monthly['expected_kw']

print('\n=== Monthly examples ===')
for _, r in monthly.iterrows():
    print(f"{r['month']} | Sum AC {r['power_kw']:.1f} kWh | Sum POA {r['poa_wm2']:.1f} W/m2 | Expected {r['expected_kw']:.1f} | PR {r['pr_monthly']:.1%}")

# Save examples to CSV
out = 'fylde_pr_examples.csv'
merged.reset_index().to_csv(out, index=False)
print(f"\nDetailed merged data written to {out}")

conn.close()
