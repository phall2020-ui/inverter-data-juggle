import sqlite3
import pandas as pd
from plant_store import PlantStore, DEFAULT_DB

TARGETS = ["Newfold Farm", "Finlay Beverages"]

store = PlantStore(DEFAULT_DB)
plants = store.list_all()

conn = sqlite3.connect(DEFAULT_DB)

for target in TARGETS:
    matched = None
    for p in plants:
        if p['alias'].lower() == target.lower():
            matched = p
            break
    if not matched:
        print(f"Plant '{target}' not found in registry. Skipping.")
        continue

    alias = matched['alias']
    plant_uid = matched['plant_uid']
    dc_kw = matched.get('dc_size_kw') or 0.0
    print('\n' + '='*80)
    print(f"Plant: {alias} ({plant_uid}) - DC {dc_kw} kW")
    print('='*80)

    # Helpers
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

    # Determine data span
    span = store.date_span(plant_uid)
    if not span:
        print('  No readings available for this plant in DB. Skipping.')
        continue

    end = span['max'][:10]
    end_date = pd.to_datetime(end)
    # Try recent 30 days first, then fall back to June-Nov window used by other analyses
    start_date = (end_date - pd.Timedelta(days=30)).strftime('%Y-%m-%d')
    end_date_str = (end_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    print(f"  Trying recent window: {start_date} to {end_date_str} (30 days)")

    inv_df = get_inverter_power(plant_uid, start_date, end_date_str)
    poa_df = get_poa_data(plant_uid, start_date, end_date_str)
    if poa_df.empty:
        # Fallback to the longer June-Nov window (as used in comprehensive analysis)
        print("  No POA in recent window — falling back to 2025-06-01 to 2025-11-01")
        start_date = '2025-06-01'
        end_date_str = '2025-11-01'
        inv_df = get_inverter_power(plant_uid, start_date, end_date_str)
        poa_df = get_poa_data(plant_uid, start_date, end_date_str)

    if inv_df.empty:
        print('  No inverter power data in window. Skipping.'); continue
    if poa_df.empty:
        print('  No POA data in window. Skipping.'); continue

    merged = inv_df.join(poa_df, how='inner')
    merged = merged.dropna(subset=['power_kw', 'poa_wm2'])
    print(f"  Merged rows (matching timestamps): {len(merged)}")

    # Half-hour PR
    merged['pr_hh'] = merged['power_kw'] / (dc_kw * merged['poa_wm2'] / 1000.0)
    hh_examples = merged[merged['poa_wm2'] >= 50].head(6)
    print('\n  Half-hour examples (timestamp | AC_kW | POA_W/m2 | Expected_kW | PR):')
    for idx, row in hh_examples.iterrows():
        ts = idx
        power_kw = row['power_kw']
        poa = row['poa_wm2']
        expected_kw = dc_kw * poa / 1000.0
        pr = row['pr_hh']
        print(f"    {ts} | {power_kw:.3f} kW | {poa:.1f} W/m2 | {expected_kw:.3f} kW | {pr:.2%}")

    # Daily PR
    merged['date'] = merged.index.date
    daily = merged.groupby('date').agg({'power_kw':'sum','poa_wm2':'sum','inverter_count':'mean'}).reset_index()
    daily['expected_kw'] = dc_kw * daily['poa_wm2'] / 1000.0
    daily['pr_daily'] = daily['power_kw'] / daily['expected_kw']
    daily_valid = daily[daily['poa_wm2'] >= 1000]
    print('\n  Daily examples (date | Sum_AC_kWh | Sum_POA_W/m2 | Expected_kWh | PR):')
    for _, r in daily_valid.head(5).iterrows():
        print(f"    {r['date']} | {r['power_kw']:.3f} kWh | {r['poa_wm2']:.1f} W/m2 | {r['expected_kw']:.3f} kWh | {r['pr_daily']:.2%}")

    # Monthly PR
    merged['month'] = merged.index.to_period('M')
    monthly = merged.groupby('month').agg({'power_kw':'sum','poa_wm2':'sum','inverter_count':'mean'}).reset_index()
    monthly['expected_kw'] = dc_kw * monthly['poa_wm2'] / 1000.0
    monthly['pr_monthly'] = monthly['power_kw'] / monthly['expected_kw']
    print('\n  Monthly examples (month | Sum_AC_kWh | Sum_POA_W/m2 | Expected_kWh | PR):')
    for _, r in monthly.iterrows():
        print(f"    {r['month']} | {r['power_kw']:.1f} kWh | {r['poa_wm2']:.1f} W/m2 | {r['expected_kw']:.1f} | {r['pr_monthly']:.2%}")

    # Save merged to CSV
    out = f"{alias.replace(' ','_').lower()}_pr_examples.csv"
    merged.reset_index().to_csv(out, index=False)
    print(f"  Detailed merged data written to {out}")

conn.close()
print('\nDone.')
