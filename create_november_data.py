"""
Create sample November 2025 data for all sites in the database.
Test the upload function with duplicate detection.
"""
import sys
import os
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

# Add Monthly reporting to path for imports
monthly_reporting_dir = os.path.join(os.path.dirname(__file__), '..', 'Monthly reporting')
sys.path.insert(0, os.path.abspath(monthly_reporting_dir))

from data_access import SolarDataExtractor

# Database path
db_path = os.path.join(monthly_reporting_dir, 'solar_assets.db')
print(f"Database path: {db_path}")

# Initialize extractor
extractor = SolarDataExtractor(db_path)

# Get existing data to understand the format
tables = extractor.list_tables()
print(f"\nExisting tables: {tables}")

ok, existing_df = extractor.query_data("SELECT * FROM solar_data")
if not ok:
    print(f"Error querying data: {existing_df}")
    exit()

print(f"\nExisting data shape: {existing_df.shape}")
print(f"Columns: {list(existing_df.columns)}")

# Get unique sites and their typical values
sites_info = existing_df.groupby('Site').agg({
    'Type': 'first',
    'kWp': 'first',
    'Actual Gen (kWh)': 'mean',
    'Forecast Gen (kWh)': 'mean',
    'Actual Irr (kWh/m2)': 'mean',
    'Forecast Irr': 'mean',
    'Actual PR (%)': 'mean',
    'Forecast PR (%)': 'mean',
    'Availability (%)': 'mean',
}).reset_index()

print(f"\n{'='*80}")
print("SITES IN DATABASE:")
print("="*80)
print(sites_info.to_string())

# Check existing dates
existing_dates = existing_df['Date'].unique()
print(f"\nExisting dates: {sorted(existing_dates)}")

# Create November data for all sites
print(f"\n{'='*80}")
print("CREATING SAMPLE NOVEMBER 2025 DATA:")
print("="*80)

# November has lower irradiance (typical UK values)
# Using October as reference with slightly lower values
october_data = existing_df[existing_df['Date'] == 'Oct-25'].copy()
print(f"October data rows: {len(october_data)}")

if len(october_data) == 0:
    print("No October data found, using average of all months")
    reference_data = existing_df.groupby('Site').mean(numeric_only=True).reset_index()
    reference_data = reference_data.merge(
        existing_df[['Site', 'Type']].drop_duplicates(),
        on='Site'
    )
else:
    reference_data = october_data

# Create November data with realistic variations
november_data = []

for idx, row in reference_data.iterrows():
    site = row['Site']
    site_type = row['Type'] if 'Type' in row else sites_info[sites_info['Site'] == site]['Type'].values[0]
    kwp = row['kWp'] if 'kWp' in row else sites_info[sites_info['Site'] == site]['kWp'].values[0]
    
    # November has lower irradiance than October (typical UK: ~25-35 kWh/m2)
    # Apply seasonal reduction factor (roughly 60-70% of October)
    irr_factor = 0.65 + np.random.uniform(-0.05, 0.05)  # Random variation
    
    actual_irr = row.get('Actual Irr (kWh/m2)', 45) * irr_factor
    forecast_irr = actual_irr * (1 + np.random.uniform(-0.05, 0.05))  # Slight forecast error
    
    # PR typically similar but slightly lower in winter
    pr_factor = 0.95 + np.random.uniform(-0.02, 0.02)
    actual_pr = min(1.0, row.get('Actual PR (%)', 0.85) * pr_factor)
    forecast_pr = row.get('Forecast PR (%)', 0.85)
    
    # Availability - typically high
    availability = min(1.0, row.get('Availability (%)', 0.99) + np.random.uniform(-0.02, 0.01))
    
    # Calculate generation
    actual_gen = kwp * actual_irr * actual_pr * availability
    forecast_gen = kwp * forecast_irr * forecast_pr * 0.99  # Assumed 99% availability in forecast
    
    # Gen deviation
    gen_dev = (actual_gen - forecast_gen) / forecast_gen if forecast_gen > 0 else 0
    
    # kWh/kWp
    kwh_kwp = actual_gen / kwp if kwp > 0 else 0
    
    # Calculated expected (based on irradiance)
    calc_exp = kwp * actual_irr * forecast_pr
    
    # Net usage (some portion consumed on-site)
    net_usage = actual_gen * np.random.uniform(0.7, 0.95)
    
    # Irradiance-based generation
    irr_based_gen = kwp * actual_irr * actual_pr
    
    # Irr variation
    irr_var = (actual_irr - forecast_irr) * kwp * actual_pr
    
    november_data.append({
        'Site': site,
        'Type': site_type,
        'kWp': kwp,
        'Date': 'Nov-25',
        'Actual Gen (kWh)': round(actual_gen, 2),
        'Forecast Gen (kWh)': round(forecast_gen, 2),
        'Gen Dev. (%)': round(gen_dev, 6),
        'kWh/kWp': round(kwh_kwp, 6),
        'Calculated Exp (kWh)': round(calc_exp, 2),
        'Net Usage (kWh)': round(net_usage, 2),
        'Actual Irr (kWh/m2)': round(actual_irr, 6),
        'Forecast Irr': round(forecast_irr, 6),
        'Irradiance-based generation': round(irr_based_gen, 2),
        'Irr Variation (kWh)': round(irr_var, 6),
        'Actual PR (%)': round(actual_pr, 6),
        'Forecast PR (%)': round(forecast_pr, 6),
        'Availability (%)': round(availability, 6),
    })

november_df = pd.DataFrame(november_data)
print(f"\nNovember sample data created: {len(november_df)} rows")
print(november_df.to_string())

# Save to CSV for reference
csv_path = os.path.join(os.path.dirname(__file__), 'november_sample_data.csv')
november_df.to_csv(csv_path, index=False)
print(f"\nSaved to: {csv_path}")

# Test 1: Try uploading with extract_unique_only (should add all 15 rows)
print(f"\n{'='*80}")
print("TEST 1: UPLOAD NOVEMBER DATA (should add all new rows)")
print("="*80)

success, message, stats = extractor.extract_unique_only(
    november_df.copy(),
    table_name='solar_data',
    key_columns=['Site', 'Date'],
    update_existing=False
)

print(f"Success: {success}")
print(f"Message: {message}")
print(f"Stats: {stats}")

# Verify data was added
ok, updated_df = extractor.query_data("SELECT * FROM solar_data")
print(f"\nTotal rows after upload: {len(updated_df)}")
print(f"Unique dates: {sorted(updated_df['Date'].unique())}")

# Test 2: Try uploading the same data again (should skip all as duplicates)
print(f"\n{'='*80}")
print("TEST 2: UPLOAD SAME DATA AGAIN (should skip all duplicates)")
print("="*80)

success2, message2, stats2 = extractor.extract_unique_only(
    november_df.copy(),
    table_name='solar_data',
    key_columns=['Site', 'Date'],
    update_existing=False
)

print(f"Success: {success2}")
print(f"Message: {message2}")
print(f"Stats: {stats2}")

# Verify row count unchanged
ok, final_df = extractor.query_data("SELECT * FROM solar_data")
print(f"\nTotal rows after second upload: {len(final_df)}")
print(f"Unique dates: {sorted(final_df['Date'].unique())}")

# Test 3: Try uploading with update_existing=True (should update existing)
print(f"\n{'='*80}")
print("TEST 3: UPLOAD WITH UPDATE OPTION (should update existing rows)")
print("="*80)

# Modify one value to see if update works
november_modified = november_df.copy()
november_modified.loc[0, 'Actual Gen (kWh)'] = november_modified.loc[0, 'Actual Gen (kWh)'] + 100

success3, message3, stats3 = extractor.extract_unique_only(
    november_modified,
    table_name='solar_data',
    key_columns=['Site', 'Date'],
    update_existing=True
)

print(f"Success: {success3}")
print(f"Message: {message3}")
print(f"Stats: {stats3}")

print(f"\n{'='*80}")
print("ALL TESTS COMPLETE")
print("="*80)
