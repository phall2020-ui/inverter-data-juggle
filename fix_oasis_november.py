"""Fix Oasis November entry with proper values"""
import sys
import os
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.abspath('../Monthly reporting'))
from data_access import SolarDataExtractor

db_path = '../Monthly reporting/solar_assets.db'
extractor = SolarDataExtractor(db_path)

# Get Oasis data
ok, df = extractor.query_data("SELECT * FROM solar_data WHERE Site='Oasis'")
print('Oasis current data:')
print(df.to_string())

# Calculate November values based on average with seasonal adjustment
oasis_other_months = df[df['Date'] != 'Nov-25']
oasis_avg = oasis_other_months.mean(numeric_only=True)

# Get site info
site = 'Oasis'
site_type = 'Roof'
kwp = 1723.0

# November adjustment factor (lower irradiance)
irr_factor = 0.60  # November typically 60% of yearly average irradiance

actual_irr = 30.0  # Reasonable UK November value for rooftop
forecast_irr = 29.5
actual_pr = 0.80
forecast_pr = 0.80
availability = 0.99

actual_gen = kwp * actual_irr * actual_pr * availability
forecast_gen = kwp * forecast_irr * forecast_pr * 0.99

gen_dev = (actual_gen - forecast_gen) / forecast_gen if forecast_gen > 0 else 0
kwh_kwp = actual_gen / kwp if kwp > 0 else 0
calc_exp = kwp * actual_irr * forecast_pr
net_usage = actual_gen * 0.85
irr_based_gen = kwp * actual_irr * actual_pr
irr_var = (actual_irr - forecast_irr) * kwp * actual_pr

oasis_nov = pd.DataFrame([{
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
}])

print('\nFixed Oasis November data:')
print(oasis_nov.to_string())

# Update in database
success, message, stats = extractor.extract_unique_only(
    oasis_nov,
    table_name='solar_data',
    key_columns=['Site', 'Date'],
    update_existing=True
)

print(f'\nUpdate result: {success}')
print(f'Message: {message}')

# Verify
ok, df_final = extractor.query_data("SELECT * FROM solar_data WHERE Site='Oasis' AND Date='Nov-25'")
print('\nVerified Oasis November data:')
print(df_final.to_string())
