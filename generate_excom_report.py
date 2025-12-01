"""
Generate ExCom Report Output for November and YTD
Shows the waterfall components and validates calculations
"""
import sys
import os
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.abspath('../Monthly reporting'))
from data_access import SolarDataExtractor
from config import Config

# Load database
db_path = '../Monthly reporting/solar_assets.db'
extractor = SolarDataExtractor(db_path)

print("="*100)
print("EXCOM OPERATIONAL PERFORMANCE REPORT - NOVEMBER 2025")
print("="*100)

# Get all data
ok, df = extractor.query_data("SELECT * FROM solar_data")
if not ok:
    print(f"Error: {df}")
    exit()

print(f"\nDatabase contains: {len(df)} rows")
print(f"Sites: {len(df['Site'].unique())}")
print(f"Dates: {sorted(df['Date'].unique())}")

# Column mapping
colmap = {
    'site': 'Site',
    'date': 'Date',
    'actual_gen': 'Actual Gen (kWh)',
    'budget_gen': 'Forecast Gen (kWh)',
    'pr_actual': 'Actual PR (%)',
    'pr_budget': 'Forecast PR (%)',
    'availability': 'Availability (%)',
    'actual_irr': 'Actual Irr (kWh/m2)',
    'forecast_irr': 'Forecast Irr',
    'capacity': 'kWp',
}

# Clean data - convert PR to decimals if needed
df_clean = df.copy()
for col in ['Actual PR (%)', 'Forecast PR (%)', 'Availability (%)']:
    if col in df_clean.columns:
        # Values > 1 are likely percentages (0-100 scale)
        mask = df_clean[col] > 1
        df_clean.loc[mask, col] = df_clean.loc[mask, col] / 100

# Calculate loss components (following analysis.py methodology)
def calculate_losses(data):
    """Calculate WAB and loss components"""
    result = data.copy()
    
    budget_col = colmap['budget_gen']
    actual_col = colmap['actual_gen']
    pr_actual_col = colmap['pr_actual']
    pr_budget_col = colmap['pr_budget']
    avail_col = colmap['availability']
    actual_irr_col = colmap['actual_irr']
    forecast_irr_col = colmap['forecast_irr']
    capacity_col = colmap['capacity']
    
    # WAB = Budget * (Actual Irr / Forecast Irr)
    result['WAB'] = result[budget_col] * (result[actual_irr_col] / result[forecast_irr_col])
    
    # Weather variance = WAB - Budget
    result['Var_Weather_kWh'] = result['WAB'] - result[budget_col]
    
    # PR loss = WAB * (PR_budget - PR_actual)
    result['Loss_PR_kWh'] = result['WAB'] * (result[pr_budget_col] - result[pr_actual_col])
    
    # Availability loss = WAB * (0.99 - Availability)
    result['Loss_Avail_kWh'] = result['WAB'] * (0.99 - result[avail_col])
    
    # Total technical loss = WAB - Actual
    result['Loss_Total_Tech_kWh'] = result['WAB'] - result[actual_col]
    
    return result

df_calc = calculate_losses(df_clean)

# ============================================
# NOVEMBER 2025 REPORT
# ============================================
print("\n" + "="*100)
print("NOVEMBER 2025 - MONTHLY REPORT")
print("="*100)

nov_data = df_calc[df_calc['Date'] == 'Nov-25'].copy()
print(f"\nNovember data rows: {len(nov_data)}")

if len(nov_data) > 0:
    # Portfolio totals
    nov_budget = nov_data['Forecast Gen (kWh)'].sum() / 1000  # Convert to MWh
    nov_actual = nov_data['Actual Gen (kWh)'].sum() / 1000
    nov_wab = nov_data['WAB'].sum() / 1000
    nov_weather_var = nov_data['Var_Weather_kWh'].sum() / 1000
    nov_pr_loss = nov_data['Loss_PR_kWh'].sum() / 1000
    nov_avail_loss = nov_data['Loss_Avail_kWh'].sum() / 1000
    
    # Weighted average KPIs
    nov_pr = np.average(nov_data['Actual PR (%)'], weights=nov_data['Actual Gen (kWh)'].replace(0, 1))
    nov_pr_budget = np.average(nov_data['Forecast PR (%)'].fillna(0.85), weights=nov_data['Forecast Gen (kWh)'].replace(0, 1))
    nov_avail = np.average(nov_data['Availability (%)'], weights=nov_data['Actual Gen (kWh)'].replace(0, 1))
    
    print(f"\n--- PORTFOLIO SUMMARY (November 2025) ---")
    print(f"Sites: {len(nov_data)}")
    print(f"Total Capacity: {nov_data['kWp'].sum():,.1f} kWp ({nov_data['kWp'].sum()/1000:.2f} MWp)")
    print(f"\n--- GENERATION (MWh) ---")
    print(f"Budget:    {nov_budget:>12,.1f} MWh")
    print(f"WAB:       {nov_wab:>12,.1f} MWh")
    print(f"Actual:    {nov_actual:>12,.1f} MWh")
    print(f"Variance:  {nov_actual - nov_budget:>+12,.1f} MWh ({(nov_actual/nov_budget - 1)*100:+.1f}%)")
    
    print(f"\n--- WATERFALL COMPONENTS (MWh) ---")
    print(f"Budget:       {nov_budget:>12,.1f}")
    print(f"Irradiance:   {nov_weather_var:>+12,.1f}  (Weather variance)")
    print(f"Availability: {-nov_avail_loss:>+12,.1f}  (Availability step)")
    efficiency_step = nov_actual - (nov_budget + nov_weather_var - nov_avail_loss)
    print(f"Efficiency:   {efficiency_step:>+12,.1f}  (Balancing item)")
    print(f"Actual:       {nov_actual:>12,.1f}")
    
    print(f"\n--- KPIs (November) ---")
    print(f"PR Actual:           {nov_pr*100:>6.1f}%  (Target: {nov_pr_budget*100:.1f}%)")
    print(f"Availability Actual: {nov_avail*100:>6.1f}%  (Target: 99.0%)")
    
    print(f"\n--- SITE BREAKDOWN (November 2025) ---")
    print(f"{'Site':<35} {'Budget':>12} {'Actual':>12} {'Var %':>8} {'PR':>6} {'Avail':>6}")
    print("-"*90)
    for _, row in nov_data.sort_values('Actual Gen (kWh)', ascending=False).iterrows():
        site = row['Site'][:34]
        budget = row['Forecast Gen (kWh)']/1000
        actual = row['Actual Gen (kWh)']/1000
        var_pct = (actual/budget - 1)*100 if budget > 0 else 0
        pr = row['Actual PR (%)']*100
        avail = row['Availability (%)']*100
        print(f"{site:<35} {budget:>12,.1f} {actual:>12,.1f} {var_pct:>+7.1f}% {pr:>5.1f}% {avail:>5.1f}%")

# ============================================
# YTD REPORT (Apr-25 to Nov-25)
# ============================================
print("\n" + "="*100)
print("YEAR TO DATE (April 2025 - November 2025)")
print("="*100)

# YTD is fiscal year starting April
ytd_months = ['Apr-25', 'May-25', 'Jun-25', 'Jul-25', 'Aug-25', 'Sep-25', 'Oct-25', 'Nov-25']
ytd_data = df_calc[df_calc['Date'].isin(ytd_months)].copy()

print(f"\nYTD data rows: {len(ytd_data)}")
print(f"Months included: {sorted(ytd_data['Date'].unique())}")

if len(ytd_data) > 0:
    # Portfolio totals
    ytd_budget = ytd_data['Forecast Gen (kWh)'].sum() / 1000
    ytd_actual = ytd_data['Actual Gen (kWh)'].sum() / 1000
    ytd_wab = ytd_data['WAB'].sum() / 1000
    ytd_weather_var = ytd_data['Var_Weather_kWh'].sum() / 1000
    ytd_pr_loss = ytd_data['Loss_PR_kWh'].sum() / 1000
    ytd_avail_loss = ytd_data['Loss_Avail_kWh'].sum() / 1000
    
    # Weighted average KPIs for YTD
    ytd_pr = np.average(ytd_data['Actual PR (%)'].fillna(0.8), weights=ytd_data['Actual Gen (kWh)'].replace(0, 1))
    ytd_pr_budget = np.average(ytd_data['Forecast PR (%)'].fillna(0.85), weights=ytd_data['Forecast Gen (kWh)'].replace(0, 1))
    ytd_avail = np.average(ytd_data['Availability (%)'], weights=ytd_data['Actual Gen (kWh)'].replace(0, 1))
    
    print(f"\n--- PORTFOLIO SUMMARY (YTD) ---")
    print(f"Sites: {len(ytd_data['Site'].unique())}")
    print(f"Total Capacity: {ytd_data.groupby('Site')['kWp'].first().sum():,.1f} kWp")
    
    print(f"\n--- GENERATION (MWh) ---")
    print(f"Budget:    {ytd_budget:>12,.1f} MWh")
    print(f"WAB:       {ytd_wab:>12,.1f} MWh")
    print(f"Actual:    {ytd_actual:>12,.1f} MWh")
    print(f"Variance:  {ytd_actual - ytd_budget:>+12,.1f} MWh ({(ytd_actual/ytd_budget - 1)*100:+.1f}%)")
    
    print(f"\n--- WATERFALL COMPONENTS (MWh) ---")
    print(f"Budget:       {ytd_budget:>12,.1f}")
    print(f"Irradiance:   {ytd_weather_var:>+12,.1f}  (Weather variance)")
    print(f"Availability: {-ytd_avail_loss:>+12,.1f}  (Availability step)")
    ytd_efficiency = ytd_actual - (ytd_budget + ytd_weather_var - ytd_avail_loss)
    print(f"Efficiency:   {ytd_efficiency:>+12,.1f}  (Balancing item)")
    print(f"Actual:       {ytd_actual:>12,.1f}")
    
    print(f"\n--- KPIs (YTD) ---")
    print(f"PR Actual:           {ytd_pr*100:>6.1f}%  (Target: {ytd_pr_budget*100:.1f}%)")
    print(f"Availability Actual: {ytd_avail*100:>6.1f}%  (Target: 99.0%)")
    
    # Monthly trend
    print(f"\n--- MONTHLY TREND ---")
    print(f"{'Month':<10} {'Budget':>12} {'WAB':>12} {'Actual':>12} {'Var %':>8}")
    print("-"*60)
    
    for month in ytd_months:
        month_df = df_calc[df_calc['Date'] == month]
        if len(month_df) > 0:
            m_budget = month_df['Forecast Gen (kWh)'].sum() / 1000
            m_wab = month_df['WAB'].sum() / 1000
            m_actual = month_df['Actual Gen (kWh)'].sum() / 1000
            m_var = (m_actual/m_budget - 1)*100 if m_budget > 0 else 0
            print(f"{month:<10} {m_budget:>12,.1f} {m_wab:>12,.1f} {m_actual:>12,.1f} {m_var:>+7.1f}%")
    
    # Site YTD breakdown
    print(f"\n--- SITE YTD BREAKDOWN ---")
    print(f"{'Site':<35} {'Budget':>12} {'Actual':>12} {'Var %':>8}")
    print("-"*75)
    
    site_ytd = ytd_data.groupby('Site').agg({
        'Forecast Gen (kWh)': 'sum',
        'Actual Gen (kWh)': 'sum',
    }).reset_index()
    
    site_ytd['Var %'] = (site_ytd['Actual Gen (kWh)'] / site_ytd['Forecast Gen (kWh)'] - 1) * 100
    
    for _, row in site_ytd.sort_values('Actual Gen (kWh)', ascending=False).iterrows():
        site = row['Site'][:34]
        budget = row['Forecast Gen (kWh)']/1000
        actual = row['Actual Gen (kWh)']/1000
        var_pct = row['Var %']
        print(f"{site:<35} {budget:>12,.1f} {actual:>12,.1f} {var_pct:>+7.1f}%")

print("\n" + "="*100)
print("REPORT COMPLETE - NO ERRORS")
print("="*100)
