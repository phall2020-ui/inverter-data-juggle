"""
Shading analysis script for Newfold Farm (or similar sites using Juggle CSV data).

Given:
  - A "summer" CSV file
  - A "winter" CSV file

Both with schema like:
    timestamp, emigId, dcCurrent, dcVoltage, poaIrradiance, ...

This script will:
  1. Join inverter records to weather-station irradiance by timestamp.
  2. Compute irradiance-normalised current I_norm = dcCurrent / poaIrradiance.
  3. Build a time-of-day profile (by half-hour) for summer and winter.
  4. Compare summer vs winter I_norm for each inverter to detect shading.
  5. Output:
       - A console summary per inverter.
       - A CSV file with detailed per-hour comparison for further review.

Usage:
    python shading_analysis_newfold.py summer.csv winter.csv

Optional arguments:
    --weather-id WETH:000274
    --irr-col poaIrradiance
    --out detailed_shading_analysis.csv
"""

import argparse
import sys
from dataclasses import dataclass
from typing import Tuple, List
import tkinter as tk
from tkinter import filedialog

import numpy as np
import pandas as pd
try:
    import openpyxl
except ImportError:
    print("Warning: openpyxl not installed. Excel output will not be available.")
    print("Install with: pip install openpyxl")


@dataclass
class Settings:
    weather_id: str = "WETH:000274"
    irradiance_col: str = "poaIrradiance"
    current_col: str = "apparentPower"  # Use power instead of dcCurrent
    timestamp_col: str = "timestamp"
    emigid_col: str = "emigId"
    irradiance_min: float = 100.0  # W/m² threshold to avoid low-light noise
    min_points_per_hour: int = 3   # minimum records to accept an hour bin


def load_and_prepare(path: str, cfg: Settings) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load a Juggle CSV file and split into inverter and weather dataframes."""
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        raise SystemExit(f"Failed to read {path}: {exc}")

    if cfg.timestamp_col not in df.columns:
        raise SystemExit(f"{path} is missing timestamp column '{cfg.timestamp_col}'")

    df["dt"] = pd.to_datetime(df[cfg.timestamp_col], errors="coerce")
    df = df.dropna(subset=["dt"]).copy()
    df["hour_float"] = df["dt"].dt.hour + df["dt"].dt.minute / 60.0

    is_weather = df[cfg.emigid_col] == cfg.weather_id
    df_weather = df[is_weather].copy()
    df_inv = df[~is_weather].copy()

    if cfg.irradiance_col not in df_weather.columns:
        raise SystemExit(
            f"{path}: weather rows for {cfg.weather_id} do not contain "
            f"irradiance column '{cfg.irradiance_col}'"
        )
    
    # Check if current column exists
    if cfg.current_col not in df_inv.columns:
        print(f"\n*** WARNING: Column '{cfg.current_col}' not found in inverter data ***")
        print(f"Available columns: {', '.join(df_inv.columns)}")
        raise SystemExit(f"Please specify the correct current column using --current-col")
    
    # Print diagnostics
    print(f"\n{path}:")
    print(f"  Date range: {df['dt'].min()} to {df['dt'].max()}")
    print(f"  Total records: {len(df)}")
    print(f"  Weather records: {len(df_weather)}")
    print(f"  Inverter records: {len(df_inv)}")
    print(f"  Unique inverters: {df_inv[cfg.emigid_col].nunique()}")
    
    # Check current values
    if len(df_inv) > 0:
        current_vals = df_inv[cfg.current_col].dropna()
        if len(current_vals) > 0:
            print(f"  {cfg.current_col} range: {current_vals.min():.3f} to {current_vals.max():.3f}")
            
            # Warn if all values are negative or zero
            if current_vals.max() <= 0:
                print(f"  *** WARNING: All {cfg.current_col} values are <= 0! ***")
                print(f"  *** This will produce invalid results. Try a different column like: ***")
                print(f"  ***   --current-col apparentPower   OR   --current-col exportEnergy ***")

    return df_inv, df_weather


def join_with_irradiance(
    df_inv: pd.DataFrame, df_weather: pd.DataFrame, cfg: Settings
) -> pd.DataFrame:
    """Join inverter data with weather irradiance on timestamp."""
    cols_weather = ["dt", cfg.irradiance_col]
    w = df_weather[cols_weather].rename(columns={cfg.irradiance_col: "irr"})

    m = df_inv.merge(w, on="dt", how="inner")
    
    print(f"  Joined records (before filtering): {len(m)}")
    
    # Filter for sufficient irradiance
    m = m[(m["irr"].notna()) & (m["irr"] >= cfg.irradiance_min)].copy()
    
    print(f"  Records with irradiance >= {cfg.irradiance_min} W/m²: {len(m)}")
    
    if len(m) == 0:
        print(f"  WARNING: No records with sufficient irradiance!")
        return m
    
    # Filter for positive power/current values
    m = m[m[cfg.current_col] > 0].copy()
    
    print(f"  Records with {cfg.current_col} > 0: {len(m)}")

    if len(m) == 0:
        print(f"  WARNING: No records with positive {cfg.current_col}!")
        return m

    m["I_norm"] = m[cfg.current_col] / m["irr"]
    
    print(f"  I_norm range: {m['I_norm'].min():.3f} to {m['I_norm'].max():.3f}")
    
    return m


def build_profile(m: pd.DataFrame, cfg: Settings) -> pd.DataFrame:
    """Build time-of-day median I_norm profile."""
    if len(m) == 0:
        print("  WARNING: No data to build profile!")
        return pd.DataFrame()
    
    g = (
        m.groupby([cfg.emigid_col, "hour_float"])["I_norm"]
        .agg(["median", "count"])
        .reset_index()
    )
    g = g.rename(columns={"median": "I_norm_median", "count": "n_points"})
    g = g[g["n_points"] >= cfg.min_points_per_hour]
    
    print(f"  Profile records (after filtering): {len(g)}")
    
    return g


def compare_profiles(
    prof_summer: pd.DataFrame, prof_winter: pd.DataFrame, cfg: Settings
) -> pd.DataFrame:
    print(f"\n--- Comparing Profiles ---")
    print(f"  Summer profile records: {len(prof_summer)}")
    print(f"  Winter profile records: {len(prof_winter)}")
    
    if len(prof_summer) > 0:
        print(f"  Summer hour range: {prof_summer['hour_float'].min():.1f} to {prof_summer['hour_float'].max():.1f}")
        print(f"  Summer inverters: {prof_summer[cfg.emigid_col].nunique()}")
    
    if len(prof_winter) > 0:
        print(f"  Winter hour range: {prof_winter['hour_float'].min():.1f} to {prof_winter['hour_float'].max():.1f}")
        print(f"  Winter inverters: {prof_winter[cfg.emigid_col].nunique()}")
    
    s = prof_summer.rename(
        columns={"I_norm_median": "I_summer", "n_points": "n_summer"}
    )
    w = prof_winter.rename(
        columns={"I_norm_median": "I_winter", "n_points": "n_winter"}
    )

    merged = s.merge(
        w,
        on=[cfg.emigid_col, "hour_float"],
        how="inner",
    )
    
    print(f"  Merged records (overlapping hours): {len(merged)}")
    
    if len(merged) > 0:
        print(f"  Sample I_summer values: min={merged['I_summer'].min():.3f}, max={merged['I_summer'].max():.3f}, NaN count={merged['I_summer'].isna().sum()}")
        print(f"  Sample I_winter values: min={merged['I_winter'].min():.3f}, max={merged['I_winter'].max():.3f}, NaN count={merged['I_winter'].isna().sum()}")

    merged["ratio_winter_to_summer"] = np.where(
        merged["I_summer"] > 0,
        merged["I_winter"] / merged["I_summer"],
        np.nan,
    )
    merged["delta"] = merged["I_winter"] - merged["I_summer"]
    
    print(f"  Valid ratios: {(~merged['ratio_winter_to_summer'].isna()).sum()} out of {len(merged)}")

    return merged


def summarise_shading(comp: pd.DataFrame, cfg: Settings) -> pd.DataFrame:
    """Summarise potential shading by inverter."""
    print(f"\n--- Summarising Shading ---")
    print(f"  Total comparison records: {len(comp)}")
    
    mid = comp[(comp["hour_float"] >= 9.0) & (comp["hour_float"] <= 15.0)].copy()
    print(f"  Records in 9am-3pm window: {len(mid)}")
    
    if len(mid) > 0:
        print(f"  Hour range in window: {mid['hour_float'].min():.1f} to {mid['hour_float'].max():.1f}")
        print(f"  Inverters with data in window: {mid[cfg.emigid_col].nunique()}")
        print(f"  Unique time slots analyzed: {mid['hour_float'].nunique()}")
        print(f"  Sample ratios: min={mid['ratio_winter_to_summer'].min():.3f}, max={mid['ratio_winter_to_summer'].max():.3f}")
        print(f"\n  NOTE: Energy loss is calculated per half-hour time slot (median values).")
        print(f"        Multiply by actual days in winter period for total seasonal loss.")
        print(f"        Example: If winter data spans 20 days, multiply loss by ~20 days worth of these hours.")

    def classify(ratio):
        if np.isnan(ratio):
            return "insufficient data"
        if ratio >= 0.95:
            return "no shading"
        if ratio >= 0.85:
            return "mild shading"
        if ratio >= 0.7:
            return "moderate shading"
        return "severe shading"

    rows = []
    for emigid, sub in mid.groupby(cfg.emigid_col):
        if len(sub) > 0:
            med = sub["ratio_winter_to_summer"].median()
            # Calculate percentage shading loss
            shading_loss_pct = (1 - med) * 100 if not np.isnan(med) else np.nan
            # Calculate average power per irradiance for both seasons
            avg_summer_power_per_irr = sub["I_summer"].mean() if len(sub) > 0 else np.nan
            avg_winter_power_per_irr = sub["I_winter"].mean() if len(sub) > 0 else np.nan
            
            # Calculate energy loss in kWh
            # I_summer and I_winter are normalized power (W per W/m²)
            # The difference represents power loss per unit irradiance per half-hour slot
            # 
            # To estimate total seasonal loss, we:
            # 1. Calculate power loss per time slot: (I_summer - I_winter) W per W/m²
            # 2. Multiply by number of winter measurements (n_winter) to get total across all days
            # 3. Multiply by 0.5 hours (30-min intervals), divide by 1000 to get kWh
            # 
            # This gives kWh per W/m². To get absolute loss, multiply by average irradiance.
            
            sub["power_loss_watts_per_Wm2"] = np.where(
                sub["I_summer"] > sub["I_winter"],
                (sub["I_summer"] - sub["I_winter"]),  # W per W/m²
                0
            )
            
            # Energy loss per time slot over all winter days in that time slot
            # n_winter = number of actual winter measurements in that half-hour slot
            sub["energy_loss_per_slot"] = sub["power_loss_watts_per_Wm2"] * sub["n_winter"] * 0.5 / 1000
            
            # Sum across all time slots to get total
            total_energy_loss_kwh_per_Wm2 = sub["energy_loss_per_slot"].sum()
            
            # Get total number of winter data points to understand time coverage
            total_winter_points = sub["n_winter"].sum() if "n_winter" in sub.columns else 0
            
            # To convert to absolute kWh, user should multiply by average winter irradiance (e.g., 400-600 W/m²)
            
            rows.append({
                "emigId": emigid,
                "median_ratio": med,
                "shading_loss_pct": shading_loss_pct,
                "energy_loss_kwh_per_Wm2": total_energy_loss_kwh_per_Wm2,
                "winter_data_points": total_winter_points,
                "avg_summer_power_per_irr": avg_summer_power_per_irr,
                "avg_winter_power_per_irr": avg_winter_power_per_irr,
                "n_hours": len(sub),
                "classification": classify(med)
            })
        else:
            rows.append({
                "emigId": emigid,
                "median_ratio": np.nan,
                "shading_loss_pct": np.nan,
                "energy_loss_kwh_per_Wm2": np.nan,
                "winter_data_points": 0,
                "avg_summer_power_per_irr": np.nan,
                "avg_winter_power_per_irr": np.nan,
                "n_hours": 0,
                "classification": "insufficient data"
            })

    # Also add inverters that have no data in the midday window
    all_inverters = comp[cfg.emigid_col].unique()
    found_inverters = {row["emigId"] for row in rows}
    for emigid in all_inverters:
        if emigid not in found_inverters:
            rows.append({
                "emigId": emigid,
                "median_ratio": np.nan,
                "shading_loss_pct": np.nan,
                "energy_loss_kwh_per_Wm2": np.nan,
                "winter_data_points": 0,
                "avg_summer_power_per_irr": np.nan,
                "avg_winter_power_per_irr": np.nan,
                "n_hours": 0,
                "classification": "insufficient data"
            })

    return pd.DataFrame(rows)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Compare summer and winter inverter CSVs to detect shading.")
    parser.add_argument("summer_csv", nargs="?", help="Path to summer CSV file (optional if using GUI)")
    parser.add_argument("winter_csv", nargs="?", help="Path to winter CSV file (optional if using GUI)")
    parser.add_argument("--weather-id", default="WETH:000274")
    parser.add_argument("--irr-col", default="poaIrradiance")
    parser.add_argument("--current-col", default="apparentPower", help="Power/current column (apparentPower, exportEnergy, or dcCurrent)")
    parser.add_argument("--out", default="shading_analysis_detailed.csv")
    args = parser.parse_args(argv)
    
    # If CSV files not provided via command line, use file dialogs
    if not args.summer_csv or not args.winter_csv:
        print("\n=== Newfold Farm Shading Analysis ===\n")
        print("Please select the CSV files using the file dialogs...\n")
        
        root = tk.Tk()
        root.withdraw()  # Hide the main window
        root.attributes('-topmost', True)  # Bring dialog to front
        
        if not args.summer_csv:
            print("Select SUMMER CSV file...")
            args.summer_csv = filedialog.askopenfilename(
                title="Select Summer CSV File",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
            )
            if not args.summer_csv:
                print("No summer file selected. Exiting.")
                root.destroy()
                return
            print(f"Summer file: {args.summer_csv}")
        
        if not args.winter_csv:
            print("\nSelect WINTER CSV file...")
            args.winter_csv = filedialog.askopenfilename(
                title="Select Winter CSV File",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
            )
            if not args.winter_csv:
                print("No winter file selected. Exiting.")
                root.destroy()
                return
            print(f"Winter file: {args.winter_csv}")
        
        root.destroy()
        print()

    cfg = Settings(
        weather_id=args.weather_id,
        irradiance_col=args.irr_col,
        current_col=args.current_col,
    )

    inv_s, w_s = load_and_prepare(args.summer_csv, cfg)
    inv_w, w_w = load_and_prepare(args.winter_csv, cfg)

    print("\n--- Processing Summer Data ---")
    ms = join_with_irradiance(inv_s, w_s, cfg)
    prof_s = build_profile(ms, cfg)

    print("\n--- Processing Winter Data ---")
    mw = join_with_irradiance(inv_w, w_w, cfg)
    prof_w = build_profile(mw, cfg)

    if len(prof_s) == 0 or len(prof_w) == 0:
        print("\n*** ERROR: Insufficient data in one or both seasons to perform comparison ***")
        print("\nPossible issues:")
        print("  1. Date ranges don't represent summer vs winter (need different seasons)")
        print("  2. Missing or invalid irradiance data")
        print("  3. Missing or invalid current data")
        print("  4. Check that dcCurrent and poaIrradiance columns exist in your CSV")
        return

    comp = compare_profiles(prof_s, prof_w, cfg)
    
    if len(comp) == 0:
        print("\n*** ERROR: No overlapping time-of-day data between summer and winter ***")
        print("\nThis typically means:")
        print("  - The CSV files are from the same time period (not different seasons)")
        print("  - Different data availability patterns between the two periods")
        print("\nTip: For shading analysis, you need:")
        print("  - Summer data: e.g., June-August")
        print("  - Winter data: e.g., December-February")
        return
    
    # Generate summary
    summary = summarise_shading(comp, cfg)
    
    # Create detailed Excel output
    excel_file = args.out.replace('.csv', '.xlsx')
    print(f"\n--- Creating detailed Excel report: {excel_file} ---")
    
    try:
        # Prepare data
        comp_detailed = comp.copy()
        comp_detailed = comp_detailed.sort_values([cfg.emigid_col, 'hour_float'])
        
        prof_s_out = prof_s.copy()
        prof_s_out = prof_s_out.sort_values([cfg.emigid_col, 'hour_float'])
        
        prof_w_out = prof_w.copy()
        prof_w_out = prof_w_out.sort_values([cfg.emigid_col, 'hour_float'])
        
        # Create methodology dataframe
        methodology = pd.DataFrame({
            'Step': [
                '1. Data Loading',
                '2. Irradiance Filtering',
                '3. Normalization',
                '4. Time-of-Day Profiling',
                '5. Seasonal Comparison',
                '6. Midday Analysis',
                '7. Classification'
            ],
            'Description': [
                f'Load summer and winter CSV files. Filter for irradiance >= {cfg.irradiance_min} W/m² and {cfg.current_col} > 0.',
                f'Only daylight hours with sufficient irradiance (>= {cfg.irradiance_min} W/m²) are included to avoid low-light noise.',
                f'Calculate normalized power: I_norm = {cfg.current_col} / {cfg.irradiance_col}. This removes the effect of varying sunlight intensity.',
                f'Group by half-hour time slot and calculate median I_norm. Requires at least {cfg.min_points_per_hour} data points per hour.',
                'Match summer and winter profiles by time-of-day (hour_float). Calculate ratio: winter/summer.',
                'Focus on 9am-3pm window when sun angle is high and shading effects are most apparent.',
                'Classify based on median ratio: >=0.95=no shading, >=0.85=mild, >=0.70=moderate, <0.70=severe'
            ],
            'Key_Metrics': [
                f'Irradiance threshold: {cfg.irradiance_min} W/m²',
                f'Power column: {cfg.current_col}',
                'I_norm = Power / Irradiance',
                f'Min points per hour: {cfg.min_points_per_hour}',
                'Ratio = I_winter / I_summer',
                'Analysis window: 9:00-15:00',
                'Thresholds: 0.95, 0.85, 0.70'
            ]
        })
        
        print(f"Writing to: {excel_file}")
        
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Write all sheets
            summary.to_excel(writer, sheet_name='Summary', index=False)
            comp_detailed.to_excel(writer, sheet_name='Hourly_Comparison', index=False)
            prof_s_out.to_excel(writer, sheet_name='Summer_Profile', index=False)
            prof_w_out.to_excel(writer, sheet_name='Winter_Profile', index=False)
            methodology.to_excel(writer, sheet_name='Methodology', index=False)
            
            # Format columns
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 80)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"✓ Excel report saved successfully: {excel_file}")
        print(f"\nThe Excel file contains 5 sheets:")
        print(f"  1. Summary - Shading classification for each inverter")
        print(f"  2. Hourly_Comparison - Hour-by-hour summer vs winter ratios")
        print(f"  3. Summer_Profile - Summer normalized power profiles")
        print(f"  4. Winter_Profile - Winter normalized power profiles")
        print(f"  5. Methodology - Detailed explanation of analysis steps")
        
    except Exception as e:
        print(f"⚠ Error creating Excel file: {e}")
        import traceback
        traceback.print_exc()
        print(f"\nSaving as CSV instead: {args.out}")
        comp.to_csv(args.out, index=False)
    
    print(f"\n--- Shading Analysis Summary ---")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
