"""
Shading Analysis Script v2.0 - Newfold Farm & Similar Sites
Includes Auto-detection of weather stations and Visual Plotting.

Usage:
    python shading_analysis_v2.py
    (Or drag and drop files when prompted)
"""

import argparse
import sys
import tkinter as tk
from tkinter import filedialog
from dataclasses import dataclass
from typing import Tuple, Optional

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

# --- Configuration ---
@dataclass
class Settings:
    irradiance_col: str = "poaIrradiance"
    # Tries 'apparentPower', then 'activePower', then 'dcCurrent' if not specified
    current_col_preferences: Tuple[str] = ("apparentPower", "activePower", "dcCurrent", "exportEnergy")
    timestamp_col: str = "timestamp"
    emigid_col: str = "emigId"
    irradiance_min: float = 50.0   # Lowered slightly to capture early morning shading
    min_points_per_hour: int = 2   # Minimum records to accept an hour bin

# --- Core Logic ---

def detect_weather_id(df: pd.DataFrame, cfg: Settings) -> str:
    """
    Scans the dataframe to find which ID actually contains irradiance data.
    """
    # Filter for rows where irradiance is not null and not zero
    valid_irr = df[df[cfg.irradiance_col] > 0]
    
    if valid_irr.empty:
        raise ValueError(f"No valid data found in column '{cfg.irradiance_col}'")
        
    unique_ids = valid_irr[cfg.emigid_col].unique()
    
    # Heuristic: Weather stations usually have 'WETH' or 'MET' in ID, 
    # but the most reliable method is "who has the data?"
    if len(unique_ids) == 1:
        print(f"✓ Auto-detected Weather Station: {unique_ids[0]}")
        return unique_ids[0]
    
    # If multiple devices have irradiance, prioritize one with "WETH"
    weth_ids = [uid for uid in unique_ids if "WETH" in str(uid)]
    if weth_ids:
        print(f"✓ Auto-detected Weather Station: {weth_ids[0]}")
        return weth_ids[0]
        
    # Fallback
    print(f"⚠ Multiple devices have irradiance data: {unique_ids}")
    print(f"  Selecting the first one: {unique_ids[0]}")
    return unique_ids[0]

def determine_power_col(df: pd.DataFrame, cfg: Settings) -> str:
    """Finds the best available power/current column."""
    for col in cfg.current_col_preferences:
        if col in df.columns:
            # Check if it actually has data
            if df[col].sum() != 0:
                print(f"✓ Using power column: {col}")
                return col
    
    raise ValueError(f"Could not find a valid power column. Checked: {cfg.current_col_preferences}")

def load_and_prepare(path: str, cfg: Settings) -> Tuple[pd.DataFrame, pd.DataFrame, str, str]:
    """Load CSV, split weather/inverter, and auto-detect settings."""
    print(f"Loading {path}...")
    try:
        df = pd.read_csv(path)
    except Exception as e:
        sys.exit(f"Error reading {path}: {e}")

    # Standardize timestamp
    if cfg.timestamp_col not in df.columns:
        # Try finding a column that looks like 'time'
        time_cols = [c for c in df.columns if 'time' in c.lower() or 'date' in c.lower()]
        if time_cols:
            cfg.timestamp_col = time_cols[0]
            print(f"  Using '{cfg.timestamp_col}' as timestamp.")
        else:
            sys.exit("Could not find timestamp column.")

    df["dt"] = pd.to_datetime(df[cfg.timestamp_col], errors="coerce")
    df = df.dropna(subset=["dt"]).copy()
    
    # Create float hour (e.g., 10:30 -> 10.5)
    df["hour_float"] = df["dt"].dt.hour + df["dt"].dt.minute / 60.0
    
    # Detect Columns & ID if not already set
    if cfg.irradiance_col not in df.columns:
        sys.exit(f"Column '{cfg.irradiance_col}' missing from CSV.")
        
    weather_id = detect_weather_id(df, cfg)
    power_col = determine_power_col(df, cfg)
    
    # Split Data
    is_weather = df[cfg.emigid_col] == weather_id
    df_weather = df[is_weather].copy()
    df_inv = df[~is_weather].copy()
    
    return df_inv, df_weather, weather_id, power_col

def process_season(df_inv, df_weather, power_col, cfg):
    """Joins data and calculates normalized efficiency."""
    # Prepare weather lookup
    w = df_weather[["dt", cfg.irradiance_col]].rename(columns={cfg.irradiance_col: "irr"})
    w = w[w["irr"] >= cfg.irradiance_min] # Filter low light
    
    # Merge
    m = df_inv.merge(w, on="dt", how="inner")
    
    # Filter non-producing inverter records
    m = m[m[power_col] > 0]
    
    # Normalize: Power / Irradiance
    m["efficiency"] = m[power_col] / m["irr"]
    
    # Group by ID and Hour
    profile = (
        m.groupby([cfg.emigid_col, "hour_float"])["efficiency"]
        .agg(["median", "count"])
        .reset_index()
    )
    
    # Filter noise
    profile = profile[profile["count"] >= cfg.min_points_per_hour]
    return profile.rename(columns={"median": "eff_median", "count": "n_points"})


def join_with_irradiance(df_inv: pd.DataFrame, df_weather: pd.DataFrame, power_col: str, cfg: Settings) -> pd.DataFrame:
    """
    Join inverter data with irradiance data and calculate efficiency.
    Returns merged dataframe with efficiency column.
    """
    # Prepare weather lookup
    w = df_weather[["dt", cfg.irradiance_col]].rename(columns={cfg.irradiance_col: "irr"})
    w = w[w["irr"] >= cfg.irradiance_min]  # Filter low light
    
    # Merge
    m = df_inv.merge(w, on="dt", how="inner")
    
    # Filter non-producing inverter records
    m = m[m[power_col] > 0]
    
    # Normalize: Power / Irradiance
    m["efficiency"] = m[power_col] / m["irr"]
    
    return m


def build_profile(merged_df: pd.DataFrame, cfg: Settings) -> pd.DataFrame:
    """
    Build hourly efficiency profile from merged data.
    This is an alias for the grouping logic used in process_season.
    """
    # Group by ID and Hour
    profile = (
        merged_df.groupby([cfg.emigid_col, "hour_float"])["efficiency"]
        .agg(["median", "count"])
        .reset_index()
    )
    
    # Filter noise
    profile = profile[profile["count"] >= cfg.min_points_per_hour]
    return profile.rename(columns={"median": "eff_median", "count": "n_points"})


def compare_profiles(prof_summer: pd.DataFrame, prof_winter: pd.DataFrame, cfg: Settings) -> pd.DataFrame:
    """
    Compare summer (baseline) and winter (test) profiles.
    Returns merged dataframe with ratio and loss delta columns.
    """
    m = prof_summer.merge(
        prof_winter, 
        on=[cfg.emigid_col, "hour_float"], 
        suffixes=('_summer', '_winter')
    )
    
    m["ratio"] = m["eff_median_winter"] / m["eff_median_summer"]
    m["loss_delta"] = m["eff_median_summer"] - m["eff_median_winter"]
    
    return m


def summarise_shading(compared_df: pd.DataFrame, cfg: Settings) -> pd.DataFrame:
    """
    Summarize shading analysis results by inverter.
    Filters for core daylight hours (9am-3pm) and classifies shading severity.
    """
    # Filter for Core Window (9am - 3pm)
    core_m = compared_df[(compared_df["hour_float"] >= 9) & (compared_df["hour_float"] <= 15)].copy()
    
    # Summarize by inverter
    summary = core_m.groupby(cfg.emigid_col)["ratio"].median().reset_index()
    summary["classification"] = np.select(
        [summary["ratio"] >= 0.95, summary["ratio"] >= 0.85, summary["ratio"] >= 0.70],
        ["No Shading", "Mild Shading", "Moderate Shading"],
        default="Severe Shading"
    )
    
    return summary

def generate_plots(merged_data: pd.DataFrame, output_pdf: str, cfg: Settings):
    """Generates a multi-page PDF with visual shading analysis."""
    print(f"\n--- Generating Visual Report: {output_pdf} ---")
    
    inverters = merged_data[cfg.emigid_col].unique()
    
    with PdfPages(output_pdf) as pdf:
        # Create a summary page
        plt.figure(figsize=(10, 6))
        plt.text(0.5, 0.5, f"Shading Analysis Report\n\nTotal Inverters: {len(inverters)}\n\nSee subsequent pages for daily profiles.", 
                 ha='center', va='center', fontsize=14)
        plt.axis('off')
        pdf.savefig()
        plt.close()
        
        for inv in inverters:
            subset = merged_data[merged_data[cfg.emigid_col] == inv].sort_values("hour_float")
            
            if len(subset) < 4: continue # Skip if barely any data
            
            # Setup Plot
            fig, ax = plt.subplots(figsize=(10, 6))
            
            # Plot Summer (Baseline)
            ax.plot(subset["hour_float"], subset["eff_summer"], 
                    color='orange', marker='o', label='Summer (Baseline)', linewidth=2)
            
            # Plot Winter (Test)
            ax.plot(subset["hour_float"], subset["eff_winter"], 
                    color='blue', marker='x', linestyle='--', label='Winter (Observed)')
            
            # Highlight the Loss (Shading)
            ax.fill_between(subset["hour_float"], subset["eff_summer"], subset["eff_winter"],
                            where=(subset["eff_winter"] < subset["eff_summer"]),
                            interpolate=True, color='red', alpha=0.2, label='Energy Loss')
            
            # Calculate average ratio for title
            avg_ratio = subset["ratio"].median()
            status = "Clean"
            if avg_ratio < 0.7: status = "SEVERE SHADING"
            elif avg_ratio < 0.85: status = "Mild/Moderate Shading"
            
            title_color = 'red' if avg_ratio < 0.85 else 'green'
            
            ax.set_title(f"Inverter: {inv} | Status: {status} (Ratio: {avg_ratio:.2f})", 
                         color=title_color, fontweight='bold')
            ax.set_xlabel("Hour of Day (24h)")
            ax.set_ylabel("Normalized Efficiency (W / W/m²)")
            ax.set_xlim(8, 17) # Focus on daylight hours
            ax.grid(True, linestyle=':', alpha=0.6)
            ax.legend()
            
            pdf.savefig(fig)
            plt.close(fig)
            
    print("✓ Plots saved.")

def main():
    parser = argparse.ArgumentParser(description="Solar Shading Analysis v2")
    parser.add_argument("--out", default="shading_report")
    args = parser.parse_args()
    
    cfg = Settings()

    # GUI File Selection
    print("Select SUMMER (Baseline) CSV...")
    root = tk.Tk(); root.withdraw(); root.attributes('-topmost', True)
    summer_path = filedialog.askopenfilename(title="Select Summer CSV", filetypes=[("CSV", "*.csv")])
    if not summer_path: return

    print("Select WINTER (Test) CSV...")
    winter_path = filedialog.askopenfilename(title="Select Winter CSV", filetypes=[("CSV", "*.csv")])
    if not winter_path: return
    root.destroy()

    # 1. Load & Detect
    inv_s, w_s, wid, p_col = load_and_prepare(summer_path, cfg)
    inv_w, w_w, _, _ = load_and_prepare(winter_path, cfg)
    
    # Ensure we use the same power column for both (in case column names shifted slightly)
    # This assumes the schema is relatively consistent, if not, we might need to re-detect for winter.
    
    print(f"\nAnalyzing using Weather Station: {wid}")
    print(f"Comparing Column: {p_col} / {cfg.irradiance_col}")

    # 2. Build Profiles
    print("Building Seasonal Profiles...")
    prof_s = process_season(inv_s, w_s, p_col, cfg)
    prof_w = process_season(inv_w, w_w, p_col, cfg)

    # 3. Compare
    print("Comparing Data...")
    m = prof_s.merge(prof_w, on=[cfg.emigid_col, "hour_float"], suffixes=('_summer', '_winter'))
    
    m["ratio"] = m["eff_winter"] / m["eff_summer"]
    m["loss_delta"] = m["eff_summer"] - m["eff_winter"]
    
    # 4. Filter for Core Window (9am - 3pm)
    core_m = m[(m["hour_float"] >= 9) & (m["hour_float"] <= 15)].copy()
    
    # 5. Summarize
    summary = core_m.groupby(cfg.emigid_col)["ratio"].median().reset_index()
    summary["classification"] = np.select(
        [summary["ratio"] >= 0.95, summary["ratio"] >= 0.85, summary["ratio"] >= 0.70],
        ["No Shading", "Mild Shading", "Moderate Shading"],
        default="Severe Shading"
    )
    
    # 6. Outputs
    excel_name = f"{args.out}.xlsx"
    pdf_name = f"{args.out}.pdf"
    
    print(f"\n--- Saving Results ---")
    try:
        with pd.ExcelWriter(excel_name, engine='openpyxl') as writer:
            summary.to_excel(writer, sheet_name="Summary", index=False)
            m.to_excel(writer, sheet_name="Detailed_Hourly", index=False)
        print(f"✓ Excel data saved to {excel_name}")
    except Exception as e:
        print(f"Error saving Excel: {e}")
        
    try:
        generate_plots(m, pdf_name, cfg)
        print(f"✓ PDF report saved to {pdf_name}")
    except Exception as e:
        print(f"Error saving Plots: {e}")

    print("\nDone.")

if __name__ == "__main__":
    main()