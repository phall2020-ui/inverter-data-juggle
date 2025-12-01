#!/usr/bin/env python3
"""
ExCom (Executive Committee) Report Generator

Generates YTD and monthly summaries for solar asset performance,
including waterfall charts showing energy flow.

Features:
- YTD summary across all sites
- Monthly breakdown with November focus
- Waterfall chart showing production stages
- Export to CSV and HTML
"""

import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

from plant_store import PlantStore, DEFAULT_DB


# Time interval in hours for half-hourly data (30 minutes = 0.5 hours)
HALF_HOUR_INTERVAL = 0.5


class SitePerformance:
    """Performance metrics for a single site."""
    
    def __init__(self, name: str):
        self.name = name
        self.total_energy_kwh = 0.0
        self.peak_power_kw = 0.0
        self.avg_poa_irradiance = 0.0
        self.operating_hours = 0.0
        self.record_count = 0
        self.monthly_data: Dict[str, float] = {}
    
    def __str__(self):
        return f"{self.name}: {self.total_energy_kwh:.0f} kWh"


class ExcomReport:
    """Executive Committee performance report."""
    
    def __init__(self, year: int = 2025):
        self.year = year
        self.generated_at = datetime.now()
        self.sites: Dict[str, SitePerformance] = {}
        self.total_ytd_energy = 0.0
        self.total_november_energy = 0.0
        self.errors: List[str] = []
    
    def add_site(self, site: SitePerformance) -> None:
        """Add a site to the report."""
        self.sites[site.name] = site
        self.total_ytd_energy += site.total_energy_kwh
        november_energy = site.monthly_data.get("11", 0.0)
        self.total_november_energy += november_energy
    
    def get_summary_df(self) -> pd.DataFrame:
        """Get summary DataFrame for all sites."""
        data = []
        for name, site in sorted(self.sites.items()):
            row = {
                "Site": name,
                "YTD Energy (kWh)": round(site.total_energy_kwh, 0),
                "Peak Power (kW)": round(site.peak_power_kw, 1),
                "Nov Energy (kWh)": round(site.monthly_data.get("11", 0.0), 0),
                "Avg POA (W/m²)": round(site.avg_poa_irradiance, 1),
                "Operating Hours": round(site.operating_hours, 1),
                "Records": site.record_count,
            }
            # Add monthly data
            for month in range(1, 13):
                month_key = f"{month:02d}"
                row[f"M{month:02d} (kWh)"] = round(site.monthly_data.get(month_key, 0.0), 0)
            data.append(row)
        
        return pd.DataFrame(data)
    
    def get_november_summary(self) -> pd.DataFrame:
        """Get November-specific summary."""
        data = []
        for name, site in sorted(self.sites.items()):
            nov_energy = site.monthly_data.get("11", 0.0)
            if nov_energy > 0:
                data.append({
                    "Site": name,
                    "November Energy (kWh)": round(nov_energy, 0),
                    "% of YTD": round(nov_energy / site.total_energy_kwh * 100, 1) if site.total_energy_kwh > 0 else 0,
                })
        
        return pd.DataFrame(data)
    
    def __str__(self):
        return (
            f"ExCom Report - {self.year}\n"
            f"Generated: {self.generated_at.strftime('%Y-%m-%d %H:%M')}\n"
            f"Sites: {len(self.sites)}\n"
            f"YTD Energy: {self.total_ytd_energy:,.0f} kWh\n"
            f"November Energy: {self.total_november_energy:,.0f} kWh"
        )


def calculate_site_metrics(store: PlantStore, site_name: str, 
                          plant_uid: str, year: int = 2025) -> SitePerformance:
    """Calculate performance metrics for a site."""
    site = SitePerformance(site_name)
    
    # Get all devices for this plant
    device_ids = store.list_emig_ids(plant_uid)
    
    if not device_ids:
        return site
    
    # Filter to inverter devices
    inverter_ids = [d for d in device_ids if d.startswith("INVERT")]
    weather_ids = [d for d in device_ids if d.startswith("WETH") or d.startswith("POA")]
    
    # Date range for the year
    start_ts = f"{year}-01-01T00:00:00"
    end_ts = f"{year}-12-31T23:59:59"
    
    # Collect inverter data
    all_readings = []
    for inv_id in inverter_ids:
        readings = store.load_readings(plant_uid, inv_id, start_ts, end_ts)
        all_readings.extend(readings)
    
    if not all_readings:
        return site
    
    # Convert to DataFrame
    df = pd.DataFrame(all_readings)
    
    # Parse timestamps
    if "ts" in df.columns:
        df["datetime"] = pd.to_datetime(df["ts"], errors="coerce")
    elif "timestamp" in df.columns:
        df["datetime"] = pd.to_datetime(df["timestamp"], errors="coerce")
    
    df = df.dropna(subset=["datetime"])
    
    if df.empty:
        return site
    
    # Extract month
    df["month"] = df["datetime"].dt.strftime("%m")
    
    # Calculate energy (apparent power over time intervals)
    power_cols = ["apparentPower", "importActivePower", "power"]
    power_col = None
    for col in power_cols:
        if col in df.columns:
            power_col = col
            break
    
    if power_col:
        # Convert to numeric
        df[power_col] = pd.to_numeric(df[power_col], errors="coerce")
        
        # Estimate energy: power (W) * time interval (hours) / 1000 to get kWh
        df["energy_kwh"] = df[power_col].fillna(0) * HALF_HOUR_INTERVAL / 1000  # W to kWh
        
        # Total energy
        site.total_energy_kwh = df["energy_kwh"].sum()
        
        # Peak power
        site.peak_power_kw = df[power_col].max() / 1000 if df[power_col].max() > 0 else 0
        
        # Monthly breakdown
        monthly = df.groupby("month")["energy_kwh"].sum()
        for month, energy in monthly.items():
            site.monthly_data[month] = energy
        
        # Operating hours (non-zero power readings * interval)
        non_zero = df[df[power_col] > 0]
        site.operating_hours = len(non_zero) * HALF_HOUR_INTERVAL
    
    site.record_count = len(df)
    
    # Get POA irradiance if available
    for weather_id in weather_ids:
        weather_readings = store.load_readings(plant_uid, weather_id, start_ts, end_ts)
        if weather_readings:
            weather_df = pd.DataFrame(weather_readings)
            if "poaIrradiance" in weather_df.columns:
                poa = pd.to_numeric(weather_df["poaIrradiance"], errors="coerce")
                site.avg_poa_irradiance = poa[poa > 0].mean() if len(poa[poa > 0]) > 0 else 0
            break
    
    return site


def generate_waterfall_data(report: ExcomReport) -> pd.DataFrame:
    """Generate data for waterfall chart showing energy flow."""
    
    # Calculate monthly totals across all sites
    monthly_totals = {}
    for month in range(1, 13):
        month_key = f"{month:02d}"
        monthly_totals[month_key] = sum(
            site.monthly_data.get(month_key, 0.0) 
            for site in report.sites.values()
        )
    
    # Build waterfall data
    data = []
    cumulative = 0.0
    
    # Add monthly bars
    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    
    for i, (month_key, energy) in enumerate(sorted(monthly_totals.items())):
        data.append({
            "Category": month_names[i],
            "Value": energy,
            "Cumulative": cumulative + energy,
            "Type": "increase",
        })
        cumulative += energy
    
    # Add total bar
    data.append({
        "Category": "YTD Total",
        "Value": cumulative,
        "Cumulative": cumulative,
        "Type": "total",
    })
    
    return pd.DataFrame(data)


def generate_waterfall_html(waterfall_df: pd.DataFrame, output_path: str) -> str:
    """Generate HTML waterfall chart."""
    
    # Build simple bar chart using HTML/CSS
    max_value = waterfall_df["Cumulative"].max()
    scale = 300 / max_value if max_value > 0 else 1
    
    bars_html = []
    for _, row in waterfall_df.iterrows():
        height = int(row["Cumulative"] * scale)
        bar_height = int(row["Value"] * scale)
        color = "#4CAF50" if row["Type"] == "increase" else "#2196F3"
        
        bars_html.append(f"""
        <div class="bar-container">
            <div class="bar" style="height: {bar_height}px; background-color: {color};">
                <span class="value">{row['Value']:,.0f}</span>
            </div>
            <div class="label">{row['Category']}</div>
        </div>
        """)
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Energy Production Waterfall Chart</title>
    <style>
        body {{ font-family: Arial, sans-serif; padding: 20px; }}
        h1 {{ color: #333; }}
        .chart-container {{ 
            display: flex; 
            align-items: flex-end; 
            height: 350px; 
            padding: 20px;
            border-bottom: 2px solid #333;
        }}
        .bar-container {{ 
            display: flex; 
            flex-direction: column; 
            align-items: center; 
            margin: 0 5px;
        }}
        .bar {{ 
            width: 40px; 
            display: flex; 
            align-items: flex-start; 
            justify-content: center;
            color: white;
            font-size: 10px;
            padding-top: 5px;
        }}
        .label {{ 
            margin-top: 5px; 
            font-size: 12px; 
            font-weight: bold;
        }}
        .value {{ 
            writing-mode: vertical-rl; 
            transform: rotate(180deg);
        }}
        .legend {{
            margin-top: 20px;
            padding: 10px;
            background: #f5f5f5;
            border-radius: 5px;
        }}
        .legend span {{
            margin-right: 20px;
        }}
        .legend-color {{
            display: inline-block;
            width: 20px;
            height: 12px;
            margin-right: 5px;
            vertical-align: middle;
        }}
        table {{
            margin-top: 20px;
            border-collapse: collapse;
            width: 100%;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 8px;
            text-align: right;
        }}
        th {{
            background-color: #4CAF50;
            color: white;
        }}
        td:first-child, th:first-child {{
            text-align: left;
        }}
    </style>
</head>
<body>
    <h1>Energy Production Waterfall Chart - 2025</h1>
    
    <div class="chart-container">
        {"".join(bars_html)}
    </div>
    
    <div class="legend">
        <span><span class="legend-color" style="background: #4CAF50;"></span>Monthly Production</span>
        <span><span class="legend-color" style="background: #2196F3;"></span>YTD Total</span>
    </div>
    
    <h2>Monthly Summary (kWh)</h2>
    <table>
        <tr>
            <th>Month</th>
            <th>Energy (kWh)</th>
            <th>Cumulative (kWh)</th>
        </tr>
        {"".join(f'<tr><td>{row["Category"]}</td><td>{row["Value"]:,.0f}</td><td>{row["Cumulative"]:,.0f}</td></tr>' for _, row in waterfall_df.iterrows())}
    </table>
    
    <p><em>Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}</em></p>
</body>
</html>
"""
    
    with open(output_path, "w") as f:
        f.write(html)
    
    return output_path


def generate_excom_report(db_path: str = None, year: int = 2025, 
                         output_dir: str = None) -> ExcomReport:
    """
    Generate ExCom report from database.
    
    Args:
        db_path: Path to database file
        year: Year for report
        output_dir: Directory for output files
    
    Returns:
        ExcomReport with all metrics
    """
    if db_path is None:
        db_path = DEFAULT_DB
    
    if output_dir is None:
        output_dir = os.path.dirname(__file__)
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    store = PlantStore(db_path)
    report = ExcomReport(year)
    
    # Get all plants
    plants = store.list_all()
    
    print(f"Generating ExCom Report for {year}")
    print("=" * 60)
    
    if not plants:
        print("No plants found in database")
        return report
    
    for plant in plants:
        site_name = plant["alias"]
        plant_uid = plant["plant_uid"]
        
        print(f"Processing {site_name}...", end=" ")
        try:
            site = calculate_site_metrics(store, site_name, plant_uid, year)
            report.add_site(site)
            print(f"✓ ({site.total_energy_kwh:,.0f} kWh)")
        except Exception as e:
            report.errors.append(f"{site_name}: {e}")
            print(f"✗ ({e})")
    
    print("=" * 60)
    print(report)
    
    # Generate outputs
    if report.sites:
        # Summary CSV
        summary_df = report.get_summary_df()
        summary_path = os.path.join(output_dir, f"excom_summary_{year}.csv")
        summary_df.to_csv(summary_path, index=False)
        print(f"\nSummary saved to: {summary_path}")
        
        # November summary CSV
        nov_df = report.get_november_summary()
        if not nov_df.empty:
            nov_path = os.path.join(output_dir, f"excom_november_{year}.csv")
            nov_df.to_csv(nov_path, index=False)
            print(f"November summary saved to: {nov_path}")
        
        # Waterfall chart
        waterfall_df = generate_waterfall_data(report)
        waterfall_csv = os.path.join(output_dir, f"waterfall_data_{year}.csv")
        waterfall_df.to_csv(waterfall_csv, index=False)
        print(f"Waterfall data saved to: {waterfall_csv}")
        
        waterfall_html = os.path.join(output_dir, f"waterfall_chart_{year}.html")
        generate_waterfall_html(waterfall_df, waterfall_html)
        print(f"Waterfall chart saved to: {waterfall_html}")
    
    return report


def print_ytd_summary(report: ExcomReport) -> None:
    """Print YTD summary to console."""
    print("\n" + "=" * 60)
    print(f"YTD SUMMARY - {report.year}")
    print("=" * 60)
    
    summary_df = report.get_summary_df()
    if summary_df.empty:
        print("No data available")
        return
    
    # Print table
    print(f"\n{'Site':<35} {'YTD (kWh)':>12} {'Nov (kWh)':>12} {'Peak (kW)':>10}")
    print("-" * 70)
    
    for _, row in summary_df.iterrows():
        print(f"{row['Site']:<35} {row['YTD Energy (kWh)']:>12,.0f} {row['Nov Energy (kWh)']:>12,.0f} {row['Peak Power (kW)']:>10,.1f}")
    
    print("-" * 70)
    print(f"{'TOTAL':<35} {report.total_ytd_energy:>12,.0f} {report.total_november_energy:>12,.0f}")


def print_november_summary(report: ExcomReport) -> None:
    """Print November summary to console."""
    print("\n" + "=" * 60)
    print(f"NOVEMBER SUMMARY - {report.year}")
    print("=" * 60)
    
    nov_df = report.get_november_summary()
    if nov_df.empty:
        print("No November data available")
        return
    
    print(f"\n{'Site':<35} {'Nov Energy (kWh)':>15} {'% of YTD':>10}")
    print("-" * 62)
    
    for _, row in nov_df.iterrows():
        print(f"{row['Site']:<35} {row['November Energy (kWh)']:>15,.0f} {row['% of YTD']:>10.1f}%")
    
    print("-" * 62)
    print(f"{'TOTAL':<35} {report.total_november_energy:>15,.0f}")


def print_waterfall(report: ExcomReport) -> None:
    """Print ASCII waterfall chart to console."""
    print("\n" + "=" * 60)
    print("WATERFALL CHART - MONTHLY PRODUCTION")
    print("=" * 60)
    
    waterfall_df = generate_waterfall_data(report)
    
    if waterfall_df.empty:
        print("No data available")
        return
    
    max_cumulative = waterfall_df["Cumulative"].max()
    scale = 40 / max_cumulative if max_cumulative > 0 else 1
    
    print()
    for _, row in waterfall_df.iterrows():
        bar_len = int(row["Value"] * scale)
        bar = "█" * bar_len
        print(f"{row['Category']:<10} {bar:<40} {row['Value']:>12,.0f} kWh")
    
    print()
    print(f"{'Total':<10} {'═' * 40} {max_cumulative:>12,.0f} kWh")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate ExCom report")
    parser.add_argument("--db-path", help="Path to database file")
    parser.add_argument("--year", type=int, default=2025, help="Year for report")
    parser.add_argument("--output-dir", help="Output directory for files")
    parser.add_argument("--ytd", action="store_true", help="Show YTD summary")
    parser.add_argument("--november", action="store_true", help="Show November summary")
    parser.add_argument("--waterfall", action="store_true", help="Show waterfall chart")
    parser.add_argument("--all", action="store_true", help="Show all summaries")
    
    args = parser.parse_args()
    
    report = generate_excom_report(
        db_path=args.db_path,
        year=args.year,
        output_dir=args.output_dir
    )
    
    show_all = args.all or not (args.ytd or args.november or args.waterfall)
    
    if args.ytd or show_all:
        print_ytd_summary(report)
    
    if args.november or show_all:
        print_november_summary(report)
    
    if args.waterfall or show_all:
        print_waterfall(report)
    
    if report.errors:
        print("\nErrors:")
        for error in report.errors:
            print(f"  - {error}")
