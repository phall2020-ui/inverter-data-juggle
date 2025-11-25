#!/usr/bin/env python3
"""
Combined inverter toolkit entrypoint.

This is a thin wrapper around the more fully featured `inverter_pipeline.py`
so that existing workflows can keep using `combined_analysis.py` while
benefiting from:
  - SQLite plant registry and device reuse
  - Auto or manual clean-period selection for fouling
  - Shading and fouling workflows from one CLI

Usage:
  # Command-line (preferred)
  python combined_analysis.py fetch --start-date 20251101 --end-date 20251120 --plant-alias newfold
  python combined_analysis.py fouling-auto data.csv --auto-clean-days 3
  python combined_analysis.py shading summer.csv winter.csv

  # Interactive menu
  python combined_analysis.py interactive
"""

import sys
import tkinter as tk
from tkinter import filedialog
from typing import Sequence

from inverter_pipeline import (
    DEFAULT_DB,
    run_fetch,
    run_fouling,
    run_fouling_auto,
    run_shading,
    build_parser as build_pipeline_parser,
)


def interactive_menu() -> None:
    """
    Basic GUI-assisted menu to launch fetch, fouling-auto, or shading.
    """
    while True:
        print("\n=== Combined Inverter Toolkit (interactive) ===")
        print("1) Fetch data (uses plant registry if available)")
        print("2) Fouling (single dataset; auto/manual clean period)")
        print("3) Shading (summer vs winter)")
        print("4) Exit")
        choice = input("Choose an option: ").strip()

        if choice == "1":
            interactive_fetch()
        elif choice == "2":
            interactive_fouling_auto()
        elif choice == "3":
            interactive_shading()
        elif choice == "4":
            return
        else:
            print("Invalid choice, please select 1-4.")


def interactive_fetch() -> None:
    api_key = input("API key (leave blank to use JUGGLE_API_KEY env): ").strip() or None
    plant_alias = input("Plant alias to use (optional): ").strip() or None
    plant_uid = input("Plant UID override (optional): ").strip() or None
    weather_id = input("Weather ID override (optional): ").strip() or None
    start_date = input("Start date YYYYMMDD: ").strip()
    end_date = input("End date YYYYMMDD: ").strip()
    save_alias = input("Save/update plant alias after fetch (optional): ").strip() or None

    print("Select output CSV path...")
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    output = filedialog.asksaveasfilename(
        title="Save Combined Data As",
        defaultextension=".csv",
        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        initialfile=f"data_{start_date}_{end_date}.csv",
    )
    root.destroy()
    if not output:
        print("No file chosen; fetch cancelled.")
        return

    args = type(
        "Args",
        (),
        {
            "api_key": api_key,
            "plant_uid": plant_uid,
            "weather_id": weather_id,
            "start_date": start_date,
            "end_date": end_date,
            "min_interval_s": 1800,
            "inverter_ids": None,
            "plant_alias": plant_alias,
            "save_plant": save_alias,
            "list_plants": False,
            "db_path": DEFAULT_DB,
            "fetch_devices": True,
            "include_weather": True,
            "output": output,
        },
    )()
    run_fetch(args)


def interactive_fouling_auto() -> None:
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    data_path = filedialog.askopenfilename(
        title="Select operational CSV",
        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
    )
    root.destroy()
    if not data_path:
        print("No file selected.")
        return

    analysis_start = input("Analysis start timestamp (optional): ").strip() or None
    analysis_end = input("Analysis end timestamp (optional): ").strip() or None
    clean_start = input("Clean start timestamp (leave blank for auto): ").strip() or None
    clean_end = input("Clean end timestamp (leave blank for auto): ").strip() or None
    auto_days = int(input("Auto clean: number of top PR days [default 3]: ").strip() or "3")
    min_points = int(input("Min points per day for clean selection [default 48]: ").strip() or "48")

    args = type(
        "Args",
        (),
        {
            "data": data_path,
            "timestamp_col": "timestamp",
            "ac_col": "ac_power",
            "poa_col": "poa",
            "dc_size_kw": 1000.0,
            "analysis_start": analysis_start,
            "analysis_end": analysis_end,
            "clean_start": clean_start,
            "clean_end": clean_end,
            "auto_clean_days": auto_days,
            "min_clean_points": min_points,
            "enriched_out": f"{data_path.rsplit('.',1)[0]}_fouling_enriched.csv",
            "clean_report_out": f"{data_path.rsplit('.',1)[0]}_clean_days.csv",
        },
    )()
    run_fouling_auto(args)


def interactive_shading() -> None:
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    print("Select SUMMER CSV...")
    summer = filedialog.askopenfilename(
        title="Select Summer CSV",
        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
    )
    print("Select WINTER CSV...")
    winter = filedialog.askopenfilename(
        title="Select Winter CSV",
        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
    )
    root.destroy()
    if not summer or not winter:
        print("Missing file; shading cancelled.")
        return

    args = type(
        "Args",
        (),
        {
            "summer_csv": summer,
            "winter_csv": winter,
            "weather_id": "WETH:000274",
            "irr_col": "poaIrradiance",
            "current_col": "apparentPower",
            "detail_out": "shading_comparison.csv",
            "summary_out": "shading_summary.csv",
        },
    )()
    run_shading(args)


def main(argv: Sequence[str] | None = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] == "interactive":
        interactive_menu()
        return

    # Delegate to the pipeline parser and handlers for consistency.
    parser = build_pipeline_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
