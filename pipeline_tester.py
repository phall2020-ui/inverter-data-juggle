"""
Generic pipeline tester CLI for the local PV toolkit.

Subcommands:
  pull      - Print shapes/columns from sample inverter & POA data.
  db-info   - List tables/row counts in the SQLite registry (if present).
  fouling   - Run fouling analysis on a small synthetic dataset.
  shading   - Run shading analysis on a small synthetic dataset.
"""

import argparse
import os
import sqlite3

import pandas as pd

from Fouling_analysis import FoulingConfig, run_fouling_analysis
from Shading_analysis import (
    Settings as ShadingSettings,
    build_profile,
    compare_profiles,
    join_with_irradiance,
    load_and_prepare,
)
from plant_store import DEFAULT_DB, PlantStore


def cmd_pull(_: argparse.Namespace) -> None:
    # Small synthetic samples; avoids live API calls.
    inv = pd.DataFrame(
        {
            "timestamp": ["2025-01-01T00:00:00", "2025-01-01T00:30:00"],
            "emigId": ["INV:1", "INV:1"],
            "poaIrradiance": [800, 850],
            "apparentPower": [400, 420],
        }
    )
    poa = pd.DataFrame(
        {
            "timestamp": ["2025-01-01T00:00:00", "2025-01-01T00:30:00"],
            "emigId": ["WETH:TEST", "WETH:TEST"],
            "poaIrradiance": [800, 850],
        }
    )
    print("Inverter sample:")
    print(inv.head())
    print("\nPOA sample:")
    print(poa.head())


def cmd_db_info(args: argparse.Namespace) -> None:
    db_path = args.db_path or DEFAULT_DB
    if not os.path.exists(db_path):
        print(f"DB not found: {db_path}")
        return
    conn = sqlite3.connect(db_path)
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cur.fetchall()]
    print(f"Tables in {db_path}: {tables}")
    for tbl in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            print(f"  {tbl}: {count} rows")
        except Exception:
            pass
    conn.close()


def cmd_fouling(_: argparse.Namespace) -> None:
    full_df = pd.DataFrame(
        {"timestamp": pd.date_range("2025-01-01", periods=4, freq="H"), "ac_power": [9, 9, 8, 8], "poa": [900, 950, 900, 850]}
    )
    clean_df = pd.DataFrame(
        {"timestamp": pd.date_range("2025-01-02", periods=4, freq="H"), "ac_power": [10, 10, 9.8, 9.6], "poa": [900, 950, 900, 850]}
    )
    cfg = FoulingConfig(dc_size_kw=10.0)
    res = run_fouling_analysis(full_df, clean_df, cfg)
    print("Fouling results:")
    print(f"  index={res['fouling_index']:.3f}, level={res['fouling_level']}, loss={res['energy_loss_kwh_per_day']:.3f} kWh/day")


def cmd_shading(_: argparse.Namespace) -> None:
    cfg = ShadingSettings(weather_id="WETH:TEST", irradiance_col="poaIrradiance", current_col="apparentPower")
    # Create small CSVs on the fly
    summer = pd.DataFrame(
        {"timestamp": ["2025-06-01T10:00:00", "2025-06-01T11:00:00"], "emigId": ["INV:1", "INV:1"], "poaIrradiance": [800, 900], "apparentPower": [400, 500]}
    )
    summer_weather = pd.DataFrame({"timestamp": summer["timestamp"], "emigId": [cfg.weather_id] * 2, "poaIrradiance": [800, 900]})
    winter = pd.DataFrame(
        {"timestamp": ["2025-12-01T10:00:00", "2025-12-01T11:00:00"], "emigId": ["INV:1", "INV:1"], "poaIrradiance": [700, 800], "apparentPower": [280, 360]}
    )
    winter_weather = pd.DataFrame({"timestamp": winter["timestamp"], "emigId": [cfg.weather_id] * 2, "poaIrradiance": [700, 800]})

    summer_path = "tmp_summer.csv"
    winter_path = "tmp_winter.csv"
    pd.concat([summer, summer_weather]).to_csv(summer_path, index=False)
    pd.concat([winter, winter_weather]).to_csv(winter_path, index=False)

    inv_s, w_s = load_and_prepare(summer_path, cfg)
    inv_w, w_w = load_and_prepare(winter_path, cfg)
    ms = join_with_irradiance(inv_s, w_s, cfg)
    mw = join_with_irradiance(inv_w, w_w, cfg)
    prof_s = build_profile(ms, cfg)
    prof_w = build_profile(mw, cfg)
    comp = compare_profiles(prof_s, prof_w, cfg)
    print("Shading comparison sample (head):")
    print(comp.head())
    os.remove(summer_path)
    os.remove(winter_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline tester CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_pull = sub.add_parser("pull", help="Show sample inverter/POA data shapes.")
    p_pull.set_defaults(func=cmd_pull)

    p_db = sub.add_parser("db-info", help="List DB tables and row counts.")
    p_db.add_argument("--db-path", default=DEFAULT_DB)
    p_db.set_defaults(func=cmd_db_info)

    p_foul = sub.add_parser("fouling", help="Run fouling analysis on synthetic sample.")
    p_foul.set_defaults(func=cmd_fouling)

    p_shade = sub.add_parser("shading", help="Run shading analysis on synthetic sample.")
    p_shade.set_defaults(func=cmd_shading)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
