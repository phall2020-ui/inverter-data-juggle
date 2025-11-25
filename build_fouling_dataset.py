"""Export a fouling-ready dataset (timestamp, ac_power, poa) from the plant registry.

The output CSV contains:
    timestamp  ISO8601 UTC string (30-minute cadence)
    ac_power   Total plant AC power in kW (sum of selected inverters)
    poa        Plane-of-array irradiance in W/m² (converted from SolarGIS kWh/m²)

Example:
    python build_fouling_dataset.py \
        --plant-alias "Blachford UK" \
        --start 20250601 --end 20251031 \
        --output blachford_fouling_dataset.csv
"""
from __future__ import annotations

import argparse
import sys
from typing import Iterable, List

import pandas as pd

from plant_store import PlantStore
from inverter_pipeline import load_db_dataframe


def _sanitize_date(date_str: str) -> str:
    digits = "".join(ch for ch in date_str if ch.isdigit())
    if len(digits) < 8:
        raise ValueError(f"Date '{date_str}' must contain at least 8 digits (YYYYMMDD).")
    return digits[:8]


def _ensure_timestamp_column(df: pd.DataFrame) -> pd.Series:
    if "ts" in df.columns:
        ts_col = df["ts"]
    elif "timestamp" in df.columns:
        ts_col = df["timestamp"]
    elif "ts:" in df.columns:
        ts_col = df["ts:"]
    else:
        raise ValueError("No timestamp column found in dataframe.")
    return pd.to_datetime(ts_col, utc=True, errors="coerce")


def _select_inverters(store: PlantStore, plant_uid: str, override: Iterable[str] | None) -> List[str]:
    if override:
        return list(override)
    return [emig for emig in store.list_emig_ids(plant_uid) if emig.startswith("INVERT:")]


def build_dataset(
    plant_alias: str,
    start_date: str,
    end_date: str,
    output: str,
    inverter_ids: Iterable[str] | None = None,
    poa_id: str = "POA:SOLARGIS:WEIGHTED",
) -> str:
    store = PlantStore()
    saved = store.load(plant_alias)
    if not saved:
        raise SystemExit(f"Plant alias '{plant_alias}' not found in registry.")
    plant_uid = saved["plant_uid"]

    inverter_ids = _select_inverters(store, plant_uid, inverter_ids)
    if not inverter_ids:
        raise SystemExit(f"No inverter IDs found for plant {plant_alias} ({plant_uid}).")

    # Load inverter readings and aggregate to plant-level AC power (kW)
    inv_df = load_db_dataframe(store, plant_alias, start_date, end_date, inverter_ids)
    if inv_df.empty:
        raise SystemExit("No inverter data returned for the requested range.")

    inv_df = inv_df.rename(columns={"emigId": "emig_id"})
    inv_df["timestamp"] = _ensure_timestamp_column(inv_df)
    inv_df = inv_df.dropna(subset=["timestamp", "importEnergy"])
    inv_df = inv_df.sort_values(["emig_id", "timestamp"])
    
    # Compute average power from energy deltas (Wh difference between readings)
    inv_df["energy_delta_wh"] = inv_df.groupby("emig_id")["importEnergy"].diff()
    inv_df["interval_hours"] = inv_df.groupby("emig_id")["timestamp"].diff().dt.total_seconds() / 3600
    inv_df["interval_hours"] = inv_df["interval_hours"].fillna(0.5)
    inv_df["ac_power_kw"] = inv_df["energy_delta_wh"] / inv_df["interval_hours"] / 1000.0
    inv_df = inv_df.dropna(subset=["ac_power_kw"])
    
    ac_power = (
        inv_df.groupby("timestamp", as_index=False)["ac_power_kw"]
        .sum()
        .rename(columns={"ac_power_kw": "ac_power"})
    )

    # Load POA data (kWh/m² per interval) and convert to W/m²
    poa_df = load_db_dataframe(store, plant_alias, start_date, end_date, [poa_id])
    if poa_df.empty:
        raise SystemExit(f"No POA data found for {poa_id} in the requested range.")

    poa_df["timestamp"] = _ensure_timestamp_column(poa_df)
    poa_df = poa_df.dropna(subset=["timestamp", "poaIrradiance"])
    poa_df = poa_df.sort_values("timestamp")
    poa_df["interval_hours"] = poa_df["timestamp"].diff().dt.total_seconds().div(3600)
    default_interval = poa_df["interval_hours"].dropna().median()
    if pd.isna(default_interval) or default_interval <= 0:
        default_interval = 0.5
    poa_df["interval_hours"] = poa_df["interval_hours"].fillna(default_interval)
    poa_df.loc[poa_df["interval_hours"] <= 0, "interval_hours"] = default_interval
    poa_df["poa"] = (
        poa_df["poaIrradiance"].astype(float) * 1000.0 / poa_df["interval_hours"]
    )

    poa = poa_df[["timestamp", "poa"]]

    merged = ac_power.merge(poa, on="timestamp", how="inner")
    merged = merged.sort_values("timestamp")
    merged["timestamp"] = merged["timestamp"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    merged.to_csv(output, index=False)
    return output


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plant-alias", required=True, help="Plant alias stored in registry.")
    parser.add_argument("--start", required=True, help="Start date YYYYMMDD.")
    parser.add_argument("--end", required=True, help="End date YYYYMMDD.")
    parser.add_argument("--output", required=True, help="Output CSV path.")
    parser.add_argument(
        "--inverters",
        help="Comma-separated inverter EMIG IDs (defaults to all INVERT:* devices for plant).",
    )
    parser.add_argument(
        "--poa-id",
        default="POA:SOLARGIS:WEIGHTED",
        help="POA device EMIG ID to use (default: POA:SOLARGIS:WEIGHTED).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    inverters = None
    if args.inverters:
        inverters = [part.strip() for part in args.inverters.split(",") if part.strip()]
    start = _sanitize_date(args.start)
    end = _sanitize_date(args.end)
    output = build_dataset(
        plant_alias=args.plant_alias,
        start_date=start,
        end_date=end,
        output=args.output,
        inverter_ids=inverters,
        poa_id=args.poa_id,
    )
    print(f"Dataset written to {output}")


if __name__ == "__main__":
    main(sys.argv[1:])
