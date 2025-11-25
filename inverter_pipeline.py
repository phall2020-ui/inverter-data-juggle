"""
Unified inverter toolkit that bundles three workflows:

1) Fetch inverter (and weather) data from the Juggle API.
2) Run fouling analysis (requires a clean-period dataset).
3) Run shading analysis comparing summer vs winter datasets.

Each workflow is exposed as an argparse subcommand so you can pick the
piece you need. Existing per-task scripts remain available, but this
wrapper avoids jumping between files.
"""

import argparse
import json
import logging
import os
import sys
from typing import List, Sequence

import tkinter as tk
from tkinter import filedialog

import pandas as pd
import requests

from fetch_inverter_data import (
    BASE_URL,
    Config as FetchConfig,
    NEWFOLD_INVERTER_IDS,
    NEWFOLD_WEATHER_ID,
    get_plant_devices,
    fetch_all_readings,
    write_combined_csv,
)
from Fouling_analysis import (
    FoulingConfig,
    auto_select_clean_period,
    filter_by_date_range,
    run_fouling_analysis,
)
from Shading_analysis import (
    Settings as ShadingSettings,
    build_profile,
    compare_profiles,
    join_with_irradiance,
    load_and_prepare,
    summarise_shading,
)
from plant_store import DEFAULT_DB, PlantStore


logger = logging.getLogger("inverter_pipeline")


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _validate_date(date_str: str) -> None:
    if not date_str or len(date_str) != 8 or not date_str.isdigit():
        raise SystemExit(f"Invalid date '{date_str}'. Use YYYYMMDD.")
    # datetime parsing could be added if needed; length/digits catches most typos.


def _sanitize_date(date_str: str) -> str:
    """Keep only digits and truncate to 8 chars (YYYYMMDD style)."""
    digits = "".join(ch for ch in date_str if ch.isdigit())
    return digits[:8]


def _date_range_to_ts(start_date: str, end_date: str) -> tuple[str, str]:
    """Convert YYYYMMDD dates to timestamp bounds for DB queries.
    
    Format to match ISO timestamps in DB: YYYY-MM-DDTHH:MM:SS
    """
    # Convert YYYYMMDD to YYYY-MM-DD
    if len(start_date) == 8 and start_date.isdigit():
        start_formatted = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
    else:
        start_formatted = start_date
    
    if len(end_date) == 8 and end_date.isdigit():
        end_formatted = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
    else:
        end_formatted = end_date
    
    return f"{start_formatted}T00:00:00", f"{end_formatted}T23:59:59"


def load_db_dataframe(store: PlantStore, plant_alias: str, start_date: str, end_date: str, emig_ids: List[str] | None = None) -> pd.DataFrame:
    """Load readings from database and flatten nested JSON values."""
    saved = store.load(plant_alias)
    if not saved:
        raise SystemExit(f"Plant alias '{plant_alias}' not found in registry.")
    plant_uid = saved["plant_uid"]
    ids = emig_ids
    if not ids:
        ids = store.list_emig_ids(plant_uid)
    if not ids:
        return pd.DataFrame()
    start_ts, end_ts = _date_range_to_ts(start_date, end_date)
    rows: list[dict] = []
    for emig in ids:
        readings = store.load_readings(plant_uid, emig, start_ts, end_ts)
        # Flatten nested dict values (e.g., {"value": 123, "unit": "W"} -> 123)
        for reading in readings:
            for key, val in reading.items():
                if isinstance(val, dict) and "value" in val:
                    reading[key] = val["value"]
        rows.extend(readings)
    return pd.DataFrame(rows)


def query_db_day(store: PlantStore, plant_alias: str, date_yyyymmdd: str, emig_ids: List[str] | None = None) -> pd.DataFrame:
    """Query a single day from database and flatten nested JSON values."""
    saved = store.load(plant_alias)
    if not saved:
        raise SystemExit(f"Plant alias '{plant_alias}' not found in registry.")
    plant_uid = saved["plant_uid"]
    ids = emig_ids or store.list_emig_ids(plant_uid)
    if not ids:
        return pd.DataFrame()
    # Convert YYYYMMDD to YYYY-MM-DD format for timestamp comparison
    if len(date_yyyymmdd) == 8 and date_yyyymmdd.isdigit():
        date_formatted = f"{date_yyyymmdd[:4]}-{date_yyyymmdd[4:6]}-{date_yyyymmdd[6:8]}"
    else:
        date_formatted = date_yyyymmdd
    start_ts = f"{date_formatted}T00:00:00"
    end_ts = f"{date_formatted}T23:59:59"
    rows: list[dict] = []
    for emig in ids:
        readings = store.load_readings(plant_uid, emig, start_ts, end_ts)
        # Flatten nested dict values
        for reading in readings:
            for key, val in reading.items():
                if isinstance(val, dict) and "value" in val:
                    reading[key] = val["value"]
        rows.extend(readings)
    return pd.DataFrame(rows)


def discover_plants(api_key: str) -> List[dict]:
    """
    Discover plants available to the API key using the Juggle endpoint.
    Tries a few possible list endpoints; logs and returns [] on failure.
    """
    headers = {"Authorization": f"token {api_key}"}
    urls = [
        f"{BASE_URL}/plants",
        f"{BASE_URL}/plants/list",
        f"{BASE_URL}/plant/list",
        f"{BASE_URL}/plants/",
        f"{BASE_URL}/plants/list/",
        f"{BASE_URL}/plant/list/",
    ]
    for url in urls:
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 404:
                logger.debug(f"Plant discovery endpoint not found: {url}")
                continue
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and "plants" in data:
                data = data["plants"]
            if not isinstance(data, list):
                continue
            normalized = []
            for p in data:
                uid = p.get("uid") or p.get("plantUid") or p.get("plant_uid") or p.get("id") or p.get("emigId")
                name = p.get("name") or p.get("plantName") or p.get("title") or uid
                if uid:
                    normalized.append({"uid": uid, "name": name})
            if normalized:
                return normalized
        except Exception as exc:  # noqa: BLE001
            logger.debug(f"Plant discovery failed at {url}: {exc}")
            continue
    # Fallback brute force scan for known prefixes if list endpoints are unavailable
    prefixes = ["ERS", "AMP"]
    found: List[dict] = []
    for prefix in prefixes:
        for i in range(1, 101):
            uid = f"{prefix}:{i:05d}"
            try:
                resp = requests.get(f"{BASE_URL}/plant/{uid}", headers=headers, timeout=10)
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()
                data = resp.json()
                name = data.get("name") or data.get("plantName") or uid
                found.append({"uid": uid, "name": name})
            except Exception:
                continue
    if found:
        return found
    logger.info("No plants discovered via API; falling back to registry/manual selection.")
    return []


# -----------------------------------------------------------------------------
# Fetch workflow
# -----------------------------------------------------------------------------

def _parse_inverter_ids(raw_ids: str) -> List[str]:
    return [part.strip() for part in raw_ids.split(",") if part.strip()]


def run_fetch(args: argparse.Namespace) -> None:
    store = PlantStore(args.db_path)

    if args.list_plants:
        records = store.list_all()
        if not records:
            logger.info("No plants saved yet.")
            return
        logger.info("Saved plants:")
        for rec in records:
            alias = rec["alias"]
            plant_uid = rec["plant_uid"]
            weather_id = rec["weather_id"] or "-"
            logger.info(f"  {alias}: plant_uid={plant_uid}, weather_id={weather_id}")
        return

    saved = store.load(args.plant_alias) if args.plant_alias else None
    if not saved and not args.plant_uid:
        saved = store.first()
        if saved:
            logger.info(f"No plant specified; defaulting to saved plant '{saved['alias']}'.")

    api_key = args.api_key or os.environ.get("JUGGLE_API_KEY")
    if not api_key:
        raise SystemExit("API key is required (pass --api-key or set JUGGLE_API_KEY).")
    api_key = api_key.strip()

    plant_uid = (args.plant_uid or (saved["plant_uid"] if saved else None) or os.environ.get("JUGGLE_PLANT_UID", "ERS:00001")).strip()
    weather_id = (args.weather_id or (saved["weather_id"] if saved else None) or NEWFOLD_WEATHER_ID).strip()
    min_interval_s = args.min_interval_s or int(os.environ.get("JUGGLE_MIN_INTERVAL_S", "1800"))
    args.start_date = _sanitize_date(args.start_date)
    args.end_date = _sanitize_date(args.end_date)
    _validate_date(args.start_date)
    _validate_date(args.end_date)

    cfg = FetchConfig(
        api_key=api_key,
        plant_uid=plant_uid,
        start_date=args.start_date,
        end_date=args.end_date,
        min_interval_s=min_interval_s,
    )

    inverter_ids = None
    if args.inverter_ids:
        inverter_ids = _parse_inverter_ids(args.inverter_ids)
    elif os.environ.get("JUGGLE_INVERTER_IDS"):
        inverter_ids = _parse_inverter_ids(os.environ["JUGGLE_INVERTER_IDS"])
    elif saved:
        inverter_ids = saved.get("inverter_ids")
        if inverter_ids == []:
            inverter_ids = None

    if inverter_ids is None and args.fetch_devices:
        try:
            inverter_ids = get_plant_devices(cfg)
            logger.info(f"Auto-discovered {len(inverter_ids)} inverters via plant endpoint.")
        except Exception as exc:
            logger.warning(f"Failed to auto-discover inverters: {exc}")

    if inverter_ids is None:
        # Fallback to any inverter IDs already stored in the DB for this plant
        cached_ids = store.list_emig_ids(plant_uid)
        if cached_ids:
            inverter_ids = cached_ids
            logger.info(f"Using {len(inverter_ids)} inverter IDs from database cache for plant {plant_uid}.")

    if inverter_ids is None or len(inverter_ids) == 0:
        logger.error(
            "No inverter IDs found for this plant. "
            "Set --inverter-ids, enable --fetch-devices, or ensure prior inverter data exists in the database."
        )
        return

    if inverter_ids is None or len(inverter_ids) == 0:
        logger.error(
            "No inverter IDs found for this plant. "
            "Set --inverter-ids, enable --fetch-devices, or ensure prior inverter data exists in the database."
        )
        return

    include_weather = args.include_weather
    all_readings = []

    if include_weather:
        if not args.force_download and store.has_fetch(plant_uid, weather_id, cfg.start_date, cfg.end_date):
            logger.info(f"Skipping weather {weather_id}; already cached for {cfg.start_date}-{cfg.end_date}.")
        else:
            try:
                logger.info(f"Fetching weather data for {weather_id} {cfg.start_date}-{cfg.end_date}...")
                weather = fetch_all_readings(cfg, weather_id)
                for rec in weather:
                    rec["emigId"] = weather_id
                all_readings.extend(weather)
                logger.info(f"  Retrieved {len(weather)} weather rows.")
                store.record_fetch(plant_uid, weather_id, cfg.start_date, cfg.end_date)
                store.store_readings(plant_uid, weather_id, weather)
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"Failed to fetch weather data for {weather_id}: {exc}")

    for emig_id in inverter_ids:
        if not args.force_download and store.has_fetch(plant_uid, emig_id, cfg.start_date, cfg.end_date):
            logger.info(f"Skipping inverter {emig_id}; already cached for {cfg.start_date}-{cfg.end_date}.")
            continue
        logger.info(f"Fetching inverter {emig_id} {cfg.start_date}-{cfg.end_date}...")
        try:
            rows = fetch_all_readings(cfg, emig_id)
            for rec in rows:
                rec["emigId"] = emig_id
            all_readings.extend(rows)
            logger.info(f"  Retrieved {len(rows)} rows.")
            store.record_fetch(plant_uid, emig_id, cfg.start_date, cfg.end_date)
            store.store_readings(plant_uid, emig_id, rows)
        except requests.HTTPError as exc:
            logger.warning(f"  Failed to fetch {emig_id}: {exc}")

    if not all_readings:
        logger.warning("No data fetched; nothing to write.")
        return

    output_file = args.output or f"newfold_data_{cfg.start_date}_{cfg.end_date}.csv"
    try:
        write_combined_csv(all_readings, output_file)
    except PermissionError as exc:
        logger.error(f"Could not write to {output_file}: {exc}")
        alt_path = output_file + ".alt.csv"
        try:
            write_combined_csv(all_readings, alt_path)
            logger.info(f"Wrote data to alternate file: {alt_path}")
        except Exception as exc2:
            logger.error(f"Alternate write also failed: {exc2}")

    if args.save_plant:
        store.save(args.save_plant, plant_uid, inverter_ids, weather_id, None)
        logger.info(f"Saved plant '{args.save_plant}' to registry {args.db_path}")


# -----------------------------------------------------------------------------
# Fouling workflow
# -----------------------------------------------------------------------------

def run_fouling(args: argparse.Namespace) -> None:
    if hasattr(args, "full_df") and hasattr(args, "clean_df"):
        full_df = args.full_df
        clean_df = args.clean_df
    else:
        full_df = pd.read_csv(args.full_data)
        clean_df = pd.read_csv(args.clean_data)

    cfg = FoulingConfig(
        dc_size_kw=args.dc_size_kw,
        column_map=None,
    )

    results = run_fouling_analysis(full_df, clean_df=clean_df, cfg=cfg)

    print("\nFouling analysis:")
    print(f"  Fouling index: {results['fouling_index']:.3f}")
    print(f"  Level: {results['fouling_level']}")
    print(f"  Energy loss (kWh/day): {results['energy_loss_kwh_per_day']:.3f}")
    print(f"  Cleaning events detected: {results['cleaning_events_detected']}")

    if args.enriched_out:
        results["df"].to_csv(args.enriched_out, index=False)
        print(f"Enriched dataset saved to {args.enriched_out}")


def run_fouling_auto(args: argparse.Namespace) -> None:
    if hasattr(args, "data_df"):
        df = args.data_df
    else:
        df = pd.read_csv(args.data)
    
    # Preprocess: extract numeric values from dict columns (handles DB JSON payloads)
    def extract_value(val):
        """Extract numeric value from dict-like structures or return as-is."""
        if isinstance(val, dict) and "value" in val:
            return val["value"]
        return val
    
    # Apply to all columns that might contain nested values
    for col in df.columns:
        if df[col].dtype == object:  # Check if column contains objects (dicts, strings, etc)
            # Try to extract values, but don't fail if it's just strings
            try:
                df[col] = df[col].apply(extract_value)
            except:
                pass

    cfg = FoulingConfig(
        timestamp=args.timestamp_col,
        ac_power=args.ac_col,
        poa=args.poa_col,
        dc_size_kw=args.dc_size_kw,
        column_map=None,
    )

    analysis_start = pd.to_datetime(args.analysis_start) if args.analysis_start else None
    analysis_end = pd.to_datetime(args.analysis_end) if args.analysis_end else None
    df = filter_by_date_range(df, cfg, analysis_start, analysis_end)

    clean_start = pd.to_datetime(args.clean_start) if args.clean_start else None
    clean_end = pd.to_datetime(args.clean_end) if args.clean_end else None

    if clean_start or clean_end:
        clean_df = filter_by_date_range(df, cfg, clean_start, clean_end)
        daily_stats = pd.DataFrame()
        if clean_df.empty:
            raise SystemExit(
                "Clean period selection produced an empty dataset. "
                "Please rerun and provide a valid clean window or separate clean CSV."
            )
        print(f"Using user-defined clean window: {args.clean_start or '-'} to {args.clean_end or '-'}")
    else:
        clean_df, daily_stats = auto_select_clean_period(
            df,
            cfg,
            days=args.auto_clean_days,
            min_points_per_day=args.min_clean_points,
        )
        if clean_df.empty:
            raise SystemExit(
                "Auto clean-period detection failed (no suitable high-PR days found). "
                "Please rerun and specify --clean-start/--clean-end or provide a clean CSV."
            )
        print(f"Auto-selected clean period spanning {clean_df[cfg.timestamp].min()} to {clean_df[cfg.timestamp].max()}")
        if not daily_stats.empty:
            print("Top clean days by median PR:")
            print(daily_stats.to_string(index=False))

    results = run_fouling_analysis(df, clean_df=clean_df, cfg=cfg)

    logger.info("Fouling analysis results")
    logger.info(f"  Fouling index: {results['fouling_index']:.3f}")
    logger.info(f"  Level: {results['fouling_level']}")
    logger.info(f"  Energy loss (kWh/day): {results['energy_loss_kwh_per_day']:.3f}")
    logger.info(f"  Cleaning events detected: {results['cleaning_events_detected']}")

    if args.enriched_out:
        results["df"].to_csv(args.enriched_out, index=False)
        logger.info(f"Enriched dataset saved to {args.enriched_out}")
    if args.clean_report_out and not daily_stats.empty:
        daily_stats.to_csv(args.clean_report_out, index=False)
        logger.info(f"Clean-day selection report saved to {args.clean_report_out}")


# -----------------------------------------------------------------------------
# Shading workflow
# -----------------------------------------------------------------------------

def run_shading(args: argparse.Namespace) -> None:
    weather_id = args.weather_id
    if args.plant_alias:
        store = PlantStore(args.db_path)
        saved = store.load(args.plant_alias)
        if saved and saved.get("weather_id"):
            weather_id = saved["weather_id"]
            logger.info(f"Using weather ID {weather_id} from plant '{args.plant_alias}'.")
    elif not weather_id:
        store = PlantStore(args.db_path)
        saved = store.first()
        if saved and saved.get("weather_id"):
            weather_id = saved["weather_id"]
            logger.info(f"No weather ID provided; defaulting to plant '{saved['alias']}' value {weather_id}.")

    cfg = ShadingSettings(
        weather_id=weather_id or "WETH:000274",
        irradiance_col=args.irr_col,
        current_col=args.current_col,
        irradiance_min=args.irr_min,
        min_points_per_hour=args.min_points_per_hour,
    )

    def prepare(source):
        if isinstance(source, pd.DataFrame):
            df = source.copy()
            # Normalize timestamp column
            if "timestamp" not in df.columns and "ts" in df.columns:
                df = df.rename(columns={"ts": "timestamp"})
            if "timestamp" not in df.columns:
                raise SystemExit("Dataframe missing 'timestamp' column.")
            df["dt"] = pd.to_datetime(df["timestamp"], errors="coerce")
            df = df.dropna(subset=["dt"]).copy()
            df["hour_float"] = df["dt"].dt.hour + df["dt"].dt.minute / 60.0
            # Split weather vs inverter
            is_weather = df["emigId"] == cfg.weather_id
            df_weather = df[is_weather].copy()
            df_inv = df[~is_weather].copy()
            if cfg.irradiance_col not in df_weather.columns:
                raise SystemExit(f"Weather data missing irradiance column '{cfg.irradiance_col}'.")
            return df_inv, df_weather
        else:
            return load_and_prepare(source, cfg)

    if hasattr(args, "summer_df") and hasattr(args, "winter_df"):
        inv_s, w_s = prepare(args.summer_df)
        inv_w, w_w = prepare(args.winter_df)
    else:
        inv_s, w_s = prepare(args.summer_csv)
        inv_w, w_w = prepare(args.winter_csv)

    print("\n--- Shading workflow ---")
    print(f"Weather ID: {cfg.weather_id}")
    print(f"Columns: irradiance='{cfg.irradiance_col}', power='{cfg.current_col}'")
    print("\n--- Summer ---")
    ms = join_with_irradiance(inv_s, w_s, cfg)
    print(f"Joined summer rows: {len(ms)} (inv={len(inv_s)}, weather={len(w_s)})")
    prof_s = build_profile(ms, cfg)
    print(f"Summer profile rows: {len(prof_s)}")

    print("\n--- Winter ---")
    mw = join_with_irradiance(inv_w, w_w, cfg)
    print(f"Joined winter rows: {len(mw)} (inv={len(inv_w)}, weather={len(w_w)})")
    prof_w = build_profile(mw, cfg)
    print(f"Winter profile rows: {len(prof_w)}")

    if len(prof_s) == 0 or len(prof_w) == 0:
        print("Insufficient data to compare profiles.")
        return

    comp = compare_profiles(prof_s, prof_w, cfg)
    if len(comp) == 0:
        print("No overlapping time-of-day data between summer and winter.")
        return

    summary = summarise_shading(comp, cfg)

    print("\nShading summary:")
    print(summary.to_string(index=False))

    if args.detail_out:
        comp.to_csv(args.detail_out, index=False)
        logger.info(f"Detailed comparison saved to {args.detail_out}")
    if args.summary_out:
        summary.to_csv(args.summary_out, index=False)
        logger.info(f"Summary saved to {args.summary_out}")


# ----------------------------------------------------------------------------- 
# Query workflow (DB)
# -----------------------------------------------------------------------------

def run_query(args: argparse.Namespace) -> None:
    store = PlantStore(args.db_path)
    date = _sanitize_date(args.date)
    df = query_db_day(store, args.plant_alias, date, _parse_inverter_ids(args.emig_ids) if args.emig_ids else None)
    if df.empty:
        span = None
        saved = store.load(args.plant_alias)
        if saved:
            span = store.date_span(saved["plant_uid"])
        print(f"No data found for {args.plant_alias} on {date}.")
        if span:
            print(f"Available date range in DB: {span['min']} to {span['max']}")
        return
    print(f"Found {len(df)} records for {args.plant_alias} on {date}. Columns: {', '.join(df.columns)}")
    if args.output:
        df.to_csv(args.output, index=False)
        print(f"Wrote results to {args.output}")


# -----------------------------------------------------------------------------
# Plant registry management
# -----------------------------------------------------------------------------

def run_plants(args: argparse.Namespace) -> None:
    store = PlantStore(args.db_path)

    if args.action == "list":
        records = store.list_all()
        if not records:
            logger.info("No plants saved.")
            return
        logger.info("Plants in registry:")
        for rec in records:
            logger.info(f"  {rec['alias']}: plant_uid={rec['plant_uid']}, weather_id={rec['weather_id'] or '-'}")
        return

    if args.action == "delete":
        if not args.alias:
            raise SystemExit("Specify --alias to delete.")
        removed = store.delete(args.alias)
        if removed:
            logger.info(f"Deleted plant '{args.alias}'")
        else:
            logger.info(f"No plant found for alias '{args.alias}'")
        return

    if args.action == "add":
        if not args.alias or not args.plant_uid:
            raise SystemExit("Adding a plant requires --alias and --plant-uid.")
        inverter_ids = _parse_inverter_ids(args.inverter_ids) if args.inverter_ids else []
        store.save(args.alias, args.plant_uid, inverter_ids, args.weather_id, None)
        logger.info(f"Saved plant '{args.alias}' with {len(inverter_ids)} inverter IDs.")
        return

    if args.action == "export":
        payload = store.export_all()
        if args.out:
            with open(args.out, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            logger.info(f"Exported {len(payload)} plants to {args.out}")
        else:
            logger.info(json.dumps(payload, indent=2))
        return

    if args.action == "import":
        if not args.from_file:
            raise SystemExit("Import requires --from-file <path to JSON>.")
        with open(args.from_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            raise SystemExit("Import file must contain a JSON array of plant records.")
        store.import_many(data)
        logger.info(f"Imported {len(data)} plants into registry.")
        return


# -----------------------------------------------------------------------------
# Interactive menu helpers
# -----------------------------------------------------------------------------

def _ask(prompt: str, default: str | None = None) -> str:
    suffix = f" [{default}]" if default is not None else ""
    val = input(f"{prompt}{suffix}: ").strip()
    return val or (default if default is not None else "")


def _ask_bool(prompt: str, default: bool = True) -> bool:
    suffix = " [Y/n]" if default else " [y/N]"
    val = input(f"{prompt}{suffix}: ").strip().lower()
    if not val:
        return default
    return val in ("y", "yes", "1", "true")

def _select_plant_alias(store: PlantStore) -> str | None:
    records = store.list_all()
    if not records:
        return None
    print("\nSaved plants:")
    for idx, rec in enumerate(records, start=1):
        print(f"  {idx}) {rec['alias']} (plant_uid={rec['plant_uid']}, weather_id={rec['weather_id'] or '-'})")
    choice = input("Select plant by number or alias (Enter to skip): ").strip()
    if not choice:
        return records[0]["alias"]  # default to first saved
    if choice.isdigit():
        idx = int(choice)
        if 1 <= idx <= len(records):
            return records[idx - 1]["alias"]
    # treat as alias
    return choice


def interactive_menu() -> None:
    while True:
        print("\n=== Inverter Toolkit Interactive ===")
        print("1) Fetch data")
        print("2) Fouling analysis (auto clean detection)")
        print("3) Shading analysis (summer vs winter)")
        print("4) Plants registry")
        print("5) Query database by date")
        print("6) List plant devices/data")
        print("7) Exit")
        choice = input("Select option: ").strip()

        if choice == "1":
            interactive_fetch()
        elif choice == "2":
            interactive_fouling_auto()
        elif choice == "3":
            interactive_shading()
        elif choice == "4":
            interactive_plants()
        elif choice == "5":
            interactive_query()
        elif choice == "6":
            interactive_list_devices()
        elif choice == "7":
            print("Goodbye.")
            return
        else:
            print("Invalid choice.")


def interactive_list_devices() -> None:
    """List all devices and data available for a plant."""
    store = PlantStore(DEFAULT_DB)
    plant_alias = _select_plant_alias(store)
    if not plant_alias:
        print("No plant selected.")
        return
    
    saved = store.load(plant_alias)
    if not saved:
        print(f"Plant '{plant_alias}' not found in registry.")
        return
    
    plant_uid = saved["plant_uid"]
    dc_capacity = saved.get("dc_size_kw")
    
    print(f"\n{'='*70}")
    print(f"Plant: {plant_alias} ({plant_uid})")
    if dc_capacity:
        print(f"DC Capacity: {dc_capacity:.1f} kW")
    else:
        print(f"DC Capacity: Not set")
    print(f"{'='*70}")
    
    # Get all device IDs
    device_ids = store.list_emig_ids(plant_uid)
    
    if not device_ids:
        print("No devices found in database.")
        return
    
    print(f"\nFound {len(device_ids)} device(s):\n")
    
    for device_id in sorted(device_ids):
        # Get a sample reading to show date range
        try:
            # Get first and last timestamp
            sample = store.load_readings(plant_uid, device_id, "2000-01-01T00:00:00", "2099-12-31T23:59:59")
            if sample:
                timestamps = [r.get('ts') for r in sample if r.get('ts')]
                if timestamps:
                    first_ts = min(timestamps)
                    last_ts = max(timestamps)
                    count = len(sample)
                    
                    # Determine device type
                    device_type = "Unknown"
                    if device_id.startswith("POA:"):
                        if "WEIGHTED" in device_id:
                            device_type = "POA Irradiance (Capacity-Weighted)"
                        else:
                            device_type = "POA Irradiance"
                    elif device_id.startswith("INV:"):
                        device_type = "Inverter"
                    elif device_id.startswith("WETH:"):
                        device_type = "Weather"
                    
                    print(f"  📊 {device_id}")
                    print(f"     Type: {device_type}")
                    print(f"     Records: {count}")
                    print(f"     Period: {first_ts} to {last_ts}")
                    print()
        except Exception as e:
            print(f"  ⚠️  {device_id} - Error reading: {e}")
            print()
    
    print(f"{'='*70}")


def interactive_fetch() -> None:
    api_key = _ask("API key (blank to use env)", "")
    store = PlantStore(DEFAULT_DB)
    plant_alias = _select_plant_alias(store)

    discovered_plants: List[dict] = []
    if api_key or os.environ.get("JUGGLE_API_KEY"):
        key_to_use = api_key or os.environ.get("JUGGLE_API_KEY")
        try:
            plants = discover_plants(key_to_use)
            if plants:
                print("\nDiscovered plants from API:")
                for idx, p in enumerate(plants, start=1):
                    print(f"  {idx}) {p['uid']} - {p['name']}")
                choice = input("Select plant number, type 'all' to fetch all, or Enter to use registry: ").strip().lower()
                if choice == "all":
                    discovered_plants = plants
                elif choice.isdigit():
                    idx = int(choice)
                    if 1 <= idx <= len(plants):
                        discovered_plants = [plants[idx - 1]]
                else:
                    print("No selection made; will use registry/manual choice.")
            else:
                print("No plants discovered via API; use registry or enter a plant UID.")
        except Exception as exc:  # noqa: BLE001
            print(f"Plant discovery failed: {exc}")

    plant_uid_override = _ask("Plant UID override (optional)", "")
    weather_id = _ask("Weather ID override (optional)", "")
    start_date = _sanitize_date(_ask("Start date YYYYMMDD"))
    end_date = _sanitize_date(_ask("End date YYYYMMDD"))
    
    # Optional: Import SolarGIS POA data
    import_solargis = _ask_bool("Import SolarGIS POA data?", False)
    solargis_folders = []
    if import_solargis:
        # Automatically use all subfolders in the SolarGIS data directory
        base_dir = os.path.expanduser("~/OneDrive - AMPYR IDEA UK Ltd/Monthly Excom/Monthly SolarGIS data")
        
        if os.path.exists(base_dir):
            # Get all subdirectories
            all_items = os.listdir(base_dir)
            solargis_folders = [
                os.path.join(base_dir, item) 
                for item in all_items 
                if os.path.isdir(os.path.join(base_dir, item))
            ]
            
            if solargis_folders:
                print(f"\n✓ Found {len(solargis_folders)} SolarGIS data folder(s):")
                for folder in solargis_folders:
                    print(f"  - {os.path.basename(folder)}")
            else:
                print(f"\n⚠ No subfolders found in {base_dir}")
                import_solargis = False
        else:
            print(f"\n⚠ SolarGIS data directory not found: {base_dir}")
            import_solargis = False
    
    # Ask for CSV output (optional for SolarGIS-only imports)
    output = None
    skip_fetch = import_solargis and not _ask_bool("Also fetch inverter data from API?", False)
    
    if not skip_fetch:
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        print("Select output CSV path (optional - Cancel to skip)...")
        output = filedialog.asksaveasfilename(
            title="Save Combined Data As (optional)",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile=f"data_{start_date}_{end_date}.csv",
        )
        root.destroy()
    
    fetch_devices = _ask_bool("Auto-discover inverter IDs?", True) if not skip_fetch else False
    include_weather = _ask_bool("Include weather data?", True) if not skip_fetch else False

    # Determine which plants to fetch
    selected_plants: List[dict] = []
    if discovered_plants:
        selected_plants = discovered_plants
    elif plant_uid_override:
        selected_plants = [{"uid": plant_uid_override, "name": plant_uid_override}]
    elif plant_alias:
        saved = store.load(plant_alias)
        if saved:
            selected_plants = [{"uid": saved["plant_uid"], "name": plant_alias}]

    # If none selected, fall back to first saved plant
    if not selected_plants:
        manual_many = _ask("Enter comma-separated plant UIDs (optional)", "")
        if manual_many:
            uids = _parse_inverter_ids(manual_many)
            selected_plants = [{"uid": u, "name": u} for u in uids]
        else:
            saved = store.first()
            if saved:
                selected_plants = [{"uid": saved["plant_uid"], "name": saved["alias"]}]
                plant_alias = saved["alias"]

    if not selected_plants:
        manual_uid = _ask("No plant selected. Enter a plant UID to fetch (or leave blank to cancel)", "")
        if manual_uid:
            selected_plants = [{"uid": manual_uid.strip(), "name": manual_uid.strip()}]
        else:
            print("No plant selected; fetch cancelled.")
            return

    base, ext = os.path.splitext(output) if output else ("", ".csv")

    for p in selected_plants:
        uid = p["uid"]
        existing_alias = store.alias_for(uid)
        name = p.get("name")
        default_alias = existing_alias or (name.split("-")[-1].strip() if name and "-" in name else name) or uid.replace(":", "_")
        alias = default_alias
        if not existing_alias:
            alias = _ask(f"Assign alias for plant {uid}", default_alias)
        store.save(alias, uid, [], None, None)

        # Only run fetch if not skipping API fetch
        if not skip_fetch:
            out_path = output if output and len(selected_plants) == 1 else (f"{base}_{alias}{ext}" if output else None)
            
            args = argparse.Namespace(
                api_key=api_key or None,
                plant_uid=uid,
                weather_id=weather_id or None,
                start_date=start_date,
                end_date=end_date,
                min_interval_s=1800,
                inverter_ids=None,
                plant_alias=alias or None,
                save_plant=alias or None,
                list_plants=False,
                db_path=DEFAULT_DB,
                fetch_devices=fetch_devices,
                include_weather=include_weather,
                output=out_path or f"temp_{alias}_{start_date}_{end_date}.csv",
                verbose=False,
                force_download=False,
            )
            run_fetch(args)
            
            # Remove temp file if no output path specified
            if not out_path and os.path.exists(args.output):
                os.remove(args.output)
        
        # Import SolarGIS POA data if requested - import for ALL plants with matching files
        if import_solargis and solargis_folders:
            print(f"\n{'='*70}")
            print(f"=== Importing SolarGIS POA Data for All Plants ===")
            print(f"{'='*70}")
            
            # Get all plants from registry
            all_plants = store.list_all()
            
            from solargis_poa_import import import_poa_for_plant_multi_folder, store_poa_in_db
            
            imported_count = 0
            skipped_count = 0
            
            for plant_rec in all_plants:
                plant_alias = plant_rec['alias']
                plant_uid = plant_rec['plant_uid']
                
                print(f"\n--- Checking {plant_alias} ({plant_uid}) ---")
                
                try:
                    poa_df = import_poa_for_plant_multi_folder(
                        plant_name=plant_alias,
                        plant_uid=plant_uid,
                        solargis_folders=solargis_folders,
                        start_date=start_date,
                        end_date=end_date,
                        store=store,
                        fuzzy_threshold=0.5
                    )
                    
                    if poa_df is not None and not poa_df.empty:
                        # Delete old POA and weather data before importing new POA
                        print(f"  🗑️  Removing old POA and weather data...")
                        poa_deleted = store.delete_devices_by_pattern(plant_uid, 'POA:%')
                        weth_deleted = store.delete_devices_by_pattern(plant_uid, 'WETH:%')
                        if poa_deleted > 0 or weth_deleted > 0:
                            print(f"     Deleted {poa_deleted} POA records, {weth_deleted} weather records")
                        
                        store_poa_in_db(store, plant_uid, poa_df)
                        print(f"  ✅ POA data imported successfully for {plant_alias}")
                        imported_count += 1
                    else:
                        print(f"  ⏭ No matching POA data found for {plant_alias}")
                        skipped_count += 1
                except Exception as e:
                    print(f"  ❌ Failed to import SolarGIS POA for {plant_alias}: {e}")
                    skipped_count += 1
            
            print(f"\n{'='*70}")
            print(f"POA Import Summary: {imported_count} imported, {skipped_count} skipped/failed")
            print(f"{'='*70}")


def interactive_fouling() -> None:
    use_db = _ask_bool("Use data from database registry?", True)
    store = PlantStore(DEFAULT_DB)
    plant_alias = _select_plant_alias(store)
    if not plant_alias:
        print("No plant alias selected; cancelling.")
        return
    saved = store.load(plant_alias)
    dc_default = saved.get("dc_size_kw") if saved else None
    dc_prompt = str(int(dc_default)) if dc_default else "1000"
    dc_size = float(_ask("DC size kW", dc_prompt))
    # Persist DC size back to registry
    if saved:
        store.save(
            plant_alias,
            saved["plant_uid"],
            saved.get("inverter_ids", []),
            saved.get("weather_id"),
            dc_size,
        )
    if use_db:
        start_date = _sanitize_date(_ask("Full dataset start YYYYMMDD"))
        end_date = _sanitize_date(_ask("Full dataset end YYYYMMDD"))
        clean_start = _sanitize_date(_ask("Clean dataset start YYYYMMDD"))
        clean_end = _sanitize_date(_ask("Clean dataset end YYYYMMDD"))
        df_full = load_db_dataframe(store, plant_alias, start_date, end_date)
        df_clean = load_db_dataframe(store, plant_alias, clean_start, clean_end)
        if df_full.empty or df_clean.empty:
            print("No data found for given date ranges in database. Please fetch data first or adjust dates.")
            return
        enriched = _ask("Enriched output CSV (optional)", "")
        args = argparse.Namespace(
            full_df=df_full,
            clean_df=df_clean,
            dc_size_kw=dc_size,
            enriched_out=enriched or None,
            verbose=False,
        )
        run_fouling(args)
        return

    # If user chose not to use DB, fall back to CSV selection.
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    print("Select FULL dataset CSV...")
    full = filedialog.askopenfilename(
        title="Select full dataset CSV",
        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
    )
    print("Select CLEAN dataset CSV...")
    clean = filedialog.askopenfilename(
        title="Select clean dataset CSV",
        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
    )
    root.destroy()
    if not full or not clean:
        print("Missing file selection; cancelled.")
        return
    df_full = pd.read_csv(full)
    df_clean = pd.read_csv(clean)
    enriched = _ask("Enriched output CSV (optional)", "")
    args = argparse.Namespace(
        full_df=df_full,
        clean_df=df_clean,
        dc_size_kw=dc_size,
        enriched_out=enriched or None,
        verbose=False,
    )
    run_fouling(args)


def interactive_fouling_auto() -> None:
    print("\n=== FOULING ANALYSIS ===")
    print("\nThis analysis detects soiling on PV modules by comparing performance against clean periods.")
    print("\nWorkflow:")
    print("  1. Select plant and specify DC capacity")
    print("  2. Load operational data")
    print("  3. Data preparation: clean bad data, aggregate to daily level")
    print("  4. Define clean baseline period (manual date range or auto-detect)")
    print("  5. Compute performance index (Daily PR = AC Energy / (DC Capacity × Daily Insolation))")
    print("  6. Compare current performance against clean baseline")
    
    # Step 1: Select plant and get DC capacity
    store = PlantStore(DEFAULT_DB)
    plant_alias = _select_plant_alias(store)
    if not plant_alias:
        print("No plant alias selected; cancelling.")
        return
    
    saved = store.load(plant_alias)
    if not saved:
        print(f"Plant '{plant_alias}' not found in registry.")
        return
    
    print(f"\n✓ Selected plant: {plant_alias} ({saved['plant_uid']})")
    
    # Step 2: Get DC capacity
    dc_size_str = _ask("\nEnter DC capacity in kW", "1000.0")
    try:
        dc_size_kw = float(dc_size_str)
    except ValueError:
        print("Invalid DC capacity. Using default 1000 kW.")
        dc_size_kw = 1000.0
    
    print(f"✓ DC capacity: {dc_size_kw} kW")
    
    # Step 3: Load operational data
    use_db = _ask_bool("\nUse data from database registry?", True)
    data_df = None
    data_file = None
    
    if use_db:
        start_date = _sanitize_date(_ask("Operational data start date (YYYYMMDD)"))
        end_date = _sanitize_date(_ask("Operational data end date (YYYYMMDD)"))
        print(f"\nLoading data from {start_date} to {end_date}...")
        data_df = load_db_dataframe(store, plant_alias, start_date, end_date)
        if data_df.empty:
            print("❌ No data found for given date range in database.")
            print("   Please fetch data first using option 1 (Fetch data) or adjust dates.")
            return
        print(f"✓ Loaded {len(data_df)} records from database")
    else:
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        print("\nSelect operational dataset CSV...")
        data_file = filedialog.askopenfilename(
            title="Select operational dataset CSV",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        root.destroy()
        if not data_file:
            print("No file selected; cancelled.")
            return
        print(f"✓ Selected file: {data_file}")
    
    # Step 4: Define analysis window (optional)
    analysis_start = _ask("\nAnalysis window start (optional, YYYY-MM-DD HH:MM:SS)", "")
    analysis_end = _ask("Analysis window end (optional, YYYY-MM-DD HH:MM:SS)", "")
    
    # Step 5: Define clean baseline period
    print("\n--- CLEAN BASELINE PERIOD ---")
    print("The clean baseline represents the expected performance when modules are clean.")
    print("You can either:")
    print("  a) Manually specify a date range when modules were known to be clean")
    print("  b) Let the algorithm auto-detect the cleanest days based on highest PR")
    
    manual_clean = _ask_bool("\nManually specify clean period date range?", False)
    
    if manual_clean:
        print("\nEnter the date range when modules were known to be clean")
        print("(e.g., 1-3 days after washing, no rain, no heavy soiling)")
        clean_start = _ask("Clean period start (YYYY-MM-DD HH:MM:SS)")
        clean_end = _ask("Clean period end (YYYY-MM-DD HH:MM:SS)")
        auto_days = 0
        print(f"\n✓ Using manual clean period: {clean_start} to {clean_end}")
    else:
        print("\nAuto-detection will:")
        print("  1. Calculate daily Performance Ratio (PR) for all days")
        print("  2. Select the top N days with highest median PR")
        print("  3. Use those days as the clean baseline")
        print("\nFormula: Daily PR = Daily AC Energy (kWh) / (DC Capacity (kW) × Daily Insolation (kWh/m²))")
        clean_start = ""
        clean_end = ""
        auto_days = int(_ask("\nNumber of top PR days to use for clean baseline", "3"))
        min_points = int(_ask("Minimum data points per day required", "48"))
        print(f"\n✓ Will auto-detect {auto_days} cleanest days (minimum {min_points} points/day)")
    
    # Step 6: Output options
    enriched = _ask("\nEnriched output CSV path (optional, Enter to skip)", "")
    clean_report = _ask("Clean-day selection report CSV (optional, Enter to skip)", "")
    
    print("\n" + "="*60)
    print("ANALYSIS CONFIGURATION SUMMARY")
    print("="*60)
    print(f"Plant:              {plant_alias}")
    print(f"DC Capacity:        {dc_size_kw} kW")
    print(f"Data Source:        {'Database' if use_db else 'CSV file'}")
    if analysis_start or analysis_end:
        print(f"Analysis Window:    {analysis_start or 'start'} to {analysis_end or 'end'}")
    if manual_clean:
        print(f"Clean Baseline:     Manual ({clean_start} to {clean_end})")
    else:
        print(f"Clean Baseline:     Auto-detect (top {auto_days} days)")
    print("="*60)
    
    proceed = _ask_bool("\nProceed with analysis?", True)
    if not proceed:
        print("Analysis cancelled.")
        return
    
    print("\nRunning fouling analysis...")
    
    args = argparse.Namespace(
        data=data_file,
        data_df=data_df,
        timestamp_col="timestamp",
        ac_col="ac_power",
        poa_col="poa",
        dc_size_kw=dc_size_kw,
        analysis_start=analysis_start or None,
        analysis_end=analysis_end or None,
        clean_start=clean_start or None,
        clean_end=clean_end or None,
        auto_clean_days=auto_days if auto_days > 0 else 3,
        min_clean_points=min_points if not manual_clean else 48,
        enriched_out=enriched or None,
        clean_report_out=clean_report or None,
        verbose=False,
    )
    run_fouling_auto(args)


def interactive_shading() -> None:
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    use_db = _ask_bool("Use data from database registry?", True)
    store = PlantStore(DEFAULT_DB)
    plant_alias = _select_plant_alias(store)
    irr_col = _ask("Irradiance column", "poaIrradiance")
    current_col = _ask("Power/current column", "apparentPower")
    irr_min = float(_ask("Irradiance min W/m^2", "100"))
    min_points = int(_ask("Min points per hour bin", "3"))
    detail_out = _ask("Detailed comparison CSV", "shading_comparison.csv")
    summary_out = _ask("Summary CSV", "shading_summary.csv")
    if use_db:
        if not plant_alias:
            print("No plant alias selected; shading cancelled.")
            return
        auto_seasons = _ask_bool("Auto-select summer (Jun-Aug) and winter (Dec-Feb) ranges from DB?", True)
        summer_start = summer_end = winter_start = winter_end = ""
        if auto_seasons:
            saved = store.load(plant_alias)
            if saved:
                plant_uid = saved["plant_uid"]
                summer_range = store.season_range(plant_uid, ["06", "07", "08"])
                winter_range = store.season_range(plant_uid, ["12", "01", "02"])
                if summer_range:
                    summer_start = summer_range["start"]
                    summer_end = summer_range["end"]
                    print(f"Auto-selected summer: {summer_start} to {summer_end} (year {summer_range['year']})")
                if winter_range:
                    winter_start = winter_range["start"]
                    winter_end = winter_range["end"]
                    print(f"Auto-selected winter: {winter_start} to {winter_end} (year {winter_range['year']})")
        if not summer_start:
            summer_start = _sanitize_date(_ask("Summer start YYYYMMDD"))
            summer_end = _sanitize_date(_ask("Summer end YYYYMMDD"))
        if not winter_start:
            winter_start = _sanitize_date(_ask("Winter start YYYYMMDD"))
            winter_end = _sanitize_date(_ask("Winter end YYYYMMDD"))
        df_summer = load_db_dataframe(store, plant_alias, summer_start, summer_end)
        df_winter = load_db_dataframe(store, plant_alias, winter_start, winter_end)
        if df_summer.empty or df_winter.empty:
            print("No data found for given date ranges in database. Please fetch data first or adjust dates.")
            return
        args = argparse.Namespace(
            summer_df=df_summer,
            winter_df=df_winter,
            plant_alias=plant_alias or None,
            weather_id=None,
            irr_col=irr_col,
            current_col=current_col,
            irr_min=irr_min,
            min_points_per_hour=min_points,
            detail_out=detail_out,
            summary_out=summary_out,
            db_path=DEFAULT_DB,
            verbose=False,
        )
    else:
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
        args = argparse.Namespace(
            summer_csv=summer,
            winter_csv=winter,
            plant_alias=plant_alias or None,
            weather_id="WETH:000274",
            irr_col=irr_col,
            current_col=current_col,
            irr_min=irr_min,
            min_points_per_hour=min_points,
            detail_out=detail_out,
            summary_out=summary_out,
            db_path=DEFAULT_DB,
            verbose=False,
        )
    run_shading(args)


def interactive_plants() -> None:
    store = PlantStore(DEFAULT_DB)
    print("\nPlant registry:")
    records = store.list_all()
    if records:
        for rec in records:
            dc_cap = f"{rec['dc_size_kw']:.1f} kW" if rec.get('dc_size_kw') else "Not set"
            print(f"- {rec['alias']}: plant_uid={rec['plant_uid']}, weather_id={rec['weather_id'] or '-'}, DC={dc_cap}")
    else:
        print("  (empty)")
    action = _ask("Action [list/add/delete]", "list").lower()
    if action == "list":
        return
    if action == "delete":
        alias = _ask("Alias to delete")
        removed = store.delete(alias)
        print("Deleted." if removed else "Alias not found.")
        return
    if action == "add":
        alias = _ask("Alias")
        plant_uid = _ask("Plant UID")
        inverter_ids_raw = _ask("Inverter IDs (comma-separated)", "")
        inverter_ids = _parse_inverter_ids(inverter_ids_raw) if inverter_ids_raw else []
        weather_id = _ask("Weather ID (optional)", "")
        store.save(alias, plant_uid, inverter_ids, weather_id or None, None)
        print(f"Saved {alias} with {len(inverter_ids)} inverters.")
        return
    print("Unknown action.")


def interactive_query() -> None:
    store = PlantStore(DEFAULT_DB)
    plant_alias = _select_plant_alias(store)
    if not plant_alias:
        print("No plant selected.")
        return
    date = _sanitize_date(_ask("Date to query (YYYYMMDD)"))
    emig_raw = _ask("Limit to EMIG IDs (comma-separated, optional)", "")
    emigs = _parse_inverter_ids(emig_raw) if emig_raw else None
    df = query_db_day(store, plant_alias, date, emigs)
    if df.empty:
        span = None
        saved = store.load(plant_alias)
        if saved:
            span = store.date_span(saved["plant_uid"])
        print(f"No data found for {plant_alias} on {date}.")
        if span:
            print(f"Available date range in DB: {span['min']} to {span['max']}")
        return
    print(f"Found {len(df)} records. Columns: {', '.join(df.columns)}")
    save = _ask_bool("Save results to CSV?", True)
    if save:
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        out = filedialog.asksaveasfilename(
            title="Save query results",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile=f"{plant_alias}_{date}_query.csv",
        )
        root.destroy()
        if out:
            df.to_csv(out, index=False)
            print(f"Wrote {len(df)} rows to {out}")
        else:
            print("No file selected; skipping save.")


def interactive_query() -> None:
    store = PlantStore(DEFAULT_DB)
    plant_alias = _select_plant_alias(store)
    if not plant_alias:
        print("No plant selected.")
        return
    date = _sanitize_date(_ask("Date to query (YYYYMMDD)"))
    emig_raw = _ask("Limit to EMIG IDs (comma-separated, optional)", "")
    emigs = _parse_inverter_ids(emig_raw) if emig_raw else None
    df = query_db_day(store, plant_alias, date, emigs)
    if df.empty:
        span = None
        saved = store.load(plant_alias)
        if saved:
            span = store.date_span(saved["plant_uid"])
        print(f"No data found for {plant_alias} on {date}.")
        if span:
            print(f"Available date range in DB: {span['min']} to {span['max']}")
        return
    print(f"Found {len(df)} records. Columns: {', '.join(df.columns)}")
    save = _ask_bool("Save results to CSV?", True)
    if save:
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        out = filedialog.asksaveasfilename(
            title="Save query results",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile=f"{plant_alias}_{date}_query.csv",
        )
        root.destroy()
        if out:
            df.to_csv(out, index=False)
            print(f"Wrote {len(df)} rows to {out}")
        else:
            print("No file selected; skipping save.")
# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Unified inverter toolkit: fetch data, fouling analysis, shading analysis."
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    sub = parser.add_subparsers(dest="command", required=True)

    # fetch
    p_fetch = sub.add_parser("fetch", help="Fetch inverter (and weather) data from Juggle.")
    p_fetch.add_argument("--api-key", help="API key (or set JUGGLE_API_KEY env var).")
    p_fetch.add_argument("--plant-uid", help="Plant UID (default: ERS:00001).")
    p_fetch.add_argument("--start-date", required=True, help="Start date YYYYMMDD.")
    p_fetch.add_argument("--end-date", required=True, help="End date YYYYMMDD.")
    p_fetch.add_argument("--min-interval-s", type=int, default=1800, help="minIntervalS (default 1800).")
    p_fetch.add_argument("--inverter-ids", help="Comma-separated inverter EMIG IDs. Defaults to Newfold list.")
    p_fetch.add_argument("--weather-id", help="Weather station EMIG ID (overrides default or stored value).")
    p_fetch.add_argument("--plant-alias", help="Alias of stored plant to reuse.")
    p_fetch.add_argument("--save-plant", help="Alias to save/update in the registry after fetching.")
    p_fetch.add_argument("--list-plants", action="store_true", help="List saved plants and exit.")
    p_fetch.add_argument("--db-path", default=DEFAULT_DB, help="Path to plant registry SQLite file.")
    p_fetch.add_argument("--fetch-devices", action="store_true", help="Auto-discover inverter IDs from the plant endpoint.")
    p_fetch.add_argument("--include-weather", action="store_true", default=True, help="Include weather station data.")
    p_fetch.add_argument("--no-weather", dest="include_weather", action="store_false", help="Skip weather data.")
    p_fetch.add_argument("--force-download", action="store_true", help="Ignore cache and re-download data.")
    p_fetch.add_argument("--output", help="Output CSV path for combined data.")
    p_fetch.set_defaults(func=run_fetch)

    # fouling
    p_foul = sub.add_parser("fouling", help="Run fouling analysis using a clean reference dataset.")
    p_foul.add_argument("full_data", help="CSV with full operational data.")
    p_foul.add_argument("clean_data", help="CSV from a known clean period.")
    p_foul.add_argument("--dc-size-kw", type=float, default=1000.0, help="DC nameplate (kW).")
    p_foul.add_argument("--enriched-out", help="Optional CSV path for enriched dataset output.")
    p_foul.set_defaults(func=run_fouling)

    # fouling-auto
    p_foul_auto = sub.add_parser("fouling-auto", help="Run fouling analysis from one dataset with auto or manual clean period selection.")
    p_foul_auto.add_argument("data", help="CSV with operational data.")
    p_foul_auto.add_argument("--timestamp-col", default="timestamp", help="Timestamp column name.")
    p_foul_auto.add_argument("--ac-col", default="ac_power", help="AC power column name.")
    p_foul_auto.add_argument("--poa-col", default="poa", help="Plane-of-array irradiance column name.")
    p_foul_auto.add_argument("--dc-size-kw", type=float, default=1000.0, help="DC nameplate (kW).")
    p_foul_auto.add_argument("--analysis-start", help="Start timestamp for analysis window (optional).")
    p_foul_auto.add_argument("--analysis-end", help="End timestamp for analysis window (optional).")
    p_foul_auto.add_argument("--clean-start", help="Manual clean-period start timestamp.")
    p_foul_auto.add_argument("--clean-end", help="Manual clean-period end timestamp.")
    p_foul_auto.add_argument("--auto-clean-days", type=int, default=3, help="Number of top PR days to use for auto clean selection.")
    p_foul_auto.add_argument("--min-clean-points", type=int, default=48, help="Minimum points per day to accept for auto selection.")
    p_foul_auto.add_argument("--enriched-out", help="Optional CSV path for enriched dataset output.")
    p_foul_auto.add_argument("--clean-report-out", help="Optional CSV path for clean-day selection diagnostics.")
    p_foul_auto.set_defaults(func=run_fouling_auto)

    # shading
    p_shade = sub.add_parser("shading", help="Compare summer vs winter datasets for shading.")
    p_shade.add_argument("summer_csv", help="Summer CSV path.")
    p_shade.add_argument("winter_csv", help="Winter CSV path.")
    p_shade.add_argument("--plant-alias", help="Plant alias to pull weather ID from registry.")
    p_shade.add_argument("--weather-id", help="Weather station EMIG ID (overrides registry).")
    p_shade.add_argument("--db-path", default=DEFAULT_DB, help="Path to plant registry SQLite file.")
    p_shade.add_argument("--irr-col", default="poaIrradiance", help="Irradiance column name.")
    p_shade.add_argument(
        "--current-col",
        default="apparentPower",
        help="Power/current column (apparentPower, exportEnergy, or dcCurrent).",
    )
    p_shade.add_argument("--irr-min", type=float, default=100.0, help="Irradiance threshold (W/m^2).")
    p_shade.add_argument("--min-points-per-hour", type=int, default=3, help="Minimum points per hour bin.")
    p_shade.add_argument("--detail-out", default="shading_comparison.csv", help="Detailed comparison CSV output.")
    p_shade.add_argument("--summary-out", default="shading_summary.csv", help="Summary CSV output.")
    p_shade.set_defaults(func=run_shading)

    # plant registry
    p_plants = sub.add_parser("plants", help="Manage plant registry (SQLite).")
    p_plants.add_argument("action", choices=["list", "add", "delete", "export", "import"], help="Registry action.")
    p_plants.add_argument("--alias", help="Alias for add/delete.")
    p_plants.add_argument("--plant-uid", help="Plant UID (required for add).")
    p_plants.add_argument("--inverter-ids", help="Comma-separated inverter IDs (for add).")
    p_plants.add_argument("--weather-id", help="Weather station ID (for add).")
    p_plants.add_argument("--out", help="Output path for export (defaults to stdout).")
    p_plants.add_argument("--from-file", help="JSON file to import plants from.")
    p_plants.add_argument("--db-path", default=DEFAULT_DB, help="Path to plant registry SQLite file.")
    p_plants.set_defaults(func=run_plants)

    # query
    p_query = sub.add_parser("query", help="Query stored readings for a plant/date.")
    p_query.add_argument("--plant-alias", required=True, help="Plant alias from registry.")
    p_query.add_argument("--date", required=True, help="Date YYYYMMDD to query.")
    p_query.add_argument("--emig-ids", help="Comma-separated EMIG IDs (optional).")
    p_query.add_argument("--output", help="Optional CSV output path.")
    p_query.add_argument("--db-path", default=DEFAULT_DB, help="Path to plant registry SQLite file.")
    p_query.set_defaults(func=run_query)

    return parser


def main(argv: Sequence[str] | None = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] == "interactive":
        _setup_logging(False)
        interactive_menu()
        return

    # If user calls 'fouling' with only one file, treat as fouling-auto
    if len(argv) >= 2 and argv[0] == "fouling" and (not argv[2:] or (argv[2].startswith('--'))):
        # Convert to fouling-auto command
        new_argv = ["fouling-auto", argv[1]] + argv[2:]
        parser = build_parser()
        args = parser.parse_args(new_argv)
        _setup_logging(getattr(args, "verbose", False))
        args.func(args)
        return

    parser = build_parser()
    args = parser.parse_args(argv)
    _setup_logging(getattr(args, "verbose", False))
    args.func(args)


if __name__ == "__main__":
    main()
