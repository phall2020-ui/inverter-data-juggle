"""
Script to fetch inverter data for all inverters at Newfold Farm using the
Juggle Energy REST/JSON API.

The Juggle API allows authenticated users to request data for plants, meters
and inverters.  In order to use this script you will need:

1. Your organisation's API key.  This can be obtained from the `API` menu
   inside the Juggle web application (see the Juggle API documentation for
   details).  Do **not** hard‑code your key in the script if you intend
   to share it with others – instead store it in an environment variable
   or a configuration file.
2. The plant UID for Newfold Farm.  At the time of writing this is
   ``ERS:00001``, but you can confirm this on the Plants page of the
   Juggle website.
3. A date range over which to retrieve data.  The API will return a
   maximum of 5,000 readings per request, so the script splits larger
   ranges into manageable chunks.  For half‑hourly data (``minIntervalS``
   equal to 1 800 seconds) the documentation indicates up to 104 days
   can be retrieved in a single call【155652860288747†L473-L496】.  The script
   uses this limit to automatically segment the date range.

The script retrieves a list of devices (meters/inverters) for the plant,
filters out the inverters, then iterates over each inverter to download
their readings at the specified resolution.  The data for each inverter
is written to a separate CSV file in the current working directory.

Usage example:

    # Export half‑hourly data for the last 30 days
    API_KEY = "your-api-key"
    PLANT_UID = "ERS:00001"  # Newfold Farm
    START_DATE = "20250901"
    END_DATE = "20250930"
    MIN_INTERVAL_S = 1800  # half‑hourly

    python fetch_inverter_data.py

"""

import csv
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import tkinter as tk
from tkinter import filedialog

import requests

# Base URL for the Juggle API (read‑only).  You shouldn't need to change
# this unless Juggle update their service.
BASE_URL = "https://www.emig.co.uk/p/api"


@dataclass
class Config:
    """Holds configuration for API requests."""

    api_key: str
    plant_uid: str
    start_date: str
    end_date: str
    min_interval_s: int = 1800  # default to half‑hourly


def get_plant_devices(cfg: Config) -> List[str]:
    """Retrieve the list of device EMIG IDs for the given plant.

    The Juggle API plant endpoint returns a JSON document containing
    metadata about the plant and a ``meters`` array listing all devices
    associated with it.  Each entry includes an ``emigId`` and a ``type``.

    Parameters
    ----------
    cfg : Config
        Configuration containing API key and plant UID.

    Returns
    -------
    List[str]
        A list of EMIG IDs for all devices of type ``INVERTER``.
    """
    url = f"{BASE_URL}/plant/{cfg.plant_uid}"
    headers = {"Authorization": f"token {cfg.api_key}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    devices = data.get("meters", [])
    # Filter for devices of type INVERTER
    inverter_ids = [dev["emigId"] for dev in devices if dev.get("type") == "INVERTER"]
    return inverter_ids


def discover_plants(api_key: str) -> List[Dict[str, str]]:
    """
    Try to list plants available to the API key.

    Attempts a few possible endpoints; returns an empty list on failure.
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
                continue
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and "plants" in data:
                data = data["plants"]
            if not isinstance(data, list):
                continue
            found: List[Dict[str, str]] = []
            for p in data:
                uid = p.get("uid") or p.get("plantUid") or p.get("plant_uid") or p.get("id") or p.get("emigId")
                name = p.get("name") or p.get("plantName") or p.get("title") or uid
                if uid:
                    found.append({"uid": uid, "name": name})
            if found:
                return found
        except Exception:
            continue
    # Brute force fallback for known prefixes
    prefixes = ["ERS", "AMP"]
    brute_found: List[Dict[str, str]] = []
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
                brute_found.append({"uid": uid, "name": name})
            except Exception:
                continue
    if brute_found:
        return brute_found
    return []

# -----------------------------------------------------------------------------
# Hard‑coded inverter list for Newfold Farm (ERS:00001)
#
# When working exclusively with a known set of inverters, it may be more
# convenient to bypass the ``plant`` API call and specify the EMIG IDs
# directly.  The list below was compiled from the Inverter Details page on
# the Juggle web portal for Newfold Farm.  It includes all inverters
# currently deployed at that site【416016716060024†screenshot】.  If new
# inverters are added or removed, update this list accordingly.
NEWFOLD_INVERTER_IDS: List[str] = [
    "INVERT:002946",  # Inverter 18
    "INVERT:002947",  # Inverter 1
    "INVERT:002948",  # Inverter 3
    "INVERT:002949",  # Inverter 4
    "INVERT:002950",  # Inverter 5
    "INVERT:002951",  # Inverter 6
    "INVERT:002952",  # Inverter 2
    "INVERT:002953",  # Inverter 8
    "INVERT:002954",  # Inverter 9
    "INVERT:002955",  # Inverter 10
    "INVERT:002956",  # Inverter 11
    "INVERT:002957",  # Inverter 12
    "INVERT:002958",  # Inverter 13
    "INVERT:002959",  # Inverter 14
    "INVERT:002960",  # Inverter 15
    "INVERT:002961",  # Inverter 16
    "INVERT:002962",  # Inverter 17
    "INVERT:002963",  # Inverter 19
    "INVERT:002964",  # Inverter 20
    "INVERT:002965",  # Inverter 21
    "INVERT:002966",  # Inverter 22
    "INVERT:002967",  # Inverter 23
    "INVERT:002968",  # Inverter 7
]

# Weather station for Newfold Farm
NEWFOLD_WEATHER_ID = "WETH:000274"

def fetch_readings_for_period(cfg: Config, emig_id: str, start_date: str, end_date: str) -> List[Dict]:
    """Fetch readings for a specific inverter and date range.

    This helper handles a single call to the readings endpoint and returns
    the ``readings`` array.  The API limits responses to 5,000 readings
    per request【155652860288747†L62-L78】, so you should split large date ranges before
    calling this function.

    Parameters
    ----------
    cfg : Config
        Configuration containing API key and minIntervalS.
    emig_id : str
        EMIG ID of the inverter to query.
    start_date : str
        Start date in ``YYYYMMDD`` format (inclusive).
    end_date : str
        End date in ``YYYYMMDD`` format (inclusive).

    Returns
    -------
    List[Dict]
        List of reading dictionaries returned by the API.
    """
    url = f"{BASE_URL}/meter/{emig_id}/readings"
    params: Dict[str, Optional[str]] = {
        "startDate": start_date,
        "endDate": end_date,
        "minIntervalS": str(cfg.min_interval_s) if cfg.min_interval_s else None,
    }
    headers = {"Authorization": f"token {cfg.api_key}"}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    return data.get("readings", [])


def fetch_all_readings(cfg: Config, emig_id: str) -> List[Dict]:
    """Fetch all readings across the configured date range, handling API limits.

    The Juggle API restricts each readings request to 5,000 data points.  The
    number of days that can be retrieved in one call depends on the
    ``minIntervalS`` value.  According to the documentation, half‑hourly
    requests (minIntervalS = 1800) can return up to 104 days【155652860288747†L473-L496】.
    This function splits the overall date range into segments that respect
    that limit and concatenates the results.

    Parameters
    ----------
    cfg : Config
        Configuration containing API key and date range.
    emig_id : str
        EMIG ID of the inverter to query.

    Returns
    -------
    List[Dict]
        Combined list of reading dictionaries for the entire date range.
    """
    readings: List[Dict] = []
    start_dt = datetime.strptime(cfg.start_date, "%Y%m%d")
    end_dt = datetime.strptime(cfg.end_date, "%Y%m%d")

    # Determine maximum days per request based on minIntervalS
    # Mapping taken from the API documentation【155652860288747†L473-L496】
    if cfg.min_interval_s <= 1800:
        max_days = 104
    elif cfg.min_interval_s <= 3600:
        max_days = 208
    else:
        # daily or lower frequency can fetch up to 5000 days (effectively unlimited for typical use)
        max_days = 5000

    current_start = start_dt
    while current_start <= end_dt:
        current_end = current_start + timedelta(days=max_days - 1)
        if current_end > end_dt:
            current_end = end_dt
        start_str = current_start.strftime("%Y%m%d")
        end_str = current_end.strftime("%Y%m%d")
        segment_readings = fetch_readings_for_period(cfg, emig_id, start_str, end_str)
        readings.extend(segment_readings)
        # Advance to the day after current_end
        current_start = current_end + timedelta(days=1)
        # Respect the request throttle (minimum interval 1.2 s【155652860288747†L60-L61】)
        time.sleep(1.2)
    return readings


def write_readings_csv(emig_id: str, readings: List[Dict], output_dir: str = ".") -> None:
    """Write readings to a CSV file.

    The CSV will include a timestamp column plus one column per field
    reported in the readings.  Energy/Power values are extracted from
    nested dictionaries (e.g., ``{"value": 1234, "unit": "Wh"}``).  Units
    are omitted in the CSV; if needed, you can inspect the API
    documentation for possible units【155652860288747†L500-L534】.

    Parameters
    ----------
    emig_id : str
        EMIG ID of the inverter (used in the filename).
    readings : List[Dict]
        List of reading dictionaries returned by the API.
    output_dir : str, optional
        Directory in which to place the CSV files.
    """
    if not readings:
        print(f"No readings returned for {emig_id}. Skipping CSV generation.")
        return
    # Determine all field names (excluding timestamp)
    fieldnames = sorted({key for r in readings for key in r.keys() if key != "ts"})
    filename = os.path.join(output_dir, emig_id.replace(":", "_") + ".csv")
    with open(filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["timestamp"] + fieldnames)
        for r in readings:
            row = [r.get("ts")]
            for field in fieldnames:
                # If the value is a dictionary with a 'value' key, extract it
                val = r.get(field)
                if isinstance(val, dict) and "value" in val:
                    row.append(val["value"])
                else:
                    row.append(val)
            writer.writerow(row)
    print(f"Saved {len(readings)} readings to {filename}")


def write_combined_csv(all_readings: List[Dict], output_file: str = "newfold_inverters_readings.csv") -> None:
    """Write combined readings for all inverters into a single CSV file.

    Each row of the output contains the timestamp, the EMIG ID of the inverter
    and one column per reading field returned by the API.  If an inverter
    does not report a particular field at a given timestamp, the value will
    be left blank.  See the Juggle API documentation for a list of possible
    reading fields【155652860288747†L500-L534】.

    Parameters
    ----------
    all_readings : List[Dict]
        A list of dictionaries containing readings merged from multiple
        inverters.  Each dictionary must include keys ``ts`` for the
        timestamp and ``emigId`` for the inverter identifier.
    output_file : str, optional
        Filename for the combined CSV.  Defaults to
        ``newfold_inverters_readings.csv``.
    """
    if not all_readings:
        print("No readings returned for any inverter.  Skipping combined CSV generation.")
        return
    # Determine all field names across all readings (exclude ts and emigId)
    fieldnames = sorted({key for r in all_readings for key in r.keys() if key not in {"ts", "emigId"}})
    with open(output_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["timestamp", "emigId"] + fieldnames)
        for r in all_readings:
            row = [r.get("ts"), r.get("emigId")]
            for field in fieldnames:
                val = r.get(field)
                if isinstance(val, dict) and "value" in val:
                    row.append(val["value"])
                else:
                    row.append(val)
            writer.writerow(row)
    print(f"Saved {len(all_readings)} combined readings to {output_file}")


def main() -> None:
    """Main entry point for the script.

    Reads configuration from environment variables or falls back to
    constants defined below.  Fetches inverter IDs, retrieves readings
    for each inverter and writes them to CSV files.
    """
    # Read configuration from environment variables where available
    api_key = os.environ.get("JUGGLE_API_KEY") or input("Enter API key: ").strip()
    if not api_key:
        print("API key is required.")
        return
    api_key = api_key.strip()

    # Try to discover plants; fall back to env/default prompt
    discovered = discover_plants(api_key)
    plant_uid = ""
    if discovered:
        print("\nDiscovered plants:")
        for idx, p in enumerate(discovered, start=1):
            print(f"  {idx}) {p['uid']} - {p['name']}")
        choice = input("Select plant number or enter a plant UID manually: ").strip()
        if choice.isdigit():
            idx = int(choice)
            if 1 <= idx <= len(discovered):
                plant_uid = discovered[idx - 1]["uid"]
        if not plant_uid and choice:
            plant_uid = choice
    if not plant_uid:
        plant_uid = os.environ.get("JUGGLE_PLANT_UID", "ERS:00001").strip()
        plant_uid = input(f"Enter plant UID [default: {plant_uid}]: ").strip() or plant_uid
    plant_uid = plant_uid.strip()

    # Prompt user for date range
    print("\n=== Newfold Farm Data Fetcher ===\n")
    start_date = input("Enter start date (YYYYMMDD) [default: 20251101]: ").strip() or "20251101"
    end_date = input("Enter end date (YYYYMMDD) [default: 20251120]: ").strip() or "20251120"
    
    # Validate date format
    try:
        datetime.strptime(start_date, "%Y%m%d")
        datetime.strptime(end_date, "%Y%m%d")
    except ValueError:
        print("Error: Invalid date format. Please use YYYYMMDD format.")
        return
    
    # Open file dialog to select save location
    print("\nPlease select save location and filename...")
    root = tk.Tk()
    root.withdraw()  # Hide the main window
    root.attributes('-topmost', True)  # Bring dialog to front
    
    output_file = filedialog.asksaveasfilename(
        title="Save Combined Data As",
        defaultextension=".csv",
        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        initialfile=f"newfold_data_{start_date}_{end_date}.csv"
    )
    root.destroy()
    
    if not output_file:
        print("No file selected. Exiting.")
        return
    
    print(f"\nWill save to: {output_file}")
    
    min_interval_s_str = os.environ.get("JUGGLE_MIN_INTERVAL_S", "1800")
    try:
        min_interval_s = int(min_interval_s_str)
    except ValueError:
        min_interval_s = 1800

    cfg = Config(
        api_key=api_key,
        plant_uid=plant_uid,
        start_date=start_date,
        end_date=end_date,
        min_interval_s=min_interval_s,
    )

    # Use the hard‑coded list of inverter IDs for Newfold Farm unless
    # ``JUGGLE_INVERTER_IDS`` is set in the environment (comma‑separated).
    env_inverter_ids = os.environ.get("JUGGLE_INVERTER_IDS")
    if env_inverter_ids:
        inverter_ids = [inv.strip() for inv in env_inverter_ids.split(",") if inv.strip()]
        print(f"Using inverter IDs from environment: {', '.join(inverter_ids)}")
    else:
        inverter_ids = NEWFOLD_INVERTER_IDS
        print(f"Using hard‑coded inverter IDs for Newfold Farm: {', '.join(inverter_ids)}")

    # Optionally include weather station data
    include_weather = os.environ.get("JUGGLE_INCLUDE_WEATHER", "true").lower() in ("true", "1", "yes")
    
    all_readings: List[Dict] = []
    
    # Fetch weather station data if enabled
    if include_weather:
        print(f"\nFetching weather data for {NEWFOLD_WEATHER_ID} from {cfg.start_date} to {cfg.end_date}...")
        try:
            weather_readings = fetch_all_readings(cfg, NEWFOLD_WEATHER_ID)
            for rec in weather_readings:
                rec["emigId"] = NEWFOLD_WEATHER_ID
            all_readings.extend(weather_readings)
            print(f"Retrieved {len(weather_readings)} weather readings")
        except requests.HTTPError as exc:
            print(f"Failed to fetch weather data: {exc}")
        except Exception as exc:
            print(f"Unexpected error processing weather data: {exc}")
    
    # Fetch inverter data
    for inverter_id in inverter_ids:
        print(f"\nFetching data for inverter {inverter_id} from {cfg.start_date} to {cfg.end_date}...")
        try:
            readings = fetch_all_readings(cfg, inverter_id)
            # Annotate readings with the inverter ID so they can be distinguished later
            for rec in readings:
                rec["emigId"] = inverter_id
            # Append to the combined list
            all_readings.extend(readings)
        except requests.HTTPError as exc:
            print(f"Failed to fetch data for {inverter_id}: {exc}")
        except Exception as exc:
            print(f"Unexpected error processing {inverter_id}: {exc}")

    # Write a single combined CSV file with all data (weather + inverters)
    if all_readings:
        write_combined_csv(all_readings, output_file)


if __name__ == "__main__":
    main()
