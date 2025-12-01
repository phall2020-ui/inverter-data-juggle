#!/usr/bin/env python3
"""
Generate sample November data (Nov 22-30) for all sites in the required format.

Creates sample inverter data that:
- Uses the same format as existing data files
- Extends existing November data from Nov 22-30
- Includes realistic inverter readings based on patterns from existing data
"""

import os
import random
from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd

# Directory containing existing data files
DATA_DIR = os.path.join(os.path.dirname(__file__), "Data")

# Sites currently in the database (extracted from existing data files)
SITES = [
    "Blachford UK",
    "Cromwell Tools",
    "Finlay Beverages",
    "FloPlast",
    "Hibernian Stadium",
    "Hibernian Training Ground",
    "Man City FC Training Ground",
    "Merry Hill Shopping Centre",
    "Metrocentre",
    "Newfold Farm",
    "Parfetts Birmingham",
    "Sheldons Bakery",
    "Smithy's Mushrooms PH2",
    "Smithy's Mushrooms",
    "Sofina Foods",
]

# Sample EMIG IDs for each site (from existing data patterns)
SITE_EMIG_IDS: Dict[str, List[str]] = {
    "Blachford UK": ["INVERT:001122", "INVERT:001123", "INVERT:001124"],
    "Cromwell Tools": ["INVERT:002001", "INVERT:002002"],
    "Finlay Beverages": ["INVERT:003001", "INVERT:003002", "INVERT:003003"],
    "FloPlast": ["INVERT:004001", "INVERT:004002"],
    "Hibernian Stadium": ["INVERT:005001"],
    "Hibernian Training Ground": ["INVERT:005002"],
    "Man City FC Training Ground": ["INVERT:006001", "INVERT:006002", "INVERT:006003"],
    "Merry Hill Shopping Centre": ["INVERT:007001", "INVERT:007002"],
    "Metrocentre": ["INVERT:008001", "INVERT:008002"],
    "Newfold Farm": ["INVERT:009001"],
    "Parfetts Birmingham": ["INVERT:010001", "INVERT:010002"],
    "Sheldons Bakery": ["INVERT:011001"],
    "Smithy's Mushrooms PH2": ["INVERT:012001"],
    "Smithy's Mushrooms": ["INVERT:012002"],
    "Sofina Foods": ["INVERT:013001", "INVERT:013002"],
}

# Weather station IDs for each site
SITE_WEATHER_IDS: Dict[str, str] = {
    "Blachford UK": "WETH:000274",
    "Cromwell Tools": "WETH:000275",
    "Finlay Beverages": "WETH:000276",
    "FloPlast": "WETH:000277",
    "Hibernian Stadium": "WETH:000278",
    "Hibernian Training Ground": "WETH:000279",
    "Man City FC Training Ground": "WETH:000280",
    "Merry Hill Shopping Centre": "WETH:000281",
    "Metrocentre": "WETH:000282",
    "Newfold Farm": "WETH:000283",
    "Parfetts Birmingham": "WETH:000284",
    "Sheldons Bakery": "WETH:000285",
    "Smithy's Mushrooms PH2": "WETH:000286",
    "Smithy's Mushrooms": "WETH:000287",
    "Sofina Foods": "WETH:000288",
}


def generate_timestamp_range(start_date: str, end_date: str) -> List[datetime]:
    """Generate half-hourly timestamps between start and end dates."""
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d") + timedelta(days=1) - timedelta(minutes=30)
    
    timestamps = []
    current = start
    while current <= end:
        timestamps.append(current)
        current += timedelta(minutes=30)
    
    return timestamps


def get_solar_factor(hour: int) -> float:
    """Get a solar production factor based on time of day."""
    # November in UK: daylight roughly 7am-4pm
    if hour < 7 or hour > 17:
        return 0.0
    elif hour < 8 or hour > 16:
        return 0.1
    elif hour < 9 or hour > 15:
        return 0.3
    elif hour < 10 or hour > 14:
        return 0.6
    else:  # 10am-2pm peak
        # Cap at 1.0 to avoid impossible solar generation beyond 100%
        return min(1.0, 0.8 + random.uniform(0, 0.2))


def generate_inverter_reading(timestamp: datetime, emig_id: str, base_power: float = 3000) -> Dict:
    """Generate a realistic inverter reading for a given timestamp."""
    hour = timestamp.hour
    solar_factor = get_solar_factor(hour)
    
    # Add some random variation and weather effects
    weather_factor = random.uniform(0.3, 1.0)  # Simulate cloud cover
    
    if solar_factor == 0:
        # Night time - no production
        return {
            "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.000000Z"),
            "emigId": emig_id,
            "apparentPower": None,
            "currentL1": None,
            "currentL2": None,
            "currentL3": None,
            "dcVoltage": round(random.uniform(0, 10), 3) if random.random() > 0.5 else None,
            "deviceTemperature": round(random.uniform(5, 15), 3),
            "exportLimit": round(random.uniform(70, 100), 3),
            "importActivePower": None,
            "importEnergy": None,
            "mainsFrequency": round(50 + random.uniform(-0.1, 0.1), 3),
            "stateCode": round(random.uniform(5, 15), 3),
            "ts:": timestamp.strftime("%Y-%m-%dT%H:%M:%S.000000Z"),
            "voltageL1L2": round(420 + random.uniform(-5, 5), 3),
            "voltageL1L3": round(418 + random.uniform(-5, 5), 3),
            "voltageL1N": round(242 + random.uniform(-3, 3), 3),
            "voltageL2L3": round(420 + random.uniform(-5, 5), 3),
            "voltageL2N": round(243 + random.uniform(-3, 3), 3),
            "voltageL3N": round(242 + random.uniform(-3, 3), 3),
        }
    
    power = base_power * solar_factor * weather_factor
    power = round(power, 0)
    current = power / (240 * 3)  # Approximate 3-phase current
    
    return {
        "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.000000Z"),
        "emigId": emig_id,
        "apparentPower": int(power),
        "currentL1": round(current + random.uniform(-0.5, 0.5), 3),
        "currentL2": round(current + random.uniform(-0.5, 0.5), 3),
        "currentL3": round(current + random.uniform(-0.5, 0.5), 3),
        "dcVoltage": round(750 + random.uniform(-2, 2), 3),
        "deviceTemperature": round(25 + solar_factor * 10 + random.uniform(-3, 3), 3),
        "exportLimit": 100.0,
        "importActivePower": int(power * 0.97),  # Slightly lower due to losses
        "importEnergy": int(100000000 + random.randint(0, 1000000)),
        "mainsFrequency": round(50 + random.uniform(-0.1, 0.1), 3),
        "stateCode": round(25 + solar_factor * 10 + random.uniform(-3, 3), 3),
        "ts:": timestamp.strftime("%Y-%m-%dT%H:%M:%S.000000Z"),
        "voltageL1L2": round(418 + random.uniform(-5, 5), 3),
        "voltageL1L3": round(416 + random.uniform(-5, 5), 3),
        "voltageL1N": round(241 + random.uniform(-3, 3), 3),
        "voltageL2L3": round(418 + random.uniform(-5, 5), 3),
        "voltageL2N": round(242 + random.uniform(-3, 3), 3),
        "voltageL3N": round(241 + random.uniform(-3, 3), 3),
    }


def generate_weather_reading(timestamp: datetime, weather_id: str) -> Dict:
    """Generate a realistic weather station reading for a given timestamp."""
    hour = timestamp.hour
    solar_factor = get_solar_factor(hour)
    
    # POA irradiance (W/m²)
    poa = 0.0
    if solar_factor > 0:
        poa = 800 * solar_factor * random.uniform(0.3, 1.0)
    
    return {
        "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.000000Z"),
        "emigId": weather_id,
        "poaIrradiance": round(poa, 3),
        "ts:": timestamp.strftime("%Y-%m-%dT%H:%M:%S.000000Z"),
    }


def generate_site_data(site: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Generate sample data for a single site."""
    timestamps = generate_timestamp_range(start_date, end_date)
    
    # Get IDs for this site
    inverter_ids = SITE_EMIG_IDS.get(site, ["INVERT:000001"])
    weather_id = SITE_WEATHER_IDS.get(site, "WETH:000001")
    
    all_readings = []
    
    # Generate inverter readings
    for ts in timestamps:
        for inv_id in inverter_ids:
            reading = generate_inverter_reading(ts, inv_id)
            all_readings.append(reading)
    
    # Generate weather readings
    for ts in timestamps:
        reading = generate_weather_reading(ts, weather_id)
        all_readings.append(reading)
    
    return pd.DataFrame(all_readings)


def save_sample_data(site: str, df: pd.DataFrame, start_date: str, end_date: str, output_dir: str = None) -> str:
    """Save sample data to a CSV file in the Data directory."""
    if output_dir is None:
        output_dir = DATA_DIR
    
    os.makedirs(output_dir, exist_ok=True)
    filename = f"data_{start_date}_{end_date}_{site}.csv"
    filepath = os.path.join(output_dir, filename)
    df.to_csv(filepath, index=False)
    return filepath


def generate_all_november_data(start_date: str = "20251122", end_date: str = "20251130") -> Dict[str, str]:
    """Generate November sample data for all sites."""
    output_files = {}
    
    print(f"Generating sample data for November {start_date[6:8]}-{end_date[6:8]}, 2025")
    print("=" * 60)
    
    for site in SITES:
        print(f"Generating data for {site}...", end=" ")
        df = generate_site_data(site, start_date, end_date)
        filepath = save_sample_data(site, df, start_date, end_date)
        output_files[site] = filepath
        print(f"✓ ({len(df)} records)")
    
    print("=" * 60)
    print(f"Generated data for {len(output_files)} sites")
    
    return output_files


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate sample November data for all sites")
    parser.add_argument("--start-date", default="20251122", help="Start date (YYYYMMDD)")
    parser.add_argument("--end-date", default="20251130", help="End date (YYYYMMDD)")
    parser.add_argument("--output-dir", help="Output directory (defaults to Data/)")
    
    args = parser.parse_args()
    
    if args.output_dir:
        output_files = {}
        for site in SITES:
            print(f"Generating data for {site}...", end=" ")
            df = generate_site_data(site, args.start_date, args.end_date)
            filepath = save_sample_data(site, df, args.start_date, args.end_date, args.output_dir)
            output_files[site] = filepath
            print(f"✓ ({len(df)} records)")
    else:
        output_files = generate_all_november_data(args.start_date, args.end_date)
    
    print("\nOutput files:")
    for site, path in output_files.items():
        print(f"  {site}: {path}")
