#!/usr/bin/env python3
"""
Upload inverter data to the plant database with duplicate detection.

Features:
- Loads data from CSV files
- Checks for existing records before inserting
- Uses INSERT OR IGNORE to prevent duplicates
- Reports upload statistics including skipped duplicates
"""

import os
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

from plant_store import PlantStore, DEFAULT_DB


class UploadResult:
    """Result of an upload operation."""
    
    def __init__(self):
        self.total_records = 0
        self.new_records = 0
        self.duplicate_records = 0
        self.sites_processed = []
        self.errors = []
    
    def __str__(self):
        return (
            f"Upload Result:\n"
            f"  Total records processed: {self.total_records}\n"
            f"  New records added: {self.new_records}\n"
            f"  Duplicates skipped: {self.duplicate_records}\n"
            f"  Sites processed: {len(self.sites_processed)}\n"
            f"  Errors: {len(self.errors)}"
        )


def normalize_timestamp(ts: str) -> str:
    """Normalize timestamp to standard format for comparison."""
    if not ts:
        return ""
    # Remove microseconds and Z suffix for comparison
    ts = ts.replace(".000000Z", "").replace("Z", "")
    if "T" not in ts:
        return ts
    # Ensure consistent format: YYYY-MM-DDTHH:MM:SS
    try:
        dt = datetime.fromisoformat(ts.replace("Z", ""))
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return ts


def load_csv_data(filepath: str) -> pd.DataFrame:
    """Load CSV data and normalize column names."""
    df = pd.read_csv(filepath)
    
    # Normalize column names
    df.columns = [c.strip() for c in df.columns]
    
    # Handle timestamp column variations
    if "ts:" in df.columns:
        df["ts"] = df["ts:"]
    elif "timestamp" in df.columns:
        df["ts"] = df["timestamp"]
    
    return df


def check_existing_records(store: PlantStore, plant_uid: str, emig_id: str, 
                          timestamps: List[str]) -> set:
    """Check which timestamps already exist in the database."""
    existing = set()
    
    if not timestamps:
        return existing
    
    # Get date range for query
    sorted_ts = sorted(timestamps)
    start_ts = normalize_timestamp(sorted_ts[0])
    end_ts = normalize_timestamp(sorted_ts[-1])
    
    # Load existing readings
    readings = store.load_readings(plant_uid, emig_id, start_ts, end_ts + "Z")
    
    for reading in readings:
        ts = reading.get("ts", "")
        normalized = normalize_timestamp(ts)
        existing.add(normalized)
    
    return existing


def upload_csv_to_db(store: PlantStore, plant_uid: str, filepath: str,
                     check_duplicates: bool = True) -> Tuple[int, int]:
    """
    Upload data from CSV file to database.
    
    Returns:
        Tuple of (new_records, duplicate_records)
    """
    df = load_csv_data(filepath)
    
    if df.empty:
        return 0, 0
    
    # Group by emigId
    if "emigId" not in df.columns:
        raise ValueError("CSV must have 'emigId' column")
    
    new_count = 0
    dup_count = 0
    
    for emig_id, group in df.groupby("emigId"):
        # Get timestamps for this device
        if "ts" in group.columns:
            timestamps = group["ts"].tolist()
        elif "timestamp" in group.columns:
            timestamps = group["timestamp"].tolist()
        else:
            continue
        
        # Check for existing records if requested
        existing_ts = set()
        if check_duplicates:
            existing_ts = check_existing_records(store, plant_uid, emig_id, timestamps)
        
        # Prepare readings for insertion
        readings_to_insert = []
        for _, row in group.iterrows():
            ts = row.get("ts") or row.get("timestamp")
            if not ts:
                continue
            
            normalized_ts = normalize_timestamp(ts)
            
            if normalized_ts in existing_ts:
                dup_count += 1
                continue
            
            # Convert row to reading dict
            reading = row.to_dict()
            
            # Ensure ts field exists
            reading["ts"] = normalized_ts
            
            readings_to_insert.append(reading)
        
        # Store new readings
        if readings_to_insert:
            store.store_readings(plant_uid, emig_id, readings_to_insert)
            new_count += len(readings_to_insert)
    
    return new_count, dup_count


def upload_site_data(store: PlantStore, site_name: str, plant_uid: str,
                    data_dir: str, date_pattern: str = None,
                    check_duplicates: bool = True) -> Tuple[int, int]:
    """
    Upload all data files for a site.
    
    Args:
        store: PlantStore instance
        site_name: Name of the site
        plant_uid: Plant UID for storage
        data_dir: Directory containing CSV files
        date_pattern: Optional date pattern to filter files (e.g., "20251122_20251130")
        check_duplicates: Whether to check for duplicates before inserting
    
    Returns:
        Tuple of (new_records, duplicate_records)
    """
    total_new = 0
    total_dup = 0
    
    # Find matching files
    for filename in os.listdir(data_dir):
        if not filename.endswith(".csv"):
            continue
        if site_name not in filename:
            continue
        if date_pattern and date_pattern not in filename:
            continue
        
        filepath = os.path.join(data_dir, filename)
        new, dup = upload_csv_to_db(store, plant_uid, filepath, check_duplicates)
        total_new += new
        total_dup += dup
    
    return total_new, total_dup


def upload_all_sites(data_dir: str = None, db_path: str = None,
                    date_pattern: str = None,
                    check_duplicates: bool = True) -> UploadResult:
    """
    Upload data for all sites found in the data directory.
    
    Args:
        data_dir: Directory containing CSV files (defaults to Data/)
        db_path: Path to database (defaults to plant_registry.sqlite)
        date_pattern: Optional date pattern to filter files
        check_duplicates: Whether to check for duplicates before inserting
    
    Returns:
        UploadResult with statistics
    """
    if data_dir is None:
        data_dir = os.path.join(os.path.dirname(__file__), "Data")
    
    if db_path is None:
        db_path = DEFAULT_DB
    
    store = PlantStore(db_path)
    result = UploadResult()
    
    # Build list of sites from filenames
    sites = set()
    for filename in os.listdir(data_dir):
        if not filename.endswith(".csv"):
            continue
        if not filename.startswith("data_"):
            continue
        
        # Extract site name from filename: data_YYYYMMDD_YYYYMMDD_SiteName.csv
        parts = filename.replace(".csv", "").split("_", 3)
        if len(parts) >= 4:
            site_name = parts[3]
            sites.add(site_name)
    
    print(f"Found {len(sites)} sites in {data_dir}")
    print("=" * 60)
    
    # Register plants if not already in registry
    for site_name in sorted(sites):
        plant_uid = f"ERS:{hash(site_name) % 100000:05d}"
        existing = store.load(site_name)
        if not existing:
            store.save(site_name, plant_uid, [], None, None)
    
    # Upload data for each site
    for site_name in sorted(sites):
        saved = store.load(site_name)
        if not saved:
            result.errors.append(f"Site not found in registry: {site_name}")
            continue
        
        plant_uid = saved["plant_uid"]
        
        print(f"Uploading {site_name}...", end=" ")
        try:
            new, dup = upload_site_data(
                store, site_name, plant_uid, data_dir, date_pattern, check_duplicates
            )
            result.total_records += new + dup
            result.new_records += new
            result.duplicate_records += dup
            result.sites_processed.append(site_name)
            print(f"✓ (new: {new}, duplicates: {dup})")
        except Exception as e:
            result.errors.append(f"{site_name}: {e}")
            print(f"✗ ({e})")
    
    print("=" * 60)
    print(result)
    
    return result


def test_duplicate_handling(db_path: str = None) -> bool:
    """
    Test that duplicate records are properly detected and skipped.
    
    Returns:
        True if test passes
    """
    import tempfile
    
    # Create temp database
    fd, temp_db = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    
    try:
        store = PlantStore(temp_db)
        
        # Register a test plant
        store.save("TestPlant", "TEST:00001", ["INV:001"], None, None)
        
        # Create test data
        test_readings = [
            {"ts": "2025-11-22T10:00:00", "value": 100},
            {"ts": "2025-11-22T10:30:00", "value": 200},
            {"ts": "2025-11-22T11:00:00", "value": 300},
        ]
        
        # First upload
        store.store_readings("TEST:00001", "INV:001", test_readings)
        
        # Check existing
        existing = check_existing_records(
            store, "TEST:00001", "INV:001",
            [r["ts"] for r in test_readings]
        )
        
        assert len(existing) == 3, f"Expected 3 existing records, got {len(existing)}"
        
        # Try to upload again - should detect duplicates
        test_readings_2 = [
            {"ts": "2025-11-22T10:00:00", "value": 100},  # duplicate
            {"ts": "2025-11-22T11:30:00", "value": 400},  # new
        ]
        
        # Create temp CSV
        fd2, temp_csv = tempfile.mkstemp(suffix=".csv")
        os.close(fd2)
        pd.DataFrame([
            {"emigId": "INV:001", "ts": "2025-11-22T10:00:00", "value": 100},
            {"emigId": "INV:001", "ts": "2025-11-22T11:30:00", "value": 400},
        ]).to_csv(temp_csv, index=False)
        
        new, dup = upload_csv_to_db(store, "TEST:00001", temp_csv, check_duplicates=True)
        
        assert dup == 1, f"Expected 1 duplicate, got {dup}"
        assert new == 1, f"Expected 1 new record, got {new}"
        
        os.unlink(temp_csv)
        
        print("✓ Duplicate handling test passed")
        return True
        
    except AssertionError as e:
        print(f"✗ Duplicate handling test failed: {e}")
        return False
    
    finally:
        os.unlink(temp_db)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Upload inverter data to database")
    parser.add_argument("--data-dir", help="Directory containing CSV files")
    parser.add_argument("--db-path", help="Path to database file")
    parser.add_argument("--date-pattern", help="Filter files by date pattern (e.g., 20251122_20251130)")
    parser.add_argument("--no-duplicate-check", action="store_true",
                       help="Skip duplicate checking (faster but may add duplicates)")
    parser.add_argument("--test", action="store_true", help="Run duplicate handling test")
    
    args = parser.parse_args()
    
    if args.test:
        success = test_duplicate_handling(args.db_path)
        exit(0 if success else 1)
    
    result = upload_all_sites(
        data_dir=args.data_dir,
        db_path=args.db_path,
        date_pattern=args.date_pattern,
        check_duplicates=not args.no_duplicate_check
    )
    
    if result.errors:
        print("\nErrors:")
        for error in result.errors:
            print(f"  - {error}")
        exit(1)
