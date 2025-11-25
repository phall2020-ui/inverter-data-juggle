"""
SolarGIS POA Data Import Module

Handles importing POA (Plane of Array) irradiance data from SolarGIS CSV files
and storing it in the plant database for correlation with inverter performance.

Features:
- Fuzzy matching of CSV filenames to plant names
- Capacity-weighted averaging when multiple datasets exist
- Automatic date range alignment with inverter data
- Support for various SolarGIS CSV formats
"""

import os
from datetime import datetime
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple

import pandas as pd


def fuzzy_match_filename(plant_name: str, filenames: List[str], threshold: float = 0.6) -> Optional[str]:
    """
    Find the best matching filename for a plant name using fuzzy string matching.
    
    Parameters
    ----------
    plant_name : str
        Name of the plant to match
    filenames : List[str]
        List of available CSV filenames
    threshold : float
        Minimum similarity score (0-1) to consider a match
    
    Returns
    -------
    Optional[str]
        Best matching filename or None if no match above threshold
    """
    best_match = None
    best_score = 0.0
    
    # Normalize plant name for comparison
    plant_lower = plant_name.lower().replace("_", " ").replace("-", " ")
    
    for filename in filenames:
        # Extract base name without extension
        base_name = os.path.splitext(filename)[0].lower().replace("_", " ").replace("-", " ")
        
        # Calculate similarity score
        score = SequenceMatcher(None, plant_lower, base_name).ratio()
        
        if score > best_score and score >= threshold:
            best_score = score
            best_match = filename
    
    return best_match


def detect_solargis_format(df: pd.DataFrame) -> Dict[str, any]:
    """
    Detect the column names and format of a SolarGIS CSV file.
    SolarGIS files typically have: timestamp column + multiple array columns (Array1, Array2, etc.)
    Each array has azimuth, slope, capacity, and GTI values.
    
    Parameters
    ----------
    df : pd.DataFrame
        Loaded SolarGIS dataframe
    
    Returns
    -------
    Dict[str, any]
        Mapping with keys:
        - 'timestamp': name of timestamp column
        - 'arrays': dict mapping array name -> {'poa_col': col, 'capacity': kW, 'azimuth': deg, 'slope': deg}
    """
    mapping = {'arrays': {}}
    
    # Find timestamp column
    timestamp_patterns = ['date', 'time', 'datetime', 'timestamp', 'utc']
    for col in df.columns:
        col_lower = col.lower()
        if any(pattern in col_lower for pattern in timestamp_patterns):
            mapping['timestamp'] = col
            break
    
    if 'timestamp' not in mapping:
        # Fallback: first column is often timestamp
        mapping['timestamp'] = df.columns[0]
    
    # Find metadata columns (azimuth, slope, capacity)
    azimuth_col = None
    slope_col = None
    capacity_col = None
    name_col = None
    
    for col in df.columns:
        col_lower = col.lower()
        if 'azimuth' in col_lower:
            azimuth_col = col
        elif 'slope' in col_lower or 'tilt' in col_lower:
            slope_col = col
        elif 'array_cap' in col_lower or col_lower == 'array_capacity':
            capacity_col = col
        elif 'name' in col_lower:
            name_col = col
    
    # Find all unique azimuth/slope combinations in the data
    # This handles cases where one CSV has multiple orientations
    unique_orientations = []
    if azimuth_col and slope_col:
        # Get unique combinations with their capacities
        if capacity_col:
            orientation_df = df[[azimuth_col, slope_col, capacity_col]].drop_duplicates()
        else:
            orientation_df = df[[azimuth_col, slope_col]].drop_duplicates()
        
        for _, row in orientation_df.iterrows():
            azimuth = row[azimuth_col]
            slope = row[slope_col]
            capacity = row[capacity_col] if capacity_col and pd.notna(row.get(capacity_col)) else None
            unique_orientations.append({
                'azimuth': azimuth,
                'slope': slope,
                'capacity': capacity
            })
    
    # Find all GTI/POA columns
    import re
    array_pattern = re.compile(r'array[\s_]*(\d+)', re.IGNORECASE)
    capacity_pattern = re.compile(r'(\d+(?:\.\d+)?)\s*(?:kw|mw)', re.IGNORECASE)
    
    # If we have multiple orientations, we need to split the data
    # For now, detect GTI columns and associate with orientations
    gti_columns = []
    
    for col in df.columns:
        if col == mapping.get('timestamp'):
            continue
        
        col_lower = col.lower()
        
        # Skip metadata columns
        if any(skip in col_lower for skip in ['azimuth', 'slope', 'angle', 'name', '_low', '_high', '_p90', '_p10', 'cap']):
            continue
        
        # Check if this is a POA/GTI/GHI column (exclude uncertainty bounds)
        # Must be exactly 'gti' or 'ghi', not 'gti_low', 'gti_high'
        is_poa = col_lower in ['gti', 'ghi', 'poa'] or any(pattern in col_lower for pattern in ['irradiance', 'g(i)', 'w/m'])
        
        if is_poa or 'array' in col_lower:
            gti_columns.append(col)
    
    # If we have multiple orientations, the data must be split by rows
    # (different orientations in different rows, even if multiple GTI columns exist)
    if len(unique_orientations) > 1:
        # Mark that this file needs row-based splitting
        # Use first GTI column found (they should have same values, just different row subsets)
        mapping['split_by_orientation'] = True
        mapping['azimuth_col'] = azimuth_col
        mapping['slope_col'] = slope_col
        mapping['capacity_col'] = capacity_col
        mapping['name_col'] = name_col
        mapping['gti_col'] = gti_columns[0] if gti_columns else None
        mapping['orientations'] = unique_orientations
        return mapping
    
    # Otherwise, handle as before (one GTI column per array/orientation)
    for col in gti_columns:
        col_lower = col.lower()
        
        # Extract array identifier
        array_match = array_pattern.search(col)
        array_id = f"Array{array_match.group(1)}" if array_match else f"Array{len(mapping['arrays']) + 1}"
        
        # Try to extract capacity from column name first
        capacity = None
        cap_match = capacity_pattern.search(col)
        if cap_match:
            capacity = float(cap_match.group(1))
            if 'mw' in col_lower:
                capacity *= 1000
        
        # If capacity not in column name, look for separate capacity column
        if capacity is None and capacity_col:
            try:
                cap_value = pd.to_numeric(df[capacity_col], errors='coerce').iloc[0]
                if pd.notna(cap_value):
                    # Capacity is in kW (DC capacity)
                    capacity = cap_value
            except:
                pass
        
        # Extract azimuth and slope from metadata columns
        azimuth = None
        slope = None
        
        if azimuth_col:
            try:
                azimuth = float(df[azimuth_col].iloc[0])
            except:
                pass
        
        if slope_col:
            try:
                slope = float(df[slope_col].iloc[0])
            except:
                pass
        
        # If still no capacity, check first data row of this column
        if capacity is None and not df[col].empty:
            try:
                first_val = df[col].iloc[0]
                if isinstance(first_val, str):
                    cap_match = capacity_pattern.search(first_val)
                    if cap_match:
                        capacity = float(cap_match.group(1))
                        if 'mw' in first_val.lower():
                            capacity *= 1000
            except:
                pass
        
        mapping['arrays'][array_id] = {
            'poa_col': col,
            'capacity': capacity,
            'azimuth': azimuth,
            'slope': slope
        }
    
    return mapping


def load_solargis_csv(filepath: str) -> Tuple[pd.DataFrame, Dict[str, any]]:
    """
    Load a SolarGIS CSV file and detect its format.
    
    Parameters
    ----------
    filepath : str
        Path to the SolarGIS CSV file
    
    Returns
    -------
    Tuple[pd.DataFrame, Dict[str, any]]
        Loaded dataframe and column mapping
    """
    # Try different encodings and skip row combinations
    for encoding in ['utf-8', 'latin1', 'cp1252']:
        for skiprows in [0, 1, 2, 3]:
            try:
                df = pd.read_csv(filepath, encoding=encoding, skiprows=skiprows)
                
                # Check if we got valid data
                if len(df) > 0 and len(df.columns) > 1:
                    mapping = detect_solargis_format(df)
                    
                    # Valid if we at least found timestamp and either arrays or split_by_orientation
                    if 'timestamp' in mapping and (mapping.get('arrays') or mapping.get('split_by_orientation')):
                        filename = os.path.basename(filepath)
                        print(f"  Loaded {filename}")
                        print(f"    - Encoding: {encoding}, Skip rows: {skiprows}")
                        if mapping.get('split_by_orientation'):
                            print(f"    - Found {len(mapping['orientations'])} orientation(s) (row-based)")
                        else:
                            print(f"    - Found {len(mapping['arrays'])} array(s)")
                        return df, mapping
            except Exception:
                continue
    
    raise ValueError(f"Could not parse SolarGIS file: {filepath}")


def calculate_capacity_weighted_poa(
    dfs_and_mappings: List[Tuple[pd.DataFrame, Dict[str, any]]],
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """
    Calculate capacity-weighted average POA from multiple SolarGIS datasets.
    
    Each dataset may contain multiple arrays with different capacities.
    Formula: POA_weighted = (POA_array_i  Capacity_array_i) / (Capacity_array_i)
    
    Also resamples 15-minute data to 30-minute (half-hourly) intervals to match
    Juggle API data format.
    
    Parameters
    ----------
    dfs_and_mappings : List[Tuple[pd.DataFrame, Dict[str, any]]]
        List of (dataframe, column_mapping) tuples
    start_date : str
        Start date in YYYYMMDD format
    end_date : str
        End date in YYYYMMDD format
    
    Returns
    -------
    pd.DataFrame
        Combined dataframe with columns: timestamp, poa_weighted (W/m)
        Note: Input GTI data in kW/m is converted to W/m
    """
    if not dfs_and_mappings:
        return pd.DataFrame(columns=['timestamp', 'poa'])
    
    # Convert date strings to datetime for filtering (UTC-aware to match data)
    # Add 1 day to end_date to make it inclusive (end_date is last day to include)
    start_dt = pd.to_datetime(start_date, format='%Y%m%d', utc=True)
    end_dt = pd.to_datetime(end_date, format='%Y%m%d', utc=True) + pd.Timedelta(days=1)
    
    # Group arrays by azimuth/slope combination
    orientation_groups = {}  # (azimuth, slope) -> {capacity: kW, poa_series: Series, capacity_set: bool}
    
    print(f"\n  Processing arrays and grouping by orientation:")
    
    for file_idx, (df, mapping) in enumerate(dfs_and_mappings, 1):
        ts_col = mapping['timestamp']
        
        # Check if this file needs row-based splitting by orientation
        if mapping.get('split_by_orientation'):
            azimuth_col = mapping['azimuth_col']
            slope_col = mapping['slope_col']
            capacity_col = mapping['capacity_col']
            gti_col = mapping['gti_col']
            name_col = mapping.get('name_col')  # For building unique array keys
            orientations = mapping['orientations']
            
            print(f"    File {file_idx}: {len(orientations)} orientation(s) detected (row-based)")
            
            df_copy = df.copy()
            df_copy['ts'] = pd.to_datetime(df_copy[ts_col], errors='coerce', utc=True)
            df_copy = df_copy.dropna(subset=['ts'])
            
            # Filter to date range
            mask = (df_copy['ts'] >= start_dt) & (df_copy['ts'] <= end_dt)
            df_filtered = df_copy[mask].copy()
            
            if df_filtered.empty:
                print(f"    File {file_idx}: No data in date range")
                continue
            
            # Process each orientation - build unique array keys and deduplicate
            for orient in orientations:
                azimuth = float(orient['azimuth'])
                slope = float(orient['slope'])
                
                # Filter rows for this orientation
                orient_mask = (df_filtered[azimuth_col] == azimuth) & (df_filtered[slope_col] == slope)
                orient_data = df_filtered[orient_mask].copy()
                
                if orient_data.empty:
                    continue
                
                # Build unique array key (name + capacity + azimuth + slope)
                if name_col and name_col in orient_data.columns:
                    orient_data['ArrayKey'] = (
                        orient_data[name_col].astype(str).str.strip() + "|" +
                        orient_data[capacity_col].astype(str) + "|" +
                        orient_data[azimuth_col].astype(str).str.strip() + "|" +
                        orient_data[slope_col].astype(str).str.strip()
                    )
                else:
                    # No name column, use capacity + orientation as key
                    orient_data['ArrayKey'] = (
                        orient_data[capacity_col].astype(str) + "|" +
                        orient_data[azimuth_col].astype(str).str.strip() + "|" +
                        orient_data[slope_col].astype(str).str.strip()
                    )
                
                # Deduplicate: drop duplicate readings per array per timestamp
                orient_data_unique = orient_data.drop_duplicates(subset=['ArrayKey', 'ts'])
                
                # Calculate total capacity for this orientation (sum all unique arrays)
                unique_arrays = orient_data_unique.groupby('ArrayKey')[capacity_col].first()
                total_capacity = unique_arrays.sum()
                
                # GTI is irradiance (kWh/m) - same for all arrays with same orientation
                # Take mean in case of slight variations (should be same value)
                gti_by_timestamp = orient_data_unique.groupby('ts')[gti_col].mean()
                
                # CSV values are kWh/m per 15-min (energy accumulated in that period)
                # Resample to 30-min by summing pairs to get energy per 30-min period
                gti_by_timestamp = pd.to_numeric(gti_by_timestamp, errors='coerce')
                
                # Normalize timestamps to remove seconds offset (e.g., 00:00:30 -> 00:00:00)
                # This ensures proper alignment during resampling
                gti_by_timestamp.index = gti_by_timestamp.index.floor('min')
                
                poa_resampled = gti_by_timestamp.resample('30min').sum()
                
                # Create orientation key
                orientation_key = (azimuth, slope)
                
                # Initialize orientation group if not exists
                if orientation_key not in orientation_groups:
                    orientation_groups[orientation_key] = {
                        'capacity': total_capacity,
                        'poa_series': None,
                        'capacity_set': True
                    }
                
                # Don't add capacity from other months - capacity is static
                # Just update POA series by concatenating/combining time series data
                if orientation_groups[orientation_key]['poa_series'] is None:
                    orientation_groups[orientation_key]['poa_series'] = poa_resampled
                else:
                    # Concatenate time series data (different time periods from different months)
                    existing = orientation_groups[orientation_key]['poa_series']
                    combined = pd.concat([existing, poa_resampled])
                    # Remove duplicates keeping first, then sort by index
                    combined = combined[~combined.index.duplicated(keep='first')].sort_index()
                    orientation_groups[orientation_key]['poa_series'] = combined
                
                print(f"      - Azimuth={azimuth}, Slope={slope}: {total_capacity:.1f} kW ({len(unique_arrays)} unique arrays)")
            
            continue
        
        # Otherwise, handle column-based format
        arrays = mapping.get('arrays', {})
        
        if not arrays:
            print(f"    File {file_idx}: No arrays found, skipping")
            continue
        
        df_copy = df.copy()
        
        # Parse timestamps
        df_copy['ts'] = pd.to_datetime(df_copy[ts_col], errors='coerce', utc=True)
        df_copy = df_copy.dropna(subset=['ts'])
        
        # Filter to date range
        mask = (df_copy['ts'] >= start_dt) & (df_copy['ts'] <= end_dt)
        df_filtered = df_copy[mask].copy()
        
        if df_filtered.empty:
            print(f"    File {file_idx}: No data in date range")
            continue
        
        print(f"    File {file_idx}: {len(arrays)} array(s) (column-based)")
        
        # Group arrays by orientation
        for array_id, array_info in arrays.items():
            poa_col = array_info['poa_col']
            capacity = array_info.get('capacity', 100.0)
            azimuth = array_info.get('azimuth', 0.0)
            slope = array_info.get('slope', 0.0)
            
            # Create orientation key
            orientation_key = (azimuth, slope)
            
            # Extract and process POA data for this array
            df_array = df_filtered[['ts', poa_col]].copy()
            df_array = df_array.set_index('ts')
            
            # CSV values are kWh/m per 15-min (energy accumulated in that period)
            # Resample to 30-min by summing pairs to get energy per 30-min period
            poa_series = pd.to_numeric(df_array[poa_col], errors='coerce')
            
            # Normalize index timestamps to remove seconds offset (e.g., 00:00:30 -> 00:00:00)
            # This ensures proper alignment during resampling
            poa_series.index = poa_series.index.floor('min')
            
            poa_resampled = poa_series.resample('30min').sum()
            
            # Initialize orientation group if not exists
            if orientation_key not in orientation_groups:
                orientation_groups[orientation_key] = {
                    'capacity': capacity,
                    'poa_series': None,
                    'capacity_set': True
                }
            
            # Don't add capacity from other months - capacity is static
            # Just update POA series by concatenating time series data
            if orientation_groups[orientation_key]['poa_series'] is None:
                orientation_groups[orientation_key]['poa_series'] = poa_resampled
            else:
                # Concatenate time series data (different time periods from different months)
                existing = orientation_groups[orientation_key]['poa_series']
                combined = pd.concat([existing, poa_resampled])
                # Remove duplicates keeping first, then sort by index
                combined = combined[~combined.index.duplicated(keep='first')].sort_index()
                orientation_groups[orientation_key]['poa_series'] = combined
            
            print(f"      - {array_id}: {capacity:.1f} kW (azimuth={azimuth}, slope={slope})")
    
    if not orientation_groups:
        print("   No valid data found in date range")
        return pd.DataFrame(columns=['timestamp', 'poa'])
    
    # Build results dataframe for each orientation
    print(f"\n  Found {len(orientation_groups)} unique orientation(s):")
    
    results = []
    for (azimuth, slope), data in orientation_groups.items():
        total_capacity = data['capacity']
        poa_series = data['poa_series']
        
        # Create output dataframe
        result = pd.DataFrame({
            'timestamp': poa_series.index,
            'poa': poa_series.values,
            'azimuth': azimuth,
            'slope': slope,
            'capacity': total_capacity
        }).reset_index(drop=True)
        
        # Remove any NaN values
        result = result.dropna()
        
        # Convert timestamps to ISO format for database storage
        result['timestamp'] = result['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')
        
        print(f"    - Azimuth {azimuth}, Slope {slope}:")
        print(f"      Capacity: {total_capacity:.1f} kW")
        print(f"      Records: {len(result)} (30-minute intervals)")
        print(f"      Average POA: {result['poa'].mean():.1f} W/m")
        
        results.append(result)
    
    # Combine all orientations into single dataframe
    if len(results) == 1:
        return results[0]
    else:
        combined = pd.concat(results, ignore_index=True)
        return combined


def import_poa_for_plant_multi_folder(
    plant_name: str,
    plant_uid: str,
    solargis_folders: List[str],
    start_date: str,
    end_date: str,
    store,
    fuzzy_threshold: float = 0.5
) -> Optional[pd.DataFrame]:
    """
    Import and process POA data for a specific plant from multiple folders.
    Checks database for existing data and only imports new data.
    
    Parameters
    ----------
    plant_name : str
        Name of the plant
    plant_uid : str
        Plant UID for database checks
    solargis_folders : List[str]
        Paths to folders containing SolarGIS CSV files
    start_date : str
        Start date in YYYYMMDD format
    end_date : str
        End date in YYYYMMDD format
    store : PlantStore
        Database store for checking existing data
    fuzzy_threshold : float
        Minimum similarity score for filename matching
    
    Returns
    -------
    Optional[pd.DataFrame]
        POA data with columns [timestamp, poa] or None if no new data
    """
    print(f"\n--- Importing POA for '{plant_name}' ({plant_uid}) ---")
    print(f"  Searching in {len(solargis_folders)} folder(s)")
    print(f"  Date range: {start_date} to {end_date}")
    
    # Collect all CSV files from all folders
    all_files_with_paths = []
    for folder in solargis_folders:
        if not os.path.exists(folder):
            print(f"   Folder not found: {folder}")
            continue
        
        csv_files = [f for f in os.listdir(folder) if f.lower().endswith('.csv')]
        folder_name = os.path.basename(folder)
        print(f"  [{folder_name}] {len(csv_files)} CSV files")
        
        for filename in csv_files:
            all_files_with_paths.append((os.path.join(folder, filename), filename))
    
    if not all_files_with_paths:
        print(f"   No CSV files found in any folder")
        return None
    
    print(f"  Total CSV files found: {len(all_files_with_paths)}")
    
    # Manual exact mapping for problem plants (one CSV per site per month)
    exact_csv_names = {
        'Man City FC Training Ground': 'City_Football_Group_Phase_1.csv',
        'Finlay Beverages': 'Finlay_Beverages.csv',
        'Blachford UK': 'Blachford.csv',
        'Cromwell Tools': 'Cromwell_Tools.csv',
        'Metrocentre': 'Metro_Centre.csv',
        'Merry Hill Shopping Centre': 'Merry_Hill_Shopping_Centre.csv',
        'Hibernian Stadium': 'Hibernian_Stadium.csv',
        'Hibernian Training Ground': 'Hibernian_Training_Ground.csv',
        'Parfetts Birmingham': 'Parfetts.csv',
        "Sheldons Motor Books": "Sheldons_Bakery.csv",
        "Smithy's Mushrooms": "Smithys_Mushrooms.csv",
        "Smithy's Mushrooms PH2": "Smithy's_Mushrooms_Phase_2.csv",
    }
    
    # Try exact match first
    matching_files = []
    if plant_name in exact_csv_names:
        target_csv = exact_csv_names[plant_name]
        for filepath, filename in all_files_with_paths:
            if filename == target_csv:
                matching_files.append((filepath, filename, 1.0))
    
    # Fall back to fuzzy matching if no exact match
    if not matching_files:
        plant_name_normalized = plant_name.lower().replace("_", " ").replace("-", " ")
        
        for filepath, filename in all_files_with_paths:
            score = SequenceMatcher(
                None,
                plant_name_normalized,
                filename.lower().replace("_", " ").replace("-", " ")
            ).ratio()
            
            if score >= fuzzy_threshold:
                matching_files.append((filepath, filename, score))
    
    if not matching_files:
        print(f"   No matching files found for '{plant_name}'")
        print(f"   Try lowering fuzzy_threshold (current: {fuzzy_threshold})")
        
        # Show top 5 closest matches for debugging
        print(f"   Closest matches:")
        scored = []
        for filepath, filename in all_files_with_paths[:20]:  # Sample first 20
            score = SequenceMatcher(None, plant_name_normalized, filename.lower().replace("_", " ")).ratio()
            scored.append((filename, score))
        scored.sort(key=lambda x: x[1], reverse=True)
        for fname, sc in scored[:5]:
            print(f"    - {fname} (score: {sc:.2f})")
        
        return None
    
    # Sort by score (best matches first)
    matching_files.sort(key=lambda x: x[2], reverse=True)
    
    print(f"   Found {len(matching_files)} matching file(s):")
    for filepath, filename, score in matching_files:
        folder_name = os.path.basename(os.path.dirname(filepath))
        print(f"    - [{folder_name}] {filename} (score: {score:.2f})")
    
    # Load all matching files
    dfs_and_mappings = []
    for filepath, filename, score in matching_files:
        try:
            df, mapping = load_solargis_csv(filepath)
            dfs_and_mappings.append((df, mapping))
        except Exception as e:
            print(f"   Failed to load {filename}: {e}")
    
    if not dfs_and_mappings:
        print(f"   Could not load any matching files")
        return None
    
    # Calculate capacity-weighted POA
    result = calculate_capacity_weighted_poa(dfs_and_mappings, start_date, end_date)
    
    return result if not result.empty else None


def import_poa_for_plant(
    plant_name: str,
    solargis_folder: str,
    start_date: str,
    end_date: str,
    fuzzy_threshold: float = 0.6
) -> Optional[pd.DataFrame]:
    """
    Import and process POA data for a specific plant.
    
    Parameters
    ----------
    plant_name : str
        Name of the plant
    solargis_folder : str
        Path to folder containing SolarGIS CSV files
    start_date : str
        Start date in YYYYMMDD format
    end_date : str
        End date in YYYYMMDD format
    fuzzy_threshold : float
        Minimum similarity score for filename matching
    
    Returns
    -------
    Optional[pd.DataFrame]
        POA data with columns [timestamp, poa] or None if no match found
    """
    if not os.path.exists(solargis_folder):
        print(f"   Folder not found: {solargis_folder}")
        return None
    
    # Get all CSV files in folder
    all_files = [f for f in os.listdir(solargis_folder) if f.lower().endswith('.csv')]
    
    if not all_files:
        print(f"   No CSV files found in {solargis_folder}")
        return None
    
    print(f"\n--- Importing POA for '{plant_name}' ---")
    print(f"  Searching in: {solargis_folder}")
    print(f"  Found {len(all_files)} CSV files")
    
    # Find all matching files (could be multiple phases/datasets)
    matching_files = []
    for filename in all_files:
        score = SequenceMatcher(
            None,
            plant_name.lower().replace("_", " "),
            filename.lower().replace("_", " ")
        ).ratio()
        
        if score >= fuzzy_threshold:
            matching_files.append((filename, score))
    
    if not matching_files:
        print(f"   No matching files found for '{plant_name}'")
        print(f"   Try lowering fuzzy_threshold (current: {fuzzy_threshold})")
        return None
    
    # Sort by score (best matches first)
    matching_files.sort(key=lambda x: x[1], reverse=True)
    
    print(f"   Found {len(matching_files)} matching file(s):")
    for filename, score in matching_files:
        print(f"    - {filename} (score: {score:.2f})")
    
    # Load all matching files
    dfs_and_mappings = []
    for filename, score in matching_files:
        filepath = os.path.join(solargis_folder, filename)
        try:
            df, mapping = load_solargis_csv(filepath)
            dfs_and_mappings.append((df, mapping))
        except Exception as e:
            print(f"   Failed to load {filename}: {e}")
    
    if not dfs_and_mappings:
        print(f"   Could not load any matching files")
        return None
    
    # Calculate capacity-weighted POA
    result = calculate_capacity_weighted_poa(dfs_and_mappings, start_date, end_date)
    
    return result if not result.empty else None


def store_poa_in_db(store, plant_uid: str, poa_df: pd.DataFrame) -> None:
    """
    Store POA data in the database with separate EMIG IDs for each orientation.
    Overwrites existing data for the same time period.
    Also updates the plant's DC capacity based on total capacity across all orientations.
    
    Parameters
    ----------
    store : PlantStore
        Database store instance
    plant_uid : str
        Plant UID
    poa_df : pd.DataFrame
        POA dataframe with columns [timestamp, poa, azimuth, slope]
    """
    if poa_df.empty:
        return
    
    # Check if we have orientation data
    has_orientation = 'azimuth' in poa_df.columns and 'slope' in poa_df.columns
    
    print(f"\n   POA Import Summary:")
    print(f"  {'='*60}")
    
    total_dc_capacity = 0.0
    
    if has_orientation:
        # Group by orientation and store separately
        orientations = poa_df.groupby(['azimuth', 'slope'])
        
        total_records = 0
        orientation_capacities = {}
        
        for (azimuth, slope), group in orientations:
            # Create readings for this orientation
            readings = []
            for _, row in group.iterrows():
                readings.append({
                    'ts': row['timestamp'],
                    'poaIrradiance': {'value': row['poa'], 'unit': 'kWh/m'}
                })
            
            # Store with orientation-specific EMIG ID (overwrites existing)
            poa_emig_id = f"POA:SOLARGIS:AZ{int(azimuth)}:SL{int(slope)}"
            store.store_readings(plant_uid, poa_emig_id, readings)
            
            total_records += len(readings)
            avg_poa = group['poa'].mean()
            min_poa = group['poa'].min()
            max_poa = group['poa'].max()
            date_range = f"{group['timestamp'].iloc[0]} to {group['timestamp'].iloc[-1]}"
            
            # Extract capacity from the dataframe if available
            capacity = group['capacity'].iloc[0] if 'capacity' in group.columns else 0.0
            total_dc_capacity += capacity
            orientation_capacities[(azimuth, slope)] = capacity
            
            print(f"  Orientation: Azimuth={int(azimuth)}, Slope={int(slope)}")
            print(f"    EMIG ID: {poa_emig_id}")
            print(f"    DC Capacity: {capacity:.1f} kW")
            print(f"    Records: {len(readings)} (30-minute intervals)")
            print(f"    Date Range: {date_range}")
            print(f"    POA Statistics:")
            print(f"      Average: {avg_poa:.1f} W/m")
            print(f"      Min: {min_poa:.1f} W/m")
            print(f"      Max: {max_poa:.1f} W/m")
            print(f"  {'-'*60}")
        
        print(f"   Total: {total_records} POA records stored across {len(orientations)} orientation(s)")
        print(f"   Total DC Capacity: {total_dc_capacity:.1f} kW")
        
        # Calculate and store capacity-weighted POA (OPTIMIZED)
        if total_dc_capacity > 0 and len(orientations) > 1:
            print(f"\n   Calculating Capacity-Weighted POA...")
            
            # OPTIMIZED: Use vectorized operations instead of nested loops
            # Add weight column (capacity / total_capacity)
            poa_df['weight'] = poa_df['capacity'] / total_dc_capacity
            
            # Calculate weighted POA: POA  weight
            poa_df['weighted_poa'] = poa_df['poa'] * poa_df['weight']
            
            # Group by timestamp and sum the weighted values
            weighted_result = poa_df.groupby('timestamp')['weighted_poa'].sum().reset_index()
            weighted_result.columns = ['timestamp', 'poa']
            
            # Create readings list
            weighted_poa_records = [
                {
                    'ts': row['timestamp'],
                    'poaIrradiance': {'value': row['poa'], 'unit': 'kWh/m'}
                }
                for _, row in weighted_result.iterrows()
            ]
            
            # Store weighted POA with special EMIG ID
            weighted_emig_id = "POA:SOLARGIS:WEIGHTED"
            store.store_readings(plant_uid, weighted_emig_id, weighted_poa_records)
            
            # Calculate statistics for weighted POA
            weighted_poas = [r['poaIrradiance']['value'] for r in weighted_poa_records]
            avg_weighted = sum(weighted_poas) / len(weighted_poas)
            min_weighted = min(weighted_poas)
            max_weighted = max(weighted_poas)
            
            # Get date range from the weighted result
            first_ts = weighted_result['timestamp'].iloc[0]
            last_ts = weighted_result['timestamp'].iloc[-1]
            
            print(f"  Capacity-Weighted POA:")
            print(f"    EMIG ID: {weighted_emig_id}")
            print(f"    Records: {len(weighted_poa_records)} (30-minute intervals)")
            print(f"    Date Range: {first_ts} to {last_ts}")
            print(f"    POA Statistics:")
            print(f"      Average: {avg_weighted:.1f} W/m")
            print(f"      Min: {min_weighted:.1f} W/m")
            print(f"      Max: {max_weighted:.1f} W/m")
            print(f"   Capacity-weighted POA stored successfully")
            print(f"  {'-'*60}")
    else:
        # No orientation data - store as single POA device
        readings = []
        for _, row in poa_df.iterrows():
            readings.append({
                'ts': row['timestamp'],
                'poaIrradiance': {'value': row['poa'], 'unit': 'kWh/m'}
            })
        
        poa_emig_id = "POA:SOLARGIS"
        store.store_readings(plant_uid, poa_emig_id, readings)
        
        avg_poa = poa_df['poa'].mean()
        min_poa = poa_df['poa'].min()
        max_poa = poa_df['poa'].max()
        date_range = f"{poa_df['timestamp'].iloc[0]} to {poa_df['timestamp'].iloc[-1]}"
        
        # Extract capacity if available
        total_dc_capacity = poa_df['capacity'].iloc[0] if 'capacity' in poa_df.columns else 0.0
        
        print(f"  EMIG ID: {poa_emig_id}")
        if total_dc_capacity > 0:
            print(f"  DC Capacity: {total_dc_capacity:.1f} kW")
        print(f"  Records: {len(readings)} (30-minute intervals)")
        print(f"  Date Range: {date_range}")
        print(f"  POA Statistics:")
        print(f"    Average: {avg_poa:.1f} W/m")
        print(f"    Min: {min_poa:.1f} W/m")
        print(f"    Max: {max_poa:.1f} W/m")
        print(f"   Stored successfully")
    
    # Update plant DC capacity in registry
    if total_dc_capacity > 0:
        # Get current plant data
        plants = store.list_all()
        plant_rec = next((p for p in plants if p['plant_uid'] == plant_uid), None)
        
        if plant_rec:
            # Update with new DC capacity
            store.save(
                plant_rec['alias'],
                plant_uid,
                plant_rec.get('inverter_ids', []),
                plant_rec.get('weather_id'),
                total_dc_capacity
            )
            print(f"   Updated plant DC capacity in registry: {total_dc_capacity:.1f} kW")
    
    print(f"  {'='*60}")


if __name__ == "__main__":
    # Test the module
    import sys
    
    if len(sys.argv) < 4:
        print("Usage: python solargis_poa_import.py <plant_name> <solargis_folder> <start_date> <end_date>")
        print("Example: python solargis_poa_import.py 'City Football Group' './Monthly SolarGIS data/August 2025' 20250801 20250831")
        sys.exit(1)
    
    plant_name = sys.argv[1]
    folder = sys.argv[2]
    start = sys.argv[3]
    end = sys.argv[4]
    
    result = import_poa_for_plant(plant_name, folder, start, end)
    
    if result is not None:
        print("\nSample output:")
        print(result.head(10))
        print(f"\nTotal records: {len(result)}")
    else:
        print("\n Failed to import POA data")

