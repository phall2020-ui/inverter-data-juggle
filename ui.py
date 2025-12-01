import streamlit as st
import pandas as pd
import os
import sys
import logging
import argparse
import io
from datetime import datetime, timedelta

# Ensure local modules can be imported
sys.path.append(os.getcwd())

# Import pipeline modules
# We wrap this in try-except to handle potential import errors gracefully
try:
    import inverter_pipeline
    from plant_store import PlantStore, DEFAULT_DB
except ImportError as e:
    st.error(f"Failed to import pipeline modules: {e}")
    st.stop()

# Configure logging to capture output
log_capture_string = io.StringIO()
ch = logging.StreamHandler(log_capture_string)
ch.setLevel(logging.INFO)
logger = logging.getLogger("inverter_pipeline")
logger.addHandler(ch)

# Mock ArgumentParser for reusing pipeline logic
class MockArgs:
    def __init__(self, **kwargs):
        # Default values for common args
        self.verbose = False
        self.db_path = DEFAULT_DB
        self.force_download = False
        
        # Update with provided kwargs
        self.__dict__.update(kwargs)
    
    def __getattr__(self, name):
        # Return None for any attribute not explicitly set
        return None

st.set_page_config(page_title="Inverter Pipeline UI", layout="wide", page_icon="⚡")

# Custom CSS for "premium" feel
st.markdown("""
<style>
    .stApp {
        background-color: #f8f9fa;
    }
    .main-header {
        font-family: 'Inter', sans-serif;
        color: #1e3a8a;
        font-weight: 700;
    }
    .stButton>button {
        background-color: #2563eb;
        color: white;
        border-radius: 6px;
        border: none;
        padding: 0.5rem 1rem;
        font-weight: 600;
    }
    .stButton>button:hover {
        background-color: #1d4ed8;
    }
</style>
""", unsafe_allow_html=True)

st.title("⚡ Inverter Data Pipeline")

# Sidebar for global settings or info
with st.sidebar:
    st.header("Settings")
    if st.checkbox("Show Verbose Logs"):
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    
    st.markdown("---")
    st.markdown("**Workspace:**")
    st.code(os.getcwd())

# Load plants globally so all tabs can access
store = PlantStore(DEFAULT_DB)
plants = store.list_all()
today = datetime.today()

# Tabs for different workflows
tab_fetch, tab_fouling, tab_shading, tab_view, tab_registry = st.tabs([
    "📥 Fetch Data", 
    "🧹 Fouling Analysis", 
    "☀️ Shading Analysis", 
    "📊 View Data",
    "📋 Plant Registry"
])

# --- VIEW DATA TAB ---
with tab_view:
    st.header("View Database Contents")
    st.info("Query and inspect raw data stored in the local database.")
    
    col1, col2 = st.columns(2)
    with col1:
        plant_options = {p['alias']: p for p in plants} if plants else {}
        view_plant_alias = st.selectbox("Select Plant", list(plant_options.keys()) if plant_options else [], key="view_plant")
        
    with col2:
        view_start = st.date_input("Start Date", today - timedelta(days=1), key="view_start")
        view_end = st.date_input("End Date", today, key="view_end")
        
    if st.button("Load Data", type="primary"):
        if not view_plant_alias:
            st.error("Please select a plant.")
        else:
            with st.spinner("Querying database..."):
                try:
                    df_view = inverter_pipeline.load_db_dataframe(
                        store, view_plant_alias,
                        view_start.strftime("%Y%m%d"),
                        view_end.strftime("%Y%m%d")
                    )
                    
                    if df_view.empty:
                        st.warning("No data found for the selected range.")
                    else:
                        st.success(f"Loaded {len(df_view)} rows.")
                        st.dataframe(df_view, use_container_width=True)
                        
                        # Download button
                        csv = df_view.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            "Download CSV",
                            csv,
                            f"{view_plant_alias}_{view_start}_{view_end}.csv",
                            "text/csv",
                            key='download-csv'
                        )
                        
                except Exception as e:
                    st.error(f"Error loading data: {e}")

# --- PLANT REGISTRY TAB ---
with tab_registry:
    st.header("Plant Registry Management")
    
    if plants:
        df_plants = pd.DataFrame(plants)
        st.dataframe(
            df_plants, 
            column_config={
                "alias": "Plant Alias",
                "plant_uid": "Plant UID",
                "weather_id": "Weather ID",
                "dc_size_kw": "DC Size (kW)"
            },
            use_container_width=True
        )
    else:
        st.info("No plants found in registry. Add one below.")

    st.markdown("### Add New Plant")
    with st.form("add_plant_form"):
        col1, col2 = st.columns(2)
        with col1:
            new_alias = st.text_input("Plant Alias (e.g., 'MySolarFarm')")
            new_uid = st.text_input("Plant UID (e.g., 'ERS:00001')")
        with col2:
            new_weather = st.text_input("Weather ID (optional)")
            new_inv_ids = st.text_input("Inverter IDs (comma separated, optional)")
        
        submitted = st.form_submit_button("Save Plant")
        if submitted:
            if new_alias and new_uid:
                inv_list = [x.strip() for x in new_inv_ids.split(",")] if new_inv_ids else []
                store.save(new_alias, new_uid, inv_list, new_weather or None, None)
                st.success(f"Plant '{new_alias}' saved successfully!")
                st.rerun()
            else:
                st.error("Alias and UID are required.")

    st.markdown("### Delete Plant")
    with st.form("delete_plant_form"):
        plant_to_delete = st.selectbox("Select Plant to Delete", [p['alias'] for p in plants] if plants else [])
        delete_submitted = st.form_submit_button("Delete Plant", type="primary")
        if delete_submitted and plant_to_delete:
            store.delete(plant_to_delete)
            st.success(f"Plant '{plant_to_delete}' deleted.")
            st.rerun()

# --- FETCH DATA TAB ---
with tab_fetch:
    st.header("Fetch Data from Juggle API")
    
    # Inputs
    col1, col2 = st.columns(2)
    with col1:
        fetch_mode = st.radio("Fetch Mode", ["Single Plant", "All Plants"], horizontal=True)
        
        # Plant selection
        plant_options = {p['alias']: p for p in plants} if plants else {}
        
        if fetch_mode == "Single Plant":
            selected_alias = st.selectbox("Select Plant", list(plant_options.keys()) if plant_options else [])
        else:
            st.info(f"Will fetch data for all {len(plants)} plants.")
            selected_alias = None
        
        # Date selection
        start_date = st.date_input("Start Date", today - timedelta(days=7))
        end_date = st.date_input("End Date", today)
    
    with col2:
        # API Key stored internally
        api_key = "380fe299-a626-48f1-8456-e701c7383a23"
        
        force_download = st.checkbox("Force Download (Ignore Cache)", value=False)
        include_weather = st.checkbox("Include Weather Data", value=True)
        fetch_devices = st.checkbox("Auto-discover Devices", value=True)

    # SolarGIS POA Import Section
    st.markdown("---")
    with st.expander("📊 Import SolarGIS POA Irradiance Data", expanded=False):
        st.info("Import plane-of-array irradiance data from SolarGIS monthly folders.")
        
        import_poa_mode = st.radio(
            "Import Mode", 
            ["Auto-detect Folders", "Manual Folder Selection"], 
            horizontal=True,
            key="poa_mode"
        )
        
        solargis_folders = []
        
        if import_poa_mode == "Auto-detect Folders":
            base_dir = os.path.expanduser("~/OneDrive - AMPYR IDEA UK Ltd/Monthly Excom/Monthly SolarGIS data")
            
            if os.path.exists(base_dir):
                all_items = os.listdir(base_dir)
                solargis_folders = [
                    os.path.join(base_dir, item) 
                    for item in all_items 
                    if os.path.isdir(os.path.join(base_dir, item))
                ]
                
                if solargis_folders:
                    st.success(f"Found {len(solargis_folders)} SolarGIS data folder(s):")
                    st.code("\n".join([os.path.basename(f) for f in solargis_folders]))
                else:
                    st.warning(f"No subfolders found in {base_dir}")
            else:
                st.error(f"SolarGIS data directory not found: {base_dir}")
        else:
            # Manual folder selection

    if st.button("Run Fetch", type="primary"):
        plants_to_process = []
        if fetch_mode == "Single Plant":
            if not selected_alias:
                st.error("Please select a plant.")
            else:
                plants_to_process = [plant_options[selected_alias]]
        else:
            plants_to_process = list(plant_options.values())
            
        if plants_to_process:
            # Clear logs
            log_capture_string.truncate(0)
            log_capture_string.seek(0)
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            total_plants = len(plants_to_process)
            
            for i, plant in enumerate(plants_to_process):
                plant_alias = plant['alias']
                status_text.text(f"Processing {plant_alias} ({i+1}/{total_plants})...")
                
                args = MockArgs(
                    plant_alias=plant_alias,
                    plant_uid=plant['plant_uid'],
                    weather_id=plant['weather_id'],
                    start_date=start_date.strftime("%Y%m%d"),
                    end_date=end_date.strftime("%Y%m%d"),
                    api_key=api_key,
                    force_download=force_download,
                    include_weather=include_weather,
                    fetch_devices=fetch_devices,
                    inverter_ids=None, # Will use DB or auto-discovery
                    save_plant=None,
                    output=None # Default naming
                )
                
                try:
                    logger.info(f"--- Starting fetch for {plant_alias} ---")
                    inverter_pipeline.run_fetch(args)
                    logger.info(f"--- Completed fetch for {plant_alias} ---")
                except SystemExit as e:
                    logger.error(f"SystemExit during fetch for {plant_alias}: {e}")
                except Exception as e:
                    logger.error(f"Error fetching {plant_alias}: {e}")
                
                progress_bar.progress((i + 1) / total_plants)
            
            status_text.success("All requested fetches completed!")
            
            # Show logs
            st.markdown("### Execution Logs")
            st.code(log_capture_string.getvalue())

# --- FOULING ANALYSIS TAB ---
with tab_fouling:
    st.header("Fouling Analysis")
    st.info("Detect soiling by comparing an analysis period against a clean baseline period.")
    
    # Data Source Selection
    foul_source = st.radio("Data Source", ["Database", "CSV Files"], horizontal=True, key="foul_source")
    
    if foul_source == "Database":
        # DB Inputs
        col1, col2 = st.columns(2)
        with col1:
            # Plant selection
            plant_options = {p['alias']: p for p in plants} if plants else {}
            foul_plant_alias = st.selectbox("Select Plant", list(plant_options.keys()) if plant_options else [], key="foul_plant")
            
            # Get DC size from registry if available
            default_dc = 1000.0
            if foul_plant_alias and plant_options:
                saved_dc = plant_options[foul_plant_alias].get('dc_size_kw')
                if saved_dc:
                    default_dc = float(saved_dc)
            
            dc_size = st.number_input("DC Size (kW)", value=default_dc, key="foul_dc")

        with col2:
            st.markdown("### Analysis Period (Compare)")
            ana_start = st.date_input("Start Date", today - timedelta(days=30), key="ana_start")
            ana_end = st.date_input("End Date", today, key="ana_end")
            
            st.markdown("### Clean Baseline Period")
            use_auto_clean = st.checkbox("Auto-detect Clean Baseline", value=True)
            
            if not use_auto_clean:
                clean_start = st.date_input("Clean Start", today - timedelta(days=60), key="clean_start")
                clean_end = st.date_input("Clean End", today - timedelta(days=50), key="clean_end")
            else:
                clean_start, clean_end = None, None

        if st.button("Run Fouling Analysis", type="primary"):
            if not foul_plant_alias:
                st.error("Please select a plant.")
            else:
                with st.spinner("Loading data and analyzing..."):
                    log_capture_string.truncate(0)
                    log_capture_string.seek(0)
                    
                    try:
                        # 1. Load Analysis Data
                        st.text(f"Loading analysis data ({ana_start} to {ana_end})...")
                        df_analysis = inverter_pipeline.load_db_dataframe(
                            store, foul_plant_alias, 
                            ana_start.strftime("%Y%m%d"), 
                            ana_end.strftime("%Y%m%d")
                        )
                        
                        if df_analysis.empty:
                            st.error("No data found for the Analysis Period.")
                            st.stop()
                            
                        # 2. Load Clean Data (if manual) or use Analysis data for auto
                        df_clean = pd.DataFrame()
                        if not use_auto_clean:
                            st.text(f"Loading clean data ({clean_start} to {clean_end})...")
                            df_clean = inverter_pipeline.load_db_dataframe(
                                store, foul_plant_alias,
                                clean_start.strftime("%Y%m%d"),
                                clean_end.strftime("%Y%m%d")
                            )
                            if df_clean.empty:
                                st.error("No data found for the Clean Period.")
                                st.stop()
                        
                        # 3. Run Analysis
                        if use_auto_clean:
                            args = MockArgs(
                                data_df=df_analysis,
                                dc_size_kw=dc_size,
                                timestamp_col="ts",
                                ac_col="ac_power",
                                poa_col="poa",
                                auto_clean_days=5,
                                min_clean_points=10,
                                enriched_out=None,
                                clean_report_out=None
                            )
                            inverter_pipeline.run_fouling_auto(args)
                        else:
                            args = MockArgs(
                                full_df=df_analysis,
                                clean_df=df_clean,
                                dc_size_kw=dc_size,
                                enriched_out=None
                            )
                            inverter_pipeline.run_fouling(args)
                            
                        st.success("Analysis complete!")
                        
                    except Exception as e:
                        st.error(f"Error: {e}")
                        logger.exception("Fouling analysis failed")
                    
                    st.markdown("### Logs")
                    st.code(log_capture_string.getvalue())

    else:
        # CSV Mode (Legacy)
        csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
        col1, col2 = st.columns(2)
        with col1:
            data_file = st.selectbox("Select Data File", csv_files, index=0 if csv_files else None)
            dc_size = st.number_input("DC Size (kW)", value=100.0, key="csv_dc")
        
        with col2:
            auto_clean = st.checkbox("Auto-detect Clean Period", value=True, key="csv_auto")
            if not auto_clean:
                clean_start = st.date_input("Clean Start", today - timedelta(days=30), key="csv_cs")
                clean_end = st.date_input("Clean End", today - timedelta(days=20), key="csv_ce")
            else:
                clean_start, clean_end = None, None
                
        if st.button("Run Fouling Analysis (CSV)"):
            if not data_file:
                st.error("Please select a data file.")
            else:
                args = MockArgs(
                    data=data_file,
                    dc_size_kw=dc_size,
                    timestamp_col="timestamp",
                    ac_col="power",
                    poa_col="poa",
                    clean_start=clean_start.strftime("%Y-%m-%d") if clean_start else None,
                    clean_end=clean_end.strftime("%Y-%m-%d") if clean_end else None,
                    auto_clean_days=5,
                    min_clean_points=10,
                    enriched_out=f"fouling_enriched_{data_file}",
                    clean_report_out="clean_report.csv"
                )
                
                with st.spinner("Running analysis..."):
                    log_capture_string.truncate(0)
                    log_capture_string.seek(0)
                    try:
                        inverter_pipeline.run_fouling_auto(args)
                        st.success("Analysis complete!")
                    except Exception as e:
                        st.error(f"Error: {e}")
                    
                    st.markdown("### Logs")
                    st.code(log_capture_string.getvalue())

# --- SHADING ANALYSIS TAB ---
with tab_shading:
    st.header("Shading Analysis")
    st.info("Compare summer vs winter profiles to detect shading.")
    
    shade_source = st.radio("Data Source", ["Database", "CSV Files"], horizontal=True, key="shade_source")
    
    if shade_source == "Database":
        col1, col2 = st.columns(2)
        with col1:
            plant_options = {p['alias']: p for p in plants} if plants else {}
            shade_plant_alias = st.selectbox("Select Plant", list(plant_options.keys()) if plant_options else [], key="shade_plant")
            
            st.markdown("### Summer Period (Baseline)")
            sum_start = st.date_input("Summer Start", datetime(today.year, 6, 1), key="sum_start")
            sum_end = st.date_input("Summer End", datetime(today.year, 8, 31), key="sum_end")
            
        with col2:
            st.markdown("### Winter Period (Compare)")
            # Default to previous winter
            win_start = st.date_input("Winter Start", datetime(today.year-1, 12, 1), key="win_start")
            win_end = st.date_input("Winter End", datetime(today.year, 2, 28), key="win_end")

        if st.button("Run Shading Analysis", type="primary"):
            if not shade_plant_alias:
                st.error("Please select a plant.")
            else:
                with st.spinner("Fetching data and analyzing..."):
                    log_capture_string.truncate(0)
                    log_capture_string.seek(0)
                    
                    try:
                        st.text("Loading Summer data...")
                        df_summer = inverter_pipeline.load_db_dataframe(
                            store, shade_plant_alias,
                            sum_start.strftime("%Y%m%d"),
                            sum_end.strftime("%Y%m%d")
                        )
                        
                        st.text("Loading Winter data...")
                        df_winter = inverter_pipeline.load_db_dataframe(
                            store, shade_plant_alias,
                            win_start.strftime("%Y%m%d"),
                            win_end.strftime("%Y%m%d")
                        )
                        
                        if df_summer.empty or df_winter.empty:
                            st.error("Data missing for one or both periods.")
                        else:
                            # Run Analysis
                            args = MockArgs(
                                summer_df=df_summer,
                                winter_df=df_winter,
                                weather_id=None, # Will infer
                                irr_col="poaIrradiance", # Try standard names
                                current_col="apparentPower",
                                irr_min=50,
                                min_points_per_hour=10,
                                detail_out="shading_detail.csv",
                                summary_out="shading_summary.csv"
                            )
                            
                            inverter_pipeline.run_shading(args)
                            st.success("Analysis complete!")
                            
                            # Display results if available
                            if os.path.exists("shading_summary.csv"):
                                st.markdown("### Summary Results")
                                st.dataframe(pd.read_csv("shading_summary.csv"))
                                
                    except Exception as e:
                        st.error(f"Error: {e}")
                        logger.exception("Shading analysis failed")
                    
                    st.markdown("### Logs")
                    st.code(log_capture_string.getvalue())

    else:
        # CSV Mode
        csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
        col1, col2 = st.columns(2)
        with col1:
            summer_file = st.selectbox("Summer Data CSV", csv_files, key="summer_csv")
        with col2:
            winter_file = st.selectbox("Winter Data CSV", csv_files, key="winter_csv")
            
        if st.button("Run Shading Analysis (CSV)"):
            if not summer_file or not winter_file:
                st.error("Please select both files.")
            else:
                args = MockArgs(
                    summer_csv=summer_file,
                    winter_csv=winter_file,
                    weather_id=None, 
                    irr_col="irradiance",
                    current_col="current",
                    irr_min=50,
                    min_points_per_hour=10,
                    detail_out="shading_detail.csv",
                    summary_out="shading_summary.csv"
                )
                
                with st.spinner("Analyzing shading..."):
                    log_capture_string.truncate(0)
                    log_capture_string.seek(0)
                    try:
                        inverter_pipeline.run_shading(args)
                        st.success("Analysis complete!")
                    except Exception as e:
                        st.error(f"Error: {e}")
                    
                    st.markdown("### Logs")
                    st.code(log_capture_string.getvalue())
