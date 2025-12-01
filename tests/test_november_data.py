#!/usr/bin/env python3
"""
Tests for sample data generation, upload, and excom report functionality.
"""

import os
import tempfile

import pandas as pd
import pytest

from plant_store import PlantStore


class TestSampleDataGeneration:
    """Tests for November sample data generation."""
    
    def test_generate_timestamp_range(self):
        """Test that timestamp range generation works correctly."""
        from generate_november_sample_data import generate_timestamp_range
        
        timestamps = generate_timestamp_range("20251122", "20251123")
        
        # Should have 48 half-hourly intervals per day (2 days = 96)
        assert len(timestamps) == 96
        
        # Check first and last timestamps
        assert timestamps[0].strftime("%Y-%m-%d") == "2025-11-22"
        assert timestamps[-1].strftime("%Y-%m-%d") == "2025-11-23"
    
    def test_generate_inverter_reading(self):
        """Test that inverter readings have required fields."""
        from generate_november_sample_data import generate_inverter_reading
        from datetime import datetime
        
        ts = datetime(2025, 11, 22, 12, 0)  # Noon
        reading = generate_inverter_reading(ts, "INVERT:001")
        
        # Check required fields
        assert "timestamp" in reading
        assert "emigId" in reading
        assert reading["emigId"] == "INVERT:001"
        assert "apparentPower" in reading
        assert "ts:" in reading
        
        # At noon, should have power output (non-negative value)
        assert reading["apparentPower"] is None or reading["apparentPower"] >= 0
    
    def test_generate_weather_reading(self):
        """Test that weather readings have required fields."""
        from generate_november_sample_data import generate_weather_reading
        from datetime import datetime
        
        ts = datetime(2025, 11, 22, 12, 0)  # Noon
        reading = generate_weather_reading(ts, "WETH:001")
        
        # Check required fields
        assert "timestamp" in reading
        assert "emigId" in reading
        assert reading["emigId"] == "WETH:001"
        assert "poaIrradiance" in reading
    
    def test_generate_site_data(self):
        """Test that site data generation produces a valid DataFrame."""
        from generate_november_sample_data import generate_site_data
        
        df = generate_site_data("Blachford UK", "20251122", "20251122")
        
        # Should have data
        assert not df.empty
        
        # Should have timestamp column
        assert "timestamp" in df.columns
        
        # Should have emigId column
        assert "emigId" in df.columns


class TestUploadFunctionality:
    """Tests for upload and duplicate handling."""
    
    def test_normalize_timestamp(self):
        """Test timestamp normalization."""
        from upload_data import normalize_timestamp
        
        assert normalize_timestamp("2025-11-22T10:00:00.000000Z") == "2025-11-22T10:00:00"
        assert normalize_timestamp("2025-11-22T10:00:00Z") == "2025-11-22T10:00:00"
        assert normalize_timestamp("2025-11-22T10:00:00") == "2025-11-22T10:00:00"
    
    def test_check_existing_records(self):
        """Test that existing records are detected."""
        from upload_data import check_existing_records
        
        # Create temp database
        fd, temp_db = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        
        try:
            store = PlantStore(temp_db)
            store.save("TestPlant", "TEST:001", [], None, None)
            
            # Store some readings
            readings = [
                {"ts": "2025-11-22T10:00:00", "value": 100},
                {"ts": "2025-11-22T10:30:00", "value": 200},
            ]
            store.store_readings("TEST:001", "INV:001", readings)
            
            # Check for existing
            existing = check_existing_records(
                store, "TEST:001", "INV:001",
                ["2025-11-22T10:00:00", "2025-11-22T11:00:00"]
            )
            
            assert "2025-11-22T10:00:00" in existing
            assert "2025-11-22T11:00:00" not in existing
        
        finally:
            os.unlink(temp_db)
    
    def test_upload_csv_with_duplicates(self):
        """Test that duplicate records are skipped on upload."""
        from upload_data import upload_csv_to_db
        
        # Create temp database
        fd, temp_db = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        
        # Create temp CSV
        fd2, temp_csv = tempfile.mkstemp(suffix=".csv")
        os.close(fd2)
        
        try:
            store = PlantStore(temp_db)
            store.save("TestPlant", "TEST:001", [], None, None)
            
            # Store initial readings
            readings = [
                {"ts": "2025-11-22T10:00:00", "value": 100},
            ]
            store.store_readings("TEST:001", "INV:001", readings)
            
            # Create CSV with one duplicate and one new
            pd.DataFrame([
                {"emigId": "INV:001", "ts": "2025-11-22T10:00:00", "value": 100},  # duplicate
                {"emigId": "INV:001", "ts": "2025-11-22T11:00:00", "value": 200},  # new
            ]).to_csv(temp_csv, index=False)
            
            # Upload
            new, dup = upload_csv_to_db(store, "TEST:001", temp_csv, check_duplicates=True)
            
            assert new == 1, f"Expected 1 new record, got {new}"
            assert dup == 1, f"Expected 1 duplicate, got {dup}"
        
        finally:
            os.unlink(temp_db)
            os.unlink(temp_csv)


class TestExcomReport:
    """Tests for ExCom report generation."""
    
    def test_site_performance_creation(self):
        """Test SitePerformance object creation."""
        from excom_report import SitePerformance
        
        site = SitePerformance("TestSite")
        assert site.name == "TestSite"
        assert site.total_energy_kwh == 0.0
        assert site.monthly_data == {}
    
    def test_excom_report_creation(self):
        """Test ExcomReport object creation."""
        from excom_report import ExcomReport, SitePerformance
        
        report = ExcomReport(2025)
        assert report.year == 2025
        assert len(report.sites) == 0
        
        # Add a site
        site = SitePerformance("TestSite")
        site.total_energy_kwh = 1000.0
        site.monthly_data = {"11": 100.0}
        
        report.add_site(site)
        
        assert len(report.sites) == 1
        assert report.total_ytd_energy == 1000.0
        assert report.total_november_energy == 100.0
    
    def test_waterfall_data_generation(self):
        """Test waterfall chart data generation."""
        from excom_report import ExcomReport, SitePerformance, generate_waterfall_data
        
        report = ExcomReport(2025)
        
        site = SitePerformance("TestSite")
        site.total_energy_kwh = 1200.0
        site.monthly_data = {f"{i:02d}": 100.0 for i in range(1, 13)}
        
        report.add_site(site)
        
        waterfall_df = generate_waterfall_data(report)
        
        # Should have 12 months + 1 total
        assert len(waterfall_df) == 13
        
        # Check total
        total_row = waterfall_df[waterfall_df["Category"] == "YTD Total"]
        assert len(total_row) == 1
        assert total_row.iloc[0]["Value"] == 1200.0
    
    def test_summary_dataframe(self):
        """Test summary DataFrame generation."""
        from excom_report import ExcomReport, SitePerformance
        
        report = ExcomReport(2025)
        
        site = SitePerformance("TestSite")
        site.total_energy_kwh = 1000.0
        site.peak_power_kw = 50.0
        site.monthly_data = {"11": 100.0}
        site.record_count = 100
        
        report.add_site(site)
        
        summary_df = report.get_summary_df()
        
        assert len(summary_df) == 1
        assert summary_df.iloc[0]["Site"] == "TestSite"
        assert summary_df.iloc[0]["YTD Energy (kWh)"] == 1000.0
        assert summary_df.iloc[0]["Nov Energy (kWh)"] == 100.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
