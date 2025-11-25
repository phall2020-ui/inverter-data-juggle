#!/usr/bin/env python3
"""
Display monthly POA totals with daily breakdown for manual verification
"""

import pandas as pd
import sqlite3
import json
from plant_store import PlantStore

def get_monthly_breakdown(store, plant_name, month_start, month_end):
    """Get daily POA values for a specific month"""
    
    plant = store.load(plant_name)
    plant_uid = plant['plant_uid']
    
    # Get weighted POA device
    device_ids = store.list_emig_ids(plant_uid)
    weighted_poa = [d for d in device_ids if 'WEIGHTED' in d]
    
    if not weighted_poa:
        return None
    
    # Query database
    conn = sqlite3.connect(store.db_path)
    try:
        cur = conn.execute(
            """
            SELECT ts, payload FROM readings
            WHERE plant_uid = ? AND emig_id = ?
            AND ts BETWEEN ? AND ?
            ORDER BY ts
            """,
            (plant_uid, weighted_poa[0], month_start, month_end)
        )
        
        day_totals = {}
        for ts, payload_json in cur.fetchall():
            payload = json.loads(payload_json)
            poa_irr = payload.get('poaIrradiance', {})
            poa_w = poa_irr.get('value', 0)
            poa_kwh = poa_w * 0.5 / 1000.0
            
            date = ts[:10]
            if date not in day_totals:
                day_totals[date] = 0
            day_totals[date] += poa_kwh
        
        return day_totals
    finally:
        conn.close()

def main():
    store = PlantStore('plant_registry.sqlite')
    
    # Test months
    test_cases = [
        ('Blachford UK', 'Jun-25', '2025-06-01', '2025-06-30T23:59:59'),
        ('Cromwell Tools', 'Jun-25', '2025-06-01', '2025-06-30T23:59:59'),
        ('Blachford UK', 'Aug-25', '2025-08-01', '2025-08-31T23:59:59'),
    ]
    
    for plant_name, month_label, start, end in test_cases:
        print(f"\n{'='*80}")
        print(f"{plant_name} - {month_label}")
        print(f"{'='*80}")
        
        daily = get_monthly_breakdown(store, plant_name, start, end)
        
        if not daily:
            print("No data")
            continue
        
        print(f"{'Date':<15} {'Daily POA (kWh/m²)':<20}")
        print('-'*80)
        
        monthly_total = 0
        for date in sorted(daily.keys()):
            value = daily[date]
            monthly_total += value
            print(f"{date:<15} {value:>18.3f}")
        
        print('-'*80)
        print(f"{'MONTHLY TOTAL':<15} {monthly_total:>18.3f}")
        print(f"\nPlease verify these daily values match your spreadsheet")
        print(f"for {plant_name} in {month_label}")

if __name__ == "__main__":
    main()
