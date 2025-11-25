"""Show database structure for inverter and POA data"""
import sqlite3
import json
from datetime import datetime

conn = sqlite3.connect('plant_registry.sqlite')
cur = conn.cursor()

# Get database schema
print("DATABASE SCHEMA")
print("="*120)

cur.execute("SELECT sql FROM sqlite_master WHERE type='table'")
for table_sql in cur.fetchall():
    print(table_sql[0])
    print()

print("\n" + "="*120)
print("SAMPLE DATA FOR EACH PLANT")
print("="*120)

# Get all plants
cur.execute("SELECT alias, plant_uid, dc_size_kw FROM plants ORDER BY alias")
plants = cur.fetchall()

for alias, plant_uid, dc_capacity in plants:
    print(f"\n{'='*120}")
    print(f"PLANT: {alias} ({plant_uid})")
    print(f"DC Capacity: {dc_capacity} kW")
    print(f"{'='*120}")
    
    # Get unique EMIG IDs for this plant
    cur.execute("""
        SELECT DISTINCT emig_id, COUNT(*) as record_count,
               MIN(ts) as first_reading, MAX(ts) as last_reading
        FROM readings
        WHERE plant_uid = ?
        GROUP BY emig_id
        ORDER BY emig_id
    """, (plant_uid,))
    
    emig_data = cur.fetchall()
    
    if not emig_data:
        print("  No readings found")
        continue
    
    print(f"\nEMIG IDs ({len(emig_data)} devices):")
    print(f"{'EMIG ID':<40} {'Records':>10} {'First Reading':<20} {'Last Reading':<20}")
    print("-"*120)
    
    inverter_ids = []
    poa_ids = []
    
    for emig_id, count, first_ts, last_ts in emig_data:
        print(f"{emig_id:<40} {count:>10} {first_ts:<20} {last_ts:<20}")
        
        if emig_id.startswith('INVERT:'):
            inverter_ids.append(emig_id)
        elif emig_id.startswith('POA:'):
            poa_ids.append(emig_id)
    
    # Show sample inverter data
    if inverter_ids:
        print(f"\n--- SAMPLE INVERTER DATA ---")
        sample_inverter = inverter_ids[0]
        cur.execute("""
            SELECT ts, payload
            FROM readings
            WHERE plant_uid = ? AND emig_id = ?
            ORDER BY ts
            LIMIT 3
        """, (plant_uid, sample_inverter))
        
        print(f"\nDevice: {sample_inverter}")
        print(f"{'Timestamp':<20} {'Payload (JSON)'}")
        print("-"*120)
        
        for ts, payload in cur.fetchall():
            payload_dict = json.loads(payload)
            # Format JSON for readability
            payload_str = json.dumps(payload_dict, indent=2)
            lines = payload_str.split('\n')
            print(f"{ts:<20} {lines[0]}")
            for line in lines[1:]:
                print(f"{'':<20} {line}")
            print()
    
    # Show sample POA data
    if poa_ids:
        print(f"\n--- SAMPLE POA DATA ---")
        
        # Show one orientation-specific POA
        orientation_poa = [p for p in poa_ids if 'AZ' in p and p != 'POA:SOLARGIS:WEIGHTED']
        if orientation_poa:
            sample_poa = orientation_poa[0]
            cur.execute("""
                SELECT ts, payload
                FROM readings
                WHERE plant_uid = ? AND emig_id = ?
                ORDER BY ts
                LIMIT 3
            """, (plant_uid, sample_poa))
            
            print(f"\nDevice: {sample_poa} (Orientation-specific)")
            print(f"{'Timestamp':<20} {'Payload (JSON)'}")
            print("-"*120)
            
            for ts, payload in cur.fetchall():
                payload_dict = json.loads(payload)
                payload_str = json.dumps(payload_dict, indent=2)
                lines = payload_str.split('\n')
                print(f"{ts:<20} {lines[0]}")
                for line in lines[1:]:
                    print(f"{'':<20} {line}")
                print()
        
        # Show weighted POA if available
        if 'POA:SOLARGIS:WEIGHTED' in poa_ids:
            cur.execute("""
                SELECT ts, payload
                FROM readings
                WHERE plant_uid = ? AND emig_id = 'POA:SOLARGIS:WEIGHTED'
                ORDER BY ts
                LIMIT 3
            """, (plant_uid,))
            
            print(f"\nDevice: POA:SOLARGIS:WEIGHTED (Capacity-weighted average)")
            print(f"{'Timestamp':<20} {'Payload (JSON)'}")
            print("-"*120)
            
            for ts, payload in cur.fetchall():
                payload_dict = json.loads(payload)
                payload_str = json.dumps(payload_dict, indent=2)
                lines = payload_str.split('\n')
                print(f"{ts:<20} {lines[0]}")
                for line in lines[1:]:
                    print(f"{'':<20} {line}")
                print()
    
    # Show statistics
    print(f"\n--- DATA SUMMARY ---")
    print(f"Inverters: {len(inverter_ids)} devices")
    print(f"POA devices: {len(poa_ids)} (including {len([p for p in poa_ids if 'AZ' in p and p != 'POA:SOLARGIS:WEIGHTED'])} orientations)")
    
    # Get total readings by type
    cur.execute("""
        SELECT 
            CASE 
                WHEN emig_id LIKE 'INVERT:%' THEN 'Inverter'
                WHEN emig_id LIKE 'POA:%' THEN 'POA'
                ELSE 'Other'
            END as device_type,
            COUNT(*) as total_readings
        FROM readings
        WHERE plant_uid = ?
        GROUP BY device_type
    """, (plant_uid,))
    
    for device_type, total in cur.fetchall():
        print(f"{device_type} readings: {total:,}")

conn.close()

print("\n" + "="*120)
print("QUERY COMPLETE")
