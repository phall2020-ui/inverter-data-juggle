"""Show example half-hourly data for Blachford UK for each month"""
import sqlite3
import json
from datetime import datetime

conn = sqlite3.connect('plant_registry.sqlite')
cur = conn.cursor()

plant_name = "Blachford UK"
plant_uid = "AMP:00024"

print(f"Half-Hourly Data Examples for {plant_name}")
print(f"Plant UID: {plant_uid}")
print("="*150)

# Get months with data
cur.execute("""
    SELECT DISTINCT strftime('%Y-%m', ts) as month
    FROM readings
    WHERE plant_uid = ? AND emig_id = 'POA:SOLARGIS:WEIGHTED'
    ORDER BY month
""", (plant_uid,))

months = [row[0] for row in cur.fetchall()]

month_names = {
    '2025-06': 'June 2025',
    '2025-07': 'July 2025',
    '2025-08': 'August 2025',
    '2025-09': 'September 2025',
    '2025-10': 'October 2025',
}

# Get inverter IDs
cur.execute("""
    SELECT DISTINCT emig_id
    FROM readings
    WHERE plant_uid = ? AND emig_id LIKE 'INVERT:%'
    ORDER BY emig_id
    LIMIT 3
""", (plant_uid,))

inverter_ids = [row[0] for row in cur.fetchall()]

for month in months:
    month_name = month_names.get(month, month)
    print(f"\n{'='*150}")
    print(f"{month_name}")
    print(f"{'='*150}")
    
    # Get a sample day (15th of the month) with data spanning morning to afternoon
    sample_date = f"{month}-15"
    
    print(f"\nSample Date: {sample_date}")
    print(f"\n{'Timestamp':<20} {'POA (kWh/m²)':<15} ", end='')
    for inv_id in inverter_ids:
        print(f"{inv_id:<25}", end=' ')
    print()
    print(f"{'':20} {'(Weighted)':<15} ", end='')
    for _ in inverter_ids:
        print(f"{'Power (W) | Energy (Wh)':<25}", end=' ')
    print()
    print("-"*150)
    
    # Get data for sample hours (6am to 6pm)
    cur.execute("""
        SELECT ts, payload
        FROM readings
        WHERE plant_uid = ? 
          AND emig_id = 'POA:SOLARGIS:WEIGHTED'
          AND ts LIKE ?
          AND CAST(substr(ts, 12, 2) AS INTEGER) BETWEEN 6 AND 18
        ORDER BY ts
        LIMIT 10
    """, (plant_uid, f"{sample_date}%"))
    
    poa_data = {row[0]: json.loads(row[1]) for row in cur.fetchall()}
    
    for timestamp in sorted(poa_data.keys()):
        poa_value = poa_data[timestamp]['poaIrradiance']['value']
        print(f"{timestamp:<20} {poa_value:<15.6f} ", end='')
        
        # Get inverter data for same timestamp
        for inv_id in inverter_ids:
            cur.execute("""
                SELECT payload
                FROM readings
                WHERE plant_uid = ? AND emig_id = ? AND ts = ?
            """, (plant_uid, inv_id, timestamp.replace('T', ' ').replace('.000000Z', '')))
            
            result = cur.fetchone()
            if result:
                inv_data = json.loads(result[0])
                power = inv_data.get('importActivePower', {}).get('value', 0)
                energy = inv_data.get('importEnergy', {}).get('value', 0)
                print(f"{power:>7.0f}W | {energy:>10.0f}Wh ", end=' ')
            else:
                print(f"{'N/A':>7} | {'N/A':>10} ", end=' ')
        
        print()
    
    # Show daily statistics for this month
    print(f"\n{month_name} Statistics:")
    print("-"*150)
    
    # POA statistics
    cur.execute("""
        SELECT 
            DATE(ts) as day,
            SUM(json_extract(payload, '$.poaIrradiance.value')) as daily_poa,
            COUNT(*) as readings
        FROM readings
        WHERE plant_uid = ? 
          AND emig_id = 'POA:SOLARGIS:WEIGHTED'
          AND strftime('%Y-%m', ts) = ?
        GROUP BY DATE(ts)
        ORDER BY day
        LIMIT 5
    """, (plant_uid, month))
    
    print(f"\n{'Date':<12} {'Daily POA (kWh/m²)':<20} {'HH Readings':<15}")
    print("-"*150)
    for day, daily_poa, readings in cur.fetchall():
        print(f"{day:<12} {daily_poa:<20.2f} {readings:<15}")
    
    # Inverter statistics for first inverter
    if inverter_ids:
        first_inv = inverter_ids[0]
        cur.execute("""
            SELECT 
                DATE(ts) as day,
                MAX(json_extract(payload, '$.importEnergy.value')) - 
                MIN(json_extract(payload, '$.importEnergy.value')) as daily_energy,
                AVG(json_extract(payload, '$.importActivePower.value')) as avg_power
            FROM readings
            WHERE plant_uid = ? 
              AND emig_id = ?
              AND strftime('%Y-%m', ts) = ?
            GROUP BY DATE(ts)
            ORDER BY day
            LIMIT 5
        """, (plant_uid, first_inv, month))
        
        print(f"\nInverter {first_inv} Daily Generation (first 5 days):")
        print(f"{'Date':<12} {'Daily Energy (kWh)':<20} {'Avg Power (W)':<15}")
        print("-"*150)
        for day, daily_energy, avg_power in cur.fetchall():
            if daily_energy and avg_power:
                print(f"{day:<12} {daily_energy/1000:<20.2f} {avg_power:<15.0f}")

conn.close()

print("\n" + "="*150)
print("Complete")
