"""Check Man City October database records"""
import sqlite3
import json

conn = sqlite3.connect('plant_registry.sqlite')
cur = conn.cursor()

print("Man City October Records by EMIG ID:")
cur.execute("""
    SELECT emig_id, COUNT(*) as count
    FROM readings
    WHERE plant_uid = 'AMP:00019'
      AND ts >= '2025-10-01'
      AND ts < '2025-11-01'
    GROUP BY emig_id
""")

for emig, count in cur.fetchall():
    print(f"  {emig}: {count} records")

print("\nSample of weighted POA values:")
cur.execute("""
    SELECT ts, payload
    FROM readings
    WHERE plant_uid = 'AMP:00019'
      AND emig_id = 'POA:SOLARGIS:WEIGHTED'
      AND ts >= '2025-10-01'
      AND ts < '2025-10-02'
    LIMIT 10
""")

for ts, payload in cur.fetchall():
    p = json.loads(payload)
    poa = p.get('poaIrradiance', {}).get('value', 0)
    print(f"  {ts}: {poa:.6f} kWh/m²")

# Sum October weighted POA
cur.execute("""
    SELECT SUM(json_extract(payload, '$.poaIrradiance.value'))
    FROM readings
    WHERE plant_uid = 'AMP:00019'
      AND emig_id = 'POA:SOLARGIS:WEIGHTED'
      AND ts >= '2025-10-01'
      AND ts < '2025-11-01'
""")
total = cur.fetchone()[0]
print(f"\nTotal October weighted POA: {total:.2f} kWh/m²")

conn.close()
