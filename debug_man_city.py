"""Debug Man City import - check individual orientations"""
import sqlite3
import json

conn = sqlite3.connect('plant_registry.sqlite')
cur = conn.cursor()

# Get all POA EMIG IDs for Man City
cur.execute("""
    SELECT DISTINCT emig_id
    FROM readings
    WHERE plant_uid = 'AMP:00019'
      AND emig_id LIKE 'POA:SOLARGIS:%'
    ORDER BY emig_id
""")

emig_ids = [row[0] for row in cur.fetchall()]

print("Man City POA Orientations (October totals):")
print("="*80)

for emig_id in emig_ids:
    if emig_id == 'POA:SOLARGIS:WEIGHTED':
        continue
        
    # Sum October
    cur.execute("""
        SELECT SUM(json_extract(payload, '$.poaIrradiance.value'))
        FROM readings
        WHERE plant_uid = 'AMP:00019'
          AND emig_id = ?
          AND ts >= '2025-10-01'
          AND ts < '2025-11-01'
    """, (emig_id,))
    
    total = cur.fetchone()[0] or 0
    print(f"{emig_id}: {total:.2f} kWh/m²")

print("\nWeighted average:")
cur.execute("""
    SELECT SUM(json_extract(payload, '$.poaIrradiance.value'))
    FROM readings
    WHERE plant_uid = 'AMP:00019'
      AND emig_id = 'POA:SOLARGIS:WEIGHTED'
      AND ts >= '2025-10-01'
      AND ts < '2025-11-01'
""")
total = cur.fetchone()[0] or 0
print(f"POA:SOLARGIS:WEIGHTED: {total:.2f} kWh/m²")

print("\nExpected reference: 37.74 kWh/m²")
print(f"Difference: {(total - 37.74):.2f} kWh/m² ({(total - 37.74)/37.74*100:+.1f}%)")

conn.close()
