"""Check dates for zero-value orientations"""
import sqlite3

conn = sqlite3.connect('plant_registry.sqlite')
cur = conn.cursor()

print("AZ60:SL5 dates:")
cur.execute("""
    SELECT DISTINCT strftime('%Y-%m', ts) as month
    FROM readings
    WHERE plant_uid = 'AMP:00019'
      AND emig_id = 'POA:SOLARGIS:AZ60:SL5'
    ORDER BY ts
""")
for row in cur.fetchall():
    print(f"  {row[0]}")

print("\nAZ93:SL0 dates:")
cur.execute("""
    SELECT DISTINCT strftime('%Y-%m', ts) as month
    FROM readings
    WHERE plant_uid = 'AMP:00019'
      AND emig_id = 'POA:SOLARGIS:AZ93:SL0'
    ORDER BY ts
""")
for row in cur.fetchall():
    print(f"  {row[0]}")

conn.close()
