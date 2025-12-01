"""Analyze the database to understand irradiance data storage"""
import sqlite3
import json

conn = sqlite3.connect('plant_registry.sqlite')
cur = conn.cursor()

# Check readings table structure
cur.execute('PRAGMA table_info(readings)')
print('Readings table columns:')
for row in cur.fetchall():
    print(f'  {row}')

# Check what EMIG IDs exist (data sources)
cur.execute('SELECT DISTINCT emig_id FROM readings ORDER BY emig_id')
emig_ids = [r[0] for r in cur.fetchall()]
print(f'\nUnique EMIG IDs ({len(emig_ids)}):')
for eid in emig_ids[:30]:
    print(f'  {eid}')
if len(emig_ids) > 30:
    print(f'  ... and {len(emig_ids) - 30} more')

# Check POA-related data
print('\n--- POA Data Sources ---')
cur.execute("""
    SELECT emig_id, COUNT(*) as count, MIN(ts) as min_ts, MAX(ts) as max_ts
    FROM readings 
    WHERE emig_id LIKE 'POA:%'
    GROUP BY emig_id
""")
for row in cur.fetchall():
    print(f'  {row[0]}: {row[1]} records, {row[2]} to {row[3]}')

# Check weather station data
print('\n--- Weather Station Data ---')
cur.execute("""
    SELECT emig_id, COUNT(*) as count, MIN(ts) as min_ts, MAX(ts) as max_ts
    FROM readings 
    WHERE emig_id LIKE 'WETH:%'
    GROUP BY emig_id
""")
for row in cur.fetchall():
    print(f'  {row[0]}: {row[1]} records, {row[2]} to {row[3]}')

# Sample a POA reading to see payload structure
print('\n--- Sample POA Reading Payload ---')
cur.execute("""
    SELECT payload FROM readings 
    WHERE emig_id LIKE 'POA:%' 
    LIMIT 1
""")
row = cur.fetchone()
if row:
    payload = json.loads(row[0])
    print(json.dumps(payload, indent=2))
else:
    print('  No POA data found')

# Sample a weather reading to see payload structure  
print('\n--- Sample Weather Reading Payload ---')
cur.execute("""
    SELECT payload FROM readings 
    WHERE emig_id LIKE 'WETH:%' 
    LIMIT 1
""")
row = cur.fetchone()
if row:
    payload = json.loads(row[0])
    print(json.dumps(payload, indent=2))
else:
    print('  No Weather data found')

# Sample an inverter reading to see payload structure  
print('\n--- Sample Inverter Reading Payload ---')
cur.execute("""
    SELECT payload FROM readings 
    WHERE emig_id LIKE 'INVERT:%' 
    LIMIT 1
""")
row = cur.fetchone()
if row:
    payload = json.loads(row[0])
    print(json.dumps(payload, indent=2))
else:
    print('  No Inverter data found')

conn.close()
