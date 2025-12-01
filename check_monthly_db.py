"""Check Monthly reporting database structure and data"""
import sqlite3
import pandas as pd
import os

# Path to Monthly reporting database
db_path = os.path.join(
    os.path.dirname(__file__), 
    '..', 
    'Monthly reporting', 
    'solar_assets.db'
)

print(f"Database path: {db_path}")
print(f"Exists: {os.path.exists(db_path)}")

if not os.path.exists(db_path):
    print("Database not found!")
    exit()

conn = sqlite3.connect(db_path)

# List tables
print("\n" + "="*80)
print("TABLES IN DATABASE:")
print("="*80)
cur = conn.cursor()
cur.execute('SELECT name FROM sqlite_master WHERE type="table"')
tables = [r[0] for r in cur.fetchall()]
print(tables)

# Check each table structure and sample
for tbl in tables:
    print(f"\n{'='*80}")
    print(f"TABLE: {tbl}")
    print("="*80)
    
    # Get columns
    cur.execute(f'PRAGMA table_info("{tbl}")')
    cols = [(r[1], r[2]) for r in cur.fetchall()]
    print(f"Columns: {cols}")
    
    # Get row count
    cur.execute(f'SELECT COUNT(*) FROM "{tbl}"')
    row_count = cur.fetchone()[0]
    print(f"Row count: {row_count}")
    
    # Sample data
    df = pd.read_sql_query(f'SELECT * FROM "{tbl}" LIMIT 5', conn)
    if len(df) > 0:
        print("\nSample rows:")
        print(df.to_string())
        
        # Get unique sites and dates if available
        for col in df.columns:
            col_lower = col.lower()
            if 'site' in col_lower or 'name' in col_lower:
                cur.execute(f'SELECT DISTINCT "{col}" FROM "{tbl}" ORDER BY "{col}"')
                unique_sites = [r[0] for r in cur.fetchall()]
                print(f"\nUnique {col} values ({len(unique_sites)}): {unique_sites[:20]}")
            elif 'date' in col_lower or 'month' in col_lower or 'period' in col_lower:
                cur.execute(f'SELECT DISTINCT "{col}" FROM "{tbl}" ORDER BY "{col}"')
                unique_dates = [r[0] for r in cur.fetchall()]
                print(f"\nUnique {col} values ({len(unique_dates)}): {unique_dates}")

conn.close()
