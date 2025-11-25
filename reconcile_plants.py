#!/usr/bin/env python3
"""
Reconcile plant names between database and reference
"""

from plant_store import PlantStore

# Reference capacities from user's spreadsheet
REFERENCE_CAPACITIES = {
    'Blachford': 333,
    'Cromwell Tools': 999,
    'Finlay Beverages': 1600,
    'Hibernian Stadium': 117,
    'Hibernian Training Ground': 90,
    'BAE Fylde': 9632,
    'BAE Samlesbury': 1723,
    'City Football Group Platt Lane': 2206,
    'Faltec Europe Ltd': 675,
    'Hazelwick School': 225,
    'Merry Hill': 996,
    'Metro Centre': 740,
    'Parfetts - Birmingham': 386,
    'Smithy\'s Mushrooms': 224,
    'Sofina Haverhill': 1015.48
}

def main():
    store = PlantStore('plant_registry.sqlite')
    db_plants = store.list_all()
    
    print("="*80)
    print("DATABASE PLANTS vs REFERENCE PLANTS")
    print("="*80)
    
    print("\nDatabase plants (15):")
    print("-"*80)
    for p in sorted(db_plants, key=lambda x: x['alias']):
        print(f"  {p['alias']}")
    
    print("\n\nReference plants (15):")
    print("-"*80)
    for name in sorted(REFERENCE_CAPACITIES.keys()):
        print(f"  {name}")
    
    print("\n\n" + "="*80)
    print("SUGGESTED MAPPINGS:")
    print("="*80)
    
    db_names = {p['alias']: p for p in db_plants}
    ref_names = set(REFERENCE_CAPACITIES.keys())
    
    # Direct matches
    print("\nDirect matches:")
    for ref in sorted(ref_names):
        for db in db_names.keys():
            if ref.lower() in db.lower() or db.lower() in ref.lower():
                print(f"  '{db}' -> '{ref}'")
                break

if __name__ == "__main__":
    main()
