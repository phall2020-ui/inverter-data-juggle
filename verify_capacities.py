#!/usr/bin/env python3
"""
Verify database capacities match reference data
"""

from plant_store import PlantStore

# Reference capacities from user's spreadsheet
REFERENCE_CAPACITIES = {
    'BAE Fylde': 9632,
    'Blachford': 333,
    'City Football Group Platt Lane': 2206,
    'Cromwell Tools': 999,
    'Faltec Europe Ltd': 675,
    'Finlay Beverages': 1600,
    'Hazelwick School': 225,
    'Hibernian Stadium': 117,
    'Hibernian Training Ground': 90,
    'Merry Hill': 996,
    'Metro Centre': 740,
    'Parfetts - Birmingham': 386,
    'Smithy\'s Mushrooms': 224,
    'BAE Samlesbury': 1723,
    'Sofina Haverhill': 1015.48
}

def normalize_name(name):
    """Normalize plant names for matching"""
    return name.lower().replace('uk', '').replace('plc', '').replace('ltd', '').strip()

def main():
    store = PlantStore('plant_registry.sqlite')
    plants = store.list_all()
    
    print("=" * 80)
    print("CAPACITY VERIFICATION")
    print("=" * 80)
    
    # Build lookup by normalized name
    db_plants = {}
    for p in plants:
        norm_name = normalize_name(p['alias'])
        db_plants[norm_name] = p
    
    print(f"\nDatabase: {len(plants)} plants")
    print(f"Reference: {len(REFERENCE_CAPACITIES)} plants\n")
    
    matches = 0
    mismatches = 0
    missing = 0
    
    for ref_name, ref_capacity in sorted(REFERENCE_CAPACITIES.items()):
        norm_ref = normalize_name(ref_name)
        
        # Try to find matching plant
        matched_plant = None
        for db_norm, db_plant in db_plants.items():
            if norm_ref in db_norm or db_norm in norm_ref:
                matched_plant = db_plant
                break
        
        if matched_plant:
            db_capacity = matched_plant.get('dc_size_kw', 0)
            diff = abs(db_capacity - ref_capacity)
            
            if diff < 1.0:
                print(f"✓ {ref_name:35} {ref_capacity:8.1f} kW (match)")
                matches += 1
            else:
                print(f"✗ {ref_name:35} Expected: {ref_capacity:8.1f} kW, Got: {db_capacity:8.1f} kW (diff: {diff:.1f})")
                mismatches += 1
        else:
            print(f"? {ref_name:35} {ref_capacity:8.1f} kW (not found in database)")
            missing += 1
    
    print("\n" + "=" * 80)
    print(f"Results: {matches} matches, {mismatches} mismatches, {missing} missing")
    print("=" * 80)
    
    if mismatches > 0:
        print("\n⚠️  CAPACITIES NEED CORRECTION - Re-import POA data for mismatched plants")
    elif missing > 0:
        print("\n⚠️  Some plants not found - Check plant names in registry")
    else:
        print("\n✅ ALL CAPACITIES CORRECT!")

if __name__ == "__main__":
    main()
