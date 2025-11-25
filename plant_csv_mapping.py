#!/usr/bin/env python3
"""
Manual plant-to-CSV mapping for accurate POA import
"""

# Manual mapping of plant aliases to CSV file base names and expected capacities
PLANT_FILE_MAPPING = {
    # Database plant alias -> (CSV base name, expected capacity kW)
    'Blachford UK': ('Blachford', 333),
    'Cromwell Tools': ('Cromwell_Tools', 999),
    'Finlay Beverages': ('Faltec_Europe_Ltd', 675),  # Finlay = Faltec based on available files
    'FloPlast': (None, None),  # No CSV file
    'Hibernian Stadium': ('Hibernian_Stadium', 117),
    'Hibernian Training Ground': ('Hibernian_Training_Ground', 90),
    'Man City FC Training Ground': ('City_Football_Group_Phase_1', 2206),
    'Merry Hill Shopping Centre': ('Merry_Hill', 996),
    'Metrocentre': ('Metro_Centre', 740),
    'Newfold Farm': (None, None),  # No CSV file
    'Parfetts Birmingham': ('Parfetts_-_Birmingham', 386),
    'Sheldons Bakery': ('Sheldons_Bakery', 225),  # Hazelwick School reference
    'Smithy\'s Mushrooms': ('Smithys_Mushrooms', 224),
    'Smithy\'s Mushrooms PH2': (None, None),  # No CSV file
    'Sofina Foods': ('Sofina_Haverhill', 1015.48),
}

def get_csv_pattern_for_plant(plant_alias):
    """Get the exact CSV filename pattern for a plant"""
    if plant_alias in PLANT_FILE_MAPPING:
        csv_base, expected_capacity = PLANT_FILE_MAPPING[plant_alias]
        return csv_base, expected_capacity
    return None, None

if __name__ == "__main__":
    print("Plant to CSV File Mapping:")
    print("=" * 70)
    for plant, (csv, capacity) in sorted(PLANT_FILE_MAPPING.items()):
        if csv:
            print(f"{plant:40} -> {csv:30} ({capacity} kW)")
        else:
            print(f"{plant:40} -> (No CSV file)")
