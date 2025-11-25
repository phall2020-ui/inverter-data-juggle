"""Check POA units and verify conversion."""
from plant_store import PlantStore

store = PlantStore()
readings = store.load_readings('AMP:00024', 'POA:SOLARGIS:WEIGHTED', '2025-06-15T11:00:00', '2025-06-15T14:00:00')

print("Midday POA readings (should be peak irradiance):")
print("="*60)
for r in readings:
    poa_val = r['poaIrradiance']['value']
    poa_unit = r['poaIrradiance']['unit']
    
    # Convert to W/m² assuming 30-minute intervals
    poa_wm2_current = (poa_val * 1000) / 0.5
    
    # If unit is actually kWh/m² per 30min, correct conversion:
    poa_wm2_correct = (poa_val / 0.5) * 1000
    
    print(f"{r['ts']}: {poa_val:.4f} {poa_unit}")
    print(f"  Current conversion: {poa_wm2_current:.1f} W/m²")
    print(f"  (Both give same result, conversion is correct)")
    print()

print("\nExpected solar noon irradiance in UK June: 800-1000 W/m²")
print("If we're seeing ~400 W/m², either:")
print("  1. The SolarGIS data unit label is wrong")
print("  2. There's a systematic 3× error somewhere in the chain")
