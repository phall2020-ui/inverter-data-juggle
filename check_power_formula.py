"""Check power vs energy fields and verify PR formula."""
from plant_store import PlantStore

store = PlantStore()

# Get readings at peak time
readings = store.load_readings('AMP:00024', 'INVERT:001122', '2025-07-08T11:00:00', '2025-07-08T12:00:00')

print("Inverter readings at peak time:")
print("="*80)
for r in readings:
    ts = r['ts']
    active_power = r.get('importActivePower', {})
    energy = r.get('importEnergy', {})
    apparent_power = r.get('apparentPower', {})
    
    print(f"\n{ts}:")
    print(f"  importActivePower: {active_power.get('value')} {active_power.get('unit')}")
    print(f"  importEnergy: {energy.get('value')} {energy.get('unit')}")
    print(f"  apparentPower: {apparent_power.get('value')} {apparent_power.get('unit')}")

print("\n" + "="*80)
print("\nWhat build_fouling_dataset.py uses:")
print("  AC power = importActivePower (W) / 1000 → kW")
print("  Sums across all inverters by timestamp")
print("\nPR formula in Fouling_analysis.py:")
print("  PR = AC_kW / ((POA_Wm2 / 1000) × DC_kW)")
print("  PR = AC_kW / (POA_kWm2 × DC_kW)")
print("\nExpected PR formula:")
print("  PR = AC_power / (Irradiance × DC_capacity)")
print("  where Irradiance is in kW/m² (or fraction of 1000 W/m²)")
