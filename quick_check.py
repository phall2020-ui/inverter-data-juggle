from plant_store import PlantStore

s = PlantStore('plant_registry.sqlite')
merry = s.load('Merry Hill Shopping Centre')
smithy = s.load("Smithy's Mushrooms")

print(f"Merry Hill: {merry.get('dc_size_kw', 0)} kW (expected: 996 kW)")
print(f"Smithy's Mushrooms: {smithy.get('dc_size_kw', 0)} kW (expected: 224 kW)")
