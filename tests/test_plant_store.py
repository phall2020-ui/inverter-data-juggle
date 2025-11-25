import os
import tempfile

from plant_store import PlantStore


def test_save_and_load_with_dc_size():
    fd, path = tempfile.mkstemp()
    os.close(fd)
    store = PlantStore(path)
    store.save("alias1", "ERS:00001", ["INV:1"], "WETH:1", 123.0)
    loaded = store.load("alias1")
    assert loaded["plant_uid"] == "ERS:00001"
    assert loaded["inverter_ids"] == ["INV:1"]
    assert loaded["weather_id"] == "WETH:1"
    assert loaded["dc_size_kw"] == 123.0


def test_store_and_query_readings():
    fd, path = tempfile.mkstemp()
    os.close(fd)
    store = PlantStore(path)
    store.save("alias1", "ERS:00001", ["INV:1"], "WETH:1", None)
    readings = [
        {"ts": "2025-01-01T00:00:00", "energy": 1},
        {"ts": "2025-01-01T00:30:00", "energy": 2},
    ]
    store.store_readings("ERS:00001", "INV:1", readings)
    fetched = store.load_readings("ERS:00001", "INV:1", "2025-01-01T00:00:00", "2025-01-01T23:59:59")
    assert len(fetched) == 2
    spans = store.emig_date_spans("ERS:00001")
    assert spans and spans[0]["emig_id"] == "INV:1"
