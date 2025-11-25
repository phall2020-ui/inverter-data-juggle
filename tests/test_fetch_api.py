from unittest import mock

import responses

from fetch_inverter_data import BASE_URL, Config, fetch_readings_for_period


@responses.activate
def test_fetch_readings_for_period_builds_url_and_params():
    cfg = Config(api_key="testkey", plant_uid="ERS:00001", start_date="20250101", end_date="20250102")
    emig_id = "INVERT:001"
    expected_url = f"{BASE_URL}/meter/{emig_id}/readings"
    responses.add(
        responses.GET,
        expected_url,
        json={"readings": [{"ts": "2025-01-01T00:00:00", "value": 1}]},
        status=200,
    )

    out = fetch_readings_for_period(cfg, emig_id, cfg.start_date, cfg.end_date)
    assert len(out) == 1
    assert responses.calls[0].request.url.startswith(expected_url)
    params = responses.calls[0].request.url.split("?")[1]
    assert "startDate=20250101" in params
    assert "endDate=20250102" in params


@responses.activate
def test_fetch_readings_handles_http_error():
    cfg = Config(api_key="testkey", plant_uid="ERS:00001", start_date="20250101", end_date="20250102")
    emig_id = "INVERT:001"
    expected_url = f"{BASE_URL}/meter/{emig_id}/readings"
    responses.add(responses.GET, expected_url, status=500)
    try:
        fetch_readings_for_period(cfg, emig_id, cfg.start_date, cfg.end_date)
    except Exception as exc:  # noqa: BLE001
        assert "500" in str(exc)
    else:
        assert False, "Expected exception on HTTP 500"
