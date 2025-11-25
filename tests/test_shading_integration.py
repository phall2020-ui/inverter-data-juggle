import pandas as pd

from Shading_analysis import (
    Settings,
    build_profile,
    compare_profiles,
    join_with_irradiance,
)


def test_shading_pipeline_small_sample(tmp_path):
    cfg = Settings(
        weather_id="WETH:TEST",
        irradiance_col="poaIrradiance",
        current_col="apparentPower",
    )

    # Build small summer/winter CSVs with overlapping hours
    summer = pd.DataFrame(
        {
            "timestamp": ["2025-06-01T10:00:00", "2025-06-01T11:00:00"],
            "emigId": ["INV:1", "INV:1"],
            "poaIrradiance": [800, 900],
            "apparentPower": [400, 500],
        }
    )
    summer_weather = pd.DataFrame(
        {
            "timestamp": ["2025-06-01T10:00:00", "2025-06-01T11:00:00"],
            "emigId": [cfg.weather_id, cfg.weather_id],
            "poaIrradiance": [800, 900],
        }
    )
    summer_csv = tmp_path / "summer.csv"
    pd.concat([summer, summer_weather]).to_csv(summer_csv, index=False)

    winter = pd.DataFrame(
        {
            "timestamp": ["2025-12-01T10:00:00", "2025-12-01T11:00:00"],
            "emigId": ["INV:1", "INV:1"],
            "poaIrradiance": [700, 800],
            "apparentPower": [280, 360],  # lower normalized power to indicate shading
        }
    )
    winter_weather = pd.DataFrame(
        {
            "timestamp": ["2025-12-01T10:00:00", "2025-12-01T11:00:00"],
            "emigId": [cfg.weather_id, cfg.weather_id],
            "poaIrradiance": [700, 800],
        }
    )
    winter_csv = tmp_path / "winter.csv"
    pd.concat([winter, winter_weather]).to_csv(winter_csv, index=False)

    # Load and process
    from Shading_analysis import load_and_prepare  # import inside to align with path expectation

    inv_s, w_s = load_and_prepare(str(summer_csv), cfg)
    inv_w, w_w = load_and_prepare(str(winter_csv), cfg)
    ms = join_with_irradiance(inv_s, w_s, cfg)
    mw = join_with_irradiance(inv_w, w_w, cfg)
    prof_s = build_profile(ms, cfg)
    prof_w = build_profile(mw, cfg)
    comp = compare_profiles(prof_s, prof_w, cfg)

    # Expect ratios < 1 indicating shading
    assert not comp.empty
    assert (comp["ratio_winter_to_summer"] < 1).all()
