import pandas as pd

from Fouling_analysis import FoulingConfig, run_fouling_analysis


def test_run_fouling_analysis_small_dataset():
    # Full dataset with slight degradation vs clean
    full_df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=4, freq="H"),
            "ac_power": [9.5, 9.0, 8.5, 8.0],
            "poa": [1000, 950, 900, 850],
        }
    )
    clean_df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-02", periods=4, freq="H"),
            "ac_power": [10.0, 10.0, 9.8, 9.6],
            "poa": [1000, 950, 900, 850],
        }
    )
    cfg = FoulingConfig(dc_size_kw=10.0)
    result = run_fouling_analysis(full_df, clean_df=clean_df, cfg=cfg)
    assert "fouling_index" in result
    assert "fouling_level" in result
    assert result["fouling_index"] >= 0
    assert result["fouling_level"] in {"Clean", "Light Soiling", "Moderate", "Severe"}
