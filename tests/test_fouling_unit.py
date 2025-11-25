import numpy as np
import pandas as pd

from Fouling_analysis import (
    calculate_fouling_index,
    calculate_pr,
    classify_fouling_level,
    estimate_clean_baseline_poa_matched,
    FoulingConfig,
)


def test_calculate_pr_basic():
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=3, freq="H"),
            "ac_power": [10.0, 5.0, 0.0],
            "poa": [1000.0, 500.0, 50.0],
        }
    )
    cfg = FoulingConfig(dc_size_kw=10.0)
    out = calculate_pr(df, cfg)
    # PR = AC / (POA_kW * DC_size)
    assert np.isclose(out.loc[0, "pr"], 10.0 / (1.0 * 10.0))
    assert np.isclose(out.loc[1, "pr"], 5.0 / (0.5 * 10.0))
    # Low irradiance should be NaN
    assert np.isnan(out.loc[2, "pr"])


def test_estimate_clean_baseline_poa_matched():
    cfg = FoulingConfig()
    clean = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=2, freq="H"),
            "ac_power": [10.0, 12.0],
            "poa": [950.0, 1050.0],
        }
    )
    clean = calculate_pr(clean, cfg)
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-02", periods=2, freq="H"),
            "ac_power": [9.0, 11.0],
            "poa": [960.0, 1040.0],
        }
    )
    df = calculate_pr(df, cfg)
    out = estimate_clean_baseline_poa_matched(df, cfg, clean_df=clean)
    assert "expected_clean_power" in out.columns
    assert out["expected_clean_power"].notna().any()


def test_fouling_index_and_classification():
    cfg = FoulingConfig()
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=4, freq="H"),
            "ac_power": [10, 10, 8, 8],
            "poa": [1000, 1000, 1000, 1000],
            "expected_clean_power": [10, 10, 10, 10],
        }
    )
    idx = calculate_fouling_index(df, cfg, expected_col="expected_clean_power", window_days=1)
    level = classify_fouling_level(idx)
    assert 0 <= idx <= 1
    assert level in {"Clean", "Light Soiling", "Moderate", "Severe"}
