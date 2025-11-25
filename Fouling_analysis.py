"""
fouling_detection.py

Module for detecting soiling (fouling) in PV systems using operational data.

Key features:
 - Requires an explicit "clean period" dataset (modules known to be clean) to
   build the performance baseline.
 - Handles different data formats via automatic column guessing from headers,
   plus optional explicit mapping.
 - Uses POA-matched normalisation (compare like-for-like irradiance conditions).
 - Optional irradiance → power regression model for expected clean performance.
 - Computes fouling index, fouling classification, and energy loss.
 - Detects likely cleaning events via PR jumps.

Requirements:
    Python 3.11+
    pandas, numpy
Optional:
    scikit-learn (for baseline regression)
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import Optional, Dict

try:
    from sklearn.linear_model import LinearRegression
except ImportError:  # degrade gracefully if sklearn isn't available
    LinearRegression = None


# ===============================================================
# --- CONFIGURABLE CONSTANTS ---
# ===============================================================

# Ignore very low irradiance where measurements are noisy
POA_MIN = 200               # W/m²

# Width of POA bins for POA-matched baseline
POA_BIN_WIDTH = 100         # W/m² bin size

# Fouling classification thresholds (fractional loss)
FOULING_CLEAN = 0.05        # 0–5% loss → "Clean"
FOULING_LIGHT = 0.10        # 5–10% loss → "Light Soiling"
FOULING_MODERATE = 0.20     # 10–20% loss → "Moderate"
# >0.20 → "Severe"


# ===============================================================
# --- CONFIG FOR COLUMN NAMES / OPTIONS ---
# ===============================================================

@dataclass
class FoulingConfig:
    """
    Configuration for a site / dataset.

    The module works on *internal* standard column names:
        - timestamp
        - ac_power
        - dc_power (optional)
        - poa (plane-of-array irradiance)
        - module_temp (optional)

    Incoming data can have arbitrary column names. We:
      1) Apply cfg.column_map (explicit mapping), then
      2) Auto-guess likely columns from headers.
    """
    # Standard internal names
    timestamp: str = "timestamp"
    ac_power: str = "ac_power"
    dc_power: Optional[str] = None
    poa: str = "poa"
    module_temp: Optional[str] = None

    # Nameplate / site config
    dc_size_kw: float = 1.0  # DC capacity used for PR

    # Optional external expected power column (not required)
    expected_power: Optional[str] = None

    # Explicit column mapping: incoming_name -> standard_name
    # e.g. {"AC_kW": "ac_power", "POA_Wm2": "poa"}
    column_map: Optional[Dict[str, str]] = None


# ===============================================================
# --- COLUMN STANDARDISATION & AUTO-GUESSING ---
# ===============================================================

def _guess_column(cols: list[str], candidates: list[str]) -> Optional[str]:
    """
    Pick the 'most likely' column from cols given a list of candidate keywords.

    Simple heuristic: score columns by how many keywords appear in the column
    name (case-insensitive). Returns the original column name if found, else None.
    """
    if not cols:
        return None

    cols_lc = {c.lower(): c for c in cols}
    best_col = None
    best_score = 0

    for lc_name, original in cols_lc.items():
        score = 0
        for kw in candidates:
            if kw in lc_name:
                score += 1
        if score > best_score:
            best_score = score
            best_col = original

    # Require at least one keyword match
    if best_score > 0:
        return best_col
    return None


def standardise_columns(df: pd.DataFrame, cfg: FoulingConfig) -> pd.DataFrame:
    """
    Map arbitrary input column names to internal standard names using:
      1) explicit cfg.column_map (if provided), then
      2) heuristic guessing from header text.

    The function does NOT fail if some columns are missing; downstream
    computations will simply skip features that are not available.
    """
    df = df.copy()

    # 1) Apply explicit mapping
    if cfg.column_map:
        rename_map = {k: v for k, v in cfg.column_map.items() if k in df.columns}
        df = df.rename(columns=rename_map)

    # 2) Auto-guess missing core fields
    cols = list(df.columns)

    def has_std(name: str) -> bool:
        return name in df.columns

    # --- timestamp ---
    if not has_std(cfg.timestamp):
        cand = _guess_column(
            cols,
            candidates=["timestamp", "time", "date", "datetime"]
        )
        if cand is not None and cand != cfg.timestamp:
            df = df.rename(columns={cand: cfg.timestamp})

    # --- ac_power ---
    if not has_std(cfg.ac_power):
        cand = _guess_column(
            cols,
            candidates=[
                "ac power", "ac_power", "ac kw", "ac_kw",
                "active power", "p_ac", "pac",
                "power_kw", "kw", "power"
            ],
        )
        if cand is not None and cand != cfg.ac_power:
            df = df.rename(columns={cand: cfg.ac_power})

    # --- poa (plane-of-array irradiance) ---
    if not has_std(cfg.poa):
        cand = _guess_column(
            cols,
            candidates=["poa", "plane", "tilt", "irr", "irradiance", "w/m2", "wm2"]
        )
        if cand is not None and cand != cfg.poa:
            df = df.rename(columns={cand: cfg.poa})

    # --- dc_power (optional) ---
    if cfg.dc_power and not has_std(cfg.dc_power):
        cand = _guess_column(
            cols,
            candidates=["dc ", "dc_", "dc power", "dc kw", "p_dc", "pdc"]
        )
        if cand is not None and cand != cfg.dc_power:
            df = df.rename(columns={cand: cfg.dc_power})

    # --- module_temp (optional) ---
    if cfg.module_temp and not has_std(cfg.module_temp):
        cand = _guess_column(
            cols,
            candidates=["module temp", "mod temp", "cell temp", "pv temp", "temperature"]
        )
        if cand is not None and cand != cfg.module_temp:
            df = df.rename(columns={cand: cfg.module_temp})

    return df


# ===============================================================
# --- PR CALCULATION ---
# ===============================================================

def calculate_pr(df: pd.DataFrame, cfg: FoulingConfig) -> pd.DataFrame:
    """
    Compute Performance Ratio and append column 'pr'.

    PR = AC / (Irradiance × DC_size), where:
      - AC is cfg.ac_power (kW)
      - Irradiance is cfg.poa (W/m²)
      - DC_size is cfg.dc_size_kw (kW DC)

    Low-light periods (POA < POA_MIN) are set to NaN.
    """
    df = df.copy()

    if cfg.ac_power not in df.columns or cfg.poa not in df.columns:
        df["pr"] = np.nan
        return df

    irr_factor = df[cfg.poa] / 1000.0  # W/m² → kW/m²
    with np.errstate(divide="ignore", invalid="ignore"):
        df["pr"] = df[cfg.ac_power] / (irr_factor * cfg.dc_size_kw)

    df.loc[df[cfg.poa] < POA_MIN, "pr"] = np.nan
    return df


# ===============================================================
# --- CLEAN PERIOD DETECTION (OPTIONAL, NOT USED IN PIPELINE) ---
# ===============================================================

def identify_clean_reference_periods(df: pd.DataFrame,
                                     pr_col: str = "pr",
                                     window: int = 3) -> pd.DataFrame:
    """
    Optional helper to identify clean periods in a single dataset.

    NOT used by run_fouling_analysis (which requires an explicit clean_df),
    but kept for manual use or future extensions.

    Simple logic:
      - rolling median on PR
      - mark data within ±5% of global median as 'is_clean'.
    """
    df = df.copy()
    if pr_col not in df.columns:
        df["is_clean"] = False
        return df

    df["pr_roll"] = df[pr_col].rolling(window, min_periods=1).median()
    pr_median = df["pr_roll"].median()

    df["is_clean"] = (
        (df["pr_roll"] > 0.95 * pr_median) &
        (df["pr_roll"] < 1.05 * pr_median) &
        df["pr_roll"].notna()
    )

    return df


# ===============================================================
# --- DATE FILTERING & AUTO CLEAN PERIOD SELECTION ---------------
# ===============================================================

def filter_by_date_range(df: pd.DataFrame,
                         cfg: FoulingConfig,
                         start: Optional[pd.Timestamp] = None,
                         end: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    """
    Restrict dataframe to a timestamp window.
    """
    if cfg.timestamp not in df.columns:
        return df

    out = df.copy()
    out[cfg.timestamp] = pd.to_datetime(out[cfg.timestamp], errors="coerce")
    if start is not None:
        out = out[out[cfg.timestamp] >= start]
    if end is not None:
        out = out[out[cfg.timestamp] <= end]
    return out


def auto_select_clean_period(df: pd.DataFrame,
                             cfg: FoulingConfig,
                             days: int = 3,
                             min_points_per_day: int = 48) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Heuristic clean-period finder based on highest daily PR medians.

    Returns (clean_df, daily_stats) where:
      - clean_df: rows belonging to the best 'days' days
      - daily_stats: per-day median/count used for selection
    """
    work = df.copy()
    work = standardise_columns(work, cfg)

    if cfg.timestamp not in work.columns:
        return pd.DataFrame(), pd.DataFrame()

    work[cfg.timestamp] = pd.to_datetime(work[cfg.timestamp], errors="coerce")
    work = calculate_pr(work, cfg)

    work = work[(work[cfg.poa] >= POA_MIN) & work["pr"].notna()].copy()
    if work.empty:
        return pd.DataFrame(), pd.DataFrame()

    work["date_only"] = work[cfg.timestamp].dt.date
    daily = (
        work.groupby("date_only")["pr"]
        .agg(["median", "count"])
        .reset_index()
    )
    daily = daily[daily["count"] >= min_points_per_day]
    if daily.empty:
        return pd.DataFrame(), pd.DataFrame()

    top = daily.sort_values("median", ascending=False).head(days)
    selected_dates = set(top["date_only"])
    clean_df = work[work["date_only"].isin(selected_dates)].copy()
    clean_df = clean_df.sort_values(cfg.timestamp)

    return clean_df, top


# ===============================================================
# --- POA-MATCHED CLEAN BASELINE ---
# ===============================================================

def estimate_clean_baseline_poa_matched(
    df: pd.DataFrame,
    cfg: FoulingConfig,
    clean_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Build a POA-binned expected clean baseline.

    clean_df:
        A dataset from a period when the modules are known to be clean.
        All rows of clean_df are treated as "clean" by this function.

    For each POA bin:
        expected_clean_power = median(clean_ac_power)
        expected_clean_pr    = median(clean_pr) (if PR is available in clean_df)
    """
    df = df.copy()
    clean_src = clean_df.copy()

    # Ensure required columns exist
    if cfg.poa not in clean_src.columns or cfg.ac_power not in clean_src.columns:
        df["expected_clean_power"] = np.nan
        df["expected_clean_pr"] = np.nan
        return df

    clean_src = clean_src[clean_src[cfg.poa] >= POA_MIN]
    if clean_src.empty:
        df["expected_clean_power"] = np.nan
        df["expected_clean_pr"] = np.nan
        return df

    # POA bins
    df["poa_bin"] = (df[cfg.poa] // POA_BIN_WIDTH) * POA_BIN_WIDTH
    clean_src["poa_bin"] = (clean_src[cfg.poa] // POA_BIN_WIDTH) * POA_BIN_WIDTH

    # Expected AC by bin
    expected_power_by_bin = (
        clean_src.groupby("poa_bin")[cfg.ac_power]
        .median()
        .rename("expected_clean_power")
    )
    df = df.merge(expected_power_by_bin, how="left",
                  left_on="poa_bin", right_index=True)

    # Expected PR by bin
    if "pr" in clean_src.columns:
        expected_pr_by_bin = (
            clean_src.groupby("poa_bin")["pr"]
            .median()
            .rename("expected_clean_pr")
        )
        df = df.merge(expected_pr_by_bin, how="left",
                      left_on="poa_bin", right_index=True)
    else:
        df["expected_clean_pr"] = np.nan

    return df


# ===============================================================
# --- CLEAN BASELINE REGRESSION MODEL (OPTIONAL) ---
# ===============================================================

def fit_clean_regression_model(
    clean_df: pd.DataFrame,
    cfg: FoulingConfig
) -> Optional[LinearRegression]:
    """
    Train a simple linear irradiance → AC power model on the explicit clean period.

    Returns None if:
      - scikit-learn is not installed, or
      - required columns are missing, or
      - there is no valid clean data.
    """
    if LinearRegression is None:
        return None

    clean_src = clean_df.copy()
    if cfg.poa not in clean_src.columns or cfg.ac_power not in clean_src.columns:
        return None

    clean_src = clean_src[clean_src[cfg.poa] >= POA_MIN]
    if clean_src.empty:
        return None

    X = clean_src[[cfg.poa]].values
    y = clean_src[cfg.ac_power].values

    model = LinearRegression()
    model.fit(X, y)
    return model


def apply_clean_model(df: pd.DataFrame,
                      model: Optional[LinearRegression],
                      cfg: FoulingConfig) -> pd.DataFrame:
    """
    Apply regression model to estimate expected clean power per row.

    Adds column:
        - expected_clean_model
    """
    df = df.copy()
    if model is None or cfg.poa not in df.columns:
        df["expected_clean_model"] = np.nan
        return df

    X = df[[cfg.poa]].values
    df["expected_clean_model"] = model.predict(X)
    return df


# ===============================================================
# --- FOULING INDEX ---
# ===============================================================

def calculate_fouling_index(df: pd.DataFrame,
                            cfg: FoulingConfig,
                            expected_col: str = "expected_clean_power",
                            window_days: int = 7) -> float:
    """
    Compute a fouling index from the ratio actual/expected over a recent window.

    Fouling index = 1 − median(actual / expected) over the window,
    clamped to [0, 1], where:
      - 0   = perfectly clean (actual ≈ expected)
      - 0.2 = ~20% loss
      - 1   = total loss (actual ≈ 0)

    Parameters
    ----------
    df : pd.DataFrame
    cfg : FoulingConfig
    expected_col : str
        Column with expected clean AC power (e.g. 'expected_clean_power')
    window_days : int
        Look-back window size in days.
    """
    df = df.copy()

    if expected_col not in df.columns or cfg.ac_power not in df.columns:
        return np.nan

    # Use last N days based on timestamp if available
    if cfg.timestamp in df.columns:
        recent = df.set_index(cfg.timestamp).sort_index().last(f"{window_days}D")
    else:
        # Fallback: last N*48 rows (~7 days at 30-min)
        recent = df.tail(window_days * 48)

    valid = recent[(recent[expected_col] > 0) & (recent[cfg.ac_power] >= 0)]
    if valid.empty:
        return np.nan

    ratio = valid[cfg.ac_power] / valid[expected_col]
    median_ratio = np.nanmedian(ratio)

    fouling_index = 1.0 - median_ratio
    return float(max(0.0, min(1.0, fouling_index)))


# ===============================================================
# --- FOULING CLASSIFICATION ---
# ===============================================================

def classify_fouling_level(fouling_index: float) -> str:
    """
    Map fouling index (fractional loss) to a qualitative level.
    """
    if np.isnan(fouling_index):
        return "Insufficient Data"

    if fouling_index <= FOULING_CLEAN:
        return "Clean"
    if fouling_index <= FOULING_LIGHT:
        return "Light Soiling"
    if fouling_index <= FOULING_MODERATE:
        return "Moderate"
    return "Severe"


# ===============================================================
# --- ENERGY LOSS ESTIMATE ---
# ===============================================================

def estimate_energy_loss(df: pd.DataFrame,
                         cfg: FoulingConfig,
                         expected_col: str = "expected_clean_power",
                         period_days: int = 7) -> float:
    """
    Estimate daily energy loss (kWh/day) due to soiling over a recent period.

    Energy loss is computed as:
        sum(expected - actual, clipped at >= 0) / period_days

    Assumes:
        - ac_power is in kW,
        - each row represents an interval already converted to energy,
          OR that ac_power is time-averaged and the sampling interval is constant.
    """
    if expected_col not in df.columns or cfg.ac_power not in df.columns:
        return np.nan

    if cfg.timestamp in df.columns:
        recent = df.set_index(cfg.timestamp).sort_index().last(f"{period_days}D").copy()
    else:
        recent = df.tail(period_days * 48).copy()

    recent["loss"] = (recent[expected_col] - recent[cfg.ac_power]).clip(lower=0)
    energy_loss = recent["loss"].sum()

    if period_days <= 0:
        return float(energy_loss)

    return float(energy_loss / period_days)


# ===============================================================
# --- CLEANING EVENT DETECTION ---
# ===============================================================

def detect_cleaning_events(df: pd.DataFrame,
                           threshold: float = 0.10) -> pd.DataFrame:
    """
    Detect likely cleaning events as sudden jumps in rolling PR.

    threshold:
        Fractional increase vs global median PR, e.g. 0.10 = +10%.
    """
    df = df.copy()
    if "pr" not in df.columns:
        df["cleaning_event"] = False
        return df

    df["pr_roll"] = df["pr"].rolling(3, min_periods=1).median()
    pr_med = df["pr_roll"].median()
    df["pr_change"] = df["pr_roll"].diff()

    df["cleaning_event"] = df["pr_change"] > threshold * pr_med
    return df


# ===============================================================
# --- HIGH-LEVEL PIPELINE (CLEAN DATASET REQUIRED) ---
# ===============================================================

def run_fouling_analysis(
    df: pd.DataFrame,
    clean_df: pd.DataFrame,
    cfg: Optional[FoulingConfig] = None,
) -> dict:
    """
    High-level fouling analysis pipeline.

    Parameters
    ----------
    df : pd.DataFrame
        Full operational dataset (any period).
    clean_df : pd.DataFrame
        Dataset from a period when modules are known to be CLEAN
        (e.g. 1–3 days after a full wash, no obvious faults).
        This dataset is used EXCLUSIVELY to build the baseline:
          - POA-matched expected clean power & PR
          - Optional irradiance→power regression model

        This parameter is mandatory; if missing or empty, a ValueError is raised.
    cfg : FoulingConfig, optional
        Configuration for column names, DC size, etc.

    Returns
    -------
    dict with keys:
        - fouling_index
        - fouling_level
        - energy_loss_kwh_per_day
        - cleaning_events_detected
        - df (enriched dataframe with expected values and cleaning event flags)
    """
    if cfg is None:
        cfg = FoulingConfig()

    if clean_df is None or len(clean_df) == 0:
        raise ValueError(
            "A clean-period dataset (clean_df) is required.\n"
            "Provide data from a known clean period (modules recently washed, "
            "no heavy soiling or major faults) so the baseline can be established."
        )

    # 1 — Standardise column names on both datasets
    df = standardise_columns(df, cfg)
    clean_df = standardise_columns(clean_df, cfg)

    # 2 — Ensure timestamps are parsed
    if cfg.timestamp in df.columns:
        df[cfg.timestamp] = pd.to_datetime(df[cfg.timestamp], errors="coerce")
    if cfg.timestamp in clean_df.columns:
        clean_df[cfg.timestamp] = pd.to_datetime(clean_df[cfg.timestamp], errors="coerce")

    # 3 — Compute PR for both datasets
    df = calculate_pr(df, cfg)
    clean_df = calculate_pr(clean_df, cfg)

    # 4 — Treat all rows in clean_df as 'is_clean'
    clean_df = clean_df.copy()
    clean_df["is_clean"] = True

    # 5 — Build POA-matched expected clean baseline from clean_df
    df = estimate_clean_baseline_poa_matched(df, cfg, clean_df=clean_df)

    # 6 — Fit and apply optional clean regression model
    model = fit_clean_regression_model(clean_df, cfg)
    df = apply_clean_model(df, model, cfg)

    # 7 — Fouling index (actual vs POA-matched expected)
    fouling_index = calculate_fouling_index(df, cfg)

    # 8 — Classification
    fouling_level = classify_fouling_level(fouling_index)

    # 9 — Energy-loss estimate
    energy_loss = estimate_energy_loss(df, cfg)

    # 10 — Cleaning events (on full df)
    df = detect_cleaning_events(df)

    return {
        "fouling_index": fouling_index,
        "fouling_level": fouling_level,
        "energy_loss_kwh_per_day": energy_loss,
        "cleaning_events_detected": int(df["cleaning_event"].sum()),
        "df": df,
    }


# ===============================================================
# --- CLI EXAMPLE ---
# ===============================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Fouling analysis: requires a full dataset AND a dataset from a known clean period "
            "(modules recently cleaned, no heavy soiling)."
        )
    )
    parser.add_argument(
        "full_data_csv",
        help="CSV file with full operational data (any period).",
    )
    parser.add_argument(
        "clean_data_csv",
        help="CSV file from a known clean period (modules clean).",
    )
    parser.add_argument(
        "--dc-size-kw",
        type=float,
        default=1000.0,
        help="DC nameplate capacity in kW (default: 1000).",
    )
    args = parser.parse_args()

    full_df = pd.read_csv(args.full_data_csv)
    clean_df = pd.read_csv(args.clean_data_csv)

    cfg = FoulingConfig(
        dc_size_kw=args.dc_size_kw,
        # column_map can be left None and the auto-guessing logic
        # will try to infer timestamp/ac/poa/etc from the headers.
        column_map=None,
    )

    results = run_fouling_analysis(full_df, clean_df=clean_df, cfg=cfg)
    print(results)
