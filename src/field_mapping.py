"""
Field Mapping: Timeform CSV ↔ CustomMetricsEngine schema translation.

The CustomMetricsEngine was built for the HRB database schema, while the
speed-figures pipeline uses Timeform CSV column names.  This module provides
bidirectional column-name translation so the engine can operate on Timeform
data without modification.

Mapping reference: src/live_ratings.py:520-537
"""

import logging

import pandas as pd

log = logging.getLogger(__name__)

# ── Timeform → CustomMetrics column mapping ────────────────────────────
# Keys = Timeform CSV column name, Values = custom_metrics expected name.
# Only columns that differ are listed; same-name columns (stallion, dam,
# dam_stallion, headgear) pass through unchanged.
TIMEFORM_TO_CUSTOM: dict[str, str] = {
    "meetingDate": "race_date",
    "courseName": "track",
    "raceNumber": "race_time",
    "horseName": "horse_name",
    "horseCode": "horse_code",
    "positionOfficial": "placing_numerical",
    "jockeyFullName": "jockey_name",
    "trainerFullName": "trainer",
    "betfairWinSP": "bfsp",
    "numberOfRunners": "number_of_runners",
    "prizeFund": "prize_money",
    "going": "going_description",
    "distanceFurlongs": "dist_furlongs",
    "raceClass": "race_class",
    "horseAge": "horse_age",
    "weightCarried": "pounds",
    "draw": "stall",
    "raceSurfaceName": "surface_type",
    "raceType": "race_type",
    "distanceCumulative": "total_dst_bt",
    "finishingTime": "comptime_numeric",
    "ispDecimal": "odds",
    "performanceRating": "official_rating",
}

# Reverse mapping for converting results back
CUSTOM_TO_TIMEFORM: dict[str, str] = {v: k for k, v in TIMEFORM_TO_CUSTOM.items()}


def timeform_to_custom_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Rename Timeform CSV columns to CustomMetricsEngine expected names.

    Columns that already match (stallion, dam, dam_stallion, headgear) are
    left unchanged.  Missing source columns are logged and skipped.

    Args:
        df: DataFrame with Timeform CSV column names.

    Returns:
        Copy of df with columns renamed for CustomMetricsEngine.
    """
    out = df.copy()

    rename_map: dict[str, str] = {}
    for tf_col, cm_col in TIMEFORM_TO_CUSTOM.items():
        if tf_col in out.columns:
            rename_map[tf_col] = cm_col
        else:
            log.debug("Timeform column %r not found — skipping mapping to %r", tf_col, cm_col)

    out.rename(columns=rename_map, inplace=True)

    # Log which custom_metrics columns are available vs missing
    expected = set(TIMEFORM_TO_CUSTOM.values()) | {"stallion", "dam", "dam_stallion", "headgear", "comment"}
    available = expected & set(out.columns)
    missing = expected - available
    log.info(
        "Field mapping: %d/%d columns available, %d missing: %s",
        len(available), len(expected), len(missing),
        sorted(missing) if missing else "none",
    )

    return out


def get_new_feature_columns(
    df_before: pd.DataFrame, df_after: pd.DataFrame
) -> list[str]:
    """Return column names that were added by CustomMetricsEngine.

    Args:
        df_before: DataFrame columns before engine ran.
        df_after: DataFrame columns after engine ran.

    Returns:
        Sorted list of new column names.
    """
    before_set = set(df_before.columns)
    after_set = set(df_after.columns)
    new_cols = sorted(after_set - before_set)
    log.info("CustomMetricsEngine produced %d new feature columns", len(new_cols))
    return new_cols
