"""
test_transformations.py
=======================
Unit tests for every transformation function defined in
``curated_layer/spark/transformations.py``.

The ``spark`` and ``mock_df`` fixtures are provided by ``conftest.py`` and
are reused (``scope="session"``) across all test classes for efficiency.

Each test class isolates one transformation so that failures are easy to
diagnose.  The ``TestApplyAll`` class validates the full pipeline end-to-end.
"""

import sys
import os

# ---------------------------------------------------------------------------
# Ensure the spark/ package is importable when running pytest from any CWD.
# ---------------------------------------------------------------------------
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "spark"),
)

import pytest
from pyspark.sql import functions as F

from transformations import (
    apply_all,
    bucket_amount,
    calculate_age,
    calculate_distance,
    encode_gender,
    extract_time_features,
    flag_high_risk,
    handle_nulls,
    normalize_amount,
    remove_duplicates,
    transaction_velocity,
)


# ===========================================================================
# 1. TestRemoveDuplicates
# ===========================================================================

class TestRemoveDuplicates:
    """Tests for :func:`remove_duplicates`."""

    def test_duplicate_count_reduced(self, mock_df):
        """The output should have fewer rows than the input because of the
        duplicate ``trans_num`` row."""
        original_count = mock_df.count()
        result = remove_duplicates(mock_df)
        assert result.count() < original_count, (
            "Expected row count to decrease after removing duplicates."
        )

    def test_all_trans_num_unique(self, mock_df):
        """Every ``trans_num`` in the result must be unique."""
        result = remove_duplicates(mock_df)
        total = result.count()
        distinct = result.select("trans_num").distinct().count()
        assert total == distinct, (
            f"Found {total - distinct} duplicate trans_num value(s) after dedup."
        )


# ===========================================================================
# 2. TestHandleNulls
# ===========================================================================

class TestHandleNulls:
    """Tests for :func:`handle_nulls`."""

    def test_no_nulls_in_key_cols(self, mock_df):
        """After applying ``handle_nulls`` the key columns must have no NULLs
        (rows that are impossible to fill are dropped)."""
        result = handle_nulls(mock_df)
        for col in ["trans_num", "Timestamp", "cc_num", "is_fraud"]:
            null_count = result.filter(F.col(col).isNull()).count()
            assert null_count == 0, (
                f"Column '{col}' still contains {null_count} NULL(s) after handle_nulls."
            )

    def test_amt_filled_to_zero(self, mock_df):
        """NULL ``amt`` values must be replaced with 0.0."""
        result = handle_nulls(mock_df)
        null_amt = result.filter(F.col("amt").isNull()).count()
        assert null_amt == 0, "NULL amt rows should be filled to 0.0."

    def test_category_filled_to_unknown(self, mock_df):
        """NULL ``category`` values must be replaced with the string
        ``"unknown"``."""
        result = handle_nulls(mock_df)
        null_cat = result.filter(F.col("category").isNull()).count()
        assert null_cat == 0, "NULL category rows should be filled to 'unknown'."


# ===========================================================================
# 3. TestExtractTimeFeatures
# ===========================================================================

class TestExtractTimeFeatures:
    """Tests for :func:`extract_time_features`."""

    def test_four_new_columns_added(self, mock_df):
        """Exactly four new columns should be present in the output."""
        result = extract_time_features(mock_df)
        for col in ["txn_hour", "txn_day_of_week", "txn_month", "is_weekend"]:
            assert col in result.columns, f"Column '{col}' not found in result."

    def test_hour_range_0_to_23(self, mock_df):
        """``txn_hour`` values must all fall within [0, 23]."""
        result = extract_time_features(mock_df)
        invalid = result.filter(
            (F.col("txn_hour") < 0) | (F.col("txn_hour") > 23)
        ).count()
        assert invalid == 0, f"{invalid} row(s) have txn_hour outside [0, 23]."

    def test_is_weekend_binary(self, mock_df):
        """``is_weekend`` must only contain the values 0 or 1."""
        result = extract_time_features(mock_df)
        invalid = result.filter(
            ~F.col("is_weekend").isin(0, 1)
        ).count()
        assert invalid == 0, f"{invalid} row(s) have is_weekend not in {{0, 1}}."


# ===========================================================================
# 4. TestNormalizeAmount
# ===========================================================================

class TestNormalizeAmount:
    """Tests for :func:`normalize_amount`."""

    def test_amt_log_column_added(self, mock_df):
        """The ``amt_log`` column must be present in the output."""
        result = normalize_amount(mock_df)
        assert "amt_log" in result.columns

    def test_amt_log_all_non_negative(self, mock_df):
        """log1p of a non-negative amount must always be >= 0."""
        result = normalize_amount(handle_nulls(mock_df))  # ensure no NULLs
        negative = result.filter(F.col("amt_log") < 0).count()
        assert negative == 0, f"{negative} row(s) have negative amt_log."


# ===========================================================================
# 5. TestBucketAmount
# ===========================================================================

class TestBucketAmount:
    """Tests for :func:`bucket_amount`."""

    def test_amt_bucket_column_added(self, mock_df):
        """The ``amt_bucket`` column must be present in the output."""
        result = bucket_amount(mock_df)
        assert "amt_bucket" in result.columns

    def test_valid_bucket_values(self, mock_df):
        """All ``amt_bucket`` values must be one of the four known labels."""
        result = bucket_amount(handle_nulls(mock_df))
        valid_buckets = {"low", "medium", "high", "very_high"}
        invalid = result.filter(
            ~F.col("amt_bucket").isin(*valid_buckets)
        ).count()
        assert invalid == 0, f"{invalid} row(s) have unexpected amt_bucket values."

    def test_low_bucket_for_small_amount(self, spark, mock_df):
        """A transaction with ``amt < 10`` must be bucketed as ``'low'``."""
        result = bucket_amount(mock_df)
        low_row = result.filter(F.col("trans_num") == "TXN001").select("amt_bucket").first()
        assert low_row is not None, "TXN001 not found in result."
        assert low_row["amt_bucket"] == "low", (
            f"Expected 'low' for amt=5.0 but got '{low_row['amt_bucket']}'."
        )


# ===========================================================================
# 6. TestCalculateAge
# ===========================================================================

class TestCalculateAge:
    """Tests for :func:`calculate_age`."""

    def test_cardholder_age_column_added(self, mock_df):
        """The ``cardholder_age`` column must be present in the output."""
        result = calculate_age(mock_df)
        assert "cardholder_age" in result.columns

    def test_ages_greater_than_zero(self, mock_df):
        """All calculated ages must be positive integers."""
        result = calculate_age(handle_nulls(mock_df))
        non_positive = result.filter(F.col("cardholder_age") <= 0).count()
        assert non_positive == 0, (
            f"{non_positive} row(s) have cardholder_age <= 0."
        )


# ===========================================================================
# 7. TestCalculateDistance
# ===========================================================================

class TestCalculateDistance:
    """Tests for :func:`calculate_distance`."""

    def test_distance_km_column_added(self, mock_df):
        """The ``distance_km`` column must be present in the output."""
        result = calculate_distance(mock_df)
        assert "distance_km" in result.columns

    def test_distances_non_negative(self, mock_df):
        """All distance values must be >= 0 km."""
        result = calculate_distance(mock_df)
        negative = result.filter(F.col("distance_km") < 0).count()
        assert negative == 0, f"{negative} row(s) have negative distance_km."


# ===========================================================================
# 8. TestEncodeGender
# ===========================================================================

class TestEncodeGender:
    """Tests for :func:`encode_gender`."""

    def test_gender_encoded_column_added(self, mock_df):
        """The ``gender_encoded`` column must be present in the output."""
        result = encode_gender(mock_df)
        assert "gender_encoded" in result.columns

    def test_female_encoded_as_0(self, mock_df):
        """Female (``'F'``) cardholders must be encoded as 0."""
        result = encode_gender(mock_df)
        female_rows = result.filter(F.col("gender") == "F")
        wrong = female_rows.filter(F.col("gender_encoded") != 0).count()
        assert wrong == 0, f"{wrong} 'F' row(s) not encoded as 0."

    def test_male_encoded_as_1(self, mock_df):
        """Male (``'M'``) cardholders must be encoded as 1."""
        result = encode_gender(mock_df)
        male_rows = result.filter(F.col("gender") == "M")
        wrong = male_rows.filter(F.col("gender_encoded") != 1).count()
        assert wrong == 0, f"{wrong} 'M' row(s) not encoded as 1."


# ===========================================================================
# 9. TestTransactionVelocity
# ===========================================================================

class TestTransactionVelocity:
    """Tests for :func:`transaction_velocity`."""

    def test_txn_velocity_day_column_added(self, mock_df):
        """The ``txn_velocity_day`` column must be present in the output."""
        deduped = remove_duplicates(mock_df)
        result = transaction_velocity(deduped)
        assert "txn_velocity_day" in result.columns

    def test_velocity_at_least_one(self, mock_df):
        """Every row must have ``txn_velocity_day >= 1`` (at minimum the row
        itself counts toward the daily total)."""
        deduped = remove_duplicates(mock_df)
        result = transaction_velocity(deduped)
        below_one = result.filter(F.col("txn_velocity_day") < 1).count()
        assert below_one == 0, (
            f"{below_one} row(s) have txn_velocity_day < 1."
        )

    def test_temp_column_removed(self, mock_df):
        """The temporary ``_txn_date`` column must NOT appear in the output."""
        deduped = remove_duplicates(mock_df)
        result = transaction_velocity(deduped)
        assert "_txn_date" not in result.columns, (
            "Temporary column '_txn_date' was not dropped."
        )


# ===========================================================================
# 10. TestFlagHighRisk
# ===========================================================================

class TestFlagHighRisk:
    """Tests for :func:`flag_high_risk`."""

    def _pipeline_up_to_velocity(self, mock_df):
        """Apply all prerequisites so that ``flag_high_risk`` can run."""
        df = remove_duplicates(mock_df)
        df = handle_nulls(df)
        df = extract_time_features(df)
        df = transaction_velocity(df)
        return df

    def test_high_risk_column_added(self, mock_df):
        """The ``high_risk`` column must be present in the output."""
        df = self._pipeline_up_to_velocity(mock_df)
        result = flag_high_risk(df)
        assert "high_risk" in result.columns

    def test_high_risk_only_0_or_1(self, mock_df):
        """``high_risk`` must be a binary flag (0 or 1 only)."""
        df = self._pipeline_up_to_velocity(mock_df)
        result = flag_high_risk(df)
        invalid = result.filter(~F.col("high_risk").isin(0, 1)).count()
        assert invalid == 0, f"{invalid} row(s) have high_risk not in {{0, 1}}."


# ===========================================================================
# 11. TestApplyAll – end-to-end pipeline
# ===========================================================================

class TestApplyAll:
    """End-to-end test for :func:`apply_all`."""

    # The 11 new columns introduced by the 10 transformations
    EXPECTED_NEW_COLS = [
        "txn_hour",
        "txn_day_of_week",
        "txn_month",
        "is_weekend",
        "amt_log",
        "amt_bucket",
        "cardholder_age",
        "distance_km",
        "gender_encoded",
        "txn_velocity_day",
        "high_risk",
    ]

    def test_all_new_columns_present(self, mock_df):
        """All 11 derived columns must be present after ``apply_all``."""
        result = apply_all(mock_df)
        for col in self.EXPECTED_NEW_COLS:
            assert col in result.columns, (
                f"Expected column '{col}' not found after apply_all."
            )

    def test_no_duplicates_after_pipeline(self, mock_df):
        """The duplicate ``trans_num`` introduced in ``mock_df`` must be
        removed by the time the full pipeline completes."""
        result = apply_all(mock_df)
        total = result.count()
        distinct = result.select("trans_num").distinct().count()
        assert total == distinct, (
            f"Found {total - distinct} duplicate trans_num(s) after apply_all."
        )
