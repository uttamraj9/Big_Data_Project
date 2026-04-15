"""
transformations.py
==================
PySpark transformation functions for the cc_fraud_trans curated layer.

Schema: transaction_id, user_id, transaction_amount, transaction_type,
        timestamp, account_balance, device_type, location,
        merchant_category, ip_address_flag, previous_fraudulent_activity,
        daily_transaction_count, avg_transaction_amount_7d,
        failed_transaction_count_7d, card_type, card_age,
        transaction_distance, authentication_method, risk_score,
        is_weekend, fraud_label

Each function takes a Spark DataFrame as input, applies a single
transformation, and returns the enriched DataFrame.

``apply_all(df)`` chains all 10 transformations in the prescribed order.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


# ---------------------------------------------------------------------------
# 1. Remove Duplicates
# ---------------------------------------------------------------------------

def remove_duplicates(df: DataFrame) -> DataFrame:
    """Remove duplicate rows based on ``transaction_id``."""
    return df.dropDuplicates(["transaction_id"])


# ---------------------------------------------------------------------------
# 2. Handle Nulls
# ---------------------------------------------------------------------------

def handle_nulls(df: DataFrame) -> DataFrame:
    """Impute defaults and drop rows missing critical columns.

    Fill strategy:
    - ``transaction_amount``  → 0.0
    - ``merchant_category``  → "unknown"
    - ``transaction_type``   → "unknown"

    Rows with NULL in ``transaction_id``, ``timestamp``, ``user_id``,
    ``transaction_amount``, or ``fraud_label`` are dropped.
    """
    df = df.fillna({
        "transaction_amount": 0.0,
        "merchant_category": "unknown",
        "transaction_type": "unknown",
    })
    key_cols = ["transaction_id", "timestamp", "user_id",
                "transaction_amount", "fraud_label"]
    df = df.dropna(subset=key_cols)
    return df


# ---------------------------------------------------------------------------
# 3. Extract Time Features
# ---------------------------------------------------------------------------

def extract_time_features(df: DataFrame) -> DataFrame:
    """Derive time-of-day and calendar features from ``timestamp``.

    New columns: ``txn_hour``, ``txn_day_of_week``, ``txn_month``,
    ``is_weekend`` (overwrites existing column).
    """
    df = df.withColumn("txn_hour", F.hour(F.col("timestamp")))
    df = df.withColumn("txn_day_of_week", F.dayofweek(F.col("timestamp")))
    df = df.withColumn("txn_month", F.month(F.col("timestamp")))
    df = df.withColumn(
        "is_weekend",
        F.when(F.col("txn_day_of_week").isin(1, 7), 1).otherwise(0),
    )
    return df


# ---------------------------------------------------------------------------
# 4. Normalize Amount
# ---------------------------------------------------------------------------

def normalize_amount(df: DataFrame) -> DataFrame:
    """Apply log1p to ``transaction_amount``.  New column: ``amt_log``."""
    df = df.withColumn("amt_log", F.log1p(F.col("transaction_amount")))
    return df


# ---------------------------------------------------------------------------
# 5. Bucket Amount
# ---------------------------------------------------------------------------

def bucket_amount(df: DataFrame) -> DataFrame:
    """Categorise ``transaction_amount`` into four ordinal buckets.

    Thresholds: low < 10 ≤ medium < 100 ≤ high < 1000 ≤ very_high.
    New column: ``amt_bucket``.
    """
    df = df.withColumn(
        "amt_bucket",
        F.when(F.col("transaction_amount") < 10, "low")
        .when(F.col("transaction_amount") < 100, "medium")
        .when(F.col("transaction_amount") < 1000, "high")
        .otherwise("very_high"),
    )
    return df


# ---------------------------------------------------------------------------
# 6. Derive Cardholder Age
# ---------------------------------------------------------------------------

def calculate_age(df: DataFrame) -> DataFrame:
    """Cast the pre-computed ``card_age`` column to int as ``cardholder_age``."""
    df = df.withColumn("cardholder_age", F.col("card_age").cast("int"))
    return df


# ---------------------------------------------------------------------------
# 7. Expose Transaction Distance
# ---------------------------------------------------------------------------

def calculate_distance(df: DataFrame) -> DataFrame:
    """Alias ``transaction_distance`` as ``distance_km`` for uniform naming."""
    df = df.withColumn("distance_km", F.col("transaction_distance"))
    return df


# ---------------------------------------------------------------------------
# 8. Encode Authentication Method
# ---------------------------------------------------------------------------

def encode_gender(df: DataFrame) -> DataFrame:
    """Binary-encode ``authentication_method``.

    High-security biometric methods (biometric, face_recognition,
    fingerprint) → 1; all others → 0.  Output column: ``gender_encoded``.
    """
    biometric = ["biometric", "face_recognition", "fingerprint"]
    df = df.withColumn(
        "gender_encoded",
        F.when(F.lower(F.col("authentication_method")).isin(biometric), 1)
        .otherwise(0),
    )
    return df


# ---------------------------------------------------------------------------
# 9. Transaction Velocity
# ---------------------------------------------------------------------------

def transaction_velocity(df: DataFrame) -> DataFrame:
    """Count daily transactions per ``user_id``.  New column: ``txn_velocity_day``."""
    df = df.withColumn("_txn_date", F.to_date(F.col("timestamp")))
    window_spec = Window.partitionBy("user_id", "_txn_date")
    df = df.withColumn("txn_velocity_day",
                       F.count("transaction_id").over(window_spec))
    df = df.drop("_txn_date")
    return df


# ---------------------------------------------------------------------------
# 10. Flag High Risk
# ---------------------------------------------------------------------------

def flag_high_risk(df: DataFrame) -> DataFrame:
    """Flag transactions where amount > 500, hour < 6, and velocity > 3.

    New column: ``high_risk`` (1 = risky, 0 = normal).
    Requires ``txn_hour`` and ``txn_velocity_day`` to be present.
    """
    df = df.withColumn(
        "high_risk",
        F.when(
            (F.col("transaction_amount") > 500)
            & (F.col("txn_hour").between(0, 5))
            & (F.col("txn_velocity_day") > 3),
            1,
        ).otherwise(0),
    )
    return df


# ---------------------------------------------------------------------------
# apply_all – convenience wrapper
# ---------------------------------------------------------------------------

def apply_all(df: DataFrame) -> DataFrame:
    """Apply all 10 transformations in the prescribed order."""
    df = remove_duplicates(df)
    df = handle_nulls(df)
    df = extract_time_features(df)
    df = normalize_amount(df)
    df = bucket_amount(df)
    df = calculate_age(df)
    df = calculate_distance(df)
    df = encode_gender(df)
    df = transaction_velocity(df)
    df = flag_high_risk(df)
    return df
