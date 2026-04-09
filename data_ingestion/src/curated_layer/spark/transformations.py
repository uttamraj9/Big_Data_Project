"""
transformations.py
==================
PySpark transformation functions for the cc_fraud_trans curated layer.

Each function takes a Spark DataFrame as input, applies a single
transformation, and returns the enriched DataFrame.

``apply_all(df)`` chains all 10 transformations in the prescribed order.
"""

from math import pi

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


# ---------------------------------------------------------------------------
# 1. Remove Duplicates
# ---------------------------------------------------------------------------

def remove_duplicates(df: DataFrame) -> DataFrame:
    """Remove duplicate rows based on ``trans_num``.

    A transaction number uniquely identifies a transaction.  Any row whose
    ``trans_num`` has already been seen earlier in the dataset is dropped so
    that downstream aggregations are not inflated.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame containing at minimum the column ``trans_num``.

    Returns
    -------
    DataFrame
        DataFrame with duplicate ``trans_num`` rows removed.
    """
    return df.dropDuplicates(["trans_num"])


# ---------------------------------------------------------------------------
# 2. Handle Nulls
# ---------------------------------------------------------------------------

def handle_nulls(df: DataFrame) -> DataFrame:
    """Impute sensible defaults for nullable columns and drop rows that are
    missing values in key business columns.

    Fill strategy
    ~~~~~~~~~~~~~
    - ``amt``      → 0.0   (no amount recorded is treated as zero)
    - ``category`` → "unknown"
    - ``merchant`` → "unknown"
    - ``gender``   → "unknown"

    After filling, any row that still has a NULL in one of the critical
    columns (``trans_num``, ``Timestamp``, ``cc_num``, ``amt``,
    ``is_fraud``) is dropped entirely because those columns are required
    for all downstream transformations and reporting.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.

    Returns
    -------
    DataFrame
        DataFrame with nulls imputed or offending rows removed.
    """
    df = df.fillna({
        "amt": 0.0,
        "category": "unknown",
        "merchant": "unknown",
        "gender": "unknown",
    })
    key_cols = ["trans_num", "Timestamp", "cc_num", "amt", "is_fraud"]
    df = df.dropna(subset=key_cols)
    return df


# ---------------------------------------------------------------------------
# 3. Extract Time Features
# ---------------------------------------------------------------------------

def extract_time_features(df: DataFrame) -> DataFrame:
    """Derive calendar and time-of-day features from the ``Timestamp`` column.

    New columns
    ~~~~~~~~~~~
    - ``txn_hour``        (int)  – hour of day, 0–23
    - ``txn_day_of_week`` (int)  – day of week as returned by
                                   ``dayofweek()`` (1 = Sunday … 7 = Saturday)
    - ``txn_month``       (int)  – calendar month, 1–12
    - ``is_weekend``      (int)  – 1 if Saturday (7) or Sunday (1), else 0

    Parameters
    ----------
    df : DataFrame
        Input DataFrame containing a ``Timestamp`` column of type timestamp.

    Returns
    -------
    DataFrame
        DataFrame with four additional time-feature columns.
    """
    df = df.withColumn("txn_hour", F.hour(F.col("Timestamp")))
    df = df.withColumn("txn_day_of_week", F.dayofweek(F.col("Timestamp")))
    df = df.withColumn("txn_month", F.month(F.col("Timestamp")))
    df = df.withColumn(
        "is_weekend",
        F.when(F.col("txn_day_of_week").isin(1, 7), 1).otherwise(0),
    )
    return df


# ---------------------------------------------------------------------------
# 4. Normalize Amount
# ---------------------------------------------------------------------------

def normalize_amount(df: DataFrame) -> DataFrame:
    """Apply a log1p transformation to the transaction amount.

    ``log1p(x) = ln(1 + x)`` compresses the heavy right-tail of transaction
    amounts while keeping zero amounts at zero (log1p(0) = 0) and avoiding
    the undefined log(0).

    New column
    ~~~~~~~~~~
    - ``amt_log`` (double) – natural log of (1 + amt)

    Parameters
    ----------
    df : DataFrame
        Input DataFrame with an ``amt`` column of type double.

    Returns
    -------
    DataFrame
        DataFrame with the additional ``amt_log`` column.
    """
    df = df.withColumn("amt_log", F.log1p(F.col("amt")))
    return df


# ---------------------------------------------------------------------------
# 5. Bucket Amount
# ---------------------------------------------------------------------------

def bucket_amount(df: DataFrame) -> DataFrame:
    """Categorise the transaction amount into four ordinal buckets.

    Bucket thresholds
    ~~~~~~~~~~~~~~~~~
    - ``low``       – amt < 10
    - ``medium``    – 10 ≤ amt < 100
    - ``high``      – 100 ≤ amt < 1 000
    - ``very_high`` – amt ≥ 1 000

    New column
    ~~~~~~~~~~
    - ``amt_bucket`` (string)

    Parameters
    ----------
    df : DataFrame
        Input DataFrame with an ``amt`` column of type double.

    Returns
    -------
    DataFrame
        DataFrame with the additional ``amt_bucket`` column.
    """
    df = df.withColumn(
        "amt_bucket",
        F.when(F.col("amt") < 10, "low")
        .when(F.col("amt") < 100, "medium")
        .when(F.col("amt") < 1000, "high")
        .otherwise("very_high"),
    )
    return df


# ---------------------------------------------------------------------------
# 6. Calculate Age
# ---------------------------------------------------------------------------

def calculate_age(df: DataFrame) -> DataFrame:
    """Calculate the cardholder's age at the time of the transaction.

    Age is computed as the whole number of years between the cardholder's
    date of birth (``dob`` column, format ``yyyy-MM-dd``) and the date of
    the transaction (``Timestamp`` column).

    New column
    ~~~~~~~~~~
    - ``cardholder_age`` (int) – age in whole years at transaction time

    Parameters
    ----------
    df : DataFrame
        Input DataFrame with ``dob`` (string, ``yyyy-MM-dd``) and
        ``Timestamp`` (timestamp) columns.

    Returns
    -------
    DataFrame
        DataFrame with the additional ``cardholder_age`` column.
    """
    dob_col = F.to_date(F.col("dob"), "yyyy-MM-dd")
    txn_date = F.to_date(F.col("Timestamp"))
    df = df.withColumn(
        "cardholder_age",
        F.floor(F.datediff(txn_date, dob_col) / 365.25).cast("int"),
    )
    return df


# ---------------------------------------------------------------------------
# 7. Calculate Distance
# ---------------------------------------------------------------------------

def calculate_distance(df: DataFrame) -> DataFrame:
    """Calculate the great-circle distance between the cardholder and merchant.

    Uses the Haversine formula with an Earth radius of 6 371 km.  All
    trigonometric operations are performed on Spark Column objects; Python's
    ``**`` operator is intentionally avoided in favour of ``F.pow()`` so
    that the expression graph remains in Spark's logical plan.

    New column
    ~~~~~~~~~~
    - ``distance_km`` (double) – distance in kilometres (≥ 0)

    Parameters
    ----------
    df : DataFrame
        Input DataFrame with columns ``lat``, ``long``, ``merch_lat``,
        ``merch_long`` (all double, in decimal degrees).

    Returns
    -------
    DataFrame
        DataFrame with the additional ``distance_km`` column.
    """
    earth_radius_km = 6371.0
    deg_to_rad = pi / 180.0

    lat1 = F.col("lat") * deg_to_rad
    lat2 = F.col("merch_lat") * deg_to_rad
    dlat = (F.col("merch_lat") - F.col("lat")) * deg_to_rad
    dlon = (F.col("merch_long") - F.col("long")) * deg_to_rad

    a = (
        F.pow(F.sin(dlat / 2), 2)
        + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin(dlon / 2), 2)
    )
    c = 2 * F.asin(F.sqrt(a))

    df = df.withColumn("distance_km", c * earth_radius_km)
    return df


# ---------------------------------------------------------------------------
# 8. Encode Gender
# ---------------------------------------------------------------------------

def encode_gender(df: DataFrame) -> DataFrame:
    """Binary-encode the ``gender`` column.

    Encoding map
    ~~~~~~~~~~~~
    - ``"F"``     → 0
    - ``"M"``     → 1
    - anything else (including ``"unknown"`` from null imputation) → -1

    New column
    ~~~~~~~~~~
    - ``gender_encoded`` (int)

    Parameters
    ----------
    df : DataFrame
        Input DataFrame with a ``gender`` column of type string.

    Returns
    -------
    DataFrame
        DataFrame with the additional ``gender_encoded`` column.
    """
    df = df.withColumn(
        "gender_encoded",
        F.when(F.col("gender") == "F", 0)
        .when(F.col("gender") == "M", 1)
        .otherwise(-1),
    )
    return df


# ---------------------------------------------------------------------------
# 9. Transaction Velocity
# ---------------------------------------------------------------------------

def transaction_velocity(df: DataFrame) -> DataFrame:
    """Count how many transactions a cardholder made on the same calendar day.

    A window is defined over (``cc_num``, ``_txn_date``) — the latter being
    a temporary date column derived from ``Timestamp`` — and the row count
    within that window is used as the velocity signal.  The temporary
    ``_txn_date`` column is removed before returning.

    New column
    ~~~~~~~~~~
    - ``txn_velocity_day`` (long) – number of transactions by this card on
      the same calendar day as the current row (always ≥ 1)

    Parameters
    ----------
    df : DataFrame
        Input DataFrame with ``cc_num`` and ``Timestamp`` columns.

    Returns
    -------
    DataFrame
        DataFrame with ``txn_velocity_day`` added and ``_txn_date`` removed.
    """
    df = df.withColumn("_txn_date", F.to_date(F.col("Timestamp")))

    window_spec = Window.partitionBy("cc_num", "_txn_date")

    df = df.withColumn("txn_velocity_day", F.count("trans_num").over(window_spec))
    df = df.drop("_txn_date")
    return df


# ---------------------------------------------------------------------------
# 10. Flag High Risk
# ---------------------------------------------------------------------------

def flag_high_risk(df: DataFrame) -> DataFrame:
    """Mark a transaction as high-risk based on amount, time, and velocity.

    A transaction is flagged (``high_risk = 1``) when **all three** of the
    following conditions hold simultaneously:

    1. ``amt > 500``              – large transaction
    2. ``txn_hour`` ∈ {0, 1, 2, 3, 4, 5}  – occurs in the small hours
    3. ``txn_velocity_day > 3``   – card used more than three times that day

    New column
    ~~~~~~~~~~
    - ``high_risk`` (int) – 1 if high-risk, 0 otherwise

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.  Must already contain ``amt``, ``txn_hour``, and
        ``txn_velocity_day`` (i.e. run after steps 3, 4, and 9).

    Returns
    -------
    DataFrame
        DataFrame with the additional ``high_risk`` column.
    """
    df = df.withColumn(
        "high_risk",
        F.when(
            (F.col("amt") > 500)
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
    """Apply all 10 transformations in the prescribed order.

    Order of application
    ~~~~~~~~~~~~~~~~~~~~
    1.  remove_duplicates
    2.  handle_nulls
    3.  extract_time_features
    4.  normalize_amount
    5.  bucket_amount
    6.  calculate_age
    7.  calculate_distance
    8.  encode_gender
    9.  transaction_velocity
    10. flag_high_risk

    Parameters
    ----------
    df : DataFrame
        Raw ``cc_fraud_trans`` DataFrame.

    Returns
    -------
    DataFrame
        Fully enriched DataFrame ready for writing to the curated Hive table.
    """
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
