"""
conftest.py
===========
pytest configuration and shared fixtures for the curated-layer unit tests.

Fixtures
--------
spark
    A local SparkSession (master="local[2]") reused across the entire test
    session.  Shuffle partitions are set to 2 to keep tests fast.

mock_df
    A small synthetic DataFrame that mimics the ``cc_fraud_trans`` schema.
    It deliberately includes:

    * One exact duplicate row (same ``transaction_id``) to test dedup logic.
    * Rows with ``transaction_amount > 500`` and timestamps in the 00-05 hour
      window (midnight / 02:00) to exercise the ``flag_high_risk`` transform.
    * Biometric ``authentication_method`` values so ``encode_gender`` logic
      can be verified (biometric/face_recognition/fingerprint → 1, others → 0).
    * A row with NULL ``transaction_amount`` and NULL ``merchant_category``
      to test the null-imputation logic in ``handle_nulls``.
    * Various amounts spanning all four ``amt_bucket`` categories.
    * Positive ``card_age`` integers for ``calculate_age``.
    * Non-negative ``transaction_distance`` values for ``calculate_distance``.
"""

import pytest
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# Schema matching cc_fraud_trans
# ---------------------------------------------------------------------------

CC_FRAUD_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("transaction_amount", DoubleType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("account_balance", DoubleType(), True),
        StructField("device_type", StringType(), True),
        StructField("location", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("ip_address_flag", IntegerType(), True),
        StructField("previous_fraudulent_activity", IntegerType(), True),
        StructField("daily_transaction_count", IntegerType(), True),
        StructField("avg_transaction_amount_7d", DoubleType(), True),
        StructField("failed_transaction_count_7d", IntegerType(), True),
        StructField("card_type", StringType(), True),
        StructField("card_age", IntegerType(), True),
        StructField("transaction_distance", DoubleType(), True),
        StructField("authentication_method", StringType(), True),
        StructField("risk_score", DoubleType(), True),
        StructField("is_weekend", IntegerType(), True),
        StructField("fraud_label", IntegerType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for the test session.

    Configuration
    ~~~~~~~~~~~~~
    * ``master = local[2]``          – two local threads, fast for unit tests
    * ``shuffle.partitions = 2``     – avoids 200-partition default overhead
    * log level set to ERROR         – suppresses verbose Spark output

    Yields
    ------
    SparkSession
        Active session; stopped automatically after all tests complete.
    """
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("curated_layer_unit_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def mock_df(spark):
    """Return a synthetic ``cc_fraud_trans`` DataFrame with 8 rows.

    Data design notes
    ~~~~~~~~~~~~~~~~~
    * Row index 1 is an exact duplicate of row index 0 (same ``transaction_id``
      ``"TXN001"``).  After ``remove_duplicates`` only one should remain.
    * Rows 2 and 3 share ``user_id="USER002"`` on the same date so that
      ``transaction_velocity`` counts them together.  Both have
      ``transaction_amount > 500`` and timestamps in the 00-05 window, allowing
      ``flag_high_risk`` to trigger once velocity exceeds the threshold.
    * Rows 2, 3, and 5 use biometric ``authentication_method`` values
      (``biometric``, ``face_recognition``, ``fingerprint``) → ``gender_encoded=1``.
      All other rows use non-biometric methods (``pin``, ``password``, ``otp``,
      ``None``) → ``gender_encoded=0``.
    * Row 6 has NULL ``transaction_amount`` (→ 0.0), NULL ``merchant_category``
      (→ "unknown"), and NULL ``transaction_type`` (→ "unknown") to exercise
      the fill logic in ``handle_nulls``.
    * Row 7 has ``transaction_amount=1500.0`` (very_high bucket) and ``fraud_label=1``.

    Returns
    -------
    DataFrame
        8-row DataFrame conforming to ``CC_FRAUD_SCHEMA``.
    """
    rows = [
        # Row 0 – will be kept (original); amt 5.0 → "low" bucket; pin auth → 0
        (
            "TXN001",          # transaction_id
            "USER001",          # user_id
            5.0,                # transaction_amount
            "purchase",         # transaction_type
            datetime(2023, 6, 15, 14, 30, 0),  # timestamp
            1200.0,             # account_balance
            "mobile",           # device_type
            "New York",         # location
            "grocery_pos",      # merchant_category
            0,                  # ip_address_flag
            0,                  # previous_fraudulent_activity
            1,                  # daily_transaction_count
            25.0,               # avg_transaction_amount_7d
            0,                  # failed_transaction_count_7d
            "visa",             # card_type
            3,                  # card_age
            5.2,                # transaction_distance
            "pin",              # authentication_method
            0.1,                # risk_score
            0,                  # is_weekend
            0,                  # fraud_label
        ),
        # Row 1 – EXACT DUPLICATE of Row 0 (same transaction_id TXN001)
        (
            "TXN001",
            "USER001",
            5.0,
            "purchase",
            datetime(2023, 6, 15, 14, 30, 0),
            1200.0,
            "mobile",
            "New York",
            "grocery_pos",
            0,
            0,
            1,
            25.0,
            0,
            "visa",
            3,
            5.2,
            "pin",
            0.1,
            0,
            0,
        ),
        # Row 2 – high-value midnight transaction; biometric auth → gender_encoded=1
        (
            "TXN002",
            "USER002",
            750.0,
            "purchase",
            datetime(2023, 6, 15, 0, 15, 0),
            500.0,
            "desktop",
            "Los Angeles",
            "shopping_net",
            1,
            1,
            1,
            400.0,
            2,
            "mastercard",
            5,
            15.3,
            "biometric",
            0.85,
            0,
            1,
        ),
        # Row 3 – high-value early-morning; same user + same day as Row 2;
        #         face_recognition auth → gender_encoded=1
        (
            "TXN003",
            "USER002",
            820.0,
            "purchase",
            datetime(2023, 6, 15, 2, 45, 0),
            450.0,
            "mobile",
            "Los Angeles",
            "shopping_pos",
            1,
            1,
            2,
            400.0,
            2,
            "mastercard",
            5,
            18.7,
            "face_recognition",
            0.9,
            0,
            1,
        ),
        # Row 4 – medium amount (55.0); weekend Saturday; password auth → 0
        (
            "TXN004",
            "USER003",
            55.0,
            "purchase",
            datetime(2023, 6, 17, 10, 0, 0),  # Saturday
            800.0,
            "mobile",
            "Chicago",
            "food_dining",
            0,
            0,
            1,
            50.0,
            0,
            "visa",
            7,
            2.1,
            "password",
            0.2,
            1,
            0,
        ),
        # Row 5 – high amount (450.0); fingerprint auth → gender_encoded=1
        (
            "TXN005",
            "USER004",
            450.0,
            "purchase",
            datetime(2023, 6, 16, 20, 0, 0),
            2000.0,
            "desktop",
            "Houston",
            "health_fitness",
            0,
            0,
            1,
            300.0,
            0,
            "amex",
            10,
            3.5,
            "fingerprint",
            0.3,
            0,
            0,
        ),
        # Row 6 – NULL transaction_amount (→ 0.0), NULL merchant_category
        #         (→ "unknown"), NULL transaction_type (→ "unknown"),
        #         NULL authentication_method (→ gender_encoded=0)
        (
            "TXN006",
            "USER005",
            None,               # transaction_amount → filled to 0.0
            None,               # transaction_type   → filled to "unknown"
            datetime(2023, 6, 18, 9, 0, 0),
            350.0,
            "mobile",
            "Seattle",
            None,               # merchant_category → filled to "unknown"
            0,
            0,
            1,
            20.0,
            1,
            "visa",
            4,
            1.2,
            None,               # authentication_method → gender_encoded=0
            0.15,
            0,
            0,
        ),
        # Row 7 – very_high amount (1500.0); otp auth → gender_encoded=0;
        #         fraud_label=1; early-morning Sunday
        (
            "TXN007",
            "USER006",
            1500.0,
            "purchase",
            datetime(2023, 6, 18, 3, 30, 0),
            100.0,
            "desktop",
            "Phoenix",
            "misc_net",
            1,
            2,
            1,
            600.0,
            3,
            "mastercard",
            6,
            25.0,
            "otp",
            0.95,
            1,
            1,
        ),
    ]

    return spark.createDataFrame(rows, schema=CC_FRAUD_SCHEMA)
