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

    * One exact duplicate row (same ``trans_num``) to test dedup logic.
    * Rows with ``amt > 500`` and ``txn_hour`` in 0-5 (via a midnight
      timestamp) to exercise the ``flag_high_risk`` transformation.
    * Valid WGS-84 lat/long pairs for both cardholder and merchant.
    * Valid ``dob`` strings in ``yyyy-MM-dd`` format so ``calculate_age``
      can be tested.
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
        StructField("trans_num", StringType(), True),
        StructField("Timestamp", TimestampType(), True),
        StructField("cc_num", StringType(), True),
        StructField("merchant", StringType(), True),
        StructField("category", StringType(), True),
        StructField("amt", DoubleType(), True),
        StructField("first", StringType(), True),
        StructField("last", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("long", DoubleType(), True),
        StructField("city_pop", IntegerType(), True),
        StructField("dob", StringType(), True),
        StructField("merch_lat", DoubleType(), True),
        StructField("merch_long", DoubleType(), True),
        StructField("is_fraud", IntegerType(), True),
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
    * Row index 1 is an exact duplicate of row index 0 (same ``trans_num``
      ``"TXN001"``).  After ``remove_duplicates`` only one should remain.
    * Rows 2 and 3 have ``amt > 500`` and timestamps in the 00–05 hour window
      (midnight / 02:00) so they can potentially trigger ``high_risk`` once
      ``txn_velocity_day > 3`` on the same day.
    * Rows 4–7 cover different genders (``"M"``, ``"F"``, ``None``,
      ``"unknown"``), various amounts spanning all four buckets, and a
      variety of dates/times for time-feature tests.
    * All ``lat``/``long``/``merch_lat``/``merch_long`` values are realistic
      US coordinates.
    * All ``dob`` values are ``yyyy-MM-dd`` strings representing adults (age
      > 18 at transaction time).

    Returns
    -------
    DataFrame
        8-row DataFrame conforming to ``CC_FRAUD_SCHEMA``.
    """
    rows = [
        # Row 0 – will be kept (original), amt 5.0 → "low" bucket
        (
            "TXN001",
            datetime(2023, 6, 15, 14, 30, 0),
            "4111111111111111",
            "merchant_a",
            "grocery_pos",
            5.0,
            "Alice",
            "Smith",
            "F",
            40.7128,
            -74.0060,
            8_336_817,
            "1985-03-22",
            40.7580,
            -73.9855,
            0,
        ),
        # Row 1 – EXACT DUPLICATE of Row 0 (same trans_num TXN001)
        (
            "TXN001",
            datetime(2023, 6, 15, 14, 30, 0),
            "4111111111111111",
            "merchant_a",
            "grocery_pos",
            5.0,
            "Alice",
            "Smith",
            "F",
            40.7128,
            -74.0060,
            8_336_817,
            "1985-03-22",
            40.7580,
            -73.9855,
            0,
        ),
        # Row 2 – high-value midnight transaction (amt=750, hour=0)
        (
            "TXN002",
            datetime(2023, 6, 15, 0, 15, 0),
            "4222222222222222",
            "merchant_b",
            "shopping_net",
            750.0,
            "Bob",
            "Jones",
            "M",
            34.0522,
            -118.2437,
            3_898_747,
            "1978-11-05",
            34.0195,
            -118.4912,
            1,
        ),
        # Row 3 – high-value early-morning transaction (amt=820, hour=2)
        #         same card + same day as Row 2 → velocity will be 2 here
        (
            "TXN003",
            datetime(2023, 6, 15, 2, 45, 0),
            "4222222222222222",
            "merchant_c",
            "shopping_pos",
            820.0,
            "Bob",
            "Jones",
            "M",
            34.0522,
            -118.2437,
            3_898_747,
            "1978-11-05",
            34.0689,
            -118.3073,
            1,
        ),
        # Row 4 – medium amount (amt=55.0), weekend Saturday
        (
            "TXN004",
            datetime(2023, 6, 17, 10, 0, 0),  # Saturday
            "4333333333333333",
            "merchant_d",
            "food_dining",
            55.0,
            "Carol",
            "Williams",
            "F",
            41.8781,
            -87.6298,
            2_696_555,
            "1990-07-14",
            41.8827,
            -87.6233,
            0,
        ),
        # Row 5 – high amount (amt=450.0), evening
        (
            "TXN005",
            datetime(2023, 6, 16, 20, 0, 0),
            "4444444444444444",
            "merchant_e",
            "health_fitness",
            450.0,
            "David",
            "Brown",
            "M",
            29.7604,
            -95.3698,
            2_304_580,
            "1972-01-30",
            29.7522,
            -95.3758,
            0,
        ),
        # Row 6 – NULL gender (will be filled to "unknown"), amt NULL (→ 0.0)
        (
            "TXN006",
            datetime(2023, 6, 18, 9, 0, 0),
            "4555555555555555",
            "merchant_f",
            "gas_transport",
            None,
            "Eve",
            "Davis",
            None,
            47.6062,
            -122.3321,
            737_015,
            "2000-05-19",
            47.6101,
            -122.3420,
            0,
        ),
        # Row 7 – very_high amount (amt=1500), fraud, Sunday
        (
            "TXN007",
            datetime(2023, 6, 18, 3, 30, 0),
            "4666666666666666",
            "merchant_g",
            "misc_net",
            1500.0,
            "Frank",
            "Miller",
            "M",
            33.4484,
            -112.0740,
            1_608_139,
            "1965-08-12",
            33.5722,
            -112.0901,
            1,
        ),
    ]

    return spark.createDataFrame(rows, schema=CC_FRAUD_SCHEMA)
