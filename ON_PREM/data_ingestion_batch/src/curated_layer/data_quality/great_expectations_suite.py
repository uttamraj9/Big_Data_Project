"""
great_expectations_suite.py
============================
Data-quality validation for the ``bd_class_project.cc_fraud_trans_curated``
Hive table using pure PySpark assertions.

``run_quality_checks(df_spark)`` validates a suite of expectations, prints a
pass/fail summary, and exits with code 1 if any expectation fails.

``main()`` builds a SparkSession with Hive support, reads the curated table,
and delegates to ``run_quality_checks``.

Schema expectations match the cc_fraud_trans_curated table which contains
both the original columns and the transformation-derived columns:
    transaction_id, user_id, transaction_amount, transaction_type, timestamp,
    account_balance, device_type, location, merchant_category, ip_address_flag,
    previous_fraudulent_activity, daily_transaction_count,
    avg_transaction_amount_7d, failed_transaction_count_7d, card_type,
    card_age, transaction_distance, authentication_method, risk_score,
    is_weekend, fraud_label,
    txn_hour, txn_day_of_week, txn_month,
    amt_log, amt_bucket, cardholder_age, distance_km,
    gender_encoded, txn_velocity_day, high_risk

Usage
-----
spark-submit --master yarn great_expectations_suite.py
"""

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("great_expectations_suite")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CURATED_TABLE = "bd_class_project.cc_fraud_trans_curated"


# ---------------------------------------------------------------------------
# Quality-check function
# ---------------------------------------------------------------------------

def run_quality_checks(df_spark):
    """Validate the curated-layer DataFrame using PySpark assertions.

    Expectations applied
    ~~~~~~~~~~~~~~~~~~~~
    * Row count >= 1
    * Not-null: ``transaction_id``, ``timestamp``, ``user_id``,
      ``transaction_amount``, ``fraud_label``
    * ``transaction_amount >= 0``, ``amt_log >= 0``
    * ``txn_hour`` in [0, 23], ``txn_month`` in [1, 12],
      ``txn_day_of_week`` in [1, 7]
    * ``cardholder_age`` (card age in years) in [0, 50]
    * ``distance_km >= 0``
    * ``txn_velocity_day >= 1``
    * ``amt_bucket`` in {``low``, ``medium``, ``high``, ``very_high``}
    * ``gender_encoded`` in {0, 1}
    * ``fraud_label``, ``is_weekend``, ``high_risk`` each in {0, 1}
    * ``transaction_id`` values are unique
    * Mean of ``fraud_label`` in [0.0, 0.5]

    Parameters
    ----------
    df_spark : pyspark.sql.DataFrame
        Fully-transformed curated DataFrame.

    Raises / Exits
    --------------
    Calls ``sys.exit(1)`` if any expectation fails; prints a summary either way.
    """
    from pyspark.sql import functions as F

    failures = []

    # ------------------------------------------------------------------
    # 1. Row count
    # ------------------------------------------------------------------
    row_count = df_spark.count()
    logger.info("Row count: %d", row_count)
    if row_count < 1:
        failures.append("Row count is 0 – expected at least 1 row")

    # ------------------------------------------------------------------
    # 2. Not-null checks for key columns
    # ------------------------------------------------------------------
    key_cols = [
        "transaction_id", "timestamp", "user_id",
        "transaction_amount", "fraud_label",
    ]
    for col in key_cols:
        null_count = df_spark.filter(F.col(col).isNull()).count()
        if null_count > 0:
            failures.append(
                "expect_column_values_to_not_be_null [{col}]: "
                "{n} null(s)".format(col=col, n=null_count)
            )

    # ------------------------------------------------------------------
    # 3. Amount validations
    # ------------------------------------------------------------------
    invalid_amt = df_spark.filter(F.col("transaction_amount") < 0).count()
    if invalid_amt > 0:
        failures.append(
            "expect_column_values_to_be_between [transaction_amount >= 0]: "
            "{n} violation(s)".format(n=invalid_amt)
        )

    invalid_amt_log = df_spark.filter(F.col("amt_log") < 0).count()
    if invalid_amt_log > 0:
        failures.append(
            "expect_column_values_to_be_between [amt_log >= 0]: "
            "{n} violation(s)".format(n=invalid_amt_log)
        )

    # ------------------------------------------------------------------
    # 4. Time features
    # ------------------------------------------------------------------
    invalid_hour = df_spark.filter(
        (F.col("txn_hour") < 0) | (F.col("txn_hour") > 23)
    ).count()
    if invalid_hour > 0:
        failures.append(
            "expect_column_values_to_be_between [txn_hour in 0-23]: "
            "{n} violation(s)".format(n=invalid_hour)
        )

    invalid_month = df_spark.filter(
        (F.col("txn_month") < 1) | (F.col("txn_month") > 12)
    ).count()
    if invalid_month > 0:
        failures.append(
            "expect_column_values_to_be_between [txn_month in 1-12]: "
            "{n} violation(s)".format(n=invalid_month)
        )

    invalid_dow = df_spark.filter(
        (F.col("txn_day_of_week") < 1) | (F.col("txn_day_of_week") > 7)
    ).count()
    if invalid_dow > 0:
        failures.append(
            "expect_column_values_to_be_between [txn_day_of_week in 1-7]: "
            "{n} violation(s)".format(n=invalid_dow)
        )

    # ------------------------------------------------------------------
    # 5. Card age (cardholder_age = card_age cast to int, not birth-based)
    # ------------------------------------------------------------------
    invalid_card_age = df_spark.filter(
        (F.col("cardholder_age") < 0) | (F.col("cardholder_age") > 50)
    ).count()
    if invalid_card_age > 0:
        failures.append(
            "expect_column_values_to_be_between [cardholder_age in 0-50]: "
            "{n} violation(s)".format(n=invalid_card_age)
        )

    # ------------------------------------------------------------------
    # 6. Distance
    # ------------------------------------------------------------------
    invalid_dist = df_spark.filter(F.col("distance_km") < 0).count()
    if invalid_dist > 0:
        failures.append(
            "expect_column_values_to_be_between [distance_km >= 0]: "
            "{n} violation(s)".format(n=invalid_dist)
        )

    # ------------------------------------------------------------------
    # 7. Transaction velocity
    # ------------------------------------------------------------------
    invalid_vel = df_spark.filter(F.col("txn_velocity_day") < 1).count()
    if invalid_vel > 0:
        failures.append(
            "expect_column_values_to_be_between [txn_velocity_day >= 1]: "
            "{n} violation(s)".format(n=invalid_vel)
        )

    # ------------------------------------------------------------------
    # 8. Amount bucket valid values
    # ------------------------------------------------------------------
    valid_buckets = ["low", "medium", "high", "very_high"]
    invalid_bucket = df_spark.filter(
        ~F.col("amt_bucket").isin(valid_buckets)
    ).count()
    if invalid_bucket > 0:
        failures.append(
            "expect_column_values_to_be_in_set [amt_bucket]: "
            "{n} invalid value(s)".format(n=invalid_bucket)
        )

    # ------------------------------------------------------------------
    # 9. Gender encoded binary
    # ------------------------------------------------------------------
    invalid_ge = df_spark.filter(
        ~F.col("gender_encoded").isin([0, 1])
    ).count()
    if invalid_ge > 0:
        failures.append(
            "expect_column_values_to_be_in_set [gender_encoded in 0,1]: "
            "{n} invalid value(s)".format(n=invalid_ge)
        )

    # ------------------------------------------------------------------
    # 10. Binary flags
    # ------------------------------------------------------------------
    for col in ["fraud_label", "is_weekend", "high_risk"]:
        invalid_flag = df_spark.filter(~F.col(col).isin([0, 1])).count()
        if invalid_flag > 0:
            failures.append(
                "expect_column_values_to_be_in_set [{col} in 0,1]: "
                "{n} invalid value(s)".format(col=col, n=invalid_flag)
            )

    # ------------------------------------------------------------------
    # 11. transaction_id uniqueness
    # ------------------------------------------------------------------
    total_rows = df_spark.count()
    distinct_ids = df_spark.select("transaction_id").distinct().count()
    dup_count = total_rows - distinct_ids
    if dup_count > 0:
        failures.append(
            "expect_column_values_to_be_unique [transaction_id]: "
            "{n} duplicate(s)".format(n=dup_count)
        )

    # ------------------------------------------------------------------
    # 12. Fraud rate sanity check
    # ------------------------------------------------------------------
    fraud_mean = df_spark.agg(
        F.mean("fraud_label").alias("m")
    ).first()["m"]

    if fraud_mean is None or not (0.0 <= fraud_mean <= 0.5):
        failures.append(
            "expect_column_mean_to_be_between [fraud_label mean in 0.0-0.5]: "
            "mean={m}".format(m=fraud_mean)
        )

    # ------------------------------------------------------------------
    # Print summary
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("  Data Quality – Validation Summary")
    print("=" * 60)
    print("  Evaluated  : {n}".format(n=12 + len(key_cols) - 1))
    print("  Passed     : {n}".format(n=(12 + len(key_cols) - 1) - len(failures)))
    print("  Failed     : {n}".format(n=len(failures)))
    for f in failures:
        print("  FAIL  {f}".format(f=f))
    print("=" * 60)
    print("  Overall result: {r}".format(r="PASS" if not failures else "FAIL"))
    print("=" * 60 + "\n")

    if failures:
        logger.error(
            "One or more data-quality checks failed – exiting with code 1."
        )
        sys.exit(1)

    logger.info("All data-quality checks passed.")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    """Build a SparkSession, read the curated Hive table, run quality checks."""
    from pyspark.sql import SparkSession

    logger.info("=== Data Quality Check – START ===")

    spark = (
        SparkSession.builder
        .appName("curated_data_quality")
        .enableHiveSupport()
        .config("spark.sql.hive.convertMetastoreParquet", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Reading curated table: %s", CURATED_TABLE)
    df = spark.table(CURATED_TABLE)
    row_count = df.count()
    logger.info("Row count: %d", row_count)

    run_quality_checks(df)

    logger.info("=== Data Quality Check – END ===")
    spark.stop()


if __name__ == "__main__":
    main()
