"""
great_expectations_suite.py
============================
Data-quality validation for the ``bd_class_project.cc_fraud_trans_curated``
Hive table using Great Expectations v0.17+.

The function ``run_quality_checks(df_spark)`` converts the Spark DataFrame
to a GX-compatible asset, runs a suite of expectations, prints a pass/fail
summary, and exits with code 1 if any expectation fails.

``main()`` builds a SparkSession with Hive support, reads the curated table,
and delegates to ``run_quality_checks``.

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

def run_quality_checks(df_spark) -> None:
    """Validate a curated-layer Spark DataFrame using Great Expectations.

    The function uses an in-memory (Ephemeral) GX context so that no local
    filesystem configuration is required.  All expectations are added to a
    single suite and validated in one pass.

    Expectations applied
    ~~~~~~~~~~~~~~~~~~~~
    * Row count >= 1
    * Not-null: ``trans_num``, ``Timestamp``, ``cc_num``, ``amt``,
      ``is_fraud``
    * ``amt >= 0``, ``amt_log >= 0``
    * ``txn_hour`` in [0, 23], ``txn_month`` in [1, 12],
      ``txn_day_of_week`` in [1, 7]
    * ``cardholder_age`` in [18, 120]
    * ``distance_km >= 0``
    * ``txn_velocity_day >= 1``
    * ``amt_bucket`` in {``low``, ``medium``, ``high``, ``very_high``}
    * ``gender_encoded`` in {-1, 0, 1}
    * ``is_fraud`` in {0, 1}, ``is_weekend`` in {0, 1},
      ``high_risk`` in {0, 1}
    * ``trans_num`` values are unique
    * Mean of ``is_fraud`` in [0.0, 0.5]

    Parameters
    ----------
    df_spark : pyspark.sql.DataFrame
        Fully-transformed curated DataFrame.

    Raises / Exits
    --------------
    Calls ``sys.exit(1)`` if any expectation fails; prints a summary either
    way.
    """
    import great_expectations as gx
    from great_expectations.core.batch import RuntimeBatchRequest

    logger.info("Initialising Great Expectations in-memory context …")

    # ------------------------------------------------------------------
    # Build an ephemeral (in-memory) GX context – no filesystem needed
    # ------------------------------------------------------------------
    context = gx.get_context(mode="ephemeral")

    # ------------------------------------------------------------------
    # Add a Spark datasource
    # ------------------------------------------------------------------
    datasource = context.sources.add_spark(name="spark_curated_source")
    data_asset = datasource.add_dataframe_asset(name="cc_fraud_trans_curated")

    # ------------------------------------------------------------------
    # Build a batch request from the Spark DataFrame
    # ------------------------------------------------------------------
    batch_request = data_asset.build_batch_request(dataframe=df_spark)

    # ------------------------------------------------------------------
    # Create an expectation suite
    # ------------------------------------------------------------------
    suite_name = "cc_fraud_trans_curated_suite"
    suite = context.add_or_update_expectation_suite(suite_name)

    # ------------------------------------------------------------------
    # Add a validator and attach expectations
    # ------------------------------------------------------------------
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite,
    )

    logger.info("Adding expectations …")

    # 1. Row count
    validator.expect_table_row_count_to_be_between(min_value=1)

    # 2. Not-null checks for key columns
    for col in ["trans_num", "Timestamp", "cc_num", "amt", "is_fraud"]:
        validator.expect_column_values_to_not_be_null(column=col)

    # 3. Amount validations
    validator.expect_column_values_to_be_between(
        column="amt", min_value=0, mostly=1.0
    )
    validator.expect_column_values_to_be_between(
        column="amt_log", min_value=0, mostly=1.0
    )

    # 4. Time features
    validator.expect_column_values_to_be_between(
        column="txn_hour", min_value=0, max_value=23
    )
    validator.expect_column_values_to_be_between(
        column="txn_month", min_value=1, max_value=12
    )
    validator.expect_column_values_to_be_between(
        column="txn_day_of_week", min_value=1, max_value=7
    )

    # 5. Cardholder age
    validator.expect_column_values_to_be_between(
        column="cardholder_age", min_value=18, max_value=120
    )

    # 6. Distance
    validator.expect_column_values_to_be_between(
        column="distance_km", min_value=0, mostly=1.0
    )

    # 7. Transaction velocity
    validator.expect_column_values_to_be_between(
        column="txn_velocity_day", min_value=1
    )

    # 8. Amount bucket
    validator.expect_column_values_to_be_in_set(
        column="amt_bucket",
        value_set=["low", "medium", "high", "very_high"],
    )

    # 9. Gender encoded
    validator.expect_column_values_to_be_in_set(
        column="gender_encoded",
        value_set=[-1, 0, 1],
    )

    # 10. Binary flags
    for col in ["is_fraud", "is_weekend", "high_risk"]:
        validator.expect_column_values_to_be_in_set(
            column=col,
            value_set=[0, 1],
        )

    # 11. Unique trans_num
    validator.expect_column_values_to_be_unique(column="trans_num")

    # 12. Fraud rate
    validator.expect_column_mean_to_be_between(
        column="is_fraud", min_value=0.0, max_value=0.5
    )

    # ------------------------------------------------------------------
    # Save suite and run validation
    # ------------------------------------------------------------------
    validator.save_expectation_suite(discard_failed_expectations=False)

    logger.info("Running validation …")
    checkpoint = context.add_or_update_checkpoint(
        name="curated_checkpoint",
        validator=validator,
    )
    results = checkpoint.run()

    # ------------------------------------------------------------------
    # Print summary
    # ------------------------------------------------------------------
    overall_success = results.success
    print("\n" + "=" * 60)
    print("  Great Expectations – Validation Summary")
    print("=" * 60)

    for run_result in results.run_results.values():
        validation_result = run_result.get("validation_result", {})
        stats = validation_result.get("statistics", {})
        evaluated = stats.get("evaluated_expectations", "?")
        successful = stats.get("successful_expectations", "?")
        failed = stats.get("unsuccessful_expectations", "?")
        print(f"  Evaluated  : {evaluated}")
        print(f"  Passed     : {successful}")
        print(f"  Failed     : {failed}")

        # Print individual failures
        for er in validation_result.get("results", []):
            if not er.get("success", True):
                exp_type = er.get("expectation_config", {}).get(
                    "expectation_type", "unknown"
                )
                col = er.get("expectation_config", {}).get("kwargs", {}).get(
                    "column", "table-level"
                )
                print(f"  FAIL  [{col}]  {exp_type}")

    print("=" * 60)
    print(f"  Overall result: {'PASS' if overall_success else 'FAIL'}")
    print("=" * 60 + "\n")

    if not overall_success:
        logger.error("One or more data-quality checks failed – exiting with code 1.")
        sys.exit(1)

    logger.info("All data-quality checks passed.")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
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
