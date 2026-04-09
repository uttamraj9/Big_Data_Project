"""
full_load.py
============
Spark job that performs a **full** (overwrite) load of the
``bd_class_project.cc_fraud_trans`` raw Hive table into the
``bd_class_project.cc_fraud_trans_curated`` curated Hive table.

Steps
-----
1. Read the entire raw table.
2. Apply all 10 transformations via ``apply_all()``.
3. Overwrite the curated Hive table with the enriched data.

Usage
-----
spark-submit --master yarn --deploy-mode client \\
    --conf spark.sql.hive.convertMetastoreParquet=false \\
    full_load.py
"""

import logging
import sys

from pyspark.sql import SparkSession

from transformations import apply_all

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("curated_full_load")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

RAW_TABLE = "bd_class_project.cc_fraud_trans"
CURATED_TABLE = "bd_class_project.cc_fraud_trans_curated"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Entry point for the full-load Spark job."""

    logger.info("=== Curated Full Load – START ===")

    # ------------------------------------------------------------------
    # Build SparkSession with Hive support
    # ------------------------------------------------------------------
    spark = (
        SparkSession.builder
        .appName("curated_full_load")
        .enableHiveSupport()
        .config("spark.sql.hive.convertMetastoreParquet", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    # Read raw table
    # ------------------------------------------------------------------
    logger.info("Reading raw table: %s", RAW_TABLE)
    raw_df = spark.table(RAW_TABLE)
    raw_count = raw_df.count()
    logger.info("Raw row count: %d", raw_count)

    if raw_count == 0:
        logger.warning("Raw table is empty – nothing to process.")
        spark.stop()
        sys.exit(0)

    # ------------------------------------------------------------------
    # Apply transformations
    # ------------------------------------------------------------------
    logger.info("Applying all transformations …")
    curated_df = apply_all(raw_df)
    curated_count = curated_df.count()
    logger.info("Curated row count (after dedup/null handling): %d", curated_count)

    # ------------------------------------------------------------------
    # Write to Hive (overwrite)
    # ------------------------------------------------------------------
    logger.info("Writing to curated table: %s (mode=overwrite)", CURATED_TABLE)
    (
        curated_df.write
        .mode("overwrite")
        .format("hive")
        .saveAsTable(CURATED_TABLE)
    )
    logger.info("Write complete.")

    # ------------------------------------------------------------------
    # Final verification count
    # ------------------------------------------------------------------
    written_count = spark.table(CURATED_TABLE).count()
    logger.info("Rows now in curated table: %d", written_count)
    logger.info("=== Curated Full Load – END ===")

    spark.stop()


if __name__ == "__main__":
    main()
