"""
incremental_load.py
===================
Spark job that performs an **incremental** (append) load from the
``bd_class_project.cc_fraud_trans`` raw Hive table into the
``bd_class_project.cc_fraud_trans_curated`` curated Hive table.

Watermark strategy
------------------
The job reads the maximum ``Timestamp`` value already present in the curated
table.  If the curated table does not yet exist (or is empty), it falls back
to the epoch sentinel ``1970-01-01 00:00:00``.  Only raw rows whose
``Timestamp`` is strictly greater than the watermark are processed.

Steps
-----
1. Determine the watermark from ``MAX(Timestamp)`` in the curated table.
2. Read raw rows where ``Timestamp > watermark``.
3. Apply all 10 transformations via ``apply_all()``.
4. Append the enriched rows to the curated Hive table.

Usage
-----
spark-submit --master yarn --deploy-mode client \\
    --conf spark.sql.hive.convertMetastoreParquet=false \\
    incremental_load.py
"""

import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from transformations import apply_all

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("curated_incremental_load")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

RAW_TABLE = "bd_class_project.cc_fraud_trans"
CURATED_TABLE = "bd_class_project.cc_fraud_trans_curated"
FALLBACK_WATERMARK = "1970-01-01 00:00:00"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_watermark(spark: SparkSession) -> str:
    """Return the maximum ``Timestamp`` already written to the curated table.

    If the curated table does not exist, or it contains no rows, the function
    returns the sentinel value ``"1970-01-01 00:00:00"`` so that the
    subsequent read pulls the entire raw table on the first run.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session with Hive support enabled.

    Returns
    -------
    str
        ISO-formatted timestamp string, e.g. ``"2024-03-15 14:22:05"``.
    """
    try:
        row = spark.sql(
            f"SELECT MAX(Timestamp) AS max_ts FROM {CURATED_TABLE}"
        ).first()
        if row and row["max_ts"] is not None:
            watermark = str(row["max_ts"])
            logger.info("Watermark from curated table: %s", watermark)
            return watermark
        else:
            logger.info(
                "Curated table empty – using fallback watermark: %s",
                FALLBACK_WATERMARK,
            )
            return FALLBACK_WATERMARK
    except Exception as exc:  # table does not exist yet
        logger.warning(
            "Could not query curated table (%s) – using fallback watermark: %s",
            exc,
            FALLBACK_WATERMARK,
        )
        return FALLBACK_WATERMARK


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Entry point for the incremental-load Spark job."""

    logger.info("=== Curated Incremental Load – START ===")

    # ------------------------------------------------------------------
    # Build SparkSession with Hive support
    # ------------------------------------------------------------------
    spark = (
        SparkSession.builder
        .appName("curated_incremental_load")
        .enableHiveSupport()
        .config("spark.sql.hive.convertMetastoreParquet", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    # Determine watermark
    # ------------------------------------------------------------------
    watermark = get_watermark(spark)

    # ------------------------------------------------------------------
    # Read only new raw rows
    # ------------------------------------------------------------------
    logger.info("Reading raw table: %s  (Timestamp > '%s')", RAW_TABLE, watermark)
    raw_df = (
        spark.table(RAW_TABLE)
        .filter(F.col("Timestamp") > F.lit(watermark).cast("timestamp"))
    )
    raw_count = raw_df.count()
    logger.info("New raw rows to process: %d", raw_count)

    if raw_count == 0:
        logger.info("No new data since watermark – exiting.")
        spark.stop()
        sys.exit(0)

    # ------------------------------------------------------------------
    # Apply transformations
    # ------------------------------------------------------------------
    logger.info("Applying all transformations …")
    curated_df = apply_all(raw_df)
    curated_count = curated_df.count()
    logger.info("Transformed row count: %d", curated_count)

    # ------------------------------------------------------------------
    # Append to Hive curated table
    # ------------------------------------------------------------------
    logger.info("Appending to curated table: %s (mode=append)", CURATED_TABLE)
    (
        curated_df.write
        .mode("append")
        .format("hive")
        .saveAsTable(CURATED_TABLE)
    )
    logger.info("Append complete.")

    # ------------------------------------------------------------------
    # Final verification count
    # ------------------------------------------------------------------
    written_count = spark.table(CURATED_TABLE).count()
    logger.info("Total rows now in curated table: %d", written_count)
    logger.info("=== Curated Incremental Load – END ===")

    spark.stop()


if __name__ == "__main__":
    main()
