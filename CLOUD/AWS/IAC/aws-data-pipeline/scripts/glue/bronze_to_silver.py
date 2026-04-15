import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_BUCKET',
    'TARGET_BUCKET',
    'SOURCE_PREFIX',
    'TARGET_PREFIX'
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_path = f"s3://{args['SOURCE_BUCKET']}/{args['SOURCE_PREFIX']}"
target_path = f"s3://{args['TARGET_BUCKET']}/{args['TARGET_PREFIX']}"

logger.info(f"Reading from: {source_path}")
logger.info(f"Writing to:   {target_path}")

# ─── Read Bronze (Parquet files written by DMS) ───────────────────────────────
df = spark.read \
    .option("mergeSchema", "true") \
    .parquet(source_path)

logger.info(f"Rows read from Bronze: {df.count()}")
logger.info(f"Schema: {df.schema.simpleString()}")

# ─── Transformations ──────────────────────────────────────────────────────────

# 1. Drop DMS metadata columns
dms_meta_cols = ['Op', 'schema_name', 'table_name']
cols_to_drop  = [c for c in dms_meta_cols if c in df.columns]
df = df.drop(*cols_to_drop)

# 2. Trim whitespace on all string columns
for col_name, dtype in df.dtypes:
    if dtype == 'string':
        df = df.withColumn(col_name, F.trim(F.col(col_name)))

# 3. Drop fully null rows
df = df.dropna(how='all')

# 4. Add audit columns
df = df \
    .withColumn('_ingestion_ts', F.current_timestamp()) \
    .withColumn('_source_bucket', F.lit(args['SOURCE_BUCKET'])) \
    .withColumn('_pipeline_job', F.lit(args['JOB_NAME']))

# 5. Deduplicate (keep latest based on ingestion order)
df = df.dropDuplicates()

logger.info(f"Rows after transformation: {df.count()}")

# ─── Write Silver (Parquet, partitioned by date) ──────────────────────────────
df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(target_path)

logger.info("Silver layer write complete.")

job.commit()
