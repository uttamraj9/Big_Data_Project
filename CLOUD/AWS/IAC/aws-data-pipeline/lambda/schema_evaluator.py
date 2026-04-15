"""
Lambda – Schema Evaluator
Triggered by S3 PutObject on the Bronze bucket.
- Reads the Parquet file schema
- Compares against expected schema stored in S3
- Starts Glue ETL job if schema is valid
- Sends SNS alert if schema mismatch detected
"""
import json
import os
import boto3
import logging
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3     = boto3.client("s3")
glue   = boto3.client("glue")
sns    = boto3.client("sns")

GLUE_JOB_NAME     = os.environ["GLUE_JOB_NAME"]
SNS_TOPIC_ARN     = os.environ.get("SNS_TOPIC_ARN", "")
SCHEMA_BUCKET     = os.environ["SCHEMA_BUCKET"]
SCHEMA_KEY        = os.environ.get("SCHEMA_KEY", "schemas/expected_schema.json")
TARGET_BUCKET     = os.environ["TARGET_BUCKET"]


def load_expected_schema() -> dict:
    """Load expected schema definition from S3."""
    try:
        obj = s3.get_object(Bucket=SCHEMA_BUCKET, Key=SCHEMA_KEY)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        logger.warning("No expected schema found – skipping validation.")
        return {}


def get_parquet_schema(bucket: str, key: str) -> dict:
    """Read Parquet metadata and return column→type mapping."""
    try:
        import pyarrow.parquet as pq
        import pyarrow.fs as pafs

        s3fs = pafs.S3FileSystem()
        pf   = pq.ParquetFile(f"{bucket}/{key}", filesystem=s3fs)
        schema = pf.schema_arrow
        return {field.name: str(field.type) for field in schema}
    except Exception as e:
        logger.error(f"Could not read parquet schema: {e}")
        return {}


def validate_schema(actual: dict, expected: dict) -> tuple[bool, list]:
    """Return (is_valid, list_of_issues)."""
    if not expected:
        return True, []

    issues = []
    for col, expected_type in expected.items():
        if col not in actual:
            issues.append(f"Missing column: {col}")
        elif actual[col] != expected_type:
            issues.append(f"Type mismatch on '{col}': expected={expected_type}, got={actual[col]}")

    return len(issues) == 0, issues


def trigger_glue_job(source_bucket: str, source_prefix: str):
    """Start the Glue ETL job."""
    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--SOURCE_BUCKET": source_bucket,
            "--TARGET_BUCKET": TARGET_BUCKET,
            "--SOURCE_PREFIX": source_prefix,
            "--TARGET_PREFIX": "transformed/",
        }
    )
    run_id = response["JobRunId"]
    logger.info(f"Started Glue job '{GLUE_JOB_NAME}', run ID: {run_id}")
    return run_id


def send_alert(subject: str, message: str):
    """Publish to SNS topic."""
    if not SNS_TOPIC_ARN:
        logger.warning("SNS_TOPIC_ARN not set – skipping alert.")
        return
    sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=message)


def lambda_handler(event, context):
    results = []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key    = unquote_plus(record["s3"]["object"]["key"])

        logger.info(f"Processing s3://{bucket}/{key}")

        # Only process Parquet files
        if not key.endswith(".parquet"):
            logger.info("Skipping non-parquet file.")
            continue

        # Schema evaluation
        actual_schema   = get_parquet_schema(bucket, key)
        expected_schema = load_expected_schema()
        is_valid, issues = validate_schema(actual_schema, expected_schema)

        if not is_valid:
            msg = f"Schema mismatch for s3://{bucket}/{key}\n" + "\n".join(issues)
            logger.error(msg)
            send_alert(subject="[PIPELINE] Schema Mismatch Detected", message=msg)
            results.append({"file": key, "status": "schema_error", "issues": issues})
            continue

        # Trigger Glue job
        prefix = "/".join(key.split("/")[:-1]) + "/"
        run_id = trigger_glue_job(bucket, prefix)
        results.append({"file": key, "status": "glue_triggered", "run_id": run_id})

    return {"statusCode": 200, "body": json.dumps(results)}
