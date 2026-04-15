#!/usr/bin/env python3
"""
test_predict_pipeline.py
========================
End-to-end integration test for the HBase → ML prediction pipeline.

Tests run inside the Docker container (model is mounted at /app/model).
Each test is independent and reports PASS/FAIL clearly.

Run:
  spark-submit --master local[*] test_predict_pipeline.py

Environment variables (same as predict_from_hbase.py):
  HBASE_HOST, HBASE_PORT, MODEL_PATH, HIVE_METASTORE_URIS
"""
import os
import sys

# ── Sample rows mimicking what the Kafka → HBase consumer stores ─────────────
# These are real records from the FastAPI endpoint, normalised to lowercase.
# (HBase stores everything as strings — we reproduce that here.)
MOCK_HBASE_ROWS = [
    {
        "transaction_id": "TXN_TEST_001",
        "user_id": "USER_6341",
        "transaction_amount": "1.81",
        "transaction_type": "Bank Transfer",
        "timestamp": "2023-10-19 20:16:00",
        "account_balance": "59442.18",
        "device_type": "Mobile",
        "location": "New York",
        "merchant_category": "Clothing",
        "ip_address_flag": "0",
        "previous_fraudulent_activity": "0",
        "daily_transaction_count": "2",
        "avg_transaction_amount_7d": "227.56",
        "failed_transaction_count_7d": "0",
        "card_type": "Discover",
        "card_age": "9",
        "transaction_distance": "4078.81",
        "authentication_method": "Password",
        "risk_score": "0.2178",
        "is_weekend": "0",
    },
    {
        "transaction_id": "TXN_TEST_002",
        "user_id": "USER_6765",
        "transaction_amount": "34.66",
        "transaction_type": "Online",
        "timestamp": "2023-10-19 20:32:00",
        "account_balance": "89813.31",
        "device_type": "Laptop",
        "location": "Sydney",
        "merchant_category": "Electronics",
        "ip_address_flag": "0",
        "previous_fraudulent_activity": "0",
        "daily_transaction_count": "13",
        "avg_transaction_amount_7d": "276.23",
        "failed_transaction_count_7d": "2",
        "card_type": "Visa",
        "card_age": "55",
        "transaction_distance": "3791.46",
        "authentication_method": "OTP",
        "risk_score": "0.8502",
        "is_weekend": "0",
    },
    {
        # Edge case: values needing normalisation ("Atm Withdrawal", "New_york")
        "transaction_id": "TXN_TEST_003",
        "user_id": "USER_9999",
        "transaction_amount": "999.99",
        "transaction_type": "ATM Withdrawal",
        "timestamp": "2023-10-20 01:00:00",
        "account_balance": "12345.67",
        "device_type": "Tablet",
        "location": "Tokyo",
        "merchant_category": "Groceries",
        "ip_address_flag": "1",
        "previous_fraudulent_activity": "1",
        "daily_transaction_count": "20",
        "avg_transaction_amount_7d": "500.0",
        "failed_transaction_count_7d": "3",
        "card_type": "Amex",
        "card_age": "120",
        "transaction_distance": "15.0",
        "authentication_method": "Biometric",
        "risk_score": "0.95",
        "is_weekend": "1",
    },
]

RESULTS = []


def ok(name):
    RESULTS.append(("PASS", name))
    print(f"  ✓  PASS  {name}")


def fail(name, reason):
    RESULTS.append(("FAIL", name))
    print(f"  ✗  FAIL  {name}")
    print(f"           {reason}")


# ── Test 1: HBase Thrift connectivity ─────────────────────────────────────────
def test_hbase_connectivity():
    name = "HBase Thrift connectivity"
    host = os.environ.get("HBASE_HOST", "172.31.6.42")
    port = int(os.environ.get("HBASE_PORT", "9090"))
    try:
        import happybase
        conn = happybase.Connection(host=host, port=port, timeout=5000)
        tables = conn.tables()
        table_names = [t.decode() for t in tables]
        conn.close()
        if "cc_fraud_realtime" in table_names:
            ok(f"{name}  →  cc_fraud_realtime found ✓")
        else:
            fail(name, f"Connected but cc_fraud_realtime not found. Tables: {table_names}")
    except Exception as e:
        fail(name,
             f"Cannot connect to HBase Thrift at {host}:{port}: {e}\n"
             f"           → ACTION REQUIRED: Start HBase Thrift Server in Cloudera Manager\n"
             f"           → Clusters → HBase → HBase Thrift Server → Start")


# ── Test 2: Spark initialises correctly ───────────────────────────────────────
def test_spark_init(spark):
    name = "Spark initialisation"
    try:
        assert spark.version is not None
        ok(f"{name}  →  Spark {spark.version}")
    except Exception as e:
        fail(name, str(e))


# ── Test 3: Schema creation ────────────────────────────────────────────────────
def test_schema_creation(spark):
    name = "Schema + DataFrame creation from mock HBase rows"
    try:
        from predict_from_hbase import RAW_SCHEMA, HBASE_COLUMNS
        schema_cols = {f.name for f in RAW_SCHEMA.fields}
        clean_rows = [{k: v for k, v in r.items() if k in schema_cols} for r in MOCK_HBASE_ROWS]
        df = spark.createDataFrame(clean_rows, schema=RAW_SCHEMA)
        assert df.count() == 3, f"Expected 3 rows, got {df.count()}"
        ok(f"{name}  →  {df.count()} rows, {len(df.columns)} columns")
    except Exception as e:
        fail(name, str(e))
        raise  # propagate — downstream tests depend on this


# ── Test 4: Feature engineering ───────────────────────────────────────────────
def test_feature_engineering(spark):
    name = "Feature engineering (OHE, timestamp extraction, casting)"
    try:
        from predict_from_hbase import RAW_SCHEMA, engineer_features, CATEGORIES

        schema_cols = {f.name for f in RAW_SCHEMA.fields}
        clean_rows = [{k: v for k, v in r.items() if k in schema_cols} for r in MOCK_HBASE_ROWS]
        raw_df = spark.createDataFrame(clean_rows, schema=RAW_SCHEMA)

        feat_df = engineer_features(raw_df)

        # Verify OHE columns exist
        expected_ohe = []
        for cat, vals in CATEGORIES.items():
            for v in vals:
                expected_ohe.append(f"{cat}_{v}")

        missing = [c for c in expected_ohe if c not in feat_df.columns]
        assert not missing, f"Missing OHE columns: {missing}"

        # Verify timestamp features
        for tc in ["hour", "dayofweek", "dayofmonth"]:
            assert tc in feat_df.columns, f"Missing timestamp feature: {tc}"

        # Verify user_id is int (not string)
        user_id_type = dict(feat_df.dtypes)["user_id"]
        assert user_id_type == "int", f"user_id should be int, got {user_id_type}"

        # Verify categorical strings dropped
        for cat in CATEGORIES:
            assert cat not in feat_df.columns, f"Categorical column '{cat}' should be dropped"

        ok(f"{name}  →  {len(feat_df.columns)} feature columns, OHE correct, user_id=int")

        # Show a few columns for visual confirmation
        feat_df.select(
            "transaction_id", "user_id", "hour", "dayofweek",
            "transaction_type_pos", "transaction_type_bank_transfer",
            "location_new_york", "location_tokyo"
        ).show(3, truncate=False)

        return feat_df

    except Exception as e:
        fail(name, str(e))
        return None


# ── Test 5: Model loading ──────────────────────────────────────────────────────
def test_model_loading():
    name = "Model loading from hostPath"
    model_path = os.environ.get("MODEL_PATH", "file:///app/model")
    try:
        from pyspark.ml import PipelineModel
        pipeline = PipelineModel.load(model_path)
        stages = pipeline.stages
        ok(f"{name}  →  {len(stages)} stages: {[type(s).__name__ for s in stages]}")
        return pipeline
    except Exception as e:
        fail(name, f"{e}\n           → Check that /app/model is mounted correctly")
        return None


# ── Test 6: End-to-end inference ──────────────────────────────────────────────
def test_inference(spark, pipeline, feat_df):
    name = "End-to-end inference (feature engineering → model → prediction)"
    if pipeline is None or feat_df is None:
        fail(name, "Skipped — model or feature DataFrame not available")
        return
    try:
        scored = pipeline.transform(feat_df)
        assert "prediction" in scored.columns, "'prediction' column missing from scored DataFrame"

        preds = scored.select("transaction_id", "prediction", "probability")
        preds.show(truncate=False)

        count = preds.count()
        assert count == len(MOCK_HBASE_ROWS), f"Expected {len(MOCK_HBASE_ROWS)} predictions, got {count}"
        ok(f"{name}  →  {count} predictions generated successfully")
    except Exception as e:
        fail(name, str(e))


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    from pyspark.sql import SparkSession

    hive_uri = os.environ.get("HIVE_METASTORE_URIS", "thrift://172.31.6.42:9083")

    spark = (
        SparkSession.builder
          .appName("test-predict-pipeline")
          .config("hive.metastore.uris", hive_uri)
          .enableHiveSupport()
          .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    print("\n" + "=" * 60)
    print("  ML Prediction Pipeline — Integration Tests")
    print("=" * 60 + "\n")

    test_hbase_connectivity()            # Test 1
    test_spark_init(spark)               # Test 2
    test_schema_creation(spark)          # Test 3
    feat_df  = test_feature_engineering(spark)   # Test 4
    pipeline = test_model_loading()              # Test 5
    test_inference(spark, pipeline, feat_df)     # Test 6

    spark.stop()

    print("\n" + "=" * 60)
    passed = sum(1 for r, _ in RESULTS if r == "PASS")
    failed = sum(1 for r, _ in RESULTS if r == "FAIL")
    print(f"  Results: {passed} PASSED  /  {failed} FAILED  /  {len(RESULTS)} total")
    print("=" * 60 + "\n")

    if failed:
        for r, name in RESULTS:
            if r == "FAIL":
                print(f"  FAILED: {name}")
        sys.exit(1)


if __name__ == "__main__":
    main()
