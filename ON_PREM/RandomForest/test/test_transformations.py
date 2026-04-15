import pytest
from pyspark.sql import SparkSession, Row
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.sql.functions import regexp_replace, udf
from pyspark.sql.types import ArrayType, DoubleType

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("TestETL").getOrCreate()

def test_pipeline_transforms(spark):
    # Sample mock data
    data = [
        Row(
            transaction_id="TXN001",
            user_id="USER_1001",
            transaction_type="purchase",
            device_type="mobile",
            location="NY",
            merchant_category="grocery",
            card_type="debit",
            authentication_method="pin"
        ),
        Row(
            transaction_id="TXN002",
            user_id="USER_1002",
            transaction_type="refund",
            device_type="web",
            location="CA",
            merchant_category="electronics",
            card_type="credit",
            authentication_method="signature"
        )
    ]

    df = spark.createDataFrame(data)

    # Transformation: clean user_id
    df = df.withColumn("user_id", regexp_replace("user_id", "USER_", "").cast("int"))

    categorical_cols = [
        "transaction_type",
        "device_type",
        "location",
        "merchant_category",
        "card_type",
        "authentication_method"
    ]

    # Apply StringIndexer and OneHotEncoder
    indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_Index", handleInvalid="keep") for col in categorical_cols]
    encoders = [OneHotEncoder(inputCol=f"{col}_Index", outputCol=f"{col}_vec") for col in categorical_cols]

    pipeline = Pipeline(stages=indexers + encoders)
    model = pipeline.fit(df)
    df = model.transform(df)

    # Convert vector to array for testing/inspection
    vector_to_array_udf = udf(lambda v: v.toArray().tolist() if v else None, ArrayType(DoubleType()))
    vec_cols = [f"{col}_vec" for col in categorical_cols]

    for vec in vec_cols:
        df = df.withColumn(vec.replace("_vec", "_arr"), vector_to_array_udf(vec))
        df = df.drop(vec)

    # Drop original categorical columns
    df = df.drop(*categorical_cols)

    # Assert transformed columns exist
    expected_cols = [f"{col}_arr" for col in categorical_cols] + ["user_id", "transaction_id"]
    actual_cols = df.columns

    for col_name in expected_cols:
        assert col_name in actual_cols, f"Missing expected column: {col_name}"

    # Optional: show dataframe for debugging
    # df.show(truncate=False)
