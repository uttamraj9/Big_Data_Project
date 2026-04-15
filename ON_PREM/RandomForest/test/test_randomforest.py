import pytest
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("TestRandomForestPipeline").getOrCreate()

@pytest.fixture
def sample_df(spark):
    # Minimal dummy data to validate pipeline structure
    data = [
        (1, 200.0, 5000.0, 2.0, 1.0, 0.2, 0, 0.9, 0.0, 3, 0.5,
         Vectors.dense([0.0, 1.0]), Vectors.dense([0.1, 0.9]),
         Vectors.dense([0.2, 0.8]), Vectors.dense([0.3, 0.7]),
         Vectors.dense([0.4, 0.6]), Vectors.dense([0.5, 0.5]), 1.0),
        (2, 100.0, 3000.0, 1.0, 1.0, 0.1, 1, 0.6, 1.0, 1, 1.5,
         Vectors.dense([1.0, 0.0]), Vectors.dense([0.8, 0.2]),
         Vectors.dense([0.7, 0.3]), Vectors.dense([0.6, 0.4]),
         Vectors.dense([0.5, 0.5]), Vectors.dense([0.4, 0.6]), 0.0)
    ]

    columns = ['transaction_amount', 'account_balance', 'card_age', 'ip_address_flag',
               'avg_transaction_amount_7d', 'risk_score', 'is_weekend', 'risk_score_dup',
               'previous_fraudulent_activity', 'daily_transaction_count', 'transaction_distance',
               'device_type_vec', 'merchant_category_vec', 'card_type_vec', 'location_vec',
               'authentication_method_vec', 'transaction_type_vec', 'fraud_label']
    return spark.createDataFrame(data, schema=columns)

def test_pipeline_training(sample_df):
    feature_cols = ['transaction_amount', 'account_balance', 'card_age', 'ip_address_flag',
                    'avg_transaction_amount_7d', 'risk_score', 'is_weekend', 'risk_score_dup',
                    'previous_fraudulent_activity', 'daily_transaction_count', 'transaction_distance',
                    'device_type_vec', 'merchant_category_vec', 'card_type_vec', 'location_vec',
                    'authentication_method_vec', 'transaction_type_vec']

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    rf = RandomForestClassifier(labelCol="fraud_label", featuresCol="features", numTrees=10)

    pipeline = Pipeline(stages=[assembler, rf])

    model = pipeline.fit(sample_df)
    predictions = model.transform(sample_df)

    assert predictions.filter(col("prediction").isNotNull()).count() > 0, "Predictions should not be null"

def test_metrics(sample_df):
    feature_cols = ['transaction_amount', 'account_balance', 'card_age', 'ip_address_flag',
                    'avg_transaction_amount_7d', 'risk_score', 'is_weekend', 'risk_score_dup',
                    'previous_fraudulent_activity', 'daily_transaction_count', 'transaction_distance',
                    'device_type_vec', 'merchant_category_vec', 'card_type_vec', 'location_vec',
                    'authentication_method_vec', 'transaction_type_vec']

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    rf = RandomForestClassifier(labelCol="fraud_label", featuresCol="features", numTrees=10)

    pipeline = Pipeline(stages=[assembler, rf])
    model = pipeline.fit(sample_df)
    predictions = model.transform(sample_df)

    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    evaluator = BinaryClassificationEvaluator(labelCol="fraud_label", rawPredictionCol="rawPrediction")
    auc = evaluator.evaluate(predictions)

    assert 0 <= auc <= 1, f"AUC score should be in [0,1], got {auc}"
