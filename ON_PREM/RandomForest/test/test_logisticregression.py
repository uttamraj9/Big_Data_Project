import pytest
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import col
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("TestLogisticRegression").getOrCreate()

# @pytest.fixture
# def sample_df(spark):
#     data = [
#         (1, 100.0, 3000.0, 1.0, 0.5, 0.1, 0, 0.7, 0.0, 2, 1.2,
#          Vectors.dense([0.1, 0.2]), Vectors.dense([0.3, 0.4]),
#          Vectors.dense([0.5, 0.6]), Vectors.dense([0.7, 0.8]),
#          Vectors.dense([0.9, 0.1]), Vectors.dense([0.2, 0.3]), 1.0),
#         (2, 150.0, 2500.0, 0.0, 0.3, 0.3, 1, 0.4, 1.0, 1, 1.5,
#          Vectors.dense([0.4, 0.3]), Vectors.dense([0.6, 0.2]),
#          Vectors.dense([0.7, 0.1]), Vectors.dense([0.8, 0.6]),
#          Vectors.dense([0.1, 0.9]), Vectors.dense([0.5, 0.5]), 0.0),
#     ]

#     columns = ['transaction_amount', 'account_balance', 'card_age', 'ip_address_flag',
#                'avg_transaction_amount_7d', 'risk_score', 'is_weekend', 'risk_score_dup',
#                'previous_fraudulent_activity', 'daily_transaction_count', 'transaction_distance',
#                'device_type_vec', 'merchant_category_vec', 'card_type_vec', 'location_vec',
#                'authentication_method_vec', 'transaction_type_vec', 'fraud_label']
#     return spark.createDataFrame(data, schema=columns)

@pytest.fixture
def sample_df(spark):
    data = [
        (1, 100.0, 3000.0, 1.0, 0.5, 0.1, 0, 0.7, 0.0, 2, 1.2,
         Vectors.dense([0.1, 0.2]), Vectors.dense([0.3, 0.4]),
         Vectors.dense([0.5, 0.6]), Vectors.dense([0.7, 0.8]),
         Vectors.dense([0.9, 0.1]), Vectors.dense([0.2, 0.3]), 1.0),
        (2, 150.0, 2500.0, 0.0, 0.3, 0.3, 1, 0.4, 1.0, 1, 1.5,
         Vectors.dense([0.4, 0.3]), Vectors.dense([0.6, 0.2]),
         Vectors.dense([0.7, 0.1]), Vectors.dense([0.8, 0.6]),
         Vectors.dense([0.1, 0.9]), Vectors.dense([0.5, 0.5]), 0.0),
        (3, 200.0, 2700.0, 1.0, 0.6, 0.2, 0, 0.5, 0.0, 3, 0.9,
         Vectors.dense([0.2, 0.1]), Vectors.dense([0.1, 0.5]),
         Vectors.dense([0.3, 0.4]), Vectors.dense([0.5, 0.5]),
         Vectors.dense([0.7, 0.2]), Vectors.dense([0.3, 0.6]), 1.0),
        (4, 120.0, 3100.0, 0.0, 0.4, 0.4, 1, 0.6, 1.0, 2, 1.0,
         Vectors.dense([0.3, 0.2]), Vectors.dense([0.7, 0.3]),
         Vectors.dense([0.8, 0.4]), Vectors.dense([0.9, 0.7]),
         Vectors.dense([0.2, 0.8]), Vectors.dense([0.6, 0.4]), 0.0),
        (5, 180.0, 2900.0, 1.0, 0.7, 0.2, 0, 0.8, 0.0, 3, 1.3,
         Vectors.dense([0.5, 0.1]), Vectors.dense([0.4, 0.6]),
         Vectors.dense([0.2, 0.7]), Vectors.dense([0.6, 0.2]),
         Vectors.dense([0.3, 0.9]), Vectors.dense([0.4, 0.4]), 1.0),
        (6, 160.0, 2600.0, 0.0, 0.5, 0.3, 1, 0.5, 1.0, 1, 1.1,
         Vectors.dense([0.6, 0.3]), Vectors.dense([0.3, 0.5]),
         Vectors.dense([0.5, 0.5]), Vectors.dense([0.7, 0.3]),
         Vectors.dense([0.4, 0.6]), Vectors.dense([0.7, 0.3]), 0.0),
        (7, 130.0, 2800.0, 1.0, 0.8, 0.2, 0, 0.6, 0.0, 2, 1.4,
         Vectors.dense([0.2, 0.4]), Vectors.dense([0.2, 0.4]),
         Vectors.dense([0.4, 0.4]), Vectors.dense([0.5, 0.6]),
         Vectors.dense([0.6, 0.1]), Vectors.dense([0.2, 0.7]), 1.0),
        (8, 110.0, 3200.0, 0.0, 0.6, 0.3, 1, 0.7, 1.0, 1, 0.8,
         Vectors.dense([0.1, 0.3]), Vectors.dense([0.8, 0.1]),
         Vectors.dense([0.9, 0.2]), Vectors.dense([0.8, 0.9]),
         Vectors.dense([0.2, 0.5]), Vectors.dense([0.6, 0.6]), 0.0),
        (9, 140.0, 3300.0, 1.0, 0.7, 0.1, 0, 0.9, 0.0, 2, 1.2,
         Vectors.dense([0.7, 0.5]), Vectors.dense([0.5, 0.6]),
         Vectors.dense([0.6, 0.6]), Vectors.dense([0.9, 0.2]),
         Vectors.dense([0.1, 0.8]), Vectors.dense([0.3, 0.3]), 1.0),
        (10, 170.0, 2400.0, 0.0, 0.4, 0.4, 1, 0.4, 1.0, 1, 1.6,
         Vectors.dense([0.6, 0.2]), Vectors.dense([0.4, 0.7]),
         Vectors.dense([0.7, 0.3]), Vectors.dense([0.7, 0.5]),
         Vectors.dense([0.3, 0.7]), Vectors.dense([0.4, 0.8]), 0.0),
    ]

    columns = ['transaction_amount', 'account_balance', 'card_age', 'ip_address_flag',
               'avg_transaction_amount_7d', 'risk_score', 'is_weekend', 'risk_score_dup',
               'previous_fraudulent_activity', 'daily_transaction_count', 'transaction_distance',
               'device_type_vec', 'merchant_category_vec', 'card_type_vec', 'location_vec',
               'authentication_method_vec', 'transaction_type_vec', 'fraud_label']
    return spark.createDataFrame(data, schema=columns)


def test_logistic_regression_pipeline(sample_df):
    feature_cols = ['transaction_amount', 'account_balance', 'card_age', 'ip_address_flag',
                    'avg_transaction_amount_7d', 'risk_score', 'is_weekend', 'risk_score_dup',
                    'previous_fraudulent_activity', 'daily_transaction_count', 'transaction_distance',
                    'device_type_vec', 'merchant_category_vec', 'card_type_vec', 'location_vec',
                    'authentication_method_vec', 'transaction_type_vec']

    assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
    df_with_features = assembler.transform(sample_df)
    final_data = df_with_features.select('features', 'fraud_label')

    train_data, test_data = final_data.randomSplit([0.7, 0.3], seed=42)

    # Handle case where test_data is empty due to small dataset
    if test_data.count() == 0:
        pytest.skip("Test data is empty after random split. Skipping evaluation.")

    lr = LogisticRegression(featuresCol='features', labelCol='fraud_label')
    model = lr.fit(train_data)
    predictions = model.transform(test_data)

    assert predictions.count() > 0
    assert predictions.filter(col("prediction").isNotNull()).count() > 0

    # AUC
    auc_eval = BinaryClassificationEvaluator(
        labelCol="fraud_label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    )
    auc = auc_eval.evaluate(predictions)
    assert 0.0 <= auc <= 1.0, f"AUC should be between 0 and 1, got {auc}"

    # Precision
    precision_eval = MulticlassClassificationEvaluator(
        labelCol="fraud_label", predictionCol="prediction", metricName="weightedPrecision"
    )
    precision = precision_eval.evaluate(predictions)
    assert 0.0 <= precision <= 1.0, f"Precision should be between 0 and 1, got {precision}"

    # Recall
    recall_eval = MulticlassClassificationEvaluator(
        labelCol="fraud_label", predictionCol="prediction", metricName="weightedRecall"
    )
    recall = recall_eval.evaluate(predictions)
    assert 0.0 <= recall <= 1.0, f"Recall should be between 0 and 1, got {recall}"
