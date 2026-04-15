from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf, col
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# Create Spark session
spark = SparkSession.builder \
    .appName("LogisticRegression") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Load data from Hive table
df = spark.sql("SELECT * FROM bd_class_project.cc_fraud_trans_processed")

# Define udf to convert array columns to vector columns
array_to_vector_udf = udf(lambda arr: Vectors.dense(arr) if arr is not None else None, VectorUDT())

# List your array columns here
arr_cols = [
    "transaction_type_arr",
    "device_type_arr",
    "location_arr",
    "merchant_category_arr",
    "card_type_arr",
    "authentication_method_arr"
]

# For each array col, create a new vector col by applying the UDF
for c in arr_cols:
    new_col_name = c.replace('_arr', '_vec')
    df = df.withColumn(new_col_name, array_to_vector_udf(col(c)))


feature_cols = ['transaction_amount', 'account_balance', 'card_age', 'ip_address_flag',
                'avg_transaction_amount_7d', 'risk_score', 'is_weekend', 'risk_score',
                'previous_fraudulent_activity', 'daily_transaction_count','transaction_distance',
                'device_type_vec', 'merchant_category_vec', 'card_type_vec', 'location_vec', 
                'authentication_method_vec', 'transaction_type_vec']

assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
df = assembler.transform(df)
final_data = df.select("transaction_id", 'features', 'fraud_label')

train_data, test_data = final_data.randomSplit([0.7, 0.3], seed=42)

lr = LogisticRegression(featuresCol='features', labelCol='fraud_label')
lr_model = lr.fit(train_data)

predictions = lr_model.transform(test_data)

# Define UDF to round probabilities to 2 decimal places
round_prob_udf = udf(lambda prob: [round(x, 2) for x in prob.toArray()], ArrayType(DoubleType()))

# Show predictions
predictions.select(
    "transaction_id",
    "fraud_label",
    "prediction",
    round_prob_udf("probability").alias("probability_2dp")
).show(50, truncate=False)

# Evaluate model
auc_evaluator = BinaryClassificationEvaluator(labelCol="fraud_label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
auc = auc_evaluator.evaluate(predictions)

# Precision
precision_evaluator = MulticlassClassificationEvaluator(labelCol="fraud_label", predictionCol="prediction", metricName="weightedPrecision")
precision = precision_evaluator.evaluate(predictions)

# Recall
recall_evaluator = MulticlassClassificationEvaluator(labelCol="fraud_label", predictionCol="prediction", metricName="weightedRecall")
recall = recall_evaluator.evaluate(predictions)

# Store metrics in a dictionary
metrics = {
    "AUC": auc,
    "Precision": precision,
    "Recall": recall
}

# Display metrics
for key, value in metrics.items():
    print("{}: {:.4f}".format(key, value))


