# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, regexp_replace, udf
from pyspark.sql.types import *
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline

# Create Spark session
spark = SparkSession.builder \
    .enableHiveSupport() \
    .getOrCreate()

# Load data from Hive table
df = spark.sql("SELECT * FROM bd_class_project.cc_fraud_trans WHERE transaction_id != 'Transaction_ID'")

total_rows = df.count()
dist_rows = df.distinct().count()
dup_rows = total_rows - dist_rows
print(dup_rows)

df = df.dropDuplicates() # Optional just in case you have duplicates

# Now we check for Null
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# we got 0, if you have Null you can either drop using:
# df = df.na.drop()
# Or fill them {here it fills 0} 
# df = df.na.fill({'Transaction_Amount': 0.0, 'Card_Age': 0}) 
# df = df.na.fill({'Transaction_Type': 'Unknown', 'Card_Type': 'Unknown'}

df = df.withColumn("user_id", regexp_replace("user_id", "USER_", "").cast("int"))
df.dtypes

categorical_cols = [
    "transaction_type",
    "device_type",
    "location",
    "merchant_category",
    "card_type",
    "authentication_method"
]

# Create indexers and encoders
indexers = [
    StringIndexer(inputCol=col, outputCol=col + "_Index", handleInvalid="keep")
    for col in categorical_cols
]

encoders = [
    OneHotEncoder(inputCol=col + "_Index", outputCol=col + "_vec")
    for col in categorical_cols
]

# Build pipeline
pipeline = Pipeline(stages=indexers + encoders)

# Fit and transform
df = pipeline.fit(df).transform(df)

#once done do
df.select([c + "_vec" for c in categorical_cols]).show(2, truncate=False)

# Convert Vector columns to Array[Double]
vector_to_array_udf = udf(
    lambda v: v.toArray().tolist() if v is not None else None,
    ArrayType(DoubleType())
)

vec_cols = [
    "transaction_type_vec",
    "device_type_vec",
    "location_vec",
    "merchant_category_vec",
    "card_type_vec",
    "authentication_method_vec"
]

for vec in vec_cols:
    # Replace '_vec' suffix with '_arr'
    arr_col = vec.replace("_vec", "_arr")
    df = df.withColumn(arr_col, vector_to_array_udf(vec))
    df = df.drop(vec)

df = df.drop(*categorical_cols)

df.write.mode("overwrite").saveAsTable("bd_class_project.cc_fraud_trans_processed")



