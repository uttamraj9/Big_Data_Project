# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, regexp_replace, udf
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.ml.linalg import Vector
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline

# Create Spark session
spark = SparkSession.builder.getOrCreate()

df = spark.sql("SELECT * FROM bd_class_project.cc_fraud_trans WHERE transaction_id != 'Transaction_ID'")

total_rows = df.count()
dist_rows = df.distinct().count()
dup_rows = total_rows - dist_rows
print(dup_rows)

df = df.dropDuplicates() # Optional just in case you have duplicates

# Now we check for Null
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# we got 0, id you have Null you can either drop using:
# df = df.na.drop()
# Or fill them {here it fills 0} 
# df = df.na.fill({'Transaction_Amount': 0.0, 'Card_Age': 0}) 
# df = df.na.fill({'Transaction_Type': 'Unknown', 'Card_Type': 'Unknown'}

# We will not need Transaction ID col for ML 
df = df.drop("transaction_id")


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
    StringIndexer(inputCol=cat_col, outputCol=cat_col + "_Index", handleInvalid="keep")
    for cat_col in categorical_cols
]

encoders = [
    OneHotEncoder(inputCol=cat_col + "_Index", outputCol=cat_col + "_Vec")
    for cat_col in categorical_cols
]

# Build pipeline
pipeline = Pipeline(stages=indexers + encoders)

# Fit and transform
df = pipeline.fit(df).transform(df)

#once done do
df.select([c + "_Vec" for c in categorical_cols]).show(2, truncate=False)

# Convert Vector columns to Array[Double]
vector_to_array_udf = udf(
    lambda v: v.toArray().tolist() if v is not None else None,
    ArrayType(DoubleType())
)

vec_cols = [
    "transaction_type_Vec",
    "device_type_Vec",
    "location_Vec",
    "merchant_category_Vec",
    "card_type_Vec",
    "authentication_method_Vec"
]

for vec in vec_cols:
    arr_col = vec + "_arr"

    # a) Vector -> Array[Double]

    df = df.withColumn(arr_col, vector_to_array_udf(F.col(vec)))

    # b) Get the length from the first row’s array
    #first_arr = df.select(arr_col).head()[0] or []
    #size = len(first_arr)

    # c) Spill each element into its own Double column
    #for i in range(size):
        #new_col = "{}_{}".format(vec, i)
        #df = df.withColumn(new_col, col(arr_col)[i])

    # d) Drop the old vector and helper array
    df = df.drop(vec)


df.write.mode("overwrite").option("Header", "true").csv("/tmp/US_UK_05052025/class_project/input/ml_data/parquet_vectoarray")
