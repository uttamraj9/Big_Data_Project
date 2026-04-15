# -*- coding: utf-8 -*-
from pyspark.sql.functions import *

from pyspark.sql.types import *

from pyspark.ml.feature import StringIndexer, OneHotEncoder

from pyspark.ml import Pipeline

from pyspark.sql.functions import hour, dayofweek, dayofmonth

from pyspark.sql.functions import to_timestamp

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Your Application Name").config("spark.some.config.option", "some-value").getOrCreate()

df = spark.sql("SELECT * FROM bd_class_project.cc_fraud_trans WHERE transaction_id != 'Transaction_ID'")

total_rows = df.count()

dist_rows = df.distinct().count()

dup_rows = total_rows - dist_rows

print(dup_rows)

# we got 0 duplicates 

# Optional just in case you have duplicates 

df = df.dropDuplicates()


# Now we check for Null

df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()



# we got 0 

# id you have Null you can either drop using: 

# df = df.na.drop() 

# Or fill them {here it fills 0}  

# df = df.na.fill({'Transaction_Amount': 0.0, 'Card_Age': 0})  

# df = df.na.fill({'Transaction_Type': 'Unknown', 'Card_Type': 'Unknown'} 



# We will not need Transaction ID col for ML  

df = df.drop("transaction_id")


# Now in our data we have user_id column, we do not need it as a string but we will need to identify the user, we need to convert the col in to integer value. We need to remove the USER_ before the ID and covert it to int. 

df = df.withColumn("user_id", regexp_replace("user_id", "USER_", "").cast("int"))


# Now we have the most important transformation for making the data ready for the ML. 

# There are 2 functions we import from PySpark ML, StingIndex and OneHotEncoding. 

categorical_cols = [ "transaction_type", "device_type", "location", "merchant_category", "card_type", "authentication_method" ]



# Create indexers and encoders 

indexers = [ StringIndexer(inputCol=cat_col, outputCol=cat_col + "_Index", handleInvalid="keep") for cat_col in categorical_cols ]

encoders = [ OneHotEncoder(inputCol=cat_col + "_Index", outputCol=cat_col + "_Vec") for cat_col in categorical_cols ]



# Build pipeline 

pipeline = Pipeline(stages=indexers + encoders)

# Fit and transform 

df = pipeline.fit(df).transform(df)

# Now we extract some useful data from timestamp and add them as col 

df = df.withColumn("Timestamp", to_timestamp("Timestamp", "yyyy-MM-dd HH:mm:ss"))


# Extract the Hour, Day of week, Day of month from the timestamp data 

df = df.withColumn("Hour", hour("Timestamp")) .withColumn("DayOfWeek", dayofweek("Timestamp")).withColumn("DayOfMonth", dayofmonth("Timestamp"))

df.show(1)

# Shows first row check if everytjing looks good 

df.count()

# 30000 – Total Rows. Quick check before writing to file 


# For ML Purposes we prefer data in parquet  

# We can also write in CSV but csv does not directly support vector data so we will have to modify the vectors to strings, which is not ideal for ML 

# For this project we stick to parquet format 

# df.write.mode("overwrite").parquet("/tmp/US_UK_05052025/rudram/e2ep/ml/data_parquet") 


df.write.mode("overwrite").parquet("/tmp/US_UK_05052025/class_project/input/ml_data/parquet_data")

# quick check if write worked 

# df_clean = spark.read.parquet("/tmp/US_UK_05052025/rudram/e2ep/ml/data_parquet") 

df_clean = spark.read.parquet("/tmp/US_UK_05052025/class_project/input/ml_data/parquet_data")

df_clean.show(1)

df_clean.dtypes