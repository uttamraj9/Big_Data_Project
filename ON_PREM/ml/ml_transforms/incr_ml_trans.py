#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, regexp_replace, row_number,
    to_timestamp, hour, dayofweek, dayofmonth, udf, max as spark_max
)
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.sql.window import Window

from pyspark.ml import Pipeline

def main():
    spark = SparkSession.builder \
        .appName("ml_transforms") \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Load the already processed table
    # curated_tbl = spark.table("bd_class_project.ml_from_csv")
    # processed_count = curated_tbl.count()
    
    # raw_table = spark.table("bd_class_project.cc_fraud_trans")
    
    # w = Window.orderBy(to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
    # src_num = raw_table.withColumn("row_num", row_number().over(w))
    # df = src_num.filter(col("row_num") > processed_count).drop("row_num")

    # if df.rdd.isEmpty():
    #     print(f"No new rows since you last processed {processed_count} records.")
    #     sys.exit(1)
        
    processed_count = spark.table("bd_class_project.ml_from_csv").count()

    # 2) Read + clean your source
    raw = (spark.table("bd_class_project.cc_fraud_trans"))

    # 3) Turn it into an RDD with a 0-based index, then keep only rows > processed_count
    rdd_with_idx = raw.rdd.zipWithIndex()
    new_rdd = (
        rdd_with_idx
        .filter(lambda pair: pair[1] >= processed_count)   # keep rows whose idx ≥ already‐seen
        .map(lambda pair: pair[0])                        # drop the index, keep the original Row
    )

    # 4) If there’s nothing new, bail out
    if new_rdd.isEmpty():
        print("No new rows since you last processed {} records.".format(processed_count))
        return

    # 5) Reconstruct a DataFrame from those new Rows
    df = spark.createDataFrame(new_rdd, schema=raw.schema)

    df = df.dropDuplicates()
    df = df.na.drop()  # if you want to drop any remaining nulls

    # 2) Drop the raw ID column
    df = df.drop("transaction_id")

    # 3) Strip USER_ prefix & cast to int
    df = df.withColumn("user_id",
          regexp_replace("user_id", "^USER_", "").cast("int")
    )

    # 4) Categorical cols
    cats = [
      "transaction_type",
      "device_type",
      "location",
      "merchant_category",
      "card_type",
      "authentication_method"
    ]
    # build indexers + encoders
    indexers = [
      StringIndexer(inputCol=c, outputCol=c+"_Idx", handleInvalid="keep")
      for c in cats
    ]
    encoders = [
      OneHotEncoder(inputCol=c+"_Idx", outputCol=c+"_Vec")
      for c in cats
    ]

    # 5) Fit the pipeline to get StringIndexerModels
    pipeline = Pipeline(stages=indexers + encoders)
    model = pipeline.fit(df)
    df = model.transform(df)

    # 6) Flatten each Vec into descriptive columns
    to_array = udf(lambda v: v.toArray().tolist() if v is not None else None,
                   ArrayType(DoubleType()))
    for idx, c in enumerate(cats):
        vec = c + "_Vec"
        labels = model.stages[idx].labels
        arr = vec + "_arr"
        df = df.withColumn(arr, to_array(col(vec)))

        for i, lbl in enumerate(labels):
            safe = lbl.lower().replace(" ", "_").replace("-", "_")
            newcol = "{}_{}".format(c, safe)
            df = df.withColumn(newcol, col(arr)[i].cast("int"))

        # drop intermediate
        df = df.drop(vec, arr)

    # 7) Parse timestamp & extract time‐based features
    df = df.withColumn("Timestamp",
          to_timestamp("Timestamp", "yyyy-MM-dd HH:mm:ss")
    ).withColumn("Hour", hour("Timestamp")) \
     .withColumn("DayOfWeek", dayofweek("Timestamp")) \
     .withColumn("DayOfMonth", dayofmonth("Timestamp"))

    # 8) (Optional) drop the index cols if you don’t need them
    df = df.drop(*[c+"_Idx" for c in cats])

    # 9) Show & save
    df.show(1, truncate=False)
    print("Total rows:", df.count())

    out_path = "/tmp/US_UK_05052025/class_project/input/ml_data/ml_csv"
    df.write.format("csv") \
      .mode("append") \
      .option("header", "true") \
      .save(out_path)

    spark.stop()


if __name__ == '__main__':
    main()
