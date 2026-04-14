#!/usr/bin/env python3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_replace, to_timestamp, hour, dayofweek, dayofmonth,
    when, lower, col, max as spark_max
)
from pyspark.ml import PipelineModel
import sys

def main():
    hive_uri = os.environ.get("HIVE_METASTORE_URIS", "thrift://172.31.6.42:9083")

    spark = (SparkSession.builder
             .appName("prediction-cron")
             .config("hive.metastore.uris", hive_uri)
             .enableHiveSupport()
             .getOrCreate())

    preds_tbl = spark.table("bd_class_project.predictions_table")
    max_ts_row = preds_tbl.select(
        spark_max(col("timestamp")).alias("max_ts")
    ).first()
    max_ts = max_ts_row["max_ts"]
    if max_ts is None:
        last_time = "1970-01-01 00:00:00"
    else:
        last_time = max_ts.strftime("%Y-%m-%d %H:%M:%S")

    raw_df = spark.sql(
        f"SELECT * FROM bd_class_project.raw_data_from_realtime "
        # f"WHERE to_timestamp(timestamp, 'yyyy-MM-dd HH:mm:ss') > timestamp('{last_time}')"
    )

    if raw_df.rdd.isEmpty():
        print("No new records since", last_time)
        spark.stop()
        sys.exit(0)

    df1 = (raw_df
      .withColumn("user_id", regexp_replace("user_id", "^USER_", "").cast("int"))
      .withColumn("ts", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
      .withColumn("hour", hour("ts"))
      .withColumn("dayofweek", dayofweek("ts"))
      .withColumn("dayofmonth", dayofmonth("ts"))
    )

    categories = {
        "transaction_type":       ["pos", "bank_transfer", "online", "atm_withdrawal"],
        "device_type":            ["mobile", "tablet", "laptop"],
        "location":               ["tokyo", "mumbai", "london", "sydney", "new_york"],
        "merchant_category":      ["restaurants", "clothing", "travel", "groceries", "electronics"],
        "card_type":              ["mastercard", "amex", "discover", "visa"],
        "authentication_method":  ["pin", "password", "biometric", "otp"]
    }

    df2 = df1
    for c, vals in categories.items():
        clean = regexp_replace(lower(col(c)), "[\\s-]+", "_")
        df2 = df2.withColumn(c, clean)
        for v in vals:
            df2 = df2.withColumn(f"{c}_{v}", when(col(c) == v, 1).otherwise(0))

    df_ready = df2.drop(*(list(categories.keys()) + ["timestamp", "ts"]))

    pipeline = PipelineModel.load("file:///app/model")
    scored   = pipeline.transform(df_ready)

    preds      = scored.select("transaction_id", "prediction")
    uniq_preds = preds.dropDuplicates(["transaction_id"])

    out_df = raw_df.join(uniq_preds, on="transaction_id", how="inner")

    out_df.show()
    a,b,c,d =  raw_df.count(), preds.count(),uniq_preds.count(), out_df.count()
    print(a,b,c,d)
    print("raw_df total = ", a)
    print("preds total = ", b)
    print("uniq_preds total = ", c)
    print("out_df total = ", d)
    out_df.write.mode("overwrite").insertInto("bd_class_project.predictions_table")

    # out_df.write.mode("append").insertInto("bd_class_project.predictions_table")
    
    spark.stop()

if __name__ == "__main__":
    main()

