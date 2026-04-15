#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

def main():
    hive_uri = os.environ.get("HIVE_METASTORE_URIS", "thrift://172.31.6.42:9083")

    spark = (
        SparkSession.builder
            .appName("RF_from_Hive")
            .config("hive.metastore.uris", hive_uri)
            .enableHiveSupport()
            .getOrCreate()
    )

    df = spark.sql(
        "SELECT * FROM bd_class_project.ml_from_csv "
        "WHERE transaction_type != 'transaction_type'"
    )

    to_drop = [
        "transaction_type", "device_type", "location",
        "merchant_category", "card_type", "authentication_method",
        "Timestamp"
    ]
    label_col = "fraud_label"
    df = df.drop(*to_drop)

    feature_cols = [c for c in df.columns if c != label_col]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="keep")

    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol=label_col,
        predictionCol="prediction",
        probabilityCol="probability",
        rawPredictionCol="rawPrediction",
        numTrees=100,
        maxDepth=5,
        seed=42
    )

    pipeline = Pipeline(stages=[assembler, rf])
    train, test = df.randomSplit([0.7, 0.3], seed=42)
    model = pipeline.fit(train)
    preds = model.transform(test)

    evaluator = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    auc = evaluator.evaluate(preds)
    print("Test AUC = {:.4f}".format(auc))

    out_dir = "file:///app/output"
    model.write().overwrite().save(out_dir)
    print("Model saved to {}".format(out_dir))

    spark.stop()

if __name__ == "__main__":
    main()
