-- =============================================================================
-- create_bronze_hive_table.hql
-- =============================================================================
-- Bronze Layer — Hive External Table DDL
--
-- PURPOSE
-- -------
-- Defines the Hive external table that exposes the raw credit card fraud
-- transaction data landed in HDFS by Sqoop (bronze_full_load.sh).
--
-- The table is EXTERNAL so that dropping it in Hive never deletes the
-- underlying HDFS files — the bronze data is owned by the ingestion pipeline,
-- not by Hive. The schema mirrors the source PostgreSQL table exactly.
--
-- This DDL is used by bronze_full_load.sh. It is kept here as a standalone
-- reference file so the schema can be reviewed, version-controlled, and
-- reapplied independently of the ingestion script.
--
-- LOCATION
-- --------
--   HDFS: /tmp/US_UK_05052025/class_project/input/raw_data_sqoop/
--   The files in this directory are comma-delimited text files written by
--   Sqoop with no header row (skip.header.line.count=0).
--
-- NOTE: Timestamp is a reserved word in HiveQL and must be backtick-quoted.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS bd_class_project;

USE bd_class_project;

DROP TABLE IF EXISTS cc_fraud_trans;

CREATE EXTERNAL TABLE IF NOT EXISTS bd_class_project.cc_fraud_trans (
    Transaction_ID               STRING,
    User_ID                      STRING,
    Transaction_Amount           DECIMAL(10,2),
    Transaction_Type             STRING,
    `Timestamp`                  TIMESTAMP,       -- reserved word, must be quoted
    Account_Balance              DECIMAL(10,2),
    Device_Type                  STRING,
    Location                     STRING,
    Merchant_Category            STRING,
    IP_Address_Flag              INT,
    Previous_Fraudulent_Activity INT,
    Daily_Transaction_Count      INT,
    Avg_Transaction_Amount_7d    DECIMAL(10,2),
    Failed_Transaction_Count_7d  INT,
    Card_Type                    STRING,
    Card_Age                     INT,
    Transaction_Distance         DECIMAL(10,2),
    Authentication_Method        STRING,
    Risk_Score                   DECIMAL(5,4),
    Is_Weekend                   INT,
    Fraud_Label                  INT              -- 0 = legitimate, 1 = fraud
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/tmp/US_UK_05052025/class_project/input/raw_data_sqoop'
TBLPROPERTIES ("skip.header.line.count"="0");
