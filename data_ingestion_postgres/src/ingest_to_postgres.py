"""
ingest_to_postgres.py
=====================
Bronze Layer — PostgreSQL Ingestion (Simulation)

PURPOSE
-------
Loads synthetic credit card fraud data into a local PostgreSQL database to
simulate the source transactional system used in a real data pipeline.

In production the source would be a live OLTP database (e.g. a bank's
transaction system). Here we populate PostgreSQL from pre-split CSV files
so that the downstream Sqoop -> HDFS -> Hive bronze layer can be exercised
end-to-end in a training environment.

MODES
-----
  full    — Replaces the entire cc_fraud_trans table with full_load.csv.
            Run once during initial setup or to reset the simulation.

  inc     — Appends only records newer than the latest Timestamp already in
            the table, using incremental_load.csv. Simulates a daily ETL job.

  stream  — Loads kafka_streaming.csv into a separate cc_fraud_streaming_data
            table. Simulates records arriving via Kafka in near-real time.

USAGE
-----
  # Set env vars (or point ENV_FILE at a .env file)
  export DB_HOST=localhost DB_PORT=5432 DB_NAME=testdb
  export DB_USERNAME=admin DB_PASSWORD=admin123

  python3 ingest_to_postgres.py full
  python3 ingest_to_postgres.py inc
  python3 ingest_to_postgres.py stream

ENVIRONMENT VARIABLES
---------------------
  DB_HOST              PostgreSQL hostname
  DB_PORT              PostgreSQL port (default 5432)
  DB_NAME              Target database name
  DB_USERNAME          Database user
  DB_PASSWORD          Database password
  ENV_FILE             (optional) Path to a .env file to source
  FULL_LOAD_CSV        Path to full load CSV  (default: ../data/split/full_load.csv)
  INCREMENTAL_LOAD_CSV Path to incremental CSV (default: ../data/split/incremental_load.csv)
  KAFKA_STREAMING_CSV  Path to streaming CSV   (default: ../data/split/kafka_streaming.csv)
  LOAD_TABLE           Target table name       (default: cc_fraud_trans)
"""

import os
import sys
import argparse
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus


def load_environment(env_path: str = None):
    """Load environment variables from a .env file, falling back to the system environment."""
    load_dotenv(dotenv_path=env_path)


def create_db_engine():
    """
    Build a SQLAlchemy engine from environment variables.
    Exits with a non-zero status if any required variable is missing
    or the database cannot be reached.
    """
    user     = os.getenv("DB_USERNAME")
    password = quote_plus(os.getenv("DB_PASSWORD", ""))
    host     = os.getenv("DB_HOST")
    port     = os.getenv("DB_PORT")
    database = os.getenv("DB_NAME")

    if not all([user, password, host, port, database]):
        print("ERROR: One or more required database environment variables are missing.")
        print("       Required: DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME")
        sys.exit(1)

    conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    try:
        # pool_pre_ping=True tests the connection before use, avoiding stale-connection errors
        engine = create_engine(conn_str, pool_pre_ping=True)
        print(f"Connected to PostgreSQL at {host}:{port}/{database}")
        return engine
    except SQLAlchemyError as e:
        print(f"ERROR: Failed to create database engine: {e}")
        sys.exit(1)


def full_load(engine, csv_path: str, table_name: str):
    """
    Full load — replace the entire target table with the contents of csv_path.

    Uses if_exists='replace' so the table is dropped and recreated on every run,
    making this idempotent and safe to re-run to reset the simulation state.
    """
    df = pd.read_csv(csv_path)
    print(f"Read {len(df):,} rows from {csv_path}")

    # Cast Timestamp column to datetime so it maps to a TIMESTAMP column in Postgres
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])

    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    print(f"Full load complete: table '{table_name}' replaced with {len(df):,} rows.")


def incremental_load(engine, csv_path: str, table_name: str):
    """
    Incremental load — append only rows newer than the latest Timestamp in the table.

    Simulates a daily ETL job that picks up new transactions without re-loading
    historical data. Only the first 1,000 new rows (sorted by Timestamp) are
    inserted per run to keep the simulation manageable.
    """
    # Step 1: Find the watermark — the highest Timestamp already in the table
    with engine.connect() as conn:
        result  = conn.execute(text(f'SELECT MAX("Timestamp") FROM {table_name}'))
        max_ts  = result.scalar() or datetime(1970, 1, 1)
        print(f"Watermark (max Timestamp in DB): {max_ts}")

    # Step 2: Read the incremental CSV and filter to only new records
    df = pd.read_csv(csv_path)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    print(f"Read {len(df):,} rows from {csv_path}")

    df_new = df[df['Timestamp'] > max_ts].sort_values(by='Timestamp').head(1000)

    if df_new.empty:
        print("No new records found — table is already up to date.")
        return

    # Step 3: Append new rows to the existing table
    df_new.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    print(f"Incremental load complete: {len(df_new):,} new rows appended to '{table_name}'.")


def stream_load(engine, csv_path: str, table_name: str):
    """
    Streaming load — replace the cc_fraud_streaming_data table with kafka_streaming.csv.

    Simulates records arriving from a Kafka topic in near-real time.
    Uses if_exists='replace' so each run reflects the latest streaming snapshot.
    """
    df = pd.read_csv(csv_path)
    print(f"Read {len(df):,} rows from {csv_path}")

    df['Timestamp'] = pd.to_datetime(df['Timestamp'])

    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    print(f"Streaming load complete: table '{table_name}' replaced with {len(df):,} rows.")


def parse_args():
    """Parse the required positional mode argument."""
    parser = argparse.ArgumentParser(
        description="Bronze layer — ingest synthetic fraud data into PostgreSQL."
    )
    parser.add_argument(
        "mode",
        choices=["full", "inc", "stream"],
        help="Ingestion mode: full (replace), inc (incremental append), stream (Kafka simulation)"
    )
    return parser.parse_args()


def main():
    args    = parse_args()
    env_file = os.getenv("ENV_FILE")
    load_environment(env_file)

    engine = create_db_engine()

    # CSV paths — override via env vars or fall back to convention-based defaults
    full_csv      = os.getenv("FULL_LOAD_CSV",        "../data/split/full_load.csv")
    inc_csv       = os.getenv("INCREMENTAL_LOAD_CSV", "../data/split/incremental_load.csv")
    streaming_csv = os.getenv("KAFKA_STREAMING_CSV",  "../data/split/kafka_streaming.csv")
    table         = os.getenv("LOAD_TABLE",           "cc_fraud_trans")

    try:
        if args.mode == "full":
            full_load(engine, full_csv, table)
        elif args.mode == "inc":
            incremental_load(engine, inc_csv, table)
        elif args.mode == "stream":
            stream_load(engine, streaming_csv, "cc_fraud_streaming_data")
    except Exception as e:
        print(f"ERROR: Ingestion job failed — {e}")
        sys.exit(1)
    finally:
        engine.dispose()
        print("Database connection closed.")


if __name__ == "__main__":
    main()
