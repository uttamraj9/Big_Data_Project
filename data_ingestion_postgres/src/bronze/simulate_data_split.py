"""
simulate_data_split.py
======================
Bronze Layer — Simulation Data Preparation

PURPOSE (Training Environment Only)
------------------------------------
This script simulates the three data arrival patterns that exist in a
real production pipeline by splitting a single synthetic CSV dataset into
three chronologically ordered subsets:

  1. Full Load (60%)       — initial historical batch loaded into
                             PostgreSQL on day one.
  2. Incremental (20%)     — new records that arrive daily and are
                             appended to the existing PostgreSQL table.
  3. Kafka Streaming (20%) — near-real-time records that would arrive
                             via a Kafka topic in production.

NOTE: In a real environment these three streams come from separate systems.
      Here they are pre-split from one file to allow the full ingestion
      pipeline to be tested end-to-end in a training setting.

OUTPUT
------
  data/split/full_load.csv         -> used by ingest_to_postgres.py (full mode)
  data/split/incremental_load.csv  -> used by ingest_to_postgres.py (inc mode)
  data/split/kafka_streaming.csv   -> used by ingest_to_postgres.py (stream mode)

USAGE
-----
  Run once before any ingestion scripts. Raw dataset must exist at
  data/raw/synthetic_fraud_dataset.csv.

  python3 simulate_data_split.py
"""

import pandas as pd
import os

# ── Split ratios ──────────────────────────────────────────────────────────────
FULL_LOAD_PERCENT        = 0.60  # 60% — initial historical batch
INCREMENTAL_LOAD_PERCENT = 0.20  # 20% — simulated daily incremental records
KAFKA_STREAMING_PERCENT  = 0.20  # 20% — simulated Kafka stream records


def main():
    input_file = 'data/raw/synthetic_fraud_dataset.csv'
    output_dir = 'data/split'

    os.makedirs(output_dir, exist_ok=True)

    # Sort ascending by Timestamp so that the three slices are chronological,
    # mimicking how records would arrive in a real pipeline (oldest data first).
    df = pd.read_csv(input_file).sort_values(by='Timestamp', ascending=True)
    total_rows = len(df)

    # Calculate row boundaries for each split
    full_end        = int(total_rows * FULL_LOAD_PERCENT)
    incremental_end = full_end + int(total_rows * INCREMENTAL_LOAD_PERCENT)

    df_full        = df.iloc[:full_end]
    df_incremental = df.iloc[full_end:incremental_end]
    df_kafka       = df.iloc[incremental_end:]

    # Write each subset to its own CSV file
    df_full.to_csv(f'{output_dir}/full_load.csv', index=False)
    df_incremental.to_csv(f'{output_dir}/incremental_load.csv', index=False)
    df_kafka.to_csv(f'{output_dir}/kafka_streaming.csv', index=False)

    print("Simulation data split complete:")
    print(f"  Full Load        ({FULL_LOAD_PERCENT * 100:.0f}%): {len(df_full):,} rows  -> {output_dir}/full_load.csv")
    print(f"  Incremental Load ({INCREMENTAL_LOAD_PERCENT * 100:.0f}%): {len(df_incremental):,} rows  -> {output_dir}/incremental_load.csv")
    print(f"  Kafka Streaming  ({KAFKA_STREAMING_PERCENT * 100:.0f}%): {len(df_kafka):,} rows  -> {output_dir}/kafka_streaming.csv")


if __name__ == "__main__":
    main()
