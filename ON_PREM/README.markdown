# Credit Card Fraud Detection System

## Overview
This project implements a comprehensive credit card fraud detection system using a combination of batch and real-time data processing, machine learning, and deployment pipelines. The architecture leverages various tools and technologies to ensure efficient data handling, model training, and real-time prediction.

## System Architecture
The system is divided into several key components:

### Data Sources
- **Full Load CSV, Incremental CSV, Real-time Streaming CSV**: Data ingestion from various sources including batch and streaming data.

### Batch ETL and ML
- **Postgres Full Load Table**: Initial data loading into a Postgres database.
- **Jenkin Jobs**: Automated jobs for data extraction and transformation.
  - **Bash Script for Incremental Load**: Incremental data loading process.
  - **Extract Incremental Data by Timestamp**: Extracts data based on timestamps.
- **HDFS Storage**: Stores raw data in Hadoop Distributed File System.
- **Hive Raw Data Table**: Manages raw data in a Hive table.

### Transformation and ML Model Creation
- **Jenkin Jobs**: Orchestrates transformation and model creation tasks.
  - **Full Load Spark Transformations**: Applies transformations on full data loads.
  - **Incremental Transformations**: Applies transformations on incremental data.
  - **ML Model (PySpark)**: Trains a machine learning model using PySpark.
- **Docker Image Build**: Builds Docker images for model deployment.
- **Kubernetes Create Model**: Deploys the trained model using Kubernetes.

### Model in Production
- **ML Model**: The deployed machine learning model for fraud detection.
- **PowerBI Dashboard (Raw Data)**: Visualizes raw data insights.
- **PowerBI (Predicted Data)**: Displays predicted fraud data.

### Real-time ETL and ML
- **FastAPI Service**: Provides a real-time API service.
- **Docker Image Build**: Builds Docker images for real-time processing.
- **Kubernetes Deployed API**: Deploys the API using Kubernetes.
- **Kafka Producer**: Produces real-time data streams.
- **Kafka Consumer**: Consumes and processes real-time data.
- **Jenkin Jobs**: Manages real-time data transformations.
  - **Bash Script for Scala**: Executes Scala scripts for processing.
  - **Extract Incremental Data by Timestamp**: Extracts real-time data by timestamp.
- **Hive Data Table**: Stores processed real-time data.
- **Transformation (PySpark)**: Applies transformations using PySpark.
- **Docker Image Build**: Builds Docker images for real-time predictions.
- **Kubernetes Cronjob**: Schedules periodic prediction jobs.
- **Hive Prediction Results Table**: Stores prediction results.

## Setup and Installation
1. Clone the repository.
2. Set up the required dependencies (Postgres, Hadoop, Hive, Kafka, etc.).
3. Build Docker images using the provided Dockerfile.
4. Deploy the services using Kubernetes configurations.
5. Configure Jenkins jobs for automation.

## Usage
- Ingest data using the batch and real-time pipelines.
- Train the ML model using the PySpark scripts.
- Deploy the model and API for real-time fraud detection.
- Monitor results via PowerBI dashboards.