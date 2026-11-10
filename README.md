# AWS Data Engineering & Real-Time Analytics Suite

End-to-End implementation for:
- **Project 3.1: Serverless Medallion Data Lake & PySpark ETL** (Amazon S3, AWS Glue PySpark, Glue Data Catalog, Athena SQL, and QuickSight).
- **Project 3.2: Real-Time Streaming Analytics** (Amazon Kinesis Data Streams, AWS Lambda Anomaly Detection, and Amazon OpenSearch Service).

---

## Architecture Overview

### Project 3.1: Serverless Medallion Data Lake
```
Transactional CSV Data
       │
       ▼
Amazon S3 (Bronze Layer)
       │
       ▼
AWS Glue PySpark Job (Cleaning, Deduplication & Schema Enforcement)
       │
       ▼
Amazon S3 (Silver Layer - Partitioned Snappy Parquet: year / month)
       │
       ▼
AWS Glue Data Catalog ──▶ Amazon Athena Serverless SQL ──▶ Amazon QuickSight BI Dashboard
```

### Project 3.2: Real-Time Streaming Analytics
```
Python Live Telemetry Producer (IoT sensors + clickstreams)
       │
       ▼
Amazon Kinesis Data Streams (iot-sensor-stream, 1 shard)
       │
       ▼
AWS Lambda Event Source Mapping (Base64 decode + real-time anomaly filter > 90°C)
       │
       ▼
Amazon OpenSearch Service Index (iot-metrics) ──▶ Live Dashboards & Visualizations
```

---

## Directory Structure

```text
aws-data-engineering-suite/
├── .env.example                       # AWS Region, Bucket, Kinesis stream, OpenSearch host configs
├── requirements.txt                   # boto3, pyspark, pandas, pyarrow, requests, urllib3, pytest
├── Makefile                           # Automation commands: setup, deploy-3.1, deploy-3.2, test, destroy
├── project_3_1_serverless_datalake/
│   ├── retail_data_generator.py       # Generates raw transactional CSVs & uploads to S3 Bronze layer
│   ├── glue_etl_bronze_to_silver.py   # PySpark script: cleans, casts & writes Snappy Parquet to S3 Silver
│   ├── catalog_and_athena.py          # Provisions Glue Database, Crawler & executes Athena SQL queries
│   └── quicksight_manifest.json       # QuickSight dataset configuration for visual BI dashboards
├── project_3_2_realtime_streaming/
│   ├── kinesis_iot_producer.py        # Live telemetry stream generator (IoT sensors + anomaly spikes)
│   ├── lambda_anomaly_processor.py    # Decodes Kinesis base64 records, flags anomalies & indexes to OpenSearch
│   └── opensearch_index_setup.py      # Creates index mappings & dashboard visual templates in OpenSearch
└── scripts/
    ├── test_data_pipelines.py         # Automated pytest verification for ETL schemas, Athena & Lambda
    └── cleanup_data_infra.py          # Zero-billing FinOps teardown (deletes Kinesis, Glue, OpenSearch, S3)
```

---

## Quickstart & Execution Guide

### 1. Bootstrap Environment
```bash
make setup
```
Initializes the Python virtual environment, installs dependencies, verifies AWS credentials, and runs pytest discovery.

### 2. Run Automated Verification Tests
```bash
make test
```
Executes the automated pytest verification suite covering:
- Lambda real-time anomaly detection (`normal` vs `spike > 90.0°C`)
- Lambda multi-record batch processing
- Retail data generator schema and intentional null injection rules
- OpenSearch `iot-metrics` schema mappings
- Athena grouped revenue SQL syntax and partitioning logic
- Medallion Silver transformation rules (deduplication, date parsing, partition derivation)

### 3. Deploy Project 3.1 (Serverless Data Lake & Glue ETL)
```bash
make deploy-3.1
```
1. Creates S3 Data Lake bucket with `bronze/`, `silver/`, and `athena-results/` prefixes.
2. Generates 500 raw transactions and uploads to `s3://<bucket>/bronze/retail_sales/sales_data.csv`.
3. Runs PySpark ETL job to clean data, remove cancellations and null amounts, and write partitioned Snappy Parquet to `s3://<bucket>/silver/retail_sales/`.
4. Provisions Glue Catalog database `retail_datalake_db`, registers table `retail_sales`, repairs partitions, and executes Athena SQL analytics.

### 4. Deploy Project 3.2 (Real-Time Streaming Analytics)
```bash
make deploy-3.2
```
1. Creates `iot-metrics` index and dashboard templates in OpenSearch.
2. Provisions Kinesis data stream `iot-sensor-stream` (1 Shard).
3. Produces live telemetry and injects critical temperature spikes (> 90.0°C).
4. Lambda event source processor decodes records, flags anomalies, and indexes documents into OpenSearch.

### 5. FinOps Zero-Billing Teardown
```bash
make destroy
```
Deletes Kinesis stream, OpenSearch domains, Glue Catalog resources, and cleans up S3 Data Lake buckets to ensure zero unexpected AWS charges.

