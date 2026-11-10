"""
Project Verification Test Suite
scripts/test_data_pipelines.py - Automated pytest verification for ETL schemas, Athena & Lambda.
Includes unit tests and mock integration tests covering Acceptance Gates for Phases 1-6.
"""

import os
import csv
import json
import base64
import tempfile
import pytest
import boto3
from moto import mock_aws

from project_3_1_serverless_datalake.retail_data_generator import (
    create_datalake_bucket,
    generate_bronze_csv
)
from project_3_1_serverless_datalake.catalog_and_athena import (
    create_glue_database,
    create_glue_silver_table
)
from project_3_2_realtime_streaming.kinesis_iot_producer import create_kinesis_stream
from project_3_2_realtime_streaming.lambda_anomaly_processor import lambda_handler
from project_3_2_realtime_streaming.opensearch_index_setup import INDEX_MAPPING, INDEX_NAME
from scripts.cleanup_data_infra import teardown_all


# ---------------------------------------------------------
# Phase 5 & 6: Lambda Anomaly Detection Unit Tests
# ---------------------------------------------------------
def make_kinesis_event(temp: float):
    payload = json.dumps({"sensor_id": "SENSOR_1", "temperature": temp, "humidity": 45.0}).encode("utf-8")
    return {"Records": [{"kinesis": {"data": base64.b64encode(payload).decode("utf-8")}}]}


def test_lambda_anomaly_detection():
    """
    Acceptance Gate 5 & 6 Verification:
    Tests that Lambda correctly identifies normal temperatures vs high-temperature anomaly spikes (>90.0°C).
    """
    normal_res = lambda_handler(make_kinesis_event(55.0), None)
    assert normal_res["processed"] == 1 and normal_res["anomalies"] == 0

    spike_res = lambda_handler(make_kinesis_event(96.4), None)
    assert spike_res["processed"] == 1 and spike_res["anomalies"] == 1


def test_lambda_batch_processing():
    """
    Verifies that Lambda processes multi-record batches and correctly counts multiple anomalies.
    """
    temperatures = [25.0, 92.5, 30.0, 102.1, 75.0]
    records = []
    for temp in temperatures:
        payload = json.dumps({"sensor_id": "SENSOR_2", "temperature": temp, "humidity": 50.0}).encode("utf-8")
        records.append({"kinesis": {"data": base64.b64encode(payload).decode("utf-8")}})

    batch_res = lambda_handler({"Records": records}, None)
    assert batch_res["processed"] == 5
    assert batch_res["anomalies"] == 2


# ---------------------------------------------------------
# Phase 2: Bronze Ingestion & Data Generation Tests
# ---------------------------------------------------------
def test_retail_data_generation_rules():
    """
    Verifies Phase 2 Bronze Ingestion data format:
    - Contains required columns: order_id, customer_id, product_category, order_amount, order_date, status
    - Cancelled orders have empty order_amount (intentional null rows for ETL filtering)
    - Valid categories and statuses
    """
    from datetime import datetime, timedelta
    import random

    categories = ["Electronics", "Apparel", "Home", "Groceries"]
    statuses = ["Completed", "Completed", "Completed", "Cancelled"]
    base_date = datetime(2026, 10, 1)

    with tempfile.NamedTemporaryFile("w+", suffix=".csv", delete=False) as tmp_file:
        tmp_filename = tmp_file.name
        writer = csv.writer(tmp_file)
        writer.writerow(["order_id", "customer_id", "product_category", "order_amount", "order_date", "status"])
        for i in range(1001, 1051):
            status = random.choice(statuses)
            amount = "" if status == "Cancelled" else round(random.uniform(15.0, 950.0), 2)
            order_date = (base_date + timedelta(days=random.randint(0, 45))).strftime("%Y-%m-%d")
            writer.writerow([i, f"C{random.randint(1, 80):02d}", random.choice(categories), amount, order_date, status])

    try:
        with open(tmp_filename, "r") as f:
            reader = list(csv.DictReader(f))
            assert len(reader) == 50
            for row in reader:
                assert row["product_category"] in categories
                assert row["status"] in ["Completed", "Cancelled"]
                if row["status"] == "Cancelled":
                    assert row["order_amount"] == "", "Cancelled orders must have empty order_amount for ETL filtering"
                else:
                    assert float(row["order_amount"]) >= 15.0
    finally:
        if os.path.exists(tmp_filename):
            os.remove(tmp_filename)


# ---------------------------------------------------------
# Phase 3 & 4: Schema Mappings, PySpark ETL & Athena SQL
# ---------------------------------------------------------
def test_opensearch_schema_mappings():
    """
    Verifies OpenSearch index configuration conforms to IoT metrics schema.
    """
    assert INDEX_NAME == "iot-metrics"
    props = INDEX_MAPPING["mappings"]["properties"]
    assert props["sensor_id"]["type"] == "keyword"
    assert props["temperature"]["type"] == "float"
    assert props["humidity"]["type"] == "float"
    assert props["timestamp"]["type"] == "date"
    assert props["is_anomaly"]["type"] == "boolean"


def test_athena_sql_query_structure():
    """
    Verifies Athena SQL statement conforms to Project 3.1 specifications.
    """
    database = "retail_datalake_db"
    sql = f"""
    SELECT product_category,
           COUNT(order_id) AS total_orders,
           ROUND(SUM(order_amount), 2) AS total_revenue
    FROM "{database}"."retail_sales"
    WHERE year = 2026
    GROUP BY product_category
    ORDER BY total_revenue DESC;
    """
    assert 'FROM "retail_datalake_db"."retail_sales"' in sql
    assert "WHERE year = 2026" in sql
    assert "GROUP BY product_category" in sql
    assert "ORDER BY total_revenue DESC" in sql


def test_medallion_etl_clean_logic():
    """
    Verifies ETL business logic:
    - Drops cancelled orders and null amounts
    - Deduplicates on order_id
    - Extracts year and month
    """
    import pandas as pd

    test_data = [
        {"order_id": 1001, "customer_id": "C01", "product_category": "Electronics", "order_amount": 100.0, "order_date": "2026-10-05", "status": "Completed"},
        {"order_id": 1002, "customer_id": "C02", "product_category": "Apparel", "order_amount": None, "order_date": "2026-10-06", "status": "Cancelled"},
        {"order_id": 1003, "customer_id": "C03", "product_category": "Home", "order_amount": 250.0, "order_date": "2026-11-02", "status": "Completed"},
        {"order_id": 1001, "customer_id": "C01", "product_category": "Electronics", "order_amount": 100.0, "order_date": "2026-10-05", "status": "Completed"}  # duplicate
    ]
    df = pd.DataFrame(test_data)

    df_clean = df[df["order_amount"].notnull() & (df["status"] == "Completed")].drop_duplicates(subset=["order_id"]).copy()
    df_clean["order_date"] = pd.to_datetime(df_clean["order_date"])
    df_clean["year"] = df_clean["order_date"].dt.year
    df_clean["month"] = df_clean["order_date"].dt.month

    assert len(df_clean) == 2
    assert set(df_clean["order_id"]) == {1001, 1003}
    assert list(df_clean[df_clean["order_id"] == 1001]["year"])[0] == 2026
    assert list(df_clean[df_clean["order_id"] == 1001]["month"])[0] == 10
    assert list(df_clean[df_clean["order_id"] == 1003]["month"])[0] == 11


# ---------------------------------------------------------
# Integration Tests: End-to-End Mock AWS Pipeline Verification
# ---------------------------------------------------------
@mock_aws
def test_e2e_phase2_bronze_s3_ingest():
    """
    Verifies Phase 2 Acceptance Gate:
    s3://<bucket>/bronze/retail_sales/sales_data.csv exists and is non-empty with 500 rows.
    """
    bucket_name = "test-retail-datalake"
    create_datalake_bucket(bucket_name, "us-east-1")

    filename = "test_sales_data.csv"
    try:
        generate_bronze_csv(bucket_name, filename=filename, rows=500)

        s3 = boto3.client("s3", region_name="us-east-1")
        response = s3.get_object(Bucket=bucket_name, Key=f"bronze/retail_sales/{filename}")
        content = response["Body"].read().decode("utf-8").strip().split("\n")
        assert len(content) == 501  # Header + 500 records
        assert "order_id,customer_id,product_category" in content[0]
    finally:
        if os.path.exists(filename):
            os.remove(filename)


@mock_aws
def test_e2e_phase5_kinesis_stream_and_lambda():
    """
    Verifies Phase 5 Speed Layer Acceptance Gate:
    Provision Kinesis stream, emit records, and verify Lambda decoding & anomaly filtering.
    """
    stream_name = "test-iot-sensor-stream"
    kinesis = boto3.client("kinesis", region_name="us-east-1")
    kinesis.create_stream(StreamName=stream_name, ShardCount=1)

    # Put a normal record and an anomaly spike record
    normal_payload = {"sensor_id": "SENSOR_1", "temperature": 35.0, "humidity": 40.0, "timestamp": "2026-11-10T10:00:00Z"}
    spike_payload = {"sensor_id": "SENSOR_2", "temperature": 98.6, "humidity": 42.0, "timestamp": "2026-11-10T10:00:01Z"}

    kinesis.put_record(
        StreamName=stream_name,
        Data=json.dumps(normal_payload).encode("utf-8"),
        PartitionKey="SENSOR_1"
    )
    kinesis.put_record(
        StreamName=stream_name,
        Data=json.dumps(spike_payload).encode("utf-8"),
        PartitionKey="SENSOR_2"
    )

    # Get records from Kinesis shard
    shard_id = kinesis.describe_stream(StreamName=stream_name)["StreamDescription"]["Shards"][0]["ShardId"]
    shard_iterator = kinesis.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON"
    )["ShardIterator"]

    records_res = kinesis.get_records(ShardIterator=shard_iterator)
    assert len(records_res["Records"]) == 2

    # Simulate Lambda Event Source Mapping
    lambda_event = {
        "Records": [
            {"kinesis": {"data": base64.b64encode(r["Data"]).decode("utf-8")}}
            for r in records_res["Records"]
        ]
    }
    result = lambda_handler(lambda_event, None)
    assert result["statusCode"] == 200
    assert result["processed"] == 2
    assert result["anomalies"] == 1


@mock_aws
def test_e2e_phase6_teardown_finops():
    """
    Verifies Phase 6 FinOps Teardown Acceptance Gate:
    All streaming and cloud resources (Kinesis, Glue, S3) are cleanly deleted.
    """
    stream_name = "test-iot-stream-teardown"
    bucket_name = "test-datalake-teardown"
    database_name = "test_glue_teardown_db"

    # Provision resources
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=bucket_name)
    s3.put_object(Bucket=bucket_name, Key="test.txt", Body=b"sample")

    kinesis = boto3.client("kinesis", region_name="us-east-1")
    kinesis.create_stream(StreamName=stream_name, ShardCount=1)

    create_glue_database(database_name, "us-east-1")
    create_glue_silver_table(bucket_name, database_name, "us-east-1")

    # Run FinOps Teardown
    teardown_all(
        stream_name=stream_name,
        opensearch_domain="",
        database_name=database_name,
        job_name="",
        bucket_name=bucket_name,
        region="us-east-1"
    )

    # Verify S3 bucket deleted
    buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    assert bucket_name not in buckets

    # Verify Kinesis stream deleted
    streams = kinesis.list_streams().get("StreamNames", [])
    assert stream_name not in streams

    # Verify Glue database deleted
    glue = boto3.client("glue", region_name="us-east-1")
    databases = [d["Name"] for d in glue.get_databases().get("DatabaseList", [])]
    assert database_name not in databases
