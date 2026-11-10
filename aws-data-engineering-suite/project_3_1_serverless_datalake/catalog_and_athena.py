"""
Project 3.1: Serverless Medallion Data Lake - Glue Data Catalog & Athena SQL Analytics
catalog_and_athena.py - Provisions Glue Database, Crawler / Table & executes Athena SQL queries.
"""

import os
import time
import boto3
from botocore.exceptions import ClientError


def create_glue_database(database_name: str = "retail_datalake_db", region: str = "us-east-1"):
    """
    Provisions the Glue Data Catalog database if it does not already exist.
    """
    glue = boto3.client("glue", region_name=region)
    try:
        glue.create_database(
            DatabaseInput={
                "Name": database_name,
                "Description": "Database for retail medallion data lake (Bronze/Silver)"
            }
        )
        print(f"Created Glue Database: {database_name}")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "AlreadyExistsException":
            print(f"Glue Database '{database_name}' already exists.")
        else:
            print(f"Notice creating Glue Database: {e}")


def create_glue_silver_table(bucket: str, database: str = "retail_datalake_db", region: str = "us-east-1"):
    """
    Creates or registers the retail_sales table in Glue Catalog pointing to Silver Snappy Parquet.
    Partitioned by year and month.
    """
    glue = boto3.client("glue", region_name=region)
    table_name = "retail_sales"
    s3_location = f"s3://{bucket}/silver/retail_sales/"

    table_input = {
        "Name": table_name,
        "Description": "Curated Silver layer retail transactions in Snappy Parquet format",
        "StorageDescriptor": {
            "Columns": [
                {"Name": "order_id", "Type": "int"},
                {"Name": "customer_id", "Type": "string"},
                {"Name": "product_category", "Type": "string"},
                {"Name": "order_amount", "Type": "double"},
                {"Name": "order_date", "Type": "date"},
                {"Name": "status", "Type": "string"}
            ],
            "Location": s3_location,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"}
            }
        },
        "PartitionKeys": [
            {"Name": "year", "Type": "int"},
            {"Name": "month", "Type": "int"}
        ],
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "EXTERNAL": "TRUE",
            "parquet.compression": "SNAPPY"
        }
    }

    try:
        glue.create_table(DatabaseName=database, TableInput=table_input)
        print(f"Created Glue Catalog table: {database}.{table_name}")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "AlreadyExistsException":
            glue.update_table(DatabaseName=database, TableInput=table_input)
            print(f"Updated Glue Catalog table: {database}.{table_name}")
        else:
            print(f"Notice creating Glue Table: {e}")


def repair_table_partitions(bucket: str, database: str = "retail_datalake_db", region: str = "us-east-1"):
    """
    Runs MSCK REPAIR TABLE via Athena to discover new Parquet partitions.
    """
    athena = boto3.client("athena", region_name=region)
    output_uri = f"s3://{bucket}/athena-results/"
    repair_sql = f'MSCK REPAIR TABLE "{database}"."retail_sales";'

    try:
        resp = athena.start_query_execution(
            QueryString=repair_sql,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output_uri}
        )
        qid = resp["QueryExecutionId"]
        while True:
            status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break
            time.sleep(2)
        print(f"Partition repair finished with state: {status}")
    except Exception as e:
        print(f"Partition repair notice: {e}")


def run_athena_analytics(bucket: str, database: str = "retail_datalake_db"):
    """
    Automate serverless SQL aggregation execution via Amazon Athena.
    Aggregates orders and revenue per product category for year 2026.
    """
    athena = boto3.client("athena")
    output_uri = f"s3://{bucket}/athena-results/"
    sql = f"""
    SELECT product_category,
           COUNT(order_id) AS total_orders,
           ROUND(SUM(order_amount), 2) AS total_revenue
    FROM "{database}"."retail_sales"
    WHERE year = 2026
    GROUP BY product_category
    ORDER BY total_revenue DESC;
    """
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_uri}
    )
    qid = resp["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(2)

    if status != "SUCCEEDED":
        state_change_reason = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"].get("StateChangeReason", "")
        raise RuntimeError(f"Athena query {qid} failed with state {status}: {state_change_reason}")

    return athena.get_query_results(QueryExecutionId=qid)


if __name__ == "__main__":
    bucket = os.environ.get("DATALAKE_BUCKET", "aws-retail-datalake-demo")
    db = os.environ.get("GLUE_DATABASE", "retail_datalake_db")
    region = os.environ.get("AWS_REGION", "us-east-1")

    create_glue_database(db, region)
    create_glue_silver_table(bucket, db, region)
    repair_table_partitions(bucket, db, region)
    results = run_athena_analytics(bucket, db)
    print("Athena Query Results:")
    for row in results.get("ResultSet", {}).get("Rows", []):
        data = [col.get("VarCharValue", "") for col in row.get("Data", [])]
        print(" | ".join(data))

