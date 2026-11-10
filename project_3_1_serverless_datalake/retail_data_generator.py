"""
Project 3.1: Serverless Medallion Data Lake - Bronze Layer Ingestion
retail_data_generator.py - Generates raw transactional CSVs & uploads to S3 Bronze layer.
"""

import os
import csv
import random
import boto3
from datetime import datetime, timedelta
from botocore.exceptions import ClientError


def create_datalake_bucket(bucket_name: str, region: str = "us-east-1"):
    """
    Creates the S3 Data Lake bucket with bronze/, silver/, and athena-results/ prefixes.
    """
    s3 = boto3.client("s3", region_name=region)
    try:
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region}
            )
        print(f"Created S3 bucket: {bucket_name}")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ["BucketAlreadyOwnedByYou", "BucketAlreadyExists"]:
            print(f"S3 bucket already exists: {bucket_name}")
        else:
            print(f"Notice during bucket creation: {e}")

    # Create folder prefixes
    prefixes = ["bronze/", "silver/", "athena-results/"]
    for prefix in prefixes:
        try:
            s3.put_object(Bucket=bucket_name, Key=prefix)
        except Exception as e:
            print(f"Prefix {prefix} notice: {e}")

    print(f"Initialized prefixes bronze/, silver/, athena-results/ in {bucket_name}")


def generate_bronze_csv(bucket_name: str, filename: str = "sales_data.csv", rows: int = 500):
    """
    Generates realistic transactional retail data containing intentional null rows
    and cancelled orders to verify ETL filtering, then uploads to S3 bronze layer.
    """
    categories = ["Electronics", "Apparel", "Home", "Groceries"]
    statuses = ["Completed", "Completed", "Completed", "Cancelled"]
    base_date = datetime(2026, 10, 1)

    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["order_id", "customer_id", "product_category", "order_amount", "order_date", "status"])
        for i in range(1001, 1001 + rows):
            status = random.choice(statuses)
            amount = "" if status == "Cancelled" else round(random.uniform(15.0, 950.0), 2)
            order_date = (base_date + timedelta(days=random.randint(0, 45))).strftime("%Y-%m-%d")
            writer.writerow([i, f"C{random.randint(1, 80):02d}", random.choice(categories), amount, order_date, status])

    s3 = boto3.client("s3")
    s3.upload_file(filename, bucket_name, f"bronze/retail_sales/{filename}")
    print(f"Uploaded {rows} raw records to s3://{bucket_name}/bronze/retail_sales/{filename}")


if __name__ == "__main__":
    bucket = os.environ.get("DATALAKE_BUCKET", "aws-retail-datalake-demo")
    region = os.environ.get("AWS_REGION", "us-east-1")
    create_datalake_bucket(bucket, region)
    generate_bronze_csv(bucket, rows=500)
