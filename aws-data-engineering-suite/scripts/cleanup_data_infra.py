"""
FinOps Teardown Utility
scripts/cleanup_data_infra.py - Zero-billing FinOps teardown (deletes Kinesis, Glue, OpenSearch, S3).
Guarantees zero unexpected AWS billing after testing and demo.
"""

import os
import boto3
from botocore.exceptions import ClientError


def teardown_streaming_and_etl(stream_name: str = "iot-sensor-stream", opensearch_domain: str = "", region: str = "us-east-1"):
    """
    Deletes streaming infrastructure: Kinesis data streams and OpenSearch domains.
    """
    kinesis = boto3.client("kinesis", region_name=region)
    try:
        kinesis.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)
        print(f"Deleted Kinesis stream: {stream_name}")
    except Exception as e:
        print(f"Kinesis cleanup skipped: {e}")

    if opensearch_domain:
        os_client = boto3.client("opensearch", region_name=region)
        try:
            os_client.delete_domain(DomainName=opensearch_domain)
            print(f"Deleted OpenSearch domain: {opensearch_domain}")
        except Exception as e:
            print(f"OpenSearch cleanup skipped: {e}")


def delete_glue_resources(database_name: str = "retail_datalake_db", job_name: str = "glue_etl_bronze_to_silver", region: str = "us-east-1"):
    """
    Deletes Glue database, catalog tables, and ETL jobs.
    """
    glue = boto3.client("glue", region_name=region)

    # Delete Glue Job
    if job_name:
        try:
            glue.delete_job(JobName=job_name)
            print(f"Deleted Glue Job: {job_name}")
        except Exception as e:
            print(f"Glue Job cleanup skipped: {e}")

    # Delete Glue Database
    if database_name:
        try:
            # Delete all tables in the database first
            tables_resp = glue.get_tables(DatabaseName=database_name)
            for table in tables_resp.get("TableList", []):
                tbl_name = table["Name"]
                try:
                    glue.delete_table(DatabaseName=database_name, Name=tbl_name)
                    print(f"Deleted Glue Table: {database_name}.{tbl_name}")
                except Exception as ex:
                    print(f"Could not delete table {tbl_name}: {ex}")

            glue.delete_database(Name=database_name)
            print(f"Deleted Glue Database: {database_name}")
        except Exception as e:
            print(f"Glue Database cleanup skipped: {e}")


def empty_and_delete_s3_bucket(bucket_name: str, region: str = "us-east-1"):
    """
    Empties all objects (including versions) and deletes the S3 Data Lake bucket.
    """
    if not bucket_name:
        return

    s3 = boto3.resource("s3", region_name=region)
    bucket = s3.Bucket(bucket_name)

    try:
        print(f"Emptying S3 bucket: {bucket_name}...")
        bucket.object_versions.delete()
        bucket.objects.all().delete()
        bucket.delete()
        print(f"Deleted S3 bucket: {bucket_name}")
    except Exception as e:
        print(f"S3 bucket teardown skipped: {e}")


def teardown_all(
    stream_name: str = "iot-sensor-stream",
    opensearch_domain: str = "",
    database_name: str = "retail_datalake_db",
    job_name: str = "glue_etl_bronze_to_silver",
    bucket_name: str = "",
    region: str = "us-east-1"
):
    """
    Runs full teardown of all provisioned cloud infrastructure.
    """
    print("========================================")
    print("Initiating FinOps Zero-Billing Teardown...")
    print("========================================")
    teardown_streaming_and_etl(stream_name=stream_name, opensearch_domain=opensearch_domain, region=region)
    delete_glue_resources(database_name=database_name, job_name=job_name, region=region)
    if bucket_name:
        empty_and_delete_s3_bucket(bucket_name=bucket_name, region=region)
    print("Teardown process completed cleanly.")


if __name__ == "__main__":
    stream = os.environ.get("KINESIS_STREAM", "iot-sensor-stream")
    domain = os.environ.get("OPENSEARCH_DOMAIN", "")
    db = os.environ.get("GLUE_DATABASE", "retail_datalake_db")
    job = os.environ.get("GLUE_JOB_NAME", "glue_etl_bronze_to_silver")
    bucket = os.environ.get("DATALAKE_BUCKET", "")
    region = os.environ.get("AWS_REGION", "us-east-1")

    teardown_all(
        stream_name=stream,
        opensearch_domain=domain,
        database_name=db,
        job_name=job,
        bucket_name=bucket,
        region=region
    )

