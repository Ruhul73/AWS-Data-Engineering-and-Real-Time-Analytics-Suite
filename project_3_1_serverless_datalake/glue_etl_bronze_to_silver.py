"""
Project 3.1: Serverless Medallion Data Lake - Silver Layer PySpark ETL
glue_etl_bronze_to_silver.py - Cleans, casts, deduplicates & writes Snappy Parquet to S3 Silver.
Runs on AWS Glue Spark 3.3+.
"""

import sys
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, year, month


def transform_bronze_to_silver(raw_df: DataFrame) -> DataFrame:
    """
    Applies Medallion data quality rules:
    - Removes rows where order_amount is null
    - Filters for status == 'Completed' (dropping Cancelled/failed orders)
    - Deduplicates on order_id
    - Casts order_amount to double
    - Parses order_date to date format (yyyy-MM-dd)
    - Derives partition columns: year and month
    """
    cleaned_df = (
        raw_df.filter(col("order_amount").isNotNull() & (col("status") == "Completed"))
        .dropDuplicates(["order_id"])
        .withColumn("order_amount", col("order_amount").cast("double"))
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("year", year(col("order_date")))
        .withColumn("month", month(col("order_date")))
    )
    return cleaned_df


def run_etl():
    """
    Main ETL job runner compatible with both AWS Glue environment and local PySpark.
    """
    # Check if running inside AWS Glue
    try:
        from awsglue.utils import getResolvedOptions
        from pyspark.context import SparkContext
        from awsglue.context import GlueContext
        from awsglue.job import Job

        args = getResolvedOptions(sys.argv, ["JOB_NAME", "DATALAKE_BUCKET"])
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args["JOB_NAME"], args)
        bucket = args["DATALAKE_BUCKET"]
        is_glue = True
    except ImportError:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("glue_etl_bronze_to_silver_local").getOrCreate()
        bucket = os.environ.get("DATALAKE_BUCKET", "aws-retail-datalake-demo")
        is_glue = False
        job = None

    bronze_path = f"s3://{bucket}/bronze/retail_sales/"
    silver_path = f"s3://{bucket}/silver/retail_sales/"

    raw_df = spark.read.option("header", "true").option("inferSchema", "true").csv(bronze_path)
    cleaned_df = transform_bronze_to_silver(raw_df)

    (
        cleaned_df.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy("year", "month")
        .save(silver_path)
    )

    if is_glue and job:
        job.commit()
    print(f"ETL job completed successfully. Silver data written to {silver_path}")


if __name__ == "__main__":
    run_etl()
