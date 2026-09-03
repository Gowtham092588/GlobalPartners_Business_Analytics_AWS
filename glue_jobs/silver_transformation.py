import sys
import boto3
import json
import logging
from datetime import datetime, timezone
from delta.tables import DeltaTable
from glue_jobs.silver_utils import (
    filter_invalid_records,
    clean_string_columns,
    keep_latest_records,
    apply_value_mappings,
    apply_transformations,
    remove_duplicates)


from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


args = getResolvedOptions(sys.argv, ["JOB_NAME", "TABLE_NAME"])
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)

AWS_REGION = "us-east-2"
BRONZE_BUCKET = "globalpartners-aws-data-bkt"
SILVER_BUCKET = "globalpartners-aws-data-bkt"
QUARANTINE_BUCKET = "globalpartners-aws-error-bkt"


def load_config(bucket, key):

    try:

        s3 = boto3.client("s3", AWS_REGION)

        response = s3.get_object(
            Bucket=bucket,
            Key=key
        )

        config_data = response["Body"].read().decode('utf-8')

        return json.loads(config_data)

    except Exception as e:
        logger.exception("No config file to load.")
        raise


def read_bronze_table(bronze_path):

    try:

        source_path = (f"s3://{BRONZE_BUCKET}/{bronze_path}")

        logger.info(f"Reading bronze data from {source_path}")

        df = (
            spark
            .read
            .format("delta")
            .load(source_path)
        )

        return df

    except Exception as e:

        logger.exception("Failed reading Bronze table.")

        raise


def merge_current_records(df, config):

    try:

        silver_path = (f"s3://{SILVER_BUCKET}/"
                       f"{config['silver_path']}"
                       )

        business_keys = config["business_keys"]

        logger.info(f"Merge latest records to {silver_path}")

        if not DeltaTable.isDeltaTable(spark, silver_path):
            logger.info("Silver Delta table does not exist. Creating it.")

            (
                df.write
                .format("delta")
                .mode("overwrite")
                .save(silver_path)
            )

            return

        delta_table = DeltaTable.forPath(
            spark,
            silver_path
        )

        merge_condition = " AND ".join([
            f"target.{keys} = source.{keys}"
            for keys in business_keys
        ])

        (
            delta_table.alias("target")
            .merge(
                df.alias("source"),
                merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        logger.info("SCD1 merge completed successfully.")

    except Exception as e:
        logger.exception("Apply SCD1 Failed.")
        raise


def write_quarantine_records(invalid_df, table_name):

    logger.info(
        f"Checking quarantine records for {table_name}"
    )

    try:

        if invalid_df.isEmpty():

            logger.info(f"No invalid records found for {table_name}")

            return

        quarantine_path = (
            f"s3://{QUARANTINE_BUCKET}/quarantine/silver/{table_name}/"
        )

        quarantine_df = (
            invalid_df.withColumn(
                "_quarantine_reason",
                F.lit("Invalid or Missing Business key")
            )
            .withColumn(
                "_quarantined_at",
                F.current_timestamp()
            )
        )

        logger.warning(
            f"Writing invalid records to {quarantine_path}"
        )

        (
            quarantine_df.write
            .format("parquet")
            .mode("append")
            .save(quarantine_path)

        )

        logger.info(
            f"Quarantine write completed for {table_name}"
        )

    except Exception as e:

        logger.exception("Writing invalid records to quarantine failed!!")

        raise


def write_silver_table(df, table_name, config):

    logger.info(
        f"Writing silver data to {table_name}"
    )

    try:

        load_type = config.get("load_type")

        silver_path = (
            f"s3://{SILVER_BUCKET}/"
            f"{config['silver_path']}"
        )

        if load_type == "FULL":

            logger.info(
                f"Performing full Silver load for {table_name}")

            (
                df.write
                .format("delta")
                .mode("overwrite")
                .save(silver_path)
            )

        elif load_type == "MERGE":

            logger.info(
                f"Performing Silver load for {table_name}")

            merge_current_records(df, config)

        else:

            raise ValueError(
                f"Unsupported load type: {load_type}"
            )

    except Exception as e:

        logger.exception("Writing to silver table failed!!")

        raise


def main():

    try:

        silver_config = load_config(
            "globalpartners-aws-data-bkt", "config/silver_table_config.json")

        table_name = args["TABLE_NAME"]

        config = silver_config[table_name]

        logger.info(f"Starting Silver transformation for {table_name}")

        bronze_df = read_bronze_table(config["bronze_path"])

        if bronze_df.rdd.isEmpty():
            logger.info("No records to process")
            return

        valid_df, invalid_df = filter_invalid_records(bronze_df, config)

        write_quarantine_records(invalid_df, table_name)

        cleaned_valid_df = clean_string_columns(
            valid_df
        )

        mapped_valid_df = apply_value_mappings(
            cleaned_valid_df, config
        )

        latest_df = keep_latest_records(mapped_valid_df, config)

        transformed_df = apply_transformations(latest_df, table_name, config)

        cleaned_df = remove_duplicates(transformed_df, config)

        write_silver_table(cleaned_df, table_name, config)

        logger.info(
            f"Silver processing completed for {table_name}"
        )

    except Exception as e:
        logger.exception(
            f"Silver transformation Glue job has failed for {table_name}")
        raise


if __name__ == "__main__":

    try:

        logger.info("Silver Glue job has started")

        main()

        job.commit()

        logger.info("Silver Glue job has completed successfully!!")

    except Exception as e:
        logger.exception("Glue Job Failed.")
        raise
