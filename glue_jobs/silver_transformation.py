import sys
import boto3
import json
import logging
from datetime import datetime, timezone
from delta.tables import DeltaTable

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


def clean_string_columns(df):

    string_columns = []

    for field in df.schema.fields:

        if isinstance(field.dataType, StringType):

            string_columns.append(field.name)

    for column_name in string_columns:

        df = (
            df
            .withColumn(
                column_name,
                F.trim(
                    F.regexp_replace(
                        F.regexp_replace(
                            F.col(column_name),
                            r"https?://\S+",
                            ""
                        ),
                        r"\s+",
                        " "
                    )
                )
            )
        )
    return df


def apply_value_mappings(df, config):

    value_mappings = config.get(
        "value_mappings",
        {}
    )

    for column_name, mappings in value_mappings.items():

        for old_value, new_value in mappings.items():

            df = df.withColumn(
                column_name,
                F.when(
                    F.col(column_name) == old_value,
                    F.lit(new_value)
                ).otherwise(
                    F.col(column_name)
                )
            )

    return df


def filter_invalid_records(df, config):

    try:

        business_keys = config["business_keys"]

        logger.info(
            f"Filtering invalid records using business keys: {business_keys}"
        )

        invalid_condition = None

        for column_name in business_keys:

            condition = F.col(column_name).isNull()

            if invalid_condition is None:

                invalid_condition = condition

            else:

                invalid_condition = (invalid_condition | condition)

        range_validations = config.get("range_validations", {})

        for column_name, valid_range in range_validations.items():

            min_value = valid_range.get("min")
            max_value = valid_range.get("max")

            logger.info(
                f"Applying range validation for {column_name}: "
                f"min={min_value}, max={max_value}"
            )

            range_condition = None

            if min_value is not None:

                range_condition = F.col(column_name) < min_value

            if max_value is not None:

                max_condition = F.col(column_name) > max_value

                if range_condition is None:

                    range_condition = max_condition

                else:

                    range_condition = (range_condition | max_condition)

            if range_condition is not None:

                if invalid_condition is None:

                    invalid_condition = range_condition

                else:

                    invalid_condition = (invalid_condition | range_condition)

        valid_df = df.filter(~invalid_condition)

        invalid_df = df.filter(invalid_condition)

        return valid_df, invalid_df

    except Exception as e:

        logger.exception(
            "Failed while filtering invalid records"
        )

        raise


def keep_latest_records(df, config):

    try:

        business_keys = config["business_keys"]

        last_updated = config["watermark_column"]

        logger.info(
            f"Keeping latest records using "
            f"business keys: {business_keys}"
        )

        window_spec = (
            Window.partitionBy(
                *business_keys).orderBy(F.col(last_updated).desc(), F.col("_ingested_at").desc())
        )

        latest_df = (
            df
            .withColumn(
                "_row_number",
                F.row_number().over(window_spec)
            )
            .filter(
                F.col("_row_number") == 1
            )
            .drop(
                "_row_number"
            )
        )

        return latest_df

    except Exception as e:
        logger.exception(" Failed while keeping latest records.")
        raise


def apply_transformations(df, table_name, config):

    logger.info(f"Applying Silver transformations for {table_name}")

    try:

        transformed_df = df
        cast_columns = config.get("cast_columns", {})

        for column_name, data_type in cast_columns.items():

            transformed_df = transformed_df.withColumn(
                column_name,
                F.col(column_name).cast(data_type)
            )

        return transformed_df

    except Exception as e:

        logger.exception("Transformation Failed.")

        raise


def remove_duplicates(df, config):

    logger.info("Removing duplicate records")

    try:

        exclude_columns = ["updated_at", "_ingested_at"]

        duplicate_columns = config.get("duplicate_columns", [])

        if not duplicate_columns:

            logger.info(
                "No duplicate columns configured"
            )

            return df

        cleaned_df = df.dropDuplicates(duplicate_columns)

        return cleaned_df

    except Exception as e:
        logger.exception("Removing duplicates failed.")
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
