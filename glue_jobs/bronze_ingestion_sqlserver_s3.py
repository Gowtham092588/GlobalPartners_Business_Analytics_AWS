import sys
import boto3
import json
import logging
from delta.tables import DeltaTable
from datetime import datetime, timezone

from pyspark.context import SparkContext
from pyspark.sql import functions as F
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


GLUE_CONNECTION_NAME = "globalpartners-rds-conn"
AWS_REGION = 'us-east-2'
S3_DATA_BUCKET = "globalpartners-aws-data-bkt"
S3_ERROR_BUCKET = "globalpartners-aws-error-bkt"


def get_s3_client():
    return boto3.client('s3', region_name=AWS_REGION)


def load_config(bucket, key):

    try:

        logger.info("Starting to load bronze config json file.")

        s3 = get_s3_client()

        response = s3.get_object(
            Bucket=bucket,
            Key=key
        )

        config_data = response["Body"].read().decode("utf-8")

        return json.loads(config_data)

    except Exception as e:

        logger.exception("Failed to load the config file")

        raise


def load_checkpoint(s3, checkpoint_key):

    try:
        response = s3.get_object(
            Bucket=S3_DATA_BUCKET,
            Key=checkpoint_key
        )

        checkpoint_data = response['Body'].read().decode('utf-8')
        checkpoint = json.loads(checkpoint_data)

        return checkpoint.get('last_updated_at', None)

    except Exception as e:
        logger.warning(
            "No checkpoint found.Initial/full load will run."
        )
        return None


def save_checkpoint(s3, checkpoint_key, last_updated_at):

    payload = {

        "last_updated_at": str(last_updated_at),
        "checkpoint_saved_at": datetime.now(timezone.utc).isoformat()
    }

    try:

        s3.put_object(
            Bucket=S3_DATA_BUCKET,
            Key=checkpoint_key,
            Body=json.dumps(payload).encode('utf-8'),
            ContentType='application/json'

        )

        logger.info(f"Checkpoint saved: {last_updated_at}")

    except Exception as e:
        logger.exception(F"No checkpoint to be saved. Check for error {e}")

        raise


def read_incremental_data(last_checkpoint, source_table, watermark_column):

    try:

        if last_checkpoint is None:

            query = f"""
            SELECT *
            FROM {source_table}
            """
            logger.info(f"Initial load for {source_table}")
        else:

            query = f"""
            SELECT *
            FROM {source_table}
            WHERE {watermark_column}
                > '{last_checkpoint}'
            """
            logger.info(f"Incremental load for {source_table}")
            logger.info(
                f"Reading records where {watermark_column} > {last_checkpoint}")

        dynamic_frame = (
            glue_context
            .create_dynamic_frame
            .from_options(
                connection_type="sqlserver",
                connection_options={
                    "useConnectionProperties": "true",
                    "connectionName": GLUE_CONNECTION_NAME,
                    "dbtable": source_table,
                    "sampleQuery": query
                }
            )
        )

        return dynamic_frame.toDF()

    except Exception as e:
        logger.exception(
            f"Failed to read data from {source_table}"
        )
        raise


def add_error(df, condition, message):

    return df.withColumn(
        "_dq_error",
        F.when(
            condition,
            F.when(
                F.col("_dq_error").isNull(),
                F.lit(message)
            )
            .otherwise(
                F.concat_ws(
                    "; ",
                    F.col("_dq_error"),
                    F.lit(message)
                )
            )
        ).otherwise(
            F.col("_dq_error")
        )
    )


def validate_dataframe(df, table_name, config):

    try:

        logger.info(f"Running DQ checks for {table_name}")

        df = df.withColumn("_dq_error", F.lit(None).cast("string"))

        required_columns = (config.get("required_columns", []))

        for column_name in required_columns:
            condition = (F.col(column_name).isNull() |
                         (
                F.trim(
                    F.col(column_name)
                    .cast("string")
                ) == ""
            )
            )
            df = add_error(df, condition, f"{column_name} is missing")

        positive_columns = (config.get("positive_columns", []))

        for column_name in positive_columns:
            condition = (F.col(column_name) <= 0)
            df = add_error(
                df, condition, f"{column_name} must be greater than 0")

        non_negative_columns = (config.get("non_negative_columns", []))

        for column_name in non_negative_columns:
            condition = (F.col(column_name) < 0)
            df = add_error(df, condition, f"{column_name} cannot be negative")

        range_rules = (config.get("range_rules", {}))

        for column_name, rule in range_rules.items():
            min_value = rule["min"]
            max_value = rule["max"]
            condition = (
                (F.col(column_name) < min_value)
                |
                (F.col(column_name) > max_value)
            )
            df = add_error(
                df, condition, f"{column_name} must be between {min_value} and {max_value}")

        duplicate_columns = (config.get("duplicate_columns", []))

        if duplicate_columns:
            duplicate_window = (Window.partitionBy(*duplicate_columns))

            df = df.withColumn("_duplicate_count",
                               F.count("*").over(
                                   duplicate_window
                               )
                               )

            df = add_error(df,
                           F.col(
                               "_duplicate_count"
                           ) > 1,
                           "Duplicate record based on "
                           + ", ".join(
                               duplicate_columns
                           )
                           )

            df = df.drop(
                "_duplicate_count"
            )

        good_df = (df.filter(
            F.col(
                "_dq_error"
            ).isNull()
        )
            .drop(
                "_dq_error"
        )
        )

        error_df = (df.filter(
            F.col(
                    "_dq_error"
                    ).isNotNull()
        )
        )

        return good_df, error_df

    except Exception:
        logger.exception(
            f"DQ validation failed for {table_name}"
        )
        raise


def write_bronze_records(df, bronze_path, config):

    if df.rdd.isEmpty():
        print("No good records to write.")
        return

    output_path = (f"s3://{S3_DATA_BUCKET}/{bronze_path}")

    business_keys = config['business_keys']
    watermark_column = config['watermark_column']
    try:

        if not DeltaTable.isDeltaTable(spark, output_path):

            logger.info(
                "Bronze Delta table does not exist. Creating it."
            )

            (
                df.write
                .format("delta")
                .mode("append")
                .save(output_path)
            )

            logger.info(f"Bronze table written to {output_path}")

            return

            match_columns = (
                business_keys + [watermark_column]
            )

            merge_condition = " AND ".join(
                [
                    f"target.{columns} = source.{columns}"
                    for columns in match_columns
                ]
            )

            delta_table = DeltaTable.forPath(spark, output_path)

            (
                delta_table.alias('target')
                .merge(
                    df.alias('source'),
                    merge_condition
                )
                .whenNotMatchedInsertAll()
            )

    except Exception as e:
        logger.exception(f"Failed to write bronze table to {output_path}")
        raise


def write_error_records(
    df,
    error_path,
    table_name
):

    if df.isEmpty():
        logger.info(f"No error records for {table_name}")
        return

    run_time = (datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"))

    output_path = (f"s3://{S3_ERROR_BUCKET}/{error_path}run_time={run_time}/")

    try:

        (
            df.write
            .mode("append")
            .format("parquet")
            .option(
                "compression",
                "snappy"
            )
            .save(output_path)
        )

        logger.info(f"Error records written to {output_path}")

    except Exception as e:
        logger.exception(f"Failed to write error records to {output_path}")
        raise


def main():

    try:

        table_name = args["TABLE_NAME"]

        bronze_config = load_config(
            "globalpartners-aws-data-bkt", "config/bronze_table_config.json")

        logger.info(f"Starting Bronze ingestion for {table_name}")

        config = bronze_config[table_name]

        s3 = get_s3_client()

        logger.info(f"Processing {table_name}")

        # 1. Read previous checkpoint
        last_checkpoint = load_checkpoint(s3, config["checkpoint_key"])

        logger.info(f"Previous checkpoint: {last_checkpoint}")

        # 2. Read only new/changed records
        source_df = read_incremental_data(
            last_checkpoint,
            config["source_table"],
            config["watermark_column"]
        )

        # 3. Stop processing this table
        # if nothing changed
        if source_df.isEmpty():
            logger.info(f"No new or updated records for {table_name}")
            return

        # 4. Count extracted records
        source_count = source_df.count()

        # 5. Find newest updated_at
        # in THIS batch
        max_updated_at = (
            source_df
            .agg(
                F.max(
                    config[
                        "watermark_column"
                    ]
                )
                .alias(
                    "max_updated_at"
                )
            )
            .collect()[0][
                "max_updated_at"
            ]
        )

        source_df = (
            source_df
            .withColumn(
                "_ingested_at",
                F.current_timestamp()
            )
        )

        # 7. Write data

        write_bronze_records(
            source_df,
            config["bronze_path"],
            config
        )

        good_df, error_df = validate_dataframe(
            source_df,
            table_name,
            config
        )

        good_count = good_df.count()
        error_count = error_df.count()

        write_error_records(
            error_df,
            config["error_path"],
            table_name
        )

        # 8. Save checkpoint ONLY
        # after writes succeed

        save_checkpoint(
            s3,
            config["checkpoint_key"],
            max_updated_at
        )

        logger.info(f"{table_name} DQ Summary")
        logger.info(f"Source records: {source_count}")
        logger.info(f"Good records: {good_count}")
        logger.info(f"Error records: {error_count}")

        logger.info("All tables processed.")

    except Exception:
        logger.exception(f"Bronze ingestion failed for {table_name}")
        raise


# =========================================================
# 9. Run Job
# =========================================================

if __name__ == "__main__":

    try:

        logger.info("Glue Job Started.")

        main()
        job.commit()

        logger.info("Glue job has completed successfully!!")

    except Exception as e:
        logger.exception("Glue Job Failed.")
        raise
