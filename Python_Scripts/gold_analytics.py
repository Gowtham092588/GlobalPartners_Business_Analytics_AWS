import boto3
import sys
import json
import logging
from functools import reduce
import operator

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


AWS_REGION = 'us-east-2'
SILVER_BUCKET = 'globalpartners-aws-data-bkt'
GOLD_BUCKET = 'globalpartners-aws-data-bkt'

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)


def load_config(bucket, key):

    try:

        s3 = boto3.client("s3", AWS_REGION)

        response = s3.get_object(
            Bucket=bucket,
            Key=key
        )

        config_data = response["Body"].read().decode("utf-8")

        logger.info("Config file loaded successfully!")

        return json.loads(config_data)

    except Exception as e:

        logger.exception("Failed to load config file.")
        raise


def read_delta_table(bucket, path):

    full_path = f"s3://{bucket}/{path}"

    logger.info(
        f"Reading Delta table from: {full_path}"
    )

    df = (
        spark
        .read
        .format("delta")
        .load(full_path)
    )

    return df


def build_latest_aggregated_dimension(silver_df, config):

    logger.info("Building latest aggregated Gold dimension")

    business_keys = config["business_keys"]
    latest_order_columns = config["latest_order_columns"]
    latest_attribute_columns = config["latest_attribute_columns"]
    aggregate = config.get("aggregations", {})

    valid_df = silver_df

    for key in business_keys:
        valid_df = valid_df.filter(F.col(key).isNotNull())

    agg_expressions = []

    for column_name, rules in aggregate.items():

        for operation_name, output_column in rules.items():

            if operation_name == 'min':

                agg_expressions.append(F.min(column_name).alias(output_column))

            elif operation_name == 'max':

                agg_expressions.append(F.max(column_name).alias(output_column))

    aggregated_df = (
        valid_df
        .groupby(*business_keys)
        .agg(*agg_expressions)
    )

    order_expressions = [
        F.col(column).desc()
        for column in latest_order_columns
    ]

    window_spec = (
        Window
        .partitionBy(*business_keys)
        .orderBy(*order_expressions)
    )

    latest_df = (
        valid_df
        .withColumn(
            "_row_num",
            F.row_number().over(window_spec)
        )
        .filter(F.col("_row_num") == 1)
        .select(
            *business_keys,
            *latest_attribute_columns
        )
    )

    transformed_df = (
        latest_df
        .join(
            aggregated_df,
            on=business_keys,
            how="inner"
        )
    )

    return transformed_df


def get_unique_dimension_records(silver_df, select_columns):

    logger.info("Getting unique items from Silver")

    unique_item_df = (
        silver_df
        .select(*select_columns)
        .dropDuplicates()
    )

    return unique_item_df


def assign_surrogate_keys(new_records_df, gold_path, surrogate_key):

    output_path = (
        f"s3://{GOLD_BUCKET}/{gold_path}"
    )

    if not DeltaTable.isDeltaTable(spark, output_path):

        logger.info("No Delta table existing.")
        max_key = 0

    else:
        existing_df = (
            spark
            .read
            .format('delta')
            .load(output_path)
        )

        max_key = (
            existing_df
            .agg(
                F.max(surrogate_key).alias("max_key")
            )
            .collect()[0]["max_key"]
        )

        if max_key is None:
            max_key = 0

    window_spec = Window.orderBy(
        *new_records_df.columns
    )

    new_records_key_df = (
        new_records_df
        .withColumn(
            surrogate_key,
            F.row_number().over(window_spec) + max_key
        )
    )

    return new_records_key_df


def apply_scd1(transformed_df, config):

    logger.info("Applying SCD1 logic")

    gold_path = config.get("gold_path")
    business_keys = config.get("business_keys")
    surrogate_key = config.get("surrogate_key")

    output_path = (f"s3://{GOLD_BUCKET}/{gold_path}")

    if not DeltaTable.isDeltaTable(spark, output_path):

        logger.info(
            f"Gold table does not exist. Creating: {output_path}"
        )

        if surrogate_key is not None:

            initial_df = assign_surrogate_keys(
                transformed_df,
                gold_path,
                surrogate_key
            )

        else:

            initial_df = transformed_df

        (
            initial_df
            .write
            .format("delta")
            .save(output_path)
        )

        logger.info("Initial SCD1 load completed")

        return

    existing_dim_df = (
        spark
        .read
        .format("delta")
        .load(output_path)
    )

    if surrogate_key is not None:

        existing_dim_key_df = (
            existing_dim_df
            .select(
                *business_keys,
                surrogate_key
            )
        )

        transformed_key_df = (
            transformed_df
            .join(existing_dim_key_df,
                  on=business_keys,
                  how="left")
        )

        existing_records_df = (
            transformed_key_df
            .filter(F.col(surrogate_key).isNotNull())
        )

        new_records_null_df = (
            transformed_key_df
            .filter(F.col(surrogate_key).isNull())
            .drop(surrogate_key)
        )

        if new_records_null_df.isEmpty():

            logger.info("No new dimension records found")

            source_df = existing_records_df

        else:

            new_records_key_df = assign_surrogate_keys(
                new_records_null_df,
                gold_path,
                surrogate_key
            )

            source_df = (
                existing_records_df
                .unionByName(new_records_key_df)
            )

    else:

        source_df = transformed_df

    merge_condition = " AND ".join(
        [
            f"target.{column} = source.{column}"
            for column in business_keys
        ]
    )

    delta_table = DeltaTable.forPath(spark, output_path)

    (
        delta_table.alias("target")
        .merge(source_df.alias("source"),
               merge_condition
               )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info("SCD1 processing completed")


def apply_scd2(transformed_df, config):

    logger.info("Applying SCD2 logic")

    gold_path = config["gold_path"]
    business_keys = config["business_keys"]
    tracked_columns = config["tracked_columns"]
    surrogate_key = config["surrogate_key"]
    start_column = config["effective_start_date"]
    end_column = config["effective_end_date"]
    current_flag_column = config["current_flag_column"]

    output_path = (f"s3://{GOLD_BUCKET}/{gold_path}")

    if surrogate_key is None:
        raise ValueError(
            "SCD2 requires a surrogate key"
        )

    if not DeltaTable.isDeltaTable(
        spark,
        output_path
    ):

        logger.info(
            "Gold SCD2 dimension does not exist. "
            "Performing initial load."
        )

        initial_df = assign_surrogate_keys(
            transformed_df,
            gold_path,
            surrogate_key
        )

        initial_df = (
            initial_df
            .withColumn(
                start_column,
                F.lit("1900-01-01").cast("date")
            )
            .withColumn(
                end_column,
                F.lit("9999-12-31").cast("date")
            )
            .withColumn(
                current_flag_column,
                F.lit(True)
            )
        )

        (
            initial_df
            .write
            .format("delta")
            .mode("overwrite")
            .save(output_path)
        )

        logger.info("Initial SCD2 load completed")

        return

    existing_df = (
        spark
        .read
        .format("delta")
        .load(output_path)
    )

    current_dim_df = (
        existing_df
        .filter(F.col(current_flag_column) == True)
    )

    source_df = transformed_df.alias("source")

    target_df = current_dim_df.alias("target")

    business_key_condition = [
        F.col(f"source.{key}") == F.col(f"target.{key}")
        for key in business_keys
    ]

    comparison_df = (
        source_df.join(
            target_df,
            on=business_key_condition,
            how="left"
        )
    )

    new_records_df = (
        comparison_df
        .filter(
            F.col(f"target.{surrogate_key}").isNull()
        )
        .select("source.*")
    )

    change_conditions = [
        ~F.col(f"source.{column}").eqNullSafe(
            F.col(f"target.{column}")
        )
        for column in tracked_columns
    ]

    change_condition = reduce(
        operator.or_,
        change_conditions
    )

    change_records_df = (
        comparison_df
        .filter(
            F.col(f"target.{surrogate_key}").isNotNull()
            & change_condition
        )
        .select("source.*")
    )

    changed_keys_df = (
        change_records_df
        .select(*business_keys)
        .dropDuplicates()
    )

    delta_table = DeltaTable.forPath(
        spark,
        output_path
    )

    expire_condition = " AND ".join(
        [
            f"target.{key} = source.{key}"
            for key in business_keys
        ]
    )

    expire_condition += (
        f" AND target.{current_flag_column} = true"
    )

    if not changed_keys_df.isEmpty():

        (
            delta_table
            .alias("target")
            .merge(
                changed_keys_df.alias("source"),
                expire_condition
            )
            .whenMatchedUpdate(
                set={
                    current_flag_column: "false",
                    end_column: "date_sub(current_date(), 1)"
                }
            )
            .execute()
        )

    records_to_insert_df = (
        new_records_df
        .unionByName(change_records_df)
    )

    if not records_to_insert_df.isEmpty():

        records_to_insert_df = assign_surrogate_keys(
            new_records_df=records_to_insert_df,
            gold_path=gold_path,
            surrogate_key=surrogate_key
        )

        records_to_insert_df = (
            records_to_insert_df
            .withColumn(
                start_column,
                F.current_date()
            )
            .withColumn(
                end_column,
                F.lit("9999-12-31").cast("date")
            )
            .withColumn(
                current_flag_column,
                F.lit(True)
            )
        )

        (
            records_to_insert_df
            .write
            .format("delta")
            .mode("append")
            .save(output_path)
        )
    # ---------------------------------------------------------
    # Update non-SCD2 attributes on current records
    # ---------------------------------------------------------

    update_columns = config.get(
        "update_columns",
        []
    )

    if update_columns:

        delta_table = DeltaTable.forPath(
            spark,
            output_path
        )

        update_condition = " AND ".join(
            [
                f"target.{key} = source.{key}"
                for key in business_keys
            ]
        )

        update_condition += (
            f" AND target.{current_flag_column} = true"
        )

        update_values = {
            column: f"source.{column}"
            for column in update_columns
        }

        (
            delta_table
            .alias("target")
            .merge(
                transformed_df.alias("source"),
                update_condition
            )
            .whenMatchedUpdate(
                set=update_values
            )
            .execute()
        )

    logger.info("SCD2 processing completed")


def validate_dimension(gold_path, surrogate_key):

    output_path = f"s3://{GOLD_BUCKET}/{gold_path}"

    gold_df = (
        spark
        .read
        .format("delta")
        .load(output_path)
    )

    null_key_count = (
        gold_df
        .filter(
            F.col(surrogate_key).isNull()
        )
        .count()
    )

    if null_key_count > 0:
        raise ValueError(
            f"{surrogate_key} contains NULL values"
        )

    duplicate_key_count = (
        gold_df
        .groupBy(surrogate_key)
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_key_count > 0:
        raise ValueError(
            f"{surrogate_key} contains duplicate values"
        )

    logger.info(
        f"Dimension validation passed for {gold_path}"
    )


def validate_business_key_uniqueness(df, business_keys):

    logger.info(f"Validating uniqueness of {business_keys}")

    duplicate_key_count = (
        df
        .groupBy(*business_keys)
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_key_count > 0:
        raise ValueError(f"Duplicate business keys found for {business_keys}")

    logger.info("Business key uniqueness validation passed")


def add_dimension_key(fact_df, lookup):

    logger.info(
        f"Adding dimension key: {lookup['surrogate_key']}"
    )

    dimension_path = lookup["dimension_path"]
    lookup_type = lookup["lookup_type"]

    source_keys = lookup["source_keys"]
    dimension_keys = lookup["dimension_keys"]

    surrogate_key = lookup["surrogate_key"]

    full_dimension_path = (
        f"s3://{GOLD_BUCKET}/{dimension_path}"
    )

    dimension_df = (
        spark
        .read
        .format("delta")
        .load(full_dimension_path)
    )

    # Build business-key join conditions
    join_conditions = [
        F.col(f"fact.{source_col}") == F.col(f"dim.{dimension_col}")

        for source_col, dimension_col in zip(source_keys, dimension_keys)
    ]

    # SCD1 dimension lookup
    if lookup_type == "SCD1":

        fact_df = (
            fact_df.alias("fact")
            .join(
                dimension_df.alias("dim"),
                on=join_conditions,
                how="left"
            )
            .select(
                "fact.*",
                F.col(
                    f"dim.{surrogate_key}"
                ).alias(surrogate_key)
            )
        )

    # SCD2 dimension lookup
    elif lookup_type == "SCD2":

        start_column = lookup[
            "effective_start_column"
        ]

        end_column = lookup[
            "effective_end_column"
        ]

        fact_date_column = lookup[
            "fact_date_column"
        ]

        join_conditions.append(
            F.to_date(
                F.col(f"fact.{fact_date_column}")
            )
            >= F.col(f"dim.{start_column}")
        )

        join_conditions.append(
            F.to_date(
                F.col(f"fact.{fact_date_column}")
            )
            <= F.col(f"dim.{end_column}")
        )

        fact_df = (
            fact_df.alias("fact")
            .join(
                dimension_df.alias("dim"),
                on=join_conditions,
                how="left"
            )
            .select(
                "fact.*",
                F.col(
                    f"dim.{surrogate_key}"
                ).alias(surrogate_key)
            )
        )

    else:

        raise ValueError(
            f"Unsupported dimension lookup type: "
            f"{lookup_type}"
        )

    return fact_df


def aggregate_order_item_options(options_df):

    logger.info("Aggregating order item options")

    option_summary_df = (
        options_df
        .withColumn(
            "OPTION_AMOUNT",
            F.col("OPTION_PRICE") * F.col("OPTION_QUANTITY")
        )
        .groupBy(
            "ORDER_ID",
            "LINEITEM_ID"
        )
        .agg(
            F.sum(
                F.when(
                    F.col("OPTION_AMOUNT") >= 0,
                    F.col("OPTION_AMOUNT")
                ).otherwise(F.lit(0))
            ).alias("OPTION_AMOUNT"),

            F.abs(
                F.sum(
                    F.when(
                        F.col("OPTION_AMOUNT") < 0,
                        F.col("OPTION_AMOUNT")
                    ).otherwise(F.lit(0))
                )
            ).alias("DISCOUNT_AMOUNT")
        )
    )

    return option_summary_df


def build_fact_order_items(silver_df, config):

    logger.info("Building fact_order_items")

    fact_df = silver_df

    for lookup in config.get("dimension_lookups", []):
        fact_df = add_dimension_key(
            fact_df,
            lookup
        )

    fact_df = fact_df.withColumn(
        "ITEM_AMOUNT",
        F.col("ITEM_PRICE") * F.col("ITEM_QUANTITY")
    )

    options_df = read_delta_table(
        SILVER_BUCKET,
        config["options_silver_path"]
    )

    option_summary_df = aggregate_order_item_options(
        options_df
    )

    fact_df = (
        fact_df
        .join(
            option_summary_df,
            on=["ORDER_ID", "LINEITEM_ID"],
            how="left"
        )
        .fillna(
            {
                "OPTION_AMOUNT": 0,
                "DISCOUNT_AMOUNT": 0
            }
        )
        .withColumn(
            "GROSS_AMOUNT",
            F.col("ITEM_AMOUNT")
            + F.col("OPTION_AMOUNT")
        )
        .withColumn(
            "REVENUE",
            F.col("GROSS_AMOUNT")
            - F.col("DISCOUNT_AMOUNT")
        )
        .withColumn(
            "DATE_KEY",
            F.to_date("CREATION_TIME_UTC")
        )
    )

    return fact_df


def build_fact_order_item_options(silver_df, config):

    logger.info("Building fact_order_item_options")

    fact_df = (
        silver_df
        .select(*config["select_columns"])
        .withColumn(
            "OPTION_AMOUNT",
            F.col("OPTION_PRICE") * F.col("OPTION_QUANTITY")
        )
        .withColumn(
            "IS_DISCOUNT",
            F.col("OPTION_PRICE") < 0
        )
    )

    return fact_df


def build_customer_daily_activity(fact_order_items_df):

    logger.info("Building customer daily activity")

    daily_activity_df = (
        fact_order_items_df
        .filter(F.col("USER_ID").isNotNull())
        .groupBy(
            "USER_ID",
            "DATE_KEY"
        )
        .agg(
            F.max("CUSTOMER_KEY")
            .alias("CUSTOMER_KEY"),

            F.countDistinct("ORDER_ID")
            .alias("DAILY_ORDERS"),

            F.sum("REVENUE")
            .alias("DAILY_REVENUE")
        )
    )

    customer_start_df = (
        daily_activity_df
        .groupBy("USER_ID")
        .agg(
            F.min("DATE_KEY")
            .alias("FIRST_ORDER_DATE")
        )
    )

    max_date = (
        daily_activity_df
        .agg(
            F.max("DATE_KEY").alias("MAX_DATE")
        )
        .collect()[0]["MAX_DATE"]
    )

    customer_calendar_df = (
        customer_start_df
        .withColumn(
            "DATE_KEY",
            F.explode(
                F.sequence(
                    F.col("FIRST_ORDER_DATE"),
                    F.lit(max_date),
                    F.expr("INTERVAL 1 DAY")
                )
            )
        )
    )

    daily_df = (
        customer_calendar_df
        .join(
            daily_activity_df,
            on=[
                "USER_ID",
                "DATE_KEY"
            ],
            how="left"
        )
        .fillna(
            {
                "DAILY_ORDERS": 0,
                "DAILY_REVENUE": 0
            }
        )
    )

    customer_key_window = (
        Window
        .partitionBy("USER_ID")
        .orderBy("DATE_KEY")
        .rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow
        )
    )

    daily_df = (
        daily_df
        .withColumn(
            "CUSTOMER_KEY",
            F.last(
                "CUSTOMER_KEY",
                ignorenulls=True
            ).over(customer_key_window)
        )
        .withColumn(
            "_DAY_NUMBER",
            F.datediff(
                F.col("DATE_KEY"),
                F.lit("1970-01-01")
            )
        )
    )

    return daily_df


def calculate_customer_behavior_metrics(
    daily_df,
    config
):

    logger.info("Calculating customer behavior metrics")

    rfm_days = config.get(
        "rfm_days",
        90
    )

    spend_period_days = config.get(
        "spend_period_days",
        30
    )

    history_window = (
        Window
        .partitionBy("USER_ID")
        .orderBy("_DAY_NUMBER")
        .rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow
        )
    )

    rfm_window = (
        Window
        .partitionBy("USER_ID")
        .orderBy("_DAY_NUMBER")
        .rangeBetween(
            -(rfm_days - 1),
            0
        )
    )

    daily_df = (
        daily_df
        .withColumn(
            "LIFETIME_ORDERS",
            F.sum("DAILY_ORDERS")
            .over(history_window)
        )
        .withColumn(
            "LIFETIME_REVENUE",
            F.sum("DAILY_REVENUE")
            .over(history_window)
        )
        .withColumn(
            "CLV",
            F.col("LIFETIME_REVENUE")
        )
        .withColumn(
            "_ORDER_DATE",
            F.when(
                F.col("DAILY_ORDERS") > 0,
                F.col("DATE_KEY")
            )
        )
        .withColumn(
            "LAST_ORDER_DATE",
            F.max("_ORDER_DATE")
            .over(history_window)
        )
        .withColumn(
            "DAYS_SINCE_LAST_ORDER",
            F.datediff(
                F.col("DATE_KEY"),
                F.col("LAST_ORDER_DATE")
            )
        )
        .withColumn(
            "RECENCY",
            F.col("DAYS_SINCE_LAST_ORDER")
        )
        .withColumn(
            "FREQUENCY",
            F.sum("DAILY_ORDERS")
            .over(rfm_window)
        )
        .withColumn(
            "MONETARY",
            F.sum("DAILY_REVENUE")
            .over(rfm_window)
        )
    )

    # ------------------------------------------------
    # Average days between order dates
    # ------------------------------------------------

    order_days_df = (
        daily_df
        .filter(F.col("DAILY_ORDERS") > 0)
        .select(
            "USER_ID",
            "DATE_KEY"
        )
        .dropDuplicates()
    )

    order_window = (
        Window
        .partitionBy("USER_ID")
        .orderBy("DATE_KEY")
    )

    order_history_window = (
        Window
        .partitionBy("USER_ID")
        .orderBy("DATE_KEY")
        .rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow
        )
    )

    order_days_df = (
        order_days_df
        .withColumn(
            "_PREVIOUS_ORDER_DATE",
            F.lag("DATE_KEY")
            .over(order_window)
        )
        .withColumn(
            "_ORDER_GAP",
            F.datediff(
                F.col("DATE_KEY"),
                F.col("_PREVIOUS_ORDER_DATE")
            )
        )
        .withColumn(
            "AVG_DAYS_BETWEEN_ORDERS",
            F.avg("_ORDER_GAP")
            .over(order_history_window)
        )
        .select(
            "USER_ID",
            "DATE_KEY",
            "AVG_DAYS_BETWEEN_ORDERS"
        )
    )

    daily_df = (
        daily_df
        .join(
            order_days_df,
            on=[
                "USER_ID",
                "DATE_KEY"
            ],
            how="left"
        )
        .withColumn(
            "AVG_DAYS_BETWEEN_ORDERS",
            F.last(
                "AVG_DAYS_BETWEEN_ORDERS",
                ignorenulls=True
            ).over(history_window)
        )
    )

    # ------------------------------------------------
    # Spend change
    # ------------------------------------------------

    current_spend_window = (
        Window
        .partitionBy("USER_ID")
        .orderBy("_DAY_NUMBER")
        .rangeBetween(
            -(spend_period_days - 1),
            0
        )
    )

    previous_spend_window = (
        Window
        .partitionBy("USER_ID")
        .orderBy("_DAY_NUMBER")
        .rangeBetween(
            -(spend_period_days * 2 - 1),
            -spend_period_days
        )
    )

    daily_df = (
        daily_df
        .withColumn(
            "_CURRENT_PERIOD_SPEND",
            F.sum("DAILY_REVENUE")
            .over(current_spend_window)
        )
        .withColumn(
            "_PREVIOUS_PERIOD_SPEND",
            F.sum("DAILY_REVENUE")
            .over(previous_spend_window)
        )
        .withColumn(
            "SPEND_CHANGE_PCT",

            F.when(
                F.col("_PREVIOUS_PERIOD_SPEND") > 0,

                (
                    (
                        F.col("_CURRENT_PERIOD_SPEND")
                        - F.col("_PREVIOUS_PERIOD_SPEND")
                    )
                    / F.col("_PREVIOUS_PERIOD_SPEND")
                ) * 100
            )
        )
    )

    return daily_df


def add_customer_segments(
    daily_df,
    config
):

    logger.info("Adding customer segments")

    churn_threshold_days = config.get(
        "churn_threshold_days",
        45
    )

    # ------------------------------------------------
    # Churn status
    # ------------------------------------------------

    daily_df = (
        daily_df
        .withColumn(
            "CHURN_STATUS",

            F.when(
                F.col("RECENCY") > churn_threshold_days,
                F.lit("AT_RISK")
            )
            .otherwise(
                F.lit("ACTIVE")
            )
        )
    )

    # ------------------------------------------------
    # RFM scores
    # ------------------------------------------------

    recency_window = (
        Window
        .partitionBy("DATE_KEY")
        .orderBy(
            F.col("RECENCY").desc()
        )
    )

    frequency_window = (
        Window
        .partitionBy("DATE_KEY")
        .orderBy(
            F.col("FREQUENCY").asc()
        )
    )

    monetary_window = (
        Window
        .partitionBy("DATE_KEY")
        .orderBy(
            F.col("MONETARY").asc()
        )
    )

    daily_df = (
        daily_df
        .withColumn(
            "R_SCORE",
            F.ntile(5).over(recency_window)
        )
        .withColumn(
            "F_SCORE",
            F.ntile(5).over(frequency_window)
        )
        .withColumn(
            "M_SCORE",
            F.ntile(5).over(monetary_window)
        )
    )

    # ------------------------------------------------
    # Customer segment
    # ------------------------------------------------

    daily_df = (
        daily_df
        .withColumn(
            "CUSTOMER_SEGMENT",

            F.when(
                (F.col("R_SCORE") >= 4)
                & (F.col("F_SCORE") >= 4)
                & (F.col("M_SCORE") >= 4),

                F.lit("VIP")
            )

            .when(
                (F.col("FREQUENCY") <= 1)
                & (F.col("R_SCORE") >= 4),

                F.lit("NEW_CUSTOMER")
            )

            .when(
                (F.col("RECENCY") > churn_threshold_days)
                & (F.col("F_SCORE") <= 2),

                F.lit("CHURN_RISK")
            )

            .otherwise(
                F.lit("REGULAR")
            )
        )
    )

    # ------------------------------------------------
    # CLV segmentation
    # ------------------------------------------------

    clv_window = (
        Window
        .partitionBy("DATE_KEY")
        .orderBy(
            F.col("CLV").asc()
        )
    )

    daily_df = (
        daily_df
        .withColumn(
            "_CLV_BUCKET",
            F.ntile(5).over(clv_window)
        )
        .withColumn(
            "CLV_SEGMENT",

            F.when(
                F.col("_CLV_BUCKET") == 5,
                F.lit("HIGH")
            )

            .when(
                F.col("_CLV_BUCKET") == 1,
                F.lit("LOW")
            )

            .otherwise(
                F.lit("MEDIUM")
            )
        )
    )

    # Remove temporary calculation columns

    daily_df = (
        daily_df
        .drop(
            "FIRST_ORDER_DATE",
            "_DAY_NUMBER",
            "_ORDER_DATE",
            "_CURRENT_PERIOD_SPEND",
            "_PREVIOUS_PERIOD_SPEND",
            "_CLV_BUCKET"
        )
    )

    return daily_df


def build_fact_customer_daily(
    fact_order_items_df,
    config
):

    logger.info(
        "Building fact_customer_daily"
    )

    daily_df = build_customer_daily_activity(
        fact_order_items_df
    )

    daily_df = calculate_customer_behavior_metrics(
        daily_df,
        config
    )

    daily_df = add_customer_segments(
        daily_df,
        config
    )

    logger.info(
        "fact_customer_daily build completed"
    )

    return daily_df


def write_gold_table(df, config):

    gold_path = config["gold_path"]
    write_mode = config["write_mode"]
    business_keys = config["business_keys"]

    output_path = (f"s3://{GOLD_BUCKET}/{gold_path}")

    logger.info(
        f"Writing Gold table to: {output_path}"
    )

    # First load
    if not DeltaTable.isDeltaTable(
        spark,
        output_path
    ):

        (
            df
            .write
            .format("delta")
            .mode("overwrite")
            .save(output_path)
        )

        logger.info(
            "Initial Gold table load completed"
        )

        return

    # Subsequent MERGE
    if write_mode == "merge":

        delta_table = DeltaTable.forPath(
            spark,
            output_path
        )

        merge_condition = " AND ".join(
            [
                f"target.{key} = source.{key}"
                for key in business_keys
            ]
        )

        (
            delta_table
            .alias("target")
            .merge(
                df.alias("source"),
                merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    # Append mode
    elif write_mode == "append":

        (
            df
            .write
            .format("delta")
            .mode("append")
            .save(output_path)
        )

    else:

        raise ValueError(
            f"Unsupported Gold write mode: "
            f"{write_mode}"
        )

    logger.info(
        "Gold table write completed"
    )


def main():

    table_name = args["TABLE_NAME"]

    gold_config = load_config(
        GOLD_BUCKET,
        "config/gold_table_config.json"
    )

    config = gold_config[table_name]

    logger.info(
        f"Starting Gold processing for {table_name}"
    )

    table_type = config["table_type"]

    # ------------------------------------------------
    # DIMENSIONS
    # ------------------------------------------------

    if table_type == "dimension":

        silver_df = read_delta_table(
            SILVER_BUCKET,
            config["silver_path"]
        )

        transformation_type = config[
            "transformation_type"
        ]

        if transformation_type == "aggregate_latest":

            transformed_df = (
                build_latest_aggregated_dimension(
                    silver_df,
                    config
                )
            )

        elif transformation_type == "distinct":

            transformed_df = (
                get_unique_dimension_records(
                    silver_df,
                    config["select_columns"]
                )
            )

        else:

            raise ValueError(
                f"Unsupported transformation type: "
                f"{transformation_type}"
            )

        validate_business_key_uniqueness(
            transformed_df,
            config["business_keys"]
        )

        scd_type = config["scd_type"]

        if scd_type == "SCD1":

            apply_scd1(
                transformed_df,
                config
            )

        elif scd_type == "SCD2":

            apply_scd2(
                transformed_df,
                config
            )

        else:

            raise ValueError(
                f"Unsupported SCD type: {scd_type}"
            )

    # ------------------------------------------------
    # FACTS
    # ------------------------------------------------

    elif table_type == "fact":

        fact_type = config["fact_type"]

        source_type = config.get(
            "source_type",
            "silver"
        )

        if source_type == "silver":

            source_df = read_delta_table(
                SILVER_BUCKET,
                config["silver_path"]
            )

        elif source_type == "gold":

            source_df = read_delta_table(
                GOLD_BUCKET,
                config["source_path"]
            )

        else:

            raise ValueError(
                f"Unsupported source type: "
                f"{source_type}"
            )

        if fact_type == "order_items":

            fact_df = build_fact_order_items(
                source_df,
                config
            )

        elif fact_type == "order_item_options":

            fact_df = (
                build_fact_order_item_options(
                    source_df,
                    config
                )
            )

        elif fact_type == "customer_daily":

            fact_df = build_fact_customer_daily(
                source_df,
                config
            )

        else:

            raise ValueError(
                f"Unsupported fact type: "
                f"{fact_type}"
            )

        write_gold_table(
            fact_df,
            config
        )

    else:

        raise ValueError(
            f"Unsupported table type: "
            f"{table_type}"
        )

    logger.info(
        f"Gold processing completed for {table_name}"
    )


if __name__ == "__main__":

    try:

        main()
        job.commit()

    except Exception as e:

        logger.exception(
            "Gold Glue job failed"
        )

        raise
