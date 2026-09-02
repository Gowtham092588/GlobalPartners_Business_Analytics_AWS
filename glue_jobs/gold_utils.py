from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)


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
            F.when(F.col("RECENCY") <= 30, 5)
            .when(F.col("RECENCY") <= 60, 4)
            .when(F.col("RECENCY") <= 90, 3)
            .when(F.col("RECENCY") <= 180, 2)
            .otherwise(1)
        )
        .withColumn(
            "F_SCORE",
            F.when(F.col("FREQUENCY") == 0, 1)
            .when(F.col("FREQUENCY") == 1, 2)
            .when(F.col("FREQUENCY") <= 3, 3)
            .when(F.col("FREQUENCY") <= 6, 4)
            .otherwise(5)
        )
        .withColumn(
            "M_SCORE",
            F.when(F.col("MONETARY") == 0, 1)
            .when(F.col("MONETARY") <= 11, 2)
            .when(F.col("MONETARY") <= 26, 3)
            .when(F.col("MONETARY") <= 51, 4)
            .otherwise(5)
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
                F.col("RECENCY") > churn_threshold_days,
                F.lit("CHURN_RISK")
            )

            .when(
                (F.col("RECENCY") <= 30)
                & (F.col("FREQUENCY") <= 1),
                F.lit("NEW_CUSTOMER")
            )

            .when(
                (F.col("R_SCORE") >= 4)
                & (F.col("F_SCORE") >= 4)
                & (F.col("M_SCORE") >= 4),
                F.lit("VIP")
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
