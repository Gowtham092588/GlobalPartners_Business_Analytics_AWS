from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)


def filter_invalid_records(df, config):

    business_keys = config["business_keys"]

    invalid_condition = None

    for column_name in business_keys:

        condition = F.col(column_name).isNull()

        if invalid_condition is None:
            invalid_condition = condition
        else:
            invalid_condition = invalid_condition | condition

    range_validations = config.get(
        "range_validations",
        {}
    )

    for column_name, valid_range in range_validations.items():

        min_value = valid_range.get("min")
        max_value = valid_range.get("max")

        range_condition = None

        if min_value is not None:
            range_condition = (
                F.col(column_name) < min_value
            )

        if max_value is not None:

            max_condition = (
                F.col(column_name) > max_value
            )

            if range_condition is None:
                range_condition = max_condition
            else:
                range_condition = (
                    range_condition | max_condition
                )

        if range_condition is not None:

            if invalid_condition is None:
                invalid_condition = range_condition
            else:
                invalid_condition = (
                    invalid_condition | range_condition
                )

    valid_df = df.filter(
        ~invalid_condition
    )

    invalid_df = df.filter(
        invalid_condition
    )

    return valid_df, invalid_df


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
