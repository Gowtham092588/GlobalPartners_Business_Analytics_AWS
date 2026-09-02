import os
import sys
import pandas as pd
import pytest
from pyspark.sql import SparkSession
from glue_jobs.silver_utils import (
    filter_invalid_records,
    clean_string_columns,
    keep_latest_records,
    apply_value_mappings,
    apply_transformations,
    remove_duplicates)
from decimal import Decimal
from datetime import datetime

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark():

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("silver-unit-tests")
        .getOrCreate()
    )

    yield spark

    spark.stop()


def test_range_validation(spark):

    data = [
        ("1001", "1", 10.00, 1),
        ("1002", "1", 500.00, 100),
        ("1003", "1", 5000.00, 1),
        ("1004", "1", 10.00, 500)
    ]

    columns = [
        "ORDER_ID",
        "LINEITEM_ID",
        "ITEM_PRICE",
        "ITEM_QUANTITY"
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    config = {
        "business_keys": [
            "ORDER_ID",
            "LINEITEM_ID"
        ],

        "range_validations": {
            "ITEM_PRICE": {
                "min": 0,
                "max": 500
            },

            "ITEM_QUANTITY": {
                "min": 1,
                "max": 100
            }
        }
    }

    valid_df, invalid_df = filter_invalid_records(
        df,
        config
    )

    assert valid_df.count() == 2
    assert invalid_df.count() == 2


def test_null_business_key_is_invalid(spark):

    data = [
        ("1001", "1", 10.00, 1),
        (None, "2", 12.00, 1),
        ("1003", None, 15.00, 1)
    ]

    columns = [
        "ORDER_ID",
        "LINEITEM_ID",
        "ITEM_PRICE",
        "ITEM_QUANTITY"
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    config = {
        "business_keys": [
            "ORDER_ID",
            "LINEITEM_ID"
        ],

        "range_validations": {}
    }

    valid_df, invalid_df = filter_invalid_records(
        df,
        config
    )

    assert valid_df.count() == 1
    assert invalid_df.count() == 2


def test_clean_string_columns(spark):

    data = [
        ("  Breakfast  ",),
        ("Sandwiches   ",),
        ("  Salads",),
        (None,)
    ]

    columns = [
        "ITEM_CATEGORY"
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    result_df = clean_string_columns(df)

    results = [
        row["ITEM_CATEGORY"]
        for row in result_df.collect()
    ]

    assert results[0] == "Breakfast"
    assert results[1] == "Sandwiches"
    assert results[2] == "Salads"
    assert results[3] is None


def test_value_mapping(spark):

    data = [
        ("Drip C",),
        ("Bowls0",),
        ("Breakfast",)
    ]

    df = spark.createDataFrame(
        data,
        ["ITEM_CATEGORY"]
    )

    config = {
        "value_mappings": {
            "ITEM_CATEGORY": {
                "Drip C": "Drip Coffee",
                "Bowls0": "Bowls"
            }
        }
    }

    result_df = apply_value_mappings(
        df,
        config
    )

    values = [
        row["ITEM_CATEGORY"]
        for row in result_df.collect()
    ]

    assert "Drip Coffee" in values
    assert "Bowls" in values
    assert "Breakfast" in values


def test_keep_latest_records(spark):

    data = [
        (
            "1001",
            "1",
            "Old Item",
            datetime(2026, 9, 1, 10, 0, 0),
            datetime(2026, 9, 1, 10, 5, 0)
        ),
        (
            "1001",
            "1",
            "Updated Item",
            datetime(2026, 9, 1, 11, 0, 0),
            datetime(2026, 9, 1, 11, 5, 0)
        ),
        (
            "1002",
            "1",
            "Another Item",
            datetime(2026, 9, 1, 9, 0, 0),
            datetime(2026, 9, 1, 9, 5, 0)
        )
    ]

    columns = [
        "ORDER_ID",
        "LINEITEM_ID",
        "ITEM_NAME",
        "updated_at",
        "_ingested_at"
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    config = {
        "business_keys": [
            "ORDER_ID",
            "LINEITEM_ID"
        ],
        "watermark_column": "updated_at"
    }

    result_df = keep_latest_records(
        df,
        config
    )

    results = result_df.collect()

    assert len(results) == 2

    order_1001 = [
        row
        for row in results
        if row["ORDER_ID"] == "1001"
    ][0]

    assert order_1001["ITEM_NAME"] == "Updated Item"


def test_apply_transformations(spark):

    data = [
        (
            "1001",
            "1",
            "12.50",
            "2"
        )
    ]

    columns = [
        "ORDER_ID",
        "LINEITEM_ID",
        "ITEM_PRICE",
        "ITEM_QUANTITY"
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    config = {
        "cast_columns": {
            "ORDER_ID": "string",
            "LINEITEM_ID": "string",
            "ITEM_PRICE": "decimal(10,2)",
            "ITEM_QUANTITY": "int"
        }
    }

    result_df = apply_transformations(
        df,
        "order_items",
        config
    )

    result = result_df.collect()[0]

    assert result["ORDER_ID"] == "1001"
    assert result["LINEITEM_ID"] == "1"
    assert result["ITEM_PRICE"] == Decimal("12.50")
    assert result["ITEM_QUANTITY"] == 2


def test_remove_duplicates(spark):

    data = [
        (
            "1001",
            "1",
            "Breakfast"
        ),
        (
            "1001",
            "1",
            "Breakfast"
        ),
        (
            "1002",
            "1",
            "Sandwiches"
        )
    ]

    columns = [
        "ORDER_ID",
        "LINEITEM_ID",
        "ITEM_CATEGORY"
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    config = {
        "duplicate_columns": [
            "ORDER_ID",
            "LINEITEM_ID",
            "ITEM_CATEGORY"
        ]
    }

    result_df = remove_duplicates(
        df,
        config
    )

    assert result_df.count() == 2


def test_remove_duplicates_without_config(spark):

    data = [
        ("1001", "1"),
        ("1001", "1")
    ]

    df = spark.createDataFrame(
        data,
        [
            "ORDER_ID",
            "LINEITEM_ID"
        ]
    )

    config = {}

    result_df = remove_duplicates(
        df,
        config
    )

    assert result_df.count() == 2
