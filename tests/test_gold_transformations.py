import os
import sys
import pytest

from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from glue_jobs.gold_utils import (
    build_latest_aggregated_dimension,
    get_unique_dimension_records,
    validate_business_key_uniqueness,
    aggregate_order_item_options,
    build_fact_order_item_options,
    build_customer_daily_activity,
    calculate_customer_behavior_metrics,
    add_customer_segments
)


# ---------------------------------------------------------
# Make Spark use the same Python environment as pytest
# ---------------------------------------------------------

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


# ---------------------------------------------------------
# Shared Spark session
# ---------------------------------------------------------

@pytest.fixture(scope="session")
def spark():

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("gold-unit-tests")
        .getOrCreate()
    )

    yield spark

    spark.stop()


# =========================================================
# 1. build_latest_aggregated_dimension
# =========================================================

def test_build_latest_aggregated_dimension(spark):

    data = [
        (
            "C1",
            date(2026, 1, 1),
            "Old Name",
            10.00
        ),
        (
            "C1",
            date(2026, 1, 5),
            "New Name",
            30.00
        ),
        (
            "C2",
            date(2026, 1, 3),
            "Customer Two",
            20.00
        ),
        (
            None,
            date(2026, 1, 4),
            "Invalid Customer",
            100.00
        )
    ]

    columns = [
        "USER_ID",
        "ORDER_DATE",
        "CUSTOMER_NAME",
        "REVENUE"
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    config = {
        "business_keys": [
            "USER_ID"
        ],
        "latest_order_columns": [
            "ORDER_DATE"
        ],
        "latest_attribute_columns": [
            "CUSTOMER_NAME"
        ],
        "aggregations": {
            "ORDER_DATE": {
                "min": "FIRST_ORDER_DATE",
                "max": "LAST_ORDER_DATE"
            },
            "REVENUE": {
                "max": "MAX_REVENUE"
            }
        }
    }

    result_df = build_latest_aggregated_dimension(
        df,
        config
    )

    results = {
        row["USER_ID"]: row
        for row in result_df.collect()
    }

    # Null business key should be removed
    assert len(results) == 2

    # Latest attribute should come from newest row
    assert results["C1"]["CUSTOMER_NAME"] == "New Name"

    assert results["C1"]["FIRST_ORDER_DATE"] == date(
        2026,
        1,
        1
    )

    assert results["C1"]["LAST_ORDER_DATE"] == date(
        2026,
        1,
        5
    )

    assert results["C1"]["MAX_REVENUE"] == 30.00


# =========================================================
# 2. get_unique_dimension_records
# =========================================================

def test_get_unique_dimension_records(spark):

    data = [
        ("Breakfast", "Food"),
        ("Breakfast", "Food"),
        ("Coffee", "Drink")
    ]

    columns = [
        "ITEM_NAME",
        "ITEM_CATEGORY"
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    result_df = get_unique_dimension_records(
        df,
        [
            "ITEM_NAME",
            "ITEM_CATEGORY"
        ]
    )

    assert result_df.count() == 2


# =========================================================
# 3. validate_business_key_uniqueness
# =========================================================

def test_business_key_uniqueness_passes(spark):

    data = [
        ("1001", "1"),
        ("1002", "1"),
        ("1003", "1")
    ]

    df = spark.createDataFrame(
        data,
        [
            "ORDER_ID",
            "LINEITEM_ID"
        ]
    )

    # Should not raise an exception
    validate_business_key_uniqueness(
        df,
        [
            "ORDER_ID",
            "LINEITEM_ID"
        ]
    )


def test_business_key_uniqueness_fails(spark):

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

    with pytest.raises(
        ValueError,
        match="Duplicate business keys"
    ):

        validate_business_key_uniqueness(
            df,
            [
                "ORDER_ID",
                "LINEITEM_ID"
            ]
        )


# =========================================================
# 4. aggregate_order_item_options
# =========================================================

def test_aggregate_order_item_options(spark):

    data = [
        (
            "1001",
            "1",
            2.00,
            2
        ),
        (
            "1001",
            "1",
            1.50,
            1
        ),
        (
            "1001",
            "1",
            -3.00,
            1
        ),
        (
            "1002",
            "1",
            5.00,
            1
        )
    ]

    columns = [
        "ORDER_ID",
        "LINEITEM_ID",
        "OPTION_PRICE",
        "OPTION_QUANTITY"
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    result_df = aggregate_order_item_options(
        df
    )

    results = {
        (
            row["ORDER_ID"],
            row["LINEITEM_ID"]
        ): row

        for row in result_df.collect()
    }

    order_1001 = results[
        ("1001", "1")
    ]

    # 2 * 2 + 1.5 * 1 = 5.5
    assert order_1001["OPTION_AMOUNT"] == 5.5

    # Negative option = -3
    # Function converts discount to positive amount
    assert order_1001["DISCOUNT_AMOUNT"] == 3.0

    order_1002 = results[
        ("1002", "1")
    ]

    assert order_1002["OPTION_AMOUNT"] == 5.0
    assert order_1002["DISCOUNT_AMOUNT"] == 0.0


# =========================================================
# 5. build_fact_order_item_options
# =========================================================

def test_build_fact_order_item_options(spark):

    data = [
        (
            "1001",
            "1",
            "Milk",
            "Almond Milk",
            2.00,
            2
        ),
        (
            "1002",
            "1",
            "Promotion",
            "Discount",
            -3.00,
            1
        )
    ]

    columns = [
        "ORDER_ID",
        "LINEITEM_ID",
        "OPTION_GROUP_NAME",
        "OPTION_NAME",
        "OPTION_PRICE",
        "OPTION_QUANTITY"
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    config = {
        "select_columns": columns
    }

    result_df = build_fact_order_item_options(
        df,
        config
    )

    results = {
        row["ORDER_ID"]: row
        for row in result_df.collect()
    }

    # 2.00 * 2
    assert results["1001"]["OPTION_AMOUNT"] == 4.0

    assert results["1001"]["IS_DISCOUNT"] is False

    # -3.00 * 1
    assert results["1002"]["OPTION_AMOUNT"] == -3.0

    assert results["1002"]["IS_DISCOUNT"] is True


# =========================================================
# 6. build_customer_daily_activity
# =========================================================

def test_build_customer_daily_activity(spark):

    data = [
        (
            "U1",
            date(2026, 1, 1),
            101,
            "O1",
            10.00
        ),
        (
            "U1",
            date(2026, 1, 1),
            101,
            "O2",
            20.00
        ),
        (
            "U1",
            date(2026, 1, 3),
            101,
            "O3",
            30.00
        ),
        (
            "U2",
            date(2026, 1, 2),
            102,
            "O4",
            40.00
        )
    ]

    columns = [
        "USER_ID",
        "DATE_KEY",
        "CUSTOMER_KEY",
        "ORDER_ID",
        "REVENUE"
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    result_df = build_customer_daily_activity(
        df
    )

    # U1 should have calendar rows:
    # Jan 1
    # Jan 2
    # Jan 3

    u1_rows = (
        result_df
        .filter(
            F.col("USER_ID") == "U1"
        )
        .orderBy("DATE_KEY")
        .collect()
    )

    assert len(u1_rows) == 3

    # Jan 1 had two distinct orders
    assert u1_rows[0]["DAILY_ORDERS"] == 2

    # Jan 1 revenue = 10 + 20
    assert u1_rows[0]["DAILY_REVENUE"] == 30.0

    # Jan 2 had no transaction
    assert u1_rows[1]["DAILY_ORDERS"] == 0

    assert u1_rows[1]["DAILY_REVENUE"] == 0.0

    # Customer key should carry forward
    assert u1_rows[1]["CUSTOMER_KEY"] == 101

    # Jan 3
    assert u1_rows[2]["DAILY_ORDERS"] == 1
    assert u1_rows[2]["DAILY_REVENUE"] == 30.0


# =========================================================
# 7. calculate_customer_behavior_metrics
# =========================================================

def test_calculate_customer_behavior_metrics(spark):

    data = [
        (
            "U1",
            date(2026, 1, 1),
            101,
            1,
            10.00
        ),
        (
            "U1",
            date(2026, 1, 2),
            101,
            0,
            0.00
        ),
        (
            "U1",
            date(2026, 1, 3),
            101,
            1,
            20.00
        )
    ]

    columns = [
        "USER_ID",
        "DATE_KEY",
        "CUSTOMER_KEY",
        "DAILY_ORDERS",
        "DAILY_REVENUE"
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    df = df.withColumn(
        "_DAY_NUMBER",
        F.datediff(
            F.col("DATE_KEY"),
            F.lit("1970-01-01")
        )
    )

    config = {
        "rfm_days": 90,
        "spend_period_days": 2
    }

    result_df = calculate_customer_behavior_metrics(
        df,
        config
    )

    latest_row = (
        result_df
        .filter(
            F.col("DATE_KEY")
            == F.lit("2026-01-03").cast("date")
        )
        .collect()[0]
    )

    # Lifetime orders = Jan 1 + Jan 3
    assert latest_row["LIFETIME_ORDERS"] == 2

    # Lifetime revenue = 10 + 20
    assert latest_row["LIFETIME_REVENUE"] == 30.0

    # CLV is lifetime revenue
    assert latest_row["CLV"] == 30.0

    # Customer ordered today
    assert latest_row["DAYS_SINCE_LAST_ORDER"] == 0

    assert latest_row["RECENCY"] == 0

    # 90-day frequency contains both orders
    assert latest_row["FREQUENCY"] == 2

    # 90-day monetary contains both revenues
    assert latest_row["MONETARY"] == 30.0

    # Orders happened Jan 1 and Jan 3
    assert latest_row["AVG_DAYS_BETWEEN_ORDERS"] == 2.0

    # Current 2-day spend:
    # Jan 2 + Jan 3 = 20
    #
    # Previous 2-day spend:
    # Dec 31 + Jan 1 = 10
    #
    # Change = (20 - 10) / 10 * 100 = 100
    assert latest_row["SPEND_CHANGE_PCT"] == 100.0


# =========================================================
# 8. add_customer_segments
# =========================================================

def test_add_customer_segments(spark):

    data = [
        (
            "U1",
            date(2026, 1, 10),
            100,
            0,
            0.0,
            0.0
        ),

        (
            "U2",
            date(2026, 1, 10),
            10,
            1,
            10.0,
            10.0
        ),

        (
            "U3",
            date(2026, 1, 10),
            20,
            5,
            40.0,
            500.0
        ),

        (
            "U4",
            date(2026, 1, 10),
            40,
            2,
            20.0,
            50.0
        ),

        (
            "U5",
            date(2026, 1, 10),
            20,
            2,
            20.0,
            30.0
        )
    ]

    columns = [
        "USER_ID",
        "DATE_KEY",
        "RECENCY",
        "FREQUENCY",
        "MONETARY",
        "CLV"
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    config = {
        "churn_threshold_days": 45
    }

    result_df = add_customer_segments(
        df,
        config
    )

    results = {
        row["USER_ID"]: row
        for row in result_df.collect()
    }

    # -----------------------------------------------------
    # U1
    # -----------------------------------------------------

    assert results["U1"]["CHURN_STATUS"] == "AT_RISK"

    assert results["U1"]["CUSTOMER_SEGMENT"] == "CHURN_RISK"

    assert results["U1"]["R_SCORE"] == 2
    assert results["U1"]["F_SCORE"] == 1
    assert results["U1"]["M_SCORE"] == 1

    assert results["U1"]["CLV_SEGMENT"] == "LOW"

    # -----------------------------------------------------
    # U2
    # -----------------------------------------------------

    assert results["U2"]["CHURN_STATUS"] == "ACTIVE"

    assert results["U2"]["CUSTOMER_SEGMENT"] == "NEW_CUSTOMER"

    assert results["U2"]["R_SCORE"] == 5
    assert results["U2"]["F_SCORE"] == 2
    assert results["U2"]["M_SCORE"] == 2

    # -----------------------------------------------------
    # U3
    # -----------------------------------------------------

    assert results["U3"]["CUSTOMER_SEGMENT"] == "VIP"

    assert results["U3"]["R_SCORE"] == 5
    assert results["U3"]["F_SCORE"] == 4
    assert results["U3"]["M_SCORE"] == 4

    # Highest CLV of the 5 customers
    assert results["U3"]["CLV_SEGMENT"] == "HIGH"

    # -----------------------------------------------------
    # U4
    # -----------------------------------------------------

    assert results["U4"]["CUSTOMER_SEGMENT"] == "REGULAR"

    # -----------------------------------------------------
    # U5
    # -----------------------------------------------------

    assert results["U5"]["CUSTOMER_SEGMENT"] == "REGULAR"
