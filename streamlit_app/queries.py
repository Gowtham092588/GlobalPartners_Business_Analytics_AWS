def get_customer_segmentation_summary_query():

    query = """
        WITH recent_customer AS (
            SELECT
                f.USER_ID,
                f.DATE_KEY,
                f.RECENCY,
                f.FREQUENCY,
                f.MONETARY,
                f.R_SCORE,
                f.F_SCORE,
                f.M_SCORE,
                f.CUSTOMER_SEGMENT,
                d.IS_LOYALTY,
                ROW_NUMBER() OVER (
                    PARTITION BY f.USER_ID
                    ORDER BY f.DATE_KEY DESC
                ) AS RN
            FROM GLOBALDBGOLD.FACT_CUSTOMER_DAILY f
            JOIN GLOBALDBGOLD.DIM_CUSTOMER d
                ON f.USER_ID = d.USER_ID
            WHERE d.IS_CURRENT = TRUE
        )
        SELECT
            CUSTOMER_SEGMENT,
            COUNT(DISTINCT USER_ID)
                AS CUSTOMER_COUNT,
            ROUND(
                AVG(RECENCY),
                2
            ) AS AVG_RECENCY,
            ROUND(
                AVG(FREQUENCY),
                2
            ) AS AVG_FREQUENCY,
            ROUND(
                AVG(MONETARY),
                2
            ) AS AVG_MONETARY,
            SUM(
                CASE
                    WHEN IS_LOYALTY = TRUE THEN 1
                    ELSE 0
                END
            ) AS LOYALTY_CUSTOMERS,
            ROUND(
                SUM(
                    CASE
                        WHEN IS_LOYALTY = TRUE THEN 1
                        ELSE 0
                    END
                ) * 100.0
                / COUNT(*),
                2
            ) AS LOYALTY_RATE
        FROM recent_customer
        WHERE RN = 1
        GROUP BY CUSTOMER_SEGMENT
        ORDER BY CUSTOMER_COUNT DESC
    """
    return query


def get_churn_risk_query():
    query = """
    WITH daily_customer_activity AS (
    SELECT
        USER_ID,
        DATE_KEY,
        DAYS_SINCE_LAST_ORDER,
        AVG_DAYS_BETWEEN_ORDERS,
        SPEND_CHANGE_PCT,
        FREQUENCY,
        MONETARY,
        CHURN_STATUS,
        CUSTOMER_SEGMENT,
        ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY DATE_KEY DESC) as RN
    FROM GLOBALDBGOLD.FACT_CUSTOMER_DAILY
    )
        SELECT
            USER_ID,
            DATE_KEY,
            DAYS_SINCE_LAST_ORDER,
            AVG_DAYS_BETWEEN_ORDERS,
            SPEND_CHANGE_PCT,
            FREQUENCY,
            MONETARY,
            CHURN_STATUS,
            CUSTOMER_SEGMENT
        FROM daily_customer_activity 
        WHERE RN=1
    """
    return query


def get_sales_trends_query():

    query = """
        SELECT
            d.YEAR,
            d.MONTH,
            f.ITEM_CATEGORY,
            SUM(f.REVENUE) AS CATEGORY_SALES,
            COUNT(DISTINCT f.ORDER_ID) AS CATEGORY_ORDERS
        FROM GLOBALDBGOLD.FACT_ORDER_ITEMS f
        INNER JOIN GLOBALDBGOLD.DIM_DATE d
            ON f.DATE_KEY = d.DATE_KEY
        GROUP BY
            d.YEAR,
            d.MONTH,
            f.ITEM_CATEGORY
        ORDER BY
            d.YEAR,
            d.MONTH,
            f.ITEM_CATEGORY
    """
    return query


def get_holiday_sales_trend_query():

    query = """
        SELECT
            f.DATE_KEY,
            d.IS_HOLIDAY,
            d.HOLIDAY_NAME,
            SUM(f.REVENUE) AS DAILY_SALES
        FROM GLOBALDBGOLD.FACT_ORDER_ITEMS f
        INNER JOIN GLOBALDBGOLD.DIM_DATE d
            ON f.DATE_KEY = d.DATE_KEY
        GROUP BY
            f.DATE_KEY,
            d.IS_HOLIDAY,
            d.HOLIDAY_NAME
        ORDER BY
            f.DATE_KEY
    """

    return query


def get_loyalty_impact_query():

    query = """
        WITH recent_daily_customer AS (
            SELECT
                f.USER_ID,
                f.DATE_KEY,
                f.LIFETIME_ORDERS,
                f.CLV,
                d.IS_LOYALTY,
                ROW_NUMBER() OVER (
                    PARTITION BY f.USER_ID
                    ORDER BY f.DATE_KEY DESC
                ) AS RN
            FROM GLOBALDBGOLD.FACT_CUSTOMER_DAILY f
            JOIN GLOBALDBGOLD.DIM_CUSTOMER d
                ON f.USER_ID = d.USER_ID
            WHERE d.IS_CURRENT = TRUE
        ),
        latest_customer AS (
            SELECT
                USER_ID,
                LIFETIME_ORDERS,
                CLV,
                IS_LOYALTY
            FROM recent_daily_customer
            WHERE RN = 1
        ),
        customer_metrics AS (
            SELECT
                IS_LOYALTY,
                COUNT(*) AS CUSTOMER_COUNT,
                AVG(CLV) AS AVG_CLV,
                AVG(LIFETIME_ORDERS) AS AVG_LIFETIME_ORDERS,
                SUM(
                    CASE
                        WHEN LIFETIME_ORDERS > 1 THEN 1
                        ELSE 0
                    END
                ) * 100.0 / COUNT(*) AS REPEAT_CUSTOMER_RATE
            FROM latest_customer
            GROUP BY IS_LOYALTY
        ),
        order_sales AS (
            SELECT
                d.IS_LOYALTY,
                f.ORDER_ID,
                SUM(f.REVENUE) AS ORDER_REVENUE
            FROM GLOBALDBGOLD.FACT_ORDER_ITEMS f
            JOIN GLOBALDBGOLD.DIM_CUSTOMER d
                ON f.USER_ID = d.USER_ID
            WHERE d.IS_CURRENT = TRUE
            GROUP BY
                d.IS_LOYALTY,
                f.ORDER_ID
        ),
        order_metrics AS (
            SELECT
                IS_LOYALTY,
                AVG(ORDER_REVENUE) AS AVG_ORDER_VALUE
            FROM order_sales
            GROUP BY IS_LOYALTY
        )
        SELECT
            c.IS_LOYALTY,
            c.CUSTOMER_COUNT,
            c.AVG_CLV,
            c.AVG_LIFETIME_ORDERS,
            c.REPEAT_CUSTOMER_RATE,
            o.AVG_ORDER_VALUE
        FROM customer_metrics c
        JOIN order_metrics o
            ON c.IS_LOYALTY = o.IS_LOYALTY
        ORDER BY c.IS_LOYALTY DESC
    """

    return query


def get_location_performance_query():

    query = """
        WITH location_metrics AS (
            SELECT
                RESTAURANT_ID,
                COUNT(
                    DISTINCT ORDER_ID
                ) AS TOTAL_ORDERS,
                ROUND(
                    SUM(REVENUE),
                    2
                ) AS TOTAL_REVENUE,
                ROUND(
                    SUM(REVENUE)
                    / NULLIF(
                        COUNT(DISTINCT ORDER_ID),
                        0
                    ),
                    2
                ) AS AVG_ORDER_VALUE,
                COUNT(
                    DISTINCT DATE_KEY
                ) AS ACTIVE_DAYS,
                COUNT(
                    DISTINCT DATE_TRUNC(
                        'week',
                        CAST(DATE_KEY AS TIMESTAMP)
                    )
                ) AS ACTIVE_WEEKS
            FROM GLOBALDBGOLD.FACT_ORDER_ITEMS
            WHERE RESTAURANT_ID IS NOT NULL
            GROUP BY RESTAURANT_ID
        ),
        calculated_metrics AS (
            SELECT
                RESTAURANT_ID,
                TOTAL_REVENUE,
                TOTAL_ORDERS,
                AVG_ORDER_VALUE,
                ACTIVE_DAYS,
                ACTIVE_WEEKS,
                ROUND(
                    CAST(TOTAL_ORDERS AS DOUBLE)
                    / NULLIF(ACTIVE_DAYS, 0),
                    2
                ) AS ORDERS_PER_ACTIVE_DAY,
                ROUND(
                    CAST(TOTAL_ORDERS AS DOUBLE)
                    / NULLIF(ACTIVE_WEEKS, 0),
                    2
                ) AS ORDERS_PER_ACTIVE_WEEK
            FROM location_metrics
        )
        SELECT
            RESTAURANT_ID,
            TOTAL_REVENUE,
            TOTAL_ORDERS,
            AVG_ORDER_VALUE,
            ACTIVE_DAYS,
            ACTIVE_WEEKS,
            ORDERS_PER_ACTIVE_DAY,
            ORDERS_PER_ACTIVE_WEEK,
            RANK() OVER (
                ORDER BY TOTAL_REVENUE DESC
            ) AS REVENUE_RANK
        FROM calculated_metrics
        ORDER BY REVENUE_RANK
    """

    return query
