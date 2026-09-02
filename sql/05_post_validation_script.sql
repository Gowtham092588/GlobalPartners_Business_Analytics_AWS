-- #-----------------------------------------------------------------------
-- # Row Count of tables in bronze and silver.
-- #----------------------------------------------------------------------           

SELECT
    'bronze_order_items' AS table_name,
    COUNT(*) AS row_count
FROM globaldbbronze.order_items

UNION ALL

SELECT
    'silver_order_items',
    COUNT(*)
FROM globaldbsilver.order_items;

SELECT
    'bronze_order_item_options' AS table_name,
    COUNT(*) AS row_count
FROM globaldbbronze.order_item_options

UNION ALL

SELECT
    'silver_order_item_options',
    COUNT(*)
FROM globaldbsilver.order_item_options;

SELECT
    'bronze_date_dim' AS table_name,
    COUNT(*) AS row_count
FROM globaldbbronze.date_dim

UNION ALL

SELECT
    'silver_date_dim',
    COUNT(*)
FROM globaldbsilver.date_dim;

-- #-----------------------------------------------------------------------
-- # Duplicate validation for dim_customer
-- #----------------------------------------------------------------------  

SELECT
    user_id,
    COUNT(*) AS current_record_count
FROM globaldbgold.dim_customer
WHERE is_current = true
GROUP BY user_id
HAVING COUNT(*) > 1;

SELECT
    user_id,
    COUNT(*) AS version_count
FROM globaldbgold.dim_customer
GROUP BY user_id
HAVING COUNT(*) > 1
ORDER BY version_count DESC;

SELECT
    customer_key,
    user_id,
    printed_card_number,
    is_loyalty,
    first_order_date,
    last_order_date,
    effective_start_date,
    effective_end_date,
    is_current
FROM globaldbgold.dim_customer
WHERE user_id = '63ee144950286a8367041911'
ORDER BY effective_start_date;

WITH customer_history AS (
    SELECT
        customer_key,
        user_id,
        printed_card_number,
        is_loyalty,
        effective_start_date,
        effective_end_date,
        is_current,

        LAG(printed_card_number) OVER (
            PARTITION BY user_id
            ORDER BY effective_start_date
        ) AS previous_card_number,

        LAG(is_loyalty) OVER (
            PARTITION BY user_id
            ORDER BY effective_start_date
        ) AS previous_loyalty

    FROM globaldbgold.dim_customer
)

-- #-----------------------------------------------------------------------
-- # Null validation for surrogate keys in fact order items table.
-- #----------------------------------------------------------------------  
    
SELECT
    COUNT(*) AS total_rows,

    SUM(
        CASE WHEN customer_key IS NULL
        THEN 1 ELSE 0 END
    ) AS null_customer_key,

    SUM(
        CASE WHEN item_key IS NULL
        THEN 1 ELSE 0 END
    ) AS null_item_key,

    SUM(
        CASE WHEN app_key IS NULL
        THEN 1 ELSE 0 END
    ) AS null_app_key,

    SUM(
        CASE WHEN restaurant_key IS NULL
        THEN 1 ELSE 0 END
    ) AS null_restaurant_key

FROM globaldbgold.fact_order_items;

SELECT
    order_id,
    lineitem_id,
    user_id,
    creation_time_utc,
    customer_key
FROM globaldbgold.fact_order_items
WHERE customer_key IS NULL
LIMIT 50;

SELECT COUNT(*) AS unresolved_customer_keys
FROM globaldbgold.fact_order_items
WHERE user_id IS NOT NULL
  AND customer_key IS NULL;
  
 SELECT COUNT(*) AS anonymous_orders
FROM globaldbgold.fact_order_items
WHERE user_id IS NULL
  AND customer_key IS NULL;

-- #-----------------------------------------------------------------------
-- # Negative value validation for fact customer daily table.
-- #----------------------------------------------------------------------    

SELECT
    COUNT(
        CASE WHEN daily_orders < 0
        THEN 1 END
    ) AS negative_daily_orders,

    COUNT(
        CASE WHEN frequency < 0
        THEN 1 END
    ) AS negative_frequency,

    COUNT(
        CASE WHEN recency < 0
        THEN 1 END
    ) AS negative_recency

FROM globaldbgold.fact_customer_daily;

-- #-----------------------------------------------------------------------
-- # Lifetime Revenue Validation.
-- #----------------------------------------------------------------------  

WITH customer_history AS (
    SELECT
        user_id,
        date_key,
        lifetime_revenue,

        LAG(lifetime_revenue) OVER (
            PARTITION BY user_id
            ORDER BY date_key
        ) AS previous_lifetime_revenue

    FROM globaldbgold.fact_customer_daily
)

SELECT *
FROM customer_history
WHERE
    previous_lifetime_revenue IS NOT NULL
    AND lifetime_revenue < previous_lifetime_revenue;
    
    
SELECT
    user_id,
    SUM(revenue) AS fact_order_revenue
FROM globaldbgold.fact_order_items
GROUP BY user_id;

WITH latest_customer AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY date_key DESC
        ) AS rn
    FROM globaldbgold.fact_customer_daily
)

SELECT
    user_id,
    lifetime_revenue
FROM latest_customer
WHERE rn = 1;
