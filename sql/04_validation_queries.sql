-- 1. Row counts
SELECT COUNT(*) AS order_items_count
FROM dbo.order_items;

SELECT COUNT(*) AS order_item_options_count
FROM dbo.order_item_options;

SELECT COUNT(*) AS date_dim_count
FROM dbo.date_dim;


-- 2. Check NULL business keys
SELECT *
FROM dbo.order_items
WHERE order_id IS NULL
   OR lineitem_id IS NULL;

SELECT *
FROM dbo.order_item_options
WHERE order_id IS NULL
   OR lineitem_id IS NULL;


-- 3. Check duplicate business keys
SELECT
    order_id,
    lineitem_id,
    COUNT(*) AS duplicate_count
FROM dbo.order_items
GROUP BY
    order_id,
    lineitem_id
HAVING COUNT(*) > 1;


-- 4. Check duplicate option records
SELECT
    order_id,
    lineitem_id,
    option_group_name,
    option_name,
    COUNT(*) AS duplicate_count
FROM dbo.order_item_options
GROUP BY
    order_id,
    lineitem_id,
    option_group_name,
    option_name
HAVING COUNT(*) > 1;


-- 5. Check negative quantities
SELECT *
FROM dbo.order_items
WHERE item_quantity < 0;

SELECT *
FROM dbo.order_item_options
WHERE option_quantity < 0;


-- 6. Check negative prices
SELECT *
FROM dbo.order_items
WHERE item_price < 0;

SELECT *
FROM dbo.order_item_options
WHERE option_price < 0;


-- 7. Check updated_at
SELECT *
FROM dbo.order_items
WHERE updated_at IS NULL;

SELECT *
FROM dbo.order_item_options
WHERE updated_at IS NULL;


-- 8. Find latest watermark
SELECT MAX(updated_at) AS max_updated_at
FROM dbo.order_items;

SELECT MAX(updated_at) AS max_updated_at
FROM dbo.order_item_options;


-- 9. Referential check:
-- option exists without matching order item
SELECT o.*
FROM dbo.order_item_options o
LEFT JOIN dbo.order_items i
    ON o.order_id = i.order_id
   AND o.lineitem_id = i.lineitem_id
WHERE i.order_id IS NULL;


-- 10. Check date range
SELECT
    MIN(creation_time_utc) AS min_creation_time,
    MAX(creation_time_utc) AS max_creation_time
FROM dbo.order_items;