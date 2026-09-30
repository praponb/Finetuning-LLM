WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        ROUND(unit_price * 1.1, 2) AS unit_price_adjusted
    FROM order_items
    WHERE product_id IN (9, 11, 29, 32)
),
filtered_order_items AS (
    -- Basic cleansing and defaults
    SELECT *, FLOOR(unit_price) AS unit_price_whole
    FROM filtered
    WHERE product_id IS NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM filtered_order_items
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price_whole DESC, id) = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER (ORDER BY unit_price DESC, id) AS unit_price_bucket,
        SUM(unit_price_whole) OVER (PARTITION BY product_id, order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price_whole
    FROM latest
    WHERE quantity BETWEEN 4 AND 5
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY running_unit_price_whole DESC, id) = 1
)
-- Join a dimension and aggregate
SELECT
    dim.brand,
    COUNT(*) AS num_rows,
    AVG(s.running_unit_price_whole) AS avg_running_unit_price_whole,
    AVG(s.unit_price_adjusted) AS avg_unit_price_adjusted
FROM deduped s
INNER JOIN products dim ON s.product_id = dim.id
GROUP BY dim.brand
