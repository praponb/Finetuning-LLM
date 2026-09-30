WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        cost + price AS margin
    FROM products
    WHERE launch_date < '2023-10-01'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT category
    FROM valid
    INTERSECT ALL
    SELECT category
    FROM products
    WHERE supplier_id IS NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.category = k.category
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        margin,
        PERCENT_RANK() OVER (PARTITION BY category ORDER BY price) AS price_percent_rank,
        ROUND(price / SUM(price) OVER (PARTITION BY supplier_id), 4) AS price_share,
        CORR(price, cost) OVER (PARTITION BY brand) AS corr_price_cost
    FROM kept_rows
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        brand,
        price,
        price_share,
        margin
    FROM with_windows
) src
UNPIVOT (
    val FOR kpi IN (price AS PRICE_VAL, price_share AS PRICE_SHARE_VAL, margin AS MARGIN)
);
