WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        CEIL(unit_price) AS unit_price_whole
    FROM order_items
    WHERE quantity BETWEEN 4 AND 7
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price_whole NULLS FIRST, id) AS dedup_rank
    FROM cleaned
    QUALIFY dedup_rank = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(quantity / unit_price, 2) AS line_total
    FROM ranked
    WHERE product_id IS NOT NULL
)
-- Multi-level aggregation
SELECT
    CASE WHEN quantity >= 9 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    product_id,
    ROUND(AVG(unit_price), 2) AS avg_unit_price,
    CORR(unit_price, quantity) AS corr_unit_price_quantity,
    ROUND(AVG(unit_price_whole), 2) AS avg_unit_price_whole,
    COUNT(*) AS row_count,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    PERCENTILE_APPROX(unit_price_whole, 0.95) AS unit_price_whole_p95
FROM valid
GROUP BY quantity_tier, product_id
ORDER BY corr_unit_price_quantity NULLS LAST;
