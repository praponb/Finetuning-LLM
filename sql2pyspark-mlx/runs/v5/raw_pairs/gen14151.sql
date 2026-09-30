WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        category,
        brand,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        ROUND(price / cost, 2) AS margin,
        price + cost AS margin_2
    FROM products
    WHERE launch_date >= DATE '2023-07-31'
      AND cost < 680
),
kept_keys AS (
    -- Set difference on keys
    SELECT brand, supplier_id
    FROM cleaned
    EXCEPT DISTINCT
    SELECT brand, supplier_id
    FROM products
    WHERE cost <= 710
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.brand = k.brand AND s.supplier_id = k.supplier_id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        category,
        cost,
        launch_date,
        supplier_id,
        margin_2,
        COUNT(margin_2) OVER (PARTITION BY category) AS category_count_margin_2,
        DENSE_RANK() OVER (PARTITION BY brand ORDER BY margin_2 NULLS FIRST) AS margin_2_dense_rank
    FROM kept_rows
),
scored_products AS (
    -- Running and moving aggregates
    SELECT *, ROUND(margin_2_dense_rank / SUM(margin_2_dense_rank) OVER (PARTITION BY category, supplier_id), 4) AS margin_2_dense_rank_share
    FROM scored
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', launch_date) AS quarter_start,
    category,
    AVG(margin_2) FILTER (WHERE category = 'Sports') AS sports_avg_margin_2,
    COUNT(*) FILTER (WHERE category = 'Home') AS home_rows,
    SIZE(COLLECT_SET(category)) AS num_categories
FROM scored_products
GROUP BY DATE_TRUNC('QUARTER', launch_date), category
ORDER BY quarter_start
