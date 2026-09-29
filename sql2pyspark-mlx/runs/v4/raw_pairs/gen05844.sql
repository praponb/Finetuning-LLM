WITH scored AS (
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
        ROW_NUMBER() OVER w_part AS event_seq,
        AVG(cost) OVER (PARTITION BY brand ORDER BY stock_qty, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS cost_avg_6_prior,
        NTILE(10) OVER (PARTITION BY category ORDER BY cost, id) AS cost_bucket,
        ROW_NUMBER() OVER w_part_2 AS row_seq,
        SUM(cost) OVER (PARTITION BY supplier_id ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost
    FROM products
    WINDOW w_part AS (PARTITION BY supplier_id ORDER BY stock_qty DESC, id), w_part_2 AS (PARTITION BY brand ORDER BY launch_date DESC, id)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        cost,
        stock_qty,
        launch_date,
        cost_bucket,
        row_seq,
        cumulative_cost,
        ROUND(price, 0) AS price_rounded,
        COALESCE(brand, 'UNKNOWN') AS brand_clean
    FROM scored
    WHERE supplier_id IS NOT NULL
      AND product_name LIKE 'Ba%'
),
scored_products AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER w_running AS row_seq_2,
        NTILE(3) OVER w_running AS cumulative_cost_bucket,
        RANK() OVER (PARTITION BY brand, category ORDER BY cost DESC) AS cost_rank
    FROM valid
    WINDOW w_running AS (PARTITION BY category ORDER BY cumulative_cost, id)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        brand,
        category,
        cost
    FROM scored_products
) src
PIVOT (
    AVG(cost) AS amount,
    COUNT(1) AS cnt
    FOR category IN ('Electronics', 'Grocery')
);
