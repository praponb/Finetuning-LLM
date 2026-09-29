WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        price,
        stock_qty,
        supplier_id,
        UPPER(TRIM(product_name)) AS product_name_normalized
    FROM products
    WHERE stock_qty BETWEEN 134 AND 349
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.quantity < 5
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_name,
        category,
        stock_qty,
        RANK() OVER (PARTITION BY supplier_id, category ORDER BY price NULLS FIRST) AS price_rank,
        ROW_NUMBER() OVER (PARTITION BY category, supplier_id ORDER BY stock_qty, id) AS row_seq
    FROM eligible
),
eligible_products AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.store_id IN (1, 3, 4)
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_name,
        category,
        MAX(row_seq) OVER w_hist AS category_max_row_seq,
        STDDEV_SAMP(stock_qty) OVER w_hist AS category_stddev_samp_stock_qty
    FROM eligible_products
    WINDOW w_hist AS (PARTITION BY category)
)
-- Grouped metrics
SELECT
    category,
    MAX(category_max_row_seq) FILTER (WHERE category = 'Fashion') AS fashion_max_category_max_row_seq,
    ROUND(SUM(category_max_row_seq) / NULLIF(COUNT(CASE WHEN category = 'Sports' THEN 1 END), 0), 2) AS category_max_row_seq_per_sports,
    PERCENTILE_APPROX(category_stddev_samp_stock_qty, ARRAY(0.25, 0.5, 0.75)) AS category_stddev_samp_stock_qty_quartiles
FROM scored
GROUP BY CUBE (category)
HAVING MAX(category_max_row_seq) FILTER (WHERE category = 'Fashion') >= 0
ORDER BY category_max_row_seq_per_sports NULLS LAST
