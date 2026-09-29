WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(price / cost, 2) AS margin,
        COALESCE(brand, 'OTHER') AS brand_filled
    FROM products
    WHERE launch_date > DATE '2023-07-27'
      AND supplier_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_name,
        brand,
        cost,
        stock_qty,
        tags,
        margin,
        NTILE(10) OVER (PARTITION BY brand ORDER BY margin DESC, id ASC) AS margin_bucket,
        SUM(margin) OVER (PARTITION BY supplier_id ORDER BY stock_qty ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_margin
    FROM filtered
    WHERE stock_qty < 154
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.product_id = 5
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        cost,
        running_margin,
        COUNT(running_margin) OVER (PARTITION BY brand) AS brand_count_running_margin,
        NTILE(5) OVER (PARTITION BY brand ORDER BY stock_qty DESC, id ASC) AS stock_qty_bucket
    FROM not_flagged
),
not_flagged_products AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.store_id IN (1, 3, 5, 6)
        )
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged_products s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.order_id IN (5, 8, 12, 13)
        )
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM kept
    EXCEPT DISTINCT
    SELECT id
    FROM products
    WHERE cost > 580
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    JOIN remaining_keys k ON s.id = k.id
),
summary AS (
    -- Grouped metrics
    SELECT
        CASE WHEN brand_count_running_margin >= 13 THEN 'LARGE' ELSE 'SMALL' END AS brand_count_running_margin_tier,
        CASE WHEN cost >= 620 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_tier,
        MIN(running_margin) AS min_running_margin,
        APPROX_PERCENTILE(stock_qty_bucket, 0.95) AS stock_qty_bucket_p95,
        GROUPING(CASE WHEN brand_count_running_margin >= 13 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_level
    FROM reconciled
    GROUP BY CUBE (CASE WHEN brand_count_running_margin >= 13 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN cost >= 620 THEN 'PREMIUM' ELSE 'STANDARD' END)
)
SELECT
    brand_count_running_margin_tier,
    cost_tier,
    min_running_margin
FROM summary
