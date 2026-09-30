WITH eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.unit_price IS NOT NULL
        )
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        e.id,
        e.product_name,
        e.brand,
        e.stock_qty,
        e.tags,
        e.supplier_id,
        b.supplier_name,
        b.city,
        b.country
    FROM eligible e
    JOIN suppliers b
        ON e.supplier_id = b.id
    WHERE b.city <> 'Tokyo'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(stock_qty) OVER w_ordered AS running_count_stock_qty,
        MIN(stock_qty) OVER w_ordered AS running_min_stock_qty,
        AVG(stock_qty) OVER w_ordered AS running_avg_stock_qty,
        SUM(stock_qty) OVER w_hist AS cumulative_stock_qty
    FROM with_dims
    WINDOW w_ordered AS (PARTITION BY city ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY country ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
eligible_products AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.order_id = 14
        )
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        e.id,
        e.stock_qty,
        e.tags,
        e.supplier_id,
        e.supplier_name,
        e.running_min_stock_qty,
        e.running_avg_stock_qty,
        s.city AS supplier_city,
        s.country AS supplier_country
    FROM eligible_products e
    JOIN suppliers s
        ON e.supplier_id = s.id
)
-- Grouped metrics
SELECT
    CASE WHEN running_avg_stock_qty >= 390 THEN 'HIGH' ELSE 'LOW' END AS running_avg_stock_qty_tier,
    supplier_id,
    MAX(stock_qty) AS max_stock_qty,
    SUM(stock_qty) FILTER (WHERE supplier_country = 'UK') AS uk_total_stock_qty,
    COUNT(*) AS cnt,
    APPROX_PERCENTILE(running_avg_stock_qty, ARRAY(0.25, 0.5, 0.75)) AS running_avg_stock_qty_quartiles,
    ROUND(AVG(running_avg_stock_qty), 2) AS avg_running_avg_stock_qty,
    GROUPING_ID() AS gid
FROM enriched
GROUP BY GROUPING SETS ((CASE WHEN running_avg_stock_qty >= 390 THEN 'HIGH' ELSE 'LOW' END, supplier_id), (CASE WHEN running_avg_stock_qty >= 390 THEN 'HIGH' ELSE 'LOW' END))
ORDER BY supplier_id NULLS LAST
