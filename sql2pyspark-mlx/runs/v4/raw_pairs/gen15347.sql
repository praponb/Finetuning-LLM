WITH joined AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        s.rating,
        s.city
    FROM products e
    INNER JOIN suppliers s
        ON e.supplier_id = s.id
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT s.*, p.rating AS supplier_rating
    FROM joined s
    INNER JOIN suppliers p
        ON s.supplier_id = p.id
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_dims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.quantity >= 6
        )
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', launch_date) AS activity_month,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list,
    SIZE(COLLECT_SET(category)) AS num_categories,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(stock_qty, 0.5) AS stock_qty_p50
FROM kept
GROUP BY DATE_TRUNC('MONTH', launch_date)
ORDER BY num_categories DESC;
