WITH not_flagged AS (
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.last_restock < s.launch_date
        )
)
SELECT
    brand,
    COUNT(DISTINCT category) AS distinct_categories,
    APPROX_PERCENTILE(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles,
    ROUND(AVG(stock_qty), 2) AS avg_stock_qty
FROM not_flagged
GROUP BY CUBE (brand);
