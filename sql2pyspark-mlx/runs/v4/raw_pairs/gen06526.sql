WITH not_flagged AS (
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.sale_date >= s.launch_date
        )
)
SELECT
    DATE_TRUNC('QUARTER', launch_date) AS quarter_start,
    CORR(price, cost) AS corr_price_cost,
    SUM(CASE WHEN brand = 'Initech' THEN price ELSE 0 END) AS initech_price,
    ROUND(SUM(stock_qty) / NULLIF(COUNT(CASE WHEN category = 'Sports' THEN 1 END), 0), 2) AS stock_qty_per_sports,
    COUNT_IF(category = 'Sports') AS sports_events,
    PERCENTILE_APPROX(stock_qty, ARRAY(0.25, 0.5, 0.75)) AS stock_qty_quartiles,
    ROUND(SUM(stock_qty) / NULLIF(COUNT(CASE WHEN brand = 'Acme' THEN 1 END), 0), 2) AS stock_qty_per_acme
FROM not_flagged
GROUP BY DATE_TRUNC('QUARTER', launch_date)
ORDER BY corr_price_cost
