WITH recent_products AS (
    SELECT *
    FROM products
    WHERE cost >= 380
),
products_stats AS (
    SELECT category, AVG(stock_qty) avg_stock_qty, ROUND((100 * SUM(CASE WHEN category = 'Grocery' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_grocery
    FROM recent_products
    GROUP BY category
)
SELECT *
FROM products_stats
WHERE pct_grocery > 45
ORDER BY 1
LIMIT 50;
