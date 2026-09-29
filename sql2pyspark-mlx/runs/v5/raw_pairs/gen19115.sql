WITH product_stats AS (
    SELECT product_id, COUNT(DISTINCT unit_price) unique_products, SUM(quantity * unit_price) revenue
    FROM order_items
    GROUP BY product_id
)
SELECT
    p.id,
    p.launch_date,
    ps.unique_products,
    ps.revenue
FROM products p
JOIN product_stats ps ON p.id = ps.product_id
WHERE ps.unique_products >= 4
ORDER BY ps.unique_products DESC, id
LIMIT 5;
