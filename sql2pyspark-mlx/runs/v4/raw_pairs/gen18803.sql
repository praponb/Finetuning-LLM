WITH product_stats AS (
    SELECT product_id, SUM(unit_price * quantity) AS revenue
    FROM order_items
    GROUP BY product_id
)
SELECT p.id, p.category, ps.revenue
FROM products p
INNER JOIN product_stats ps ON p.id = ps.product_id
WHERE ps.revenue > 71
