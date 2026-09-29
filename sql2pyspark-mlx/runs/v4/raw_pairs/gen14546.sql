WITH product_stats AS (
    SELECT product_id, SUM(revenue) AS total_revenue
    FROM store_sales
    GROUP BY product_id
)
SELECT p.id, p.price, ps.total_revenue
FROM products p
INNER JOIN product_stats ps ON p.id = ps.product_id
WHERE ps.total_revenue > 11000
ORDER BY price
