WITH product_summary AS (
    SELECT product_id, SUM(quantity) AS total_quantity
    FROM order_items
    GROUP BY product_id
)
SELECT p.id, p.price, ps.total_quantity
FROM products p
JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.total_quantity >= 31
ORDER BY id DESC
