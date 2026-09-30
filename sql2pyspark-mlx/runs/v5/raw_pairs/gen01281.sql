WITH product_stats AS (
    SELECT product_id, SUM(quantity) AS total_quantity
    FROM inventory
    WHERE store_id IN (3, 6)
    GROUP BY product_id
)
SELECT p.id, p.brand, ps.total_quantity
FROM products p
JOIN product_stats ps ON p.id = ps.product_id
WHERE ps.total_quantity > 1270
