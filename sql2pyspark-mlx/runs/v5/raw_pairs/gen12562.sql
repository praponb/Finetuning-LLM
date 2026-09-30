WITH product_summary AS (
    SELECT product_id, SUM(quantity) AS total_quantity, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM inventory
    WHERE product_id IN (1, 3)
    GROUP BY product_id
)
SELECT p.price, ps.total_quantity, ps.avg_quantity
FROM products p
LEFT OUTER JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.avg_quantity > 42
