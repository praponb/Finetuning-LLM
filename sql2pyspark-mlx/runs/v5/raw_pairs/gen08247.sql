WITH product_summary AS (
    SELECT product_id, COUNT(*) AS cnt
    FROM inventory
    WHERE product_id IN (6, 8, 10)
    GROUP BY product_id
)
SELECT p.product_name, p.brand, ps.cnt
FROM products p
JOIN product_summary ps ON p.id = ps.product_id;
