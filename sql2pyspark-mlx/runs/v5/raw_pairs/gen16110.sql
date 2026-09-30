WITH product_stats AS (
    SELECT product_id, COUNT(quantity) AS quantity_count, COUNT(*) AS cnt
    FROM inventory
    GROUP BY product_id
)
SELECT
    p.product_name,
    p.cost,
    ps.quantity_count,
    ps.cnt
FROM products p
INNER JOIN product_stats ps ON p.id = ps.product_id
