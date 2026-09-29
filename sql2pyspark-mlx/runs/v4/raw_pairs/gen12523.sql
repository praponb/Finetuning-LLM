WITH product_totals AS (
    SELECT product_id, MIN(quantity) AS min_quantity
    FROM inventory
    GROUP BY product_id
)
SELECT p.id, p.category, pt.min_quantity
FROM products p
INNER JOIN product_totals pt ON p.id = pt.product_id
WHERE pt.min_quantity >= 210
ORDER BY category, id
LIMIT 20;
