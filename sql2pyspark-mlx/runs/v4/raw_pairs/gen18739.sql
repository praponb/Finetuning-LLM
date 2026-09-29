WITH product_totals AS (
    SELECT product_id, COUNT(*) AS total_count
    FROM inventory
    WHERE quantity > 232
    GROUP BY product_id
)
SELECT p.id, p.category, pt.total_count
FROM products p
LEFT OUTER JOIN product_totals pt ON p.id = pt.product_id
ORDER BY category, pt.total_count DESC;
