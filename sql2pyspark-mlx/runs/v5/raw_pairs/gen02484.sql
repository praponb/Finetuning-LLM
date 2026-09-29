WITH product_totals AS (
    SELECT product_id, COUNT(quantity) AS quantity_count, AVG(quantity) AS avg_quantity
    FROM inventory
    WHERE product_id IN (3, 4, 9, 10)
    GROUP BY product_id
)
SELECT
    p.launch_date,
    p.supplier_id,
    pt.quantity_count,
    pt.avg_quantity
FROM products p
LEFT OUTER JOIN product_totals pt ON p.id = pt.product_id
WHERE pt.avg_quantity >= 140
ORDER BY p.supplier_id DESC;
