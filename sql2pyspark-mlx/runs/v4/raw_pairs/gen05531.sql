WITH product_totals AS (
    SELECT product_id, MAX(quantity) max_quantity
    FROM inventory
    GROUP BY product_id
)
SELECT p.category, p.supplier_id, pt.max_quantity
FROM products p
LEFT JOIN product_totals pt ON p.id = pt.product_id;
