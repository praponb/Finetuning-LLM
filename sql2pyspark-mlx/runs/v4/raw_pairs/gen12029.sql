WITH inventory_by_product AS (
    SELECT product_id, MAX(quantity) - MIN(quantity) AS quantity_range, AVG(quantity) AS avg_quantity
    FROM inventory
    GROUP BY product_id
)
SELECT
    p.id,
    p.brand,
    p.supplier_id,
    ibp.quantity_range,
    ibp.avg_quantity
FROM products p
LEFT JOIN inventory_by_product ibp ON p.id = ibp.product_id
ORDER BY ibp.quantity_range
