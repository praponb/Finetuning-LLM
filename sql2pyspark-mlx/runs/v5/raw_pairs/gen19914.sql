SELECT
    p.id,
    p.category,
    COUNT(i.id) AS inventory_count,
    MAX(i.last_restock) AS last_last_restock,
    COUNT(i.quantity) AS quantity_count
FROM products p
INNER JOIN inventory i ON p.id = i.product_id
WHERE p.supplier_id IN (10, 17, 40)
GROUP BY p.id, p.category
