SELECT
    i.product_id,
    i.last_restock,
    p.brand,
    p.price,
    s.rating,
    CASE WHEN i.quantity >= 198 THEN 'High' WHEN i.quantity >= 100 THEN 'Medium' ELSE 'Low' END AS quantity_level
FROM inventory i
FULL OUTER JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON s.id = p.supplier_id
WHERE i.product_id IN (3, 10) AND i.store_id IN (1, 3, 4)
