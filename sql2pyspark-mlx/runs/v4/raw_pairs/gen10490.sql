SELECT
    i.id,
    i.product_id,
    p.category,
    p.cost,
    s.city,
    s.country
FROM inventory i
RIGHT JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE i.store_id IN (1, 5, 6)
ORDER BY s.city ASC
