SELECT
    i.id,
    i.last_restock,
    p.price,
    p.supplier_id,
    s.city,
    s.rating
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE s.supplier_name LIKE 'P%';
