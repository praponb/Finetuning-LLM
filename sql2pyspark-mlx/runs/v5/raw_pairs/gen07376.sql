SELECT
    i.store_id,
    p.price,
    p.cost,
    s.city,
    s.supplier_name
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
LEFT JOIN suppliers s ON s.id = p.supplier_id
WHERE s.city = 'Bangkok';
