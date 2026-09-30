SELECT
    p.price,
    p.supplier_id,
    p.cost,
    s.country,
    s.rating,
    s.supplier_name
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE ARRAY_CONTAINS(p.tags, 'premium');
