SELECT
    p.supplier_id,
    p.category,
    s.city,
    s.rating
FROM products p
LEFT JOIN suppliers s ON s.id = p.supplier_id
WHERE ARRAY_CONTAINS(p.tags, 'eco');
