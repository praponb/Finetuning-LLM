SELECT
    p.supplier_id,
    p.category,
    p.brand,
    s.country
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
ORDER BY category
